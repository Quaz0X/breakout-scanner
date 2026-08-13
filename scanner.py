#!/usr/bin/env python3
"""
Intraday scalp scanner — calibrated for 60-120 minute holds.

Design notes (read these before changing anything):

  * HOLD_CANDLES defaults to 6 (= 90 min on 15m candles). Expected move
    scales as ATR * sqrt(HOLD_CANDLES), so this number directly controls
    how generous the cost-edge gate is. Raising it inflates every edge
    figure and lets marginal setups through. Do not raise it to "get more
    alerts".
  * rvol is a HARD GATE, not a score component alone. A compressed coil
    that nobody is trading is not scalpable — there is no one to sell to.
  * Resistance is measured from candles 24..6 back (6h..90m ago). Older
    levels are irrelevant on this horizon; more recent ones would just be
    rewarding momentum that already happened.
  * Funding rate scoring was removed. It is a futures signal and this is
    a spot workflow.

Every run sends exactly one Telegram message, even when nothing qualifies.
"""

from __future__ import annotations

import math
import os
import statistics
import sys
import time
from dataclasses import dataclass, field
from typing import Any

import requests

# --------------------------------------------------------------------------
# Config
# --------------------------------------------------------------------------

def _env_f(key: str, default: float) -> float:
    try:
        return float(os.environ.get(key, default))
    except (TypeError, ValueError):
        return default


def _env_i(key: str, default: int) -> int:
    try:
        return int(os.environ.get(key, default))
    except (TypeError, ValueError):
        return default


# Binance returns HTTP 451 to US IPs (GitHub runners are US-based).
# These hosts are tried in order until one answers.
BASE_HOSTS = [
    h.strip()
    for h in os.environ.get(
        "BINANCE_HOSTS",
        "https://data-api.binance.vision,https://api-gcp.binance.com,https://api.binance.com",
    ).split(",")
    if h.strip()
]

TELEGRAM_BOT_TOKEN = os.environ.get("TELEGRAM_BOT_TOKEN", "")
TELEGRAM_CHAT_ID = os.environ.get("TELEGRAM_CHAT_ID", "")

INTERVAL = os.environ.get("INTERVAL", "15m")
CANDLE_MINUTES = 15

# --- horizon -------------------------------------------------------------
# 6 candles x 15m = 90 minutes. This is the middle of a 60-120 min hold.
HOLD_CANDLES = _env_i("HOLD_CANDLES", 6)

# --- costs ---------------------------------------------------------------
TAKER_FEE_PCT = _env_f("TAKER_FEE_PCT", 0.10)   # 0.075 if paying fees in BNB
MIN_COST_EDGE = _env_f("MIN_COST_EDGE", 4.0)    # expected move / round-trip cost

# --- hard gates ----------------------------------------------------------
MIN_QUOTE_VOL_24H = _env_f("MIN_QUOTE_VOL_24H", 10_000_000)
MAX_SPREAD_PCT = _env_f("MAX_SPREAD_PCT", 0.12)
MIN_NEAR_DEPTH_USD = _env_f("MIN_NEAR_DEPTH_USD", 25_000)
MIN_ATR_PCT = _env_f("MIN_ATR_PCT", 0.15)       # below this it cannot pay for itself
MAX_ATR_PCT = _env_f("MAX_ATR_PCT", 1.50)       # above this one candle eats the stop
MIN_RVOL = _env_f("MIN_RVOL", 0.80)             # hard gate: no participation, no trade
MAX_STOP_PCT = _env_f("MAX_STOP_PCT", 1.50)     # risk cap per trade
MAX_TRIGGER_DIST_PCT = _env_f("MAX_TRIGGER_DIST_PCT", 1.20)  # must be reachable in the window

SHORTLIST_SIZE = _env_i("SHORTLIST_SIZE", 25)
MAX_REPORTED = _env_i("MAX_REPORTED", 5)

# --- resistance window (candles back from now) ---------------------------
RES_LOOKBACK_FAR = _env_i("RES_LOOKBACK_FAR", 24)   # 6h ago
RES_LOOKBACK_NEAR = _env_i("RES_LOOKBACK_NEAR", 6)  # 90m ago

# --- scoring weights (must total 100) ------------------------------------
W_CVD = 20
W_PROXIMITY = 18
W_RVOL = 14
W_COST = 14
W_BOOK = 12
W_SQUEEZE = 12
W_TIGHT = 10

# --- exclusions ----------------------------------------------------------
STABLES = {
    "USDC", "FDUSD", "TUSD", "BUSD", "DAI", "USDP", "EURI", "AEUR",
    "USD1", "XUSD", "PYUSD", "EUR", "USDE",
}
COMMODITY_TOKENS = {"XAUT", "PAXG", "WBTC", "WBETH", "BETH"}
LEVERAGED_SUFFIXES = ("UPUSDT", "DOWNUSDT", "BULLUSDT", "BEARUSDT")

# Tokenized equities. Extend via env: BSTOCK_EXTRA="FOOB,BARB"
BSTOCKS = {
    "AAPLB", "ABNBB", "AMDB", "AMZNB", "AVGOB", "BABAB", "BRKBB", "CBB",
    "COINB", "CRCLB", "CRMB", "CRWVB", "DISB", "GLDB", "GMEB", "GOOGLB",
    "HOODB", "IBITB", "INTCB", "JPMB", "LLYB", "MARAB", "MCDB", "METAB",
    "MRVLB", "MSFTB", "MSTRB", "NBISB", "NFLXB", "NKEB", "NVDAB", "ORCLB",
    "PGB", "PLTRB", "PYPLB", "QQQB", "RBLXB", "RDDTB", "SBUXB", "SHOPB",
    "SMCIB", "SNAPB", "SPYB", "TQQQB", "TSLAB", "TSMB", "UBERB", "UNHB",
    "VB", "VOOB", "VTIB", "WMTB", "XOMB", "ZB",
}
BSTOCKS |= {
    x.strip().upper()
    for x in os.environ.get("BSTOCK_EXTRA", "").split(",")
    if x.strip()
}

session = requests.Session()
session.headers.update({"User-Agent": "intraday-scalp-scanner/2.0"})

_active_host: str | None = None


# --------------------------------------------------------------------------
# HTTP
# --------------------------------------------------------------------------

def api_get(path: str, params: dict | None = None, retries: int = 3) -> Any:
    """GET against the first Binance host that answers. Caches the winner."""
    global _active_host

    hosts = [_active_host] if _active_host else list(BASE_HOSTS)
    if _active_host and _active_host in BASE_HOSTS:
        hosts = [_active_host] + [h for h in BASE_HOSTS if h != _active_host]

    last_err: Exception | None = None
    for host in hosts:
        for attempt in range(retries):
            try:
                r = session.get(f"{host}{path}", params=params, timeout=15)
                if r.status_code == 451:
                    last_err = RuntimeError(f"451 geo-block from {host}")
                    break  # try next host, retrying will not help
                if r.status_code == 429:
                    time.sleep(2 + attempt * 3)
                    continue
                r.raise_for_status()
                _active_host = host
                return r.json()
            except requests.RequestException as exc:
                last_err = exc
                time.sleep(1 + attempt)
    raise RuntimeError(f"all Binance hosts failed for {path}: {last_err}")


# --------------------------------------------------------------------------
# Indicators
# --------------------------------------------------------------------------

def atr_pct(candles: list[dict], period: int = 14) -> float:
    """Wilder true range, averaged simple, expressed as % of last close."""
    if len(candles) < period + 1:
        return 0.0
    trs = []
    for i in range(len(candles) - period, len(candles)):
        prev_close = candles[i - 1]["close"]
        hi, lo = candles[i]["high"], candles[i]["low"]
        trs.append(max(hi - lo, abs(hi - prev_close), abs(lo - prev_close)))
    last = candles[-1]["close"]
    if last <= 0 or not trs:
        return 0.0
    return (sum(trs) / len(trs)) / last * 100.0


def rvol(candles: list[dict], recent: int = 3, baseline: int = 24) -> float:
    """Volume of the last `recent` closed candles vs median of the prior window.

    The live (still forming) candle is excluded by the caller, so this is a
    like-for-like comparison of completed periods.
    """
    if len(candles) < recent + baseline:
        return 0.0
    recent_vol = sum(c["volume"] for c in candles[-recent:]) / recent
    prior = [c["volume"] for c in candles[-(recent + baseline):-recent]]
    med = statistics.median(prior) if prior else 0.0
    if med <= 0:
        return 0.0
    return recent_vol / med


def cvd_delta_pct(candles: list[dict], window: int) -> float:
    """Net taker delta over `window` candles as % of volume traded.

    Binance kline field 9 is taker_buy_base_volume: the portion of volume
    where the buyer was the aggressor. delta = 2*buy - total.
    """
    if len(candles) < window:
        return 0.0
    chunk = candles[-window:]
    total = sum(c["volume"] for c in chunk)
    if total <= 0:
        return 0.0
    buy = sum(c["taker_buy_base"] for c in chunk)
    return (2 * buy - total) / total * 100.0


def squeeze_ratio(candles: list[dict], recent: int = 12, baseline: int = 72) -> float:
    """Recent range compression vs its own history. <1.0 means coiling."""
    if len(candles) < baseline:
        return 1.0
    def avg_range(chunk: list[dict]) -> float:
        rs = [c["high"] - c["low"] for c in chunk if c["close"] > 0]
        return sum(rs) / len(rs) if rs else 0.0
    r_recent = avg_range(candles[-recent:])
    r_base = avg_range(candles[-baseline:-recent])
    if r_base <= 0:
        return 1.0
    return r_recent / r_base


def resistance_level(candles: list[dict]) -> float:
    """Highest high in the 6h..90m window.

    Deliberately excludes the most recent candles so that a move already in
    progress is not mistaken for a level waiting to be broken.
    """
    if len(candles) < RES_LOOKBACK_FAR + 1:
        return 0.0
    window = candles[-RES_LOOKBACK_FAR:-RES_LOOKBACK_NEAR]
    if not window:
        return 0.0
    return max(c["high"] for c in window)


def swing_low(candles: list[dict], lookback: int = 8) -> float:
    if len(candles) < lookback:
        return 0.0
    return min(c["low"] for c in candles[-lookback:])


# --------------------------------------------------------------------------
# Data fetch
# --------------------------------------------------------------------------

def parse_klines(raw: list) -> list[dict]:
    out = []
    for k in raw:
        try:
            out.append({
                "open_time": int(k[0]),
                "open": float(k[1]),
                "high": float(k[2]),
                "low": float(k[3]),
                "close": float(k[4]),
                "volume": float(k[5]),
                "quote_volume": float(k[7]),
                "trades": int(k[8]),
                "taker_buy_base": float(k[9]),
            })
        except (IndexError, TypeError, ValueError):
            continue
    return out


def eligible_symbols() -> set[str]:
    info = api_get("/api/v3/exchangeInfo")
    out = set()
    for s in info.get("symbols", []):
        sym = s.get("symbol", "")
        if s.get("status") != "TRADING":
            continue
        if s.get("quoteAsset") != "USDT":
            continue
        if not s.get("isSpotTradingAllowed", False):
            continue
        base = s.get("baseAsset", "")
        if base in STABLES or base in COMMODITY_TOKENS:
            continue
        if sym.endswith(LEVERAGED_SUFFIXES):
            continue
        # Tokenized equities (bStocks) track market hours, gap over weekends
        # and do not respond to crypto-style structure. Blocked outright.
        if base in BSTOCKS:
            continue
        out.add(sym)
    return out


def liquid_shortlist() -> list[dict]:
    tickers = api_get("/api/v3/ticker/24hr")
    allowed = eligible_symbols()
    rows = []
    for t in tickers:
        sym = t.get("symbol", "")
        if sym not in allowed:
            continue
        try:
            qv = float(t["quoteVolume"])
            last = float(t["lastPrice"])
        except (KeyError, TypeError, ValueError):
            continue
        if qv < MIN_QUOTE_VOL_24H or last <= 0:
            continue
        rows.append({"symbol": sym, "quote_vol": qv, "last": last})
    rows.sort(key=lambda r: r["quote_vol"], reverse=True)
    return rows[:SHORTLIST_SIZE]


def book_metrics(symbol: str, price: float) -> tuple[float, float]:
    """Returns (live spread %, bid depth in USD within 0.5% of mid)."""
    book = api_get("/api/v3/depth", {"symbol": symbol, "limit": 100})
    bids = book.get("bids", [])
    asks = book.get("asks", [])
    if not bids or not asks:
        return 999.0, 0.0
    best_bid = float(bids[0][0])
    best_ask = float(asks[0][0])
    mid = (best_bid + best_ask) / 2
    if mid <= 0:
        return 999.0, 0.0
    spread = (best_ask - best_bid) / mid * 100.0
    floor_px = mid * 0.995
    depth = sum(float(p) * float(q) for p, q in bids if float(p) >= floor_px)
    return spread, depth


# --------------------------------------------------------------------------
# Scoring
# --------------------------------------------------------------------------

@dataclass
class Setup:
    symbol: str
    price: float
    trigger: float
    stop: float
    trigger_dist_pct: float
    stop_pct: float
    rr: float
    atr_pct: float
    exp_move_pct: float
    cost_pct: float
    edge: float
    spread_pct: float
    rvol: float
    cvd_1h: float
    cvd_3h: float
    squeeze: float
    depth_usd: float
    score: float = 0.0
    parts: dict = field(default_factory=dict)
    note: str = ""


def clamp01(x: float) -> float:
    return max(0.0, min(1.0, x))


def score_setup(s: Setup) -> Setup:
    p: dict[str, float] = {}

    # CVD — is the aggression on the buy side, and is it building?
    cvd_score = clamp01((s.cvd_1h + 5) / 25) * 0.7 + clamp01((s.cvd_3h + 10) / 30) * 0.3
    p["cvd"] = cvd_score * W_CVD

    # Proximity — closer to trigger is better, but already-broken is not a setup.
    if s.trigger_dist_pct <= 0:
        prox = 0.3
    else:
        prox = clamp01(1 - (s.trigger_dist_pct / MAX_TRIGGER_DIST_PCT))
    p["proximity"] = prox * W_PROXIMITY

    # rvol — gate is 0.8; score rewards genuine expansion above 1.0
    p["rvol"] = clamp01((s.rvol - MIN_RVOL) / (2.0 - MIN_RVOL)) * W_RVOL

    # cost edge — 4x is the floor, 10x is excellent
    p["cost"] = clamp01((s.edge - MIN_COST_EDGE) / (10.0 - MIN_COST_EDGE)) * W_COST

    # book — depth relative to the floor
    p["book"] = clamp01((s.depth_usd - MIN_NEAR_DEPTH_USD) / (150_000 - MIN_NEAR_DEPTH_USD)) * W_BOOK

    # squeeze — compression is a bonus, never the main reason
    p["squeeze"] = clamp01((1.0 - s.squeeze) / 0.4) * W_SQUEEZE

    # tightness — spread relative to the gate
    p["tight"] = clamp01(1 - (s.spread_pct / MAX_SPREAD_PCT)) * W_TIGHT

    s.parts = p
    s.score = sum(p.values())

    # Absorption warning: price pressed into the level with buy aggression
    # but no expansion in range means supply is meeting it.
    if s.cvd_1h > 8 and s.trigger_dist_pct < 0.5 and s.rvol < 1.1:
        s.note = "🔴 buying absorbed AT the level — supply is meeting it"
    elif s.cvd_1h > 8 and s.trigger_dist_pct >= 0.5:
        s.note = "🔵 buying absorbed mid-range — possible accumulation"
    elif s.cvd_1h < -8:
        s.note = "⚪ net selling into the level — flow is against a long"

    return s


def analyse(symbol: str) -> tuple[Setup | None, str]:
    """Returns (setup, rejection_reason). Exactly one will be truthy."""
    raw = api_get("/api/v3/klines", {"symbol": symbol, "interval": INTERVAL, "limit": 120})
    candles = parse_klines(raw)
    if len(candles) < 100:
        return None, "insufficient history"

    # Drop the live, still-forming candle so every metric uses closed data.
    live = candles[-1]
    closed = candles[:-1]

    price = live["close"]
    if price <= 0:
        return None, "bad price"

    a_pct = atr_pct(closed)
    if a_pct < MIN_ATR_PCT:
        return None, f"ATR {a_pct:.2f}% too dead"
    if a_pct > MAX_ATR_PCT:
        return None, f"ATR {a_pct:.2f}% too wild"

    rv = rvol(closed)
    if rv < MIN_RVOL:
        return None, f"rvol {rv:.2f}x below gate"

    res = resistance_level(closed)
    if res <= 0:
        return None, "no resistance level"
    trigger = res * 1.0005
    trig_dist = (trigger - price) / price * 100.0
    if trig_dist > MAX_TRIGGER_DIST_PCT:
        return None, f"trigger {trig_dist:.2f}% out of reach"
    if trig_dist < -0.30:
        return None, "already extended past trigger"

    sl = swing_low(closed)
    atr_abs = a_pct / 100.0 * price
    stop = min(sl, price - 1.5 * atr_abs) - 0.0001 * price
    stop_pct = (price - stop) / price * 100.0
    if stop_pct > MAX_STOP_PCT:
        return None, f"stop {stop_pct:.2f}% too wide"
    if stop_pct <= 0:
        return None, "invalid stop"

    spread, depth = book_metrics(symbol, price)
    if spread > MAX_SPREAD_PCT:
        return None, f"spread {spread:.3f}% too wide"
    if depth < MIN_NEAR_DEPTH_USD:
        return None, f"depth ${depth:,.0f} too thin"

    cost = spread * 2 + TAKER_FEE_PCT * 2
    exp_move = a_pct * math.sqrt(HOLD_CANDLES)
    edge = exp_move / cost if cost > 0 else 0.0
    if edge < MIN_COST_EDGE:
        return None, f"edge {edge:.1f}x below floor"

    target = trigger + exp_move / 100.0 * price
    rr = (target - price) / (price - stop) if price > stop else 0.0

    s = Setup(
        symbol=symbol,
        price=price,
        trigger=trigger,
        stop=stop,
        trigger_dist_pct=trig_dist,
        stop_pct=stop_pct,
        rr=rr,
        atr_pct=a_pct,
        exp_move_pct=exp_move,
        cost_pct=cost,
        edge=edge,
        spread_pct=spread,
        rvol=rv,
        cvd_1h=cvd_delta_pct(closed, 4),
        cvd_3h=cvd_delta_pct(closed, 12),
        squeeze=squeeze_ratio(closed),
        depth_usd=depth,
    )
    return score_setup(s), ""


# --------------------------------------------------------------------------
# Output
# --------------------------------------------------------------------------

def grade(score: float) -> str:
    if score >= 70:
        return "strong"
    if score >= 55:
        return "moderate"
    if score >= 42:
        return "weak"
    return "poor"


def fmt(v: float) -> str:
    if v >= 100:
        return f"{v:,.2f}"
    if v >= 1:
        return f"{v:.4g}"
    return f"{v:.6g}"


def build_message(setups: list[Setup], scanned: int, analysed: int,
                  rejects: dict[str, int]) -> str:
    stamp = time.strftime("%d %b %H:%M UTC", time.gmtime())
    hold_min = HOLD_CANDLES * CANDLE_MINUTES
    lines = [
        f"Scalp setups — {stamp}",
        f"{scanned} pairs passed liquidity · {analysed} analysed · "
        f"horizon {hold_min}min",
        "",
    ]

    if not setups:
        lines.append("No setups cleared the gates this hour.")
        if rejects:
            top = sorted(rejects.items(), key=lambda kv: kv[1], reverse=True)[:4]
            lines.append("Main rejections: " + " · ".join(f"{k} ({v})" for k, v in top))
        lines.append("")
        lines.append("A silent hour is a result, not a failure.")
        return "\n".join(lines)

    for i, s in enumerate(setups, 1):
        lines.append(f"{i}. {s.symbol} — {s.score:.1f}/100 ({grade(s.score)})")
        lines.append(f"price {fmt(s.price)}")
        lines.append(f"trigger {fmt(s.trigger)} ({s.trigger_dist_pct:+.2f}% away)")
        lines.append(f"stop {fmt(s.stop)} (risk {s.stop_pct:.2f}%) · R:R {s.rr:.2f}")
        lines.append(
            f"cvd {s.parts['cvd']:.0f}/{W_CVD} · near {s.parts['proximity']:.0f}/{W_PROXIMITY}"
            f" · rvol {s.parts['rvol']:.0f}/{W_RVOL} · cost {s.parts['cost']:.0f}/{W_COST}"
        )
        lines.append(
            f"book {s.parts['book']:.0f}/{W_BOOK} · squeeze {s.parts['squeeze']:.0f}/{W_SQUEEZE}"
            f" · tight {s.parts['tight']:.0f}/{W_TIGHT}"
        )
        if s.note:
            lines.append(s.note)
        lines.append(
            f"ATR {s.atr_pct:.2f}%/15m · exp move ~{s.exp_move_pct:.2f}%"
            f" · cost {s.cost_pct:.2f}% · edge {s.edge:.1f}x"
        )
        lines.append(
            f"spread {s.spread_pct:.3f}% · rvol {s.rvol:.2f}x"
            f" · cvd 1h {s.cvd_1h:+.1f}% / 3h {s.cvd_3h:+.1f}%"
        )
        lines.append(f"squeeze ratio {s.squeeze:.2f} · bid depth 0.5%: ${s.depth_usd:,.0f}")
        lines.append("")

    lines.append(
        f"Structure only, not a prediction. Costs modelled at taker fees both "
        f"sides ({TAKER_FEE_PCT}%); limit entries change the maths. "
        f"Expected move assumes a {hold_min}min hold. Sizing and risk are yours."
    )
    return "\n".join(lines)


def send_telegram(text: str) -> None:
    if not TELEGRAM_BOT_TOKEN or not TELEGRAM_CHAT_ID:
        print("[warn] telegram credentials missing; printing instead")
        print(text)
        return
    # Telegram caps messages at 4096 chars
    for i in range(0, len(text), 3900):
        chunk = text[i:i + 3900]
        try:
            r = session.post(
                f"https://api.telegram.org/bot{TELEGRAM_BOT_TOKEN}/sendMessage",
                data={"chat_id": TELEGRAM_CHAT_ID, "text": chunk,
                      "disable_web_page_preview": "true"},
                timeout=20,
            )
            if r.status_code != 200:
                print(f"[warn] telegram {r.status_code}: {r.text[:200]}")
        except requests.RequestException as exc:
            print(f"[warn] telegram send failed: {exc}")


# --------------------------------------------------------------------------
# Main
# --------------------------------------------------------------------------

def main() -> int:
    try:
        shortlist = liquid_shortlist()
    except Exception as exc:
        send_telegram(f"⚠️ Scanner failed at shortlist stage: {exc}")
        return 1

    setups: list[Setup] = []
    rejects: dict[str, int] = {}
    analysed = 0

    for row in shortlist:
        sym = row["symbol"]
        try:
            s, reason = analyse(sym)
            analysed += 1
            if s:
                setups.append(s)
            elif reason:
                key = reason.split()[0]
                rejects[key] = rejects.get(key, 0) + 1
        except Exception as exc:
            print(f"[warn] {sym}: {exc}")
        time.sleep(0.12)  # stay well inside the weight limit

    setups.sort(key=lambda x: x.score, reverse=True)
    msg = build_message(setups[:MAX_REPORTED], len(shortlist), analysed, rejects)
    send_telegram(msg)
    print(msg)
    return 0


if __name__ == "__main__":
    sys.exit(main())
