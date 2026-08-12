#!/usr/bin/env python3
"""
Intraday breakout screener for 30min - 4hr holds -> Telegram.

Built for short holding periods, which changes almost everything versus a
swing screen:

  * 15m candles over 24h, not 4h candles over 15 days
  * "Squeeze" means the last 2 hours vs the prior 10, not days vs weeks
  * Resistance comes from intraday levels (3-12h old), not last week's
  * SPREAD AND COST ARE HARD FILTERS. When you are targeting a 1-2% move,
    a 0.25% round-trip cost eats most of the edge. A setup you cannot
    trade profitably is not a setup.
  * Liquidity floor is much higher - thin books that are fine for a
    multi-day hold will slip you badly entering and exiting in minutes.

CHANGES IN THIS REVISION
------------------------
1. LIVE CANDLE EXCLUDED from every indicator. Binance returns the
   in-progress candle as the last element. With cron at :00 the live 15m
   candle is seconds old, so the old code understated rvol by up to 25%
   on every run, depressed ATR, and let a partial candle define support.
   The live candle is now used only for current price and a separate
   "pace" reading.
2. CVD is unchanged in method (it was already correct: taker_buy*2 - vol)
   but is now measured over TWO windows and checked for divergence
   against price, so one violent candle cannot flip the whole reading.
3. Live spreads fetched for the WHOLE universe in one bookTicker call
   BEFORE shortlisting, instead of gating on stale 24h ticker quotes.
4. Funding fetched for all symbols in ONE premiumIndex call.
5. Book ratio measured inside a symmetric +/-0.5% band. Previously it
   compared 50 raw bid levels against 50 ask levels, which span totally
   different distances on an illiquid book and made the number noise.
6. Reward:risk is now a hard gate. Expected move divided by stop
   distance below MIN_RR disqualifies regardless of score.
7. Absorption is context-aware. Buying absorbed at resistance is
   distribution (penalty); the same pattern mid-range is accumulation
   (bonus). The old code always scored it as a bonus while printing it
   as a caution.
8. CVD confidence no longer multiplied by rvol - rvol already has its
   own 14 points, so low volume was being counted against a setup twice.
9. "Already broken" is graduated by ATR instead of a flat 0.70 haircut
   stacked on top of a zeroed proximity score.
10. Open interest change pulled from Binance futures where available.
11. Per-symbol work runs in a small thread pool.

Read-only. Public endpoints. No API keys. No order-placing code path.

Environment variables:
    TELEGRAM_BOT_TOKEN    required
    TELEGRAM_CHAT_ID      required
    BINANCE_BASE_URL      optional, default https://api.binance.com
    BINANCE_FAPI_URL      optional, default https://fapi.binance.com
    MIN_QUOTE_VOLUME      optional, default 10000000
    MAX_SPREAD_PCT        optional, default 0.12
    MIN_COST_EDGE         optional, default 4.0
    MIN_RR                optional, default 1.5   (expected move / risk)
    MAX_RISK_PCT          optional, default 4.0   (max stop distance)
    MIN_NEAR_DEPTH        optional, default 25000 (bid depth within 0.5%)
    MAX_ATR_PCT           optional, default 3.0   (per-15m volatility cap)
    SHORTLIST_SIZE        optional, default 25
    TAKER_FEE_PCT         optional, default 0.10
    HOLD_CANDLES          optional, default 12 (12 x 15m = 3h hold)
    BREAKOUT_MODE         optional, "anticipate" (default) or "confirm"
    WORKERS               optional, default 5
    EXTRA_EXCLUDE         optional, comma-separated base assets
"""

from __future__ import annotations

import os
import sys
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime, timezone

import requests

BASE_URL = os.environ.get("BINANCE_BASE_URL", "https://api.binance.com").rstrip("/")
FAPI_URL = os.environ.get("BINANCE_FAPI_URL", "https://fapi.binance.com").rstrip("/")
BOT_TOKEN = os.environ.get("TELEGRAM_BOT_TOKEN", "").strip()
CHAT_ID = os.environ.get("TELEGRAM_CHAT_ID", "").strip()

MIN_QUOTE_VOLUME = float(os.environ.get("MIN_QUOTE_VOLUME", 10_000_000))
MAX_SPREAD_PCT = float(os.environ.get("MAX_SPREAD_PCT", 0.12))
MIN_COST_EDGE = float(os.environ.get("MIN_COST_EDGE", 4.0))
# Expected move divided by stop distance. ZEC ranked second on a 51.9
# score while its stop was 2.27% away against a 1.69% expected move -
# risking more than the target. Structure score cannot see that.
MIN_RR = float(os.environ.get("MIN_RR", 1.5))
MAX_RISK_PCT = float(os.environ.get("MAX_RISK_PCT", 4.0))
MIN_NEAR_DEPTH = float(os.environ.get("MIN_NEAR_DEPTH", 25_000))
MAX_ATR_PCT = float(os.environ.get("MAX_ATR_PCT", 3.0))
SHORTLIST_SIZE = int(os.environ.get("SHORTLIST_SIZE", 25))
TAKER_FEE_PCT = float(os.environ.get("TAKER_FEE_PCT", 0.10))
HOLD_CANDLES = int(os.environ.get("HOLD_CANDLES", 12))
# "anticipate" scores highest just BELOW an untested level.
# "confirm" scores highest just ABOVE one that has broken on volume.
BREAKOUT_MODE = os.environ.get("BREAKOUT_MODE", "anticipate").strip().lower()
WORKERS = int(os.environ.get("WORKERS", 5))

TIMEOUT = 20
SESSION = requests.Session()
SESSION.headers.update({"User-Agent": "scalp-screener/2.0"})

STABLE_BASES = {
    "USDC", "FDUSD", "TUSD", "BUSD", "DAI", "USDP", "EUR", "GBP", "AEUR",
    "USD1", "PYUSD", "EURI", "XUSD", "USDE", "USDS", "RLUSD", "BFUSD",
    "FRAX", "XAUT", "PAXG", "WBTC", "WBETH", "BNSOL",
}

TOKENIZED_EQUITY = {
    "AAOIB", "AAPLB", "AMATB", "AMDB", "AMZNB", "ARMB", "AVGOB", "AXTIB",
    "BABAB", "CBRSB", "COINB", "CRCLB", "CRWVB", "DELLB", "DRAMB", "EWYB",
    "FLNCB", "GLWB", "GOOGLB", "GSB", "HOODB", "IBMB", "INTCB", "INTWB",
    "KORUB", "LITEB", "METAB", "MRVLB", "MSFTB", "MSTRB", "MVLLB", "NBISB",
    "NOKB", "NVDAB", "ORCLB", "PLTRB", "PYPLB", "QCOMB", "QNTB", "QQQB",
    "RKLBB", "SKHYB", "SMHB", "SNDKB", "SNXXB", "SOXLB", "SOXSB", "SPCXB",
    "SPYB", "TQQQB", "TSLAB", "TSMB", "WDCB",
    "QQQON", "AAPLON", "GOOGLON", "TSLAON", "NVDAON", "SPYON",
    "METAON", "MSFTON", "AMZNON", "COINON", "MSTRON",
}

EXTRA_EXCLUDE = {
    s.strip().upper() for s in os.environ.get("EXTRA_EXCLUDE", "").split(",")
    if s.strip()
}

LEVERAGED_MARKERS = ("UPUSDT", "DOWNUSDT", "BULLUSDT", "BEARUSDT")


class GeoBlocked(Exception):
    """Binance returned 451 - runner IP is in a Restricted Location."""


def api_get(base, path, params=None, retries=3):
    url = f"{base}{path}"
    last = None
    for attempt in range(retries):
        try:
            r = SESSION.get(url, params=params, timeout=TIMEOUT)
        except requests.RequestException as exc:
            last = exc
            time.sleep(2 * (attempt + 1))
            continue
        if r.status_code == 451:
            raise GeoBlocked(
                "Binance returned HTTP 451. Runner IP is in a Restricted "
                "Location - set BINANCE_BASE_URL to a working host."
            )
        if r.status_code in (429, 418):
            time.sleep(int(r.headers.get("Retry-After", 5 * (attempt + 1))))
            last = RuntimeError(f"rate limited {r.status_code}")
            continue
        if r.status_code >= 400:
            raise RuntimeError(f"HTTP {r.status_code} {path}: {r.text[:200]}")
        return r.json()
    raise RuntimeError(f"failed after {retries}: {last}")


def _clamp(x, lo=0.0, hi=1.0):
    return max(lo, min(hi, x))


def _pos_in_range(series):
    """Where the final value sits between the series min and max, 0-1.
    Works with negative values, which a cumulative delta series often is."""
    lo, hi = min(series), max(series)
    if hi <= lo:
        return 0.5
    return (series[-1] - lo) / (hi - lo)


# --------------------------------------------------------------------------
# Universe
# --------------------------------------------------------------------------

def fetch_live_spreads():
    """Best bid/ask for every symbol in one call.

    The 24h ticker carries bid/ask too, but they can be badly stale - the
    HEI case passed a 0.12% gate on a stale quote while its live spread
    was 0.154%. Since cost edge is the gate that decides whether a scalp
    is viable at all, it has to be computed from live quotes, and it is
    cheaper to fetch all of them once than to discover the problem one
    symbol at a time after the shortlist is already built.
    """
    out = {}
    try:
        for t in api_get(BASE_URL, "/api/v3/ticker/bookTicker"):
            try:
                bid, ask = float(t["bidPrice"]), float(t["askPrice"])
                if bid > 0 and ask > 0:
                    out[t["symbol"]] = (bid, ask)
            except (KeyError, ValueError):
                continue
    except Exception as exc:
        print(f"bookTicker failed, falling back to 24h quotes: {exc}",
              file=sys.stderr)
    return out


def fetch_all_funding():
    """Funding rate for every perp in one call instead of 25 separate ones."""
    out = {}
    try:
        data = api_get(FAPI_URL, "/fapi/v1/premiumIndex", retries=2)
        if isinstance(data, dict):
            data = [data]
        for d in data:
            try:
                out[d["symbol"]] = float(d["lastFundingRate"])
            except (KeyError, ValueError, TypeError):
                continue
    except Exception as exc:
        print(f"premiumIndex batch failed: {exc}", file=sys.stderr)
    return out


def build_universe(live_spreads):
    info = api_get(BASE_URL, "/api/v3/exchangeInfo")
    valid = set()
    for s in info.get("symbols", []):
        if s.get("status") != "TRADING" or s.get("quoteAsset") != "USDT":
            continue
        base, sym = s.get("baseAsset", ""), s["symbol"]
        if (base in STABLE_BASES or base in TOKENIZED_EQUITY
                or base in EXTRA_EXCLUDE or base == "BTC"):
            continue
        if any(sym.endswith(m) for m in LEVERAGED_MARKERS):
            continue
        valid.add(sym)

    stats = api_get(BASE_URL, "/api/v3/ticker/24hr")
    out = []
    for t in stats:
        sym = t["symbol"]
        if sym not in valid:
            continue
        try:
            qv = float(t["quoteVolume"])
            if qv < MIN_QUOTE_VOLUME:
                continue
            hi, lo = float(t["highPrice"]), float(t["lowPrice"])
            last = float(t["lastPrice"])
            if hi <= lo or last <= 0:
                continue

            # Prefer the live quote; fall back to the 24h ticker's.
            if sym in live_spreads:
                bid, ask = live_spreads[sym]
            else:
                bid, ask = float(t["bidPrice"]), float(t["askPrice"])
            if bid <= 0 or ask <= 0 or ask <= bid:
                continue

            # SPREAD IS A HARD GATE. On a 1% target, a 0.2% spread is 20%
            # of the move gone before you start.
            spread_pct = (ask - bid) / last * 100
            if spread_pct > MAX_SPREAD_PCT:
                continue

            out.append({
                "symbol": sym, "last": last, "quote_volume": qv,
                "spread_pct": spread_pct,
                "change_pct": float(t["priceChangePercent"]),
                "range_pos": (last - lo) / (hi - lo),
            })
        except (KeyError, ValueError):
            continue
    return out


def shortlist(cands):
    primed = [c for c in cands if c["range_pos"] >= 0.55]
    primed.sort(key=lambda c: (c["range_pos"], c["quote_volume"]), reverse=True)
    return primed[:SHORTLIST_SIZE]


# --------------------------------------------------------------------------
# Data
# --------------------------------------------------------------------------

def get_klines(symbol, interval, limit):
    """Returns (completed_candles, live_candle).

    Binance includes the in-progress candle as the last element. Every
    indicator here must run on completed candles only - with cron at :00
    the live 15m candle is seconds old, and averaging it into a 4-candle
    volume mean understates rvol by up to 25%.
    """
    raw = api_get(BASE_URL, "/api/v3/klines",
                  {"symbol": symbol, "interval": interval, "limit": limit})
    rows = [{
        "open_time": int(k[0]),
        "open": float(k[1]), "high": float(k[2]), "low": float(k[3]),
        "close": float(k[4]), "volume": float(k[5]),
        "close_time": int(k[6]), "taker_buy": float(k[9]),
    } for k in raw]
    if len(rows) < 2:
        return rows, None
    return rows[:-1], rows[-1]


def get_book(symbol):
    """Depth and a bid/ask ratio measured inside the same +/-0.5% band.

    The previous version summed 50 raw bid levels against 50 raw ask
    levels. On a thin book those levels span wildly different distances
    from mid, so the ratio was comparing quantities sitting at different
    prices - noise dressed up as a signal.
    """
    try:
        d = api_get(BASE_URL, "/api/v3/depth", {"symbol": symbol, "limit": 100})
        bids, asks = d.get("bids", []), d.get("asks", [])
        if not bids or not asks:
            return None, None, None
        best_bid, best_ask = float(bids[0][0]), float(asks[0][0])
        mid = (best_bid + best_ask) / 2
        lo, hi = mid * 0.995, mid * 1.005
        near_bid = sum(float(p) * float(q) for p, q in bids if float(p) >= lo)
        near_ask = sum(float(p) * float(q) for p, q in asks if float(p) <= hi)
        ratio = (near_bid / near_ask) if near_ask > 0 else None
        return ratio, near_bid, (best_ask - best_bid) / mid * 100
    except Exception:
        return None, None, None


def get_oi_change(symbol):
    """Open interest change over the last 2 hours, as a percent.

    Rising price on rising OI is new money. Rising price on falling OI is
    short covering, which tends to stall. Returns None when the symbol
    has no perp or the endpoint is unavailable - never fatal.
    """
    try:
        d = api_get(FAPI_URL, "/futures/data/openInterestHist",
                    {"symbol": symbol, "period": "15m", "limit": 8}, retries=1)
        if not d or len(d) < 2:
            return None
        first = float(d[0]["sumOpenInterest"])
        last = float(d[-1]["sumOpenInterest"])
        if first <= 0:
            return None
        return (last - first) / first * 100
    except Exception:
        return None


# --------------------------------------------------------------------------
# CVD - real cumulative volume delta
# --------------------------------------------------------------------------

def cvd_delta(candles):
    """Taker buy volume minus taker sell volume.

    Binance kline index 9 is takerBuyBaseAssetVolume. Taker sell is
    therefore (volume - taker_buy), so the delta is 2*taker_buy - volume.
    This measures which side crossed the spread - who was aggressive.

    It is NOT the same as a close-location proxy (Chaikin A/D), which
    only asks where in its range a candle closed. The two diverge hardest
    on absorption: aggressive buyers hitting into resting sell limits give
    positive CVD but a close near the low. That case matters, so the real
    metric is the one to use.
    """
    vol = sum(k["volume"] for k in candles)
    delta = sum(2 * k["taker_buy"] - k["volume"] for k in candles)
    return delta, vol, (delta / vol if vol > 0 else 0.0)


def cvd_series(candles):
    cum, out = 0.0, []
    for k in candles:
        cum += 2 * k["taker_buy"] - k["volume"]
        out.append(cum)
    return out


# --------------------------------------------------------------------------
# Analysis
# --------------------------------------------------------------------------

def analyse(symbol, spread_pct, quote_volume, funding):
    # 97 fetched so 96 completed remain after dropping the live candle.
    c15, live15 = get_klines(symbol, "15m", 97)
    if len(c15) < 60 or live15 is None:
        return None
    c5, live5 = get_klines(symbol, "5m", 61)
    if len(c5) < 36:
        return None

    # Current price comes from the live candle; everything else does not.
    last = live15["close"]
    if last <= 0:
        return None

    # --- squeeze: last 2h vs prior 10h ---------------------------------
    recent, prior = c15[-8:], c15[-48:-8]
    r_rec = sum((k["high"] - k["low"]) / k["close"] for k in recent) / len(recent)
    r_pri = sum((k["high"] - k["low"]) / k["close"] for k in prior) / len(prior)
    contraction = (r_rec / r_pri) if r_pri > 0 else 99.0

    # --- ATR on completed 15m candles ----------------------------------
    trs = []
    for i in range(1, len(c15)):
        h, l, pc = c15[i]["high"], c15[i]["low"], c15[i - 1]["close"]
        trs.append(max(h - l, abs(h - pc), abs(l - pc)))
    atr = sum(trs[-14:]) / 14
    atr_pct = atr / last * 100

    # --- intraday resistance: 3h to 12h ago ----------------------------
    # The level must be one price already failed at, not a high printed
    # minutes ago.
    window = c15[-48:-12]
    resistance = max(k["high"] for k in window)
    raw_dist = (resistance - last) / last * 100   # negative once broken
    already_broken = raw_dist < 0

    # Support from completed candles only. Previously the live candle
    # could set this, so a stop could be placed off a 30-second low.
    support = min(k["low"] for k in c15[-12:])
    risk_pct = abs(last - support) / last * 100

    # --- book (fetched before cost so the live spread is used) ---------
    book_ratio, near_depth, live_spread = get_book(symbol)
    if live_spread is not None and live_spread > 0:
        spread_pct = live_spread

    # --- COST EDGE ------------------------------------------------------
    # Price wanders roughly with the square root of time, so a
    # HOLD_CANDLES-long hold gives about atr * sqrt(HOLD_CANDLES).
    round_trip = spread_pct * 2 + TAKER_FEE_PCT * 2
    expected_move = atr_pct * (HOLD_CANDLES ** 0.5)
    cost_edge = (expected_move / round_trip) if round_trip > 0 else 0.0
    rr = (expected_move / risk_pct) if risk_pct > 0 else 0.0

    # --- relative volume, completed candles only -----------------------
    v_rec = sum(k["volume"] for k in c15[-4:]) / 4
    base_slice = c15[-96:-4]
    v_base = (sum(k["volume"] for k in base_slice) / len(base_slice)
              if base_slice else 0.0)
    rvol = (v_rec / v_base) if v_base > 0 else 0.0

    # Is the candle forming RIGHT NOW running hot? Reported separately so
    # it informs timing without contaminating rvol.
    now_ms = int(time.time() * 1000)
    elapsed = (now_ms - live15["open_time"]) / (15 * 60 * 1000)
    live_pace = None
    if 0.08 < elapsed <= 1.0 and v_base > 0:
        live_pace = (live15["volume"] / elapsed) / v_base

    # --- CVD over two windows ------------------------------------------
    # One window is fragile: a single violent candle can flip the sign of
    # a short window entirely. Two windows plus a divergence check make
    # the reading much harder to fool.
    _, _, cvd_1h = cvd_delta(c5[-12:])     # last hour
    _, _, cvd_3h = cvd_delta(c5[-36:])     # last three hours
    cvd_share = (cvd_1h * 0.6) + (cvd_3h * 0.4)

    # Divergence: price pressed against the top of its 3h range while
    # cumulative delta is well off its own high means the move is not
    # being carried by aggressive buying.
    seg3 = c5[-36:]
    series = cvd_series(seg3)
    px_pos = _pos_in_range([k["close"] for k in seg3] + [last])
    cvd_pos = _pos_in_range(series)
    divergence = px_pos > 0.85 and cvd_pos < 0.60

    # Absorption: real buying that is not moving price. Whether that is
    # bullish depends entirely on WHERE it happens - accumulation in the
    # middle of a range, distribution into overhead supply.
    seg1 = c5[-12:]
    px_chg = ((seg1[-1]["close"] - seg1[0]["open"]) / seg1[0]["open"]
              if seg1[0]["open"] > 0 else 0.0)
    absorbing = cvd_1h > 0.03 and px_chg <= 0.003
    near_level = abs(raw_dist) < max(0.5, atr_pct)
    absorption_bull = absorbing and not near_level
    absorption_bear = absorbing and near_level

    oi_change = get_oi_change(symbol)

    # --- score ----------------------------------------------------------
    cvd_part = _clamp((cvd_share + 0.04) / 0.16)
    if absorption_bull:
        cvd_part = min(1.0, cvd_part + 0.25)
    if absorption_bear:
        cvd_part *= 0.60
    if divergence:
        cvd_part *= 0.50
    # A CVD reading on almost no volume is not evidence of anything, but
    # it should not be punished twice - rvol already carries 14 points of
    # its own. Only genuinely dead tape gets discounted here.
    if rvol < 0.40:
        cvd_part *= 0.70

    # Proximity. Anticipate mode wants price just below an untested
    # level. Confirm mode wants it just above one that has broken. Either
    # way, being far past the level is extension, not opportunity.
    if not already_broken:
        proximity = _clamp((4.0 - raw_dist) / 4.0)
        if BREAKOUT_MODE == "confirm":
            proximity *= 0.60
    else:
        over_atr = abs(raw_dist) / atr_pct if atr_pct > 0 else 99.0
        if BREAKOUT_MODE == "confirm":
            # best right after the break, decaying past ~1.5 ATR above
            proximity = _clamp((1.5 - over_atr) / 1.5)
        else:
            proximity = _clamp((1.0 - over_atr) / 1.0) * 0.50

    parts = {
        "squeeze": _clamp((1.10 - contraction) / 0.65),
        "proximity": proximity,
        "rvol": _clamp((rvol - 0.70) / 1.30),
        "cvd": cvd_part,
        # Saturates at 10x. Beyond that the extra "edge" is just
        # volatility, and volatility is not free.
        "cost_edge": _clamp((cost_edge - MIN_COST_EDGE) / (10.0 - MIN_COST_EDGE)),
        "book": _clamp(((book_ratio or 1.0) - 1.0) / 1.0),
        "funding": 0.5 if funding is None else _clamp((0.0006 - funding) / 0.0012),
        # Full marks at 1.5% or tighter, zero at MAX_RISK_PCT.
        "tightness": _clamp((MAX_RISK_PCT - risk_pct) / (MAX_RISK_PCT - 1.5)),
    }

    weights = {"squeeze": 21, "proximity": 16, "rvol": 14, "cvd": 14,
               "cost_edge": 10, "tightness": 12, "book": 8, "funding": 5}
    total = sum(parts[k] * weights[k] for k in weights)

    # --- HARD TRADEABILITY GATES ---------------------------------------
    blockers = []
    if cost_edge < MIN_COST_EDGE:
        blockers.append(f"cost edge {cost_edge:.1f}x < {MIN_COST_EDGE}x")
    if rr < MIN_RR:
        blockers.append(f"R:R {rr:.2f} < {MIN_RR} "
                        f"(move {expected_move:.2f}% vs risk {risk_pct:.2f}%)")
    if risk_pct > MAX_RISK_PCT:
        blockers.append(f"stop {risk_pct:.1f}% away > {MAX_RISK_PCT}%")
    if near_depth is not None and near_depth < MIN_NEAR_DEPTH:
        blockers.append(f"depth ${near_depth:,.0f} < ${MIN_NEAR_DEPTH:,.0f}")
    if atr_pct > MAX_ATR_PCT:
        blockers.append(f"ATR {atr_pct:.1f}%/15m > {MAX_ATR_PCT}%")
    if spread_pct > MAX_SPREAD_PCT:
        blockers.append(f"live spread {spread_pct:.3f}% > {MAX_SPREAD_PCT}%")

    return {
        "symbol": symbol, "price": last, "score": round(total, 1),
        "risk_pct": risk_pct, "rr": rr, "blockers": blockers,
        "parts": {k: round(parts[k] * weights[k], 1) for k in weights},
        "resistance": resistance, "support": support,
        "already_broken": already_broken, "dist_pct": abs(raw_dist),
        "contraction": contraction, "rvol": rvol, "live_pace": live_pace,
        "cvd_share": cvd_share, "cvd_1h": cvd_1h, "cvd_3h": cvd_3h,
        "divergence": divergence,
        "absorption_bull": absorption_bull, "absorption_bear": absorption_bear,
        "atr_pct": atr_pct, "spread_pct": spread_pct,
        "cost_edge": cost_edge, "book_ratio": book_ratio,
        "expected_move": expected_move, "round_trip": round_trip,
        "near_depth": near_depth, "funding": funding, "oi_change": oi_change,
        "quote_volume": quote_volume,
    }


# --------------------------------------------------------------------------

def send_telegram(text):
    if not BOT_TOKEN or not CHAT_ID:
        print("Telegram not configured:\n" + text)
        return
    r = SESSION.post(f"https://api.telegram.org/bot{BOT_TOKEN}/sendMessage",
                     timeout=TIMEOUT,
                     json={"chat_id": CHAT_ID, "text": text,
                           "parse_mode": "HTML",
                           "disable_web_page_preview": True})
    if r.status_code >= 400:
        print(f"Telegram error {r.status_code}: {r.text[:300]}", file=sys.stderr)


def format_alert(results, deep, liquid):
    stamp = datetime.now(timezone.utc).strftime("%d %b %H:%M UTC")
    L = [f"<b>Intraday setups</b> — {stamp}",
         f"<i>{liquid} pairs passed liquidity+spread, {deep} analysed "
         f"· mode: {BREAKOUT_MODE}</i>", ""]

    for i, r in enumerate(results, 1):
        p = r["parts"]
        band = ("strong" if r["score"] >= 70 else
                "moderate" if r["score"] >= 55 else
                "weak" if r["score"] >= 40 else "poor")
        L.append(f"<b>{i}. {r['symbol']}</b> — {r['score']}/100 ({band})")
        L.append(f"  price {r['price']:.6g}")
        if r["already_broken"]:
            L.append(f"  ⚠️ already {r['dist_pct']:.2f}% above "
                     f"{r['resistance']:.6g} — move underway")
        else:
            L.append(f"  trigger {r['resistance']:.6g} ({r['dist_pct']:.2f}% away)")
        L.append(f"  stop below {r['support']:.6g} (risk {r['risk_pct']:.2f}%) "
                 f"· R:R {r['rr']:.2f}")
        L.append(f"  squeeze {p['squeeze']:.0f}/21 · near {p['proximity']:.0f}/16 "
                 f"· rvol {p['rvol']:.0f}/14 · cvd {p['cvd']:.0f}/14")
        L.append(f"  tight {p['tightness']:.0f}/12 · cost {p['cost_edge']:.0f}/10 "
                 f"· book {p['book']:.0f}/8 · fund {p['funding']:.0f}/5")
        if r.get("blockers"):
            L.append("  🚫 NOT TRADEABLE — " + "; ".join(r["blockers"]))
        if r["divergence"]:
            L.append("  🔻 price at highs, CVD is not — move not carried by buyers")
        if r["absorption_bear"]:
            L.append("  🔴 buying absorbed AT the level — supply is meeting it")
        if r["absorption_bull"]:
            L.append("  🔵 buying absorbed mid-range — possible accumulation")
        L.append(f"  <i>ATR {r['atr_pct']:.2f}%/15m · exp move ~{r['expected_move']:.2f}% "
                 f"· cost {r['round_trip']:.2f}% · edge {r['cost_edge']:.1f}x</i>")
        cvd_line = (f"  <i>spread {r['spread_pct']:.3f}% (live) · rvol {r['rvol']:.2f}x "
                    f"· cvd 1h {r['cvd_1h']:+.1%} / 3h {r['cvd_3h']:+.1%}</i>")
        L.append(cvd_line)
        extras = []
        if r["live_pace"]:
            extras.append(f"live candle pace {r['live_pace']:.2f}x")
        if r["oi_change"] is not None:
            extras.append(f"OI 2h {r['oi_change']:+.2f}%")
        if r["near_depth"]:
            extras.append(f"bid depth 0.5%: ${r['near_depth']:,.0f}")
        if extras:
            L.append("  <i>" + " · ".join(extras) + "</i>")
        L.append("")

    if results and results[0]["score"] < 55:
        L.append("<i>⚠️ Weak field — nothing here is a clean setup.</i>")
    L.append("<i>Intraday structure only, not a prediction. Costs are "
             "modelled at taker fees both sides; limit entries change the "
             "maths. Sizing and risk are yours.</i>")
    return "\n".join(L)


def main():
    try:
        live_spreads = fetch_live_spreads()
        universe = build_universe(live_spreads)
        funding_map = fetch_all_funding()
    except GeoBlocked as exc:
        send_telegram(f"⚠️ <b>Scan blocked</b>\n{exc}")
        return 2
    except Exception as exc:
        send_telegram(f"⚠️ <b>Scan failed</b>\n<code>{exc}</code>")
        return 1

    liquid = len(universe)
    picks = shortlist(universe)
    print(f"{liquid} passed liquidity+spread, {len(picks)} shortlisted, "
          f"{len(funding_map)} funding rates, {len(live_spreads)} live quotes")
    if not picks:
        send_telegram(
            "\u26a0\ufe0f <b>Scan ran \u2014 quiet market</b>\n"
            f"{liquid} pairs passed liquidity+spread, but none are in the "
            "upper half of their 24h range. Nothing setting up. "
            "This is a real result, not a failure.")
        print("nothing in the upper half of range - sent status message")
        return 0

    results = []
    geo_blocked = False

    def work(c):
        return analyse(c["symbol"], c["spread_pct"], c["quote_volume"],
                       funding_map.get(c["symbol"]))

    with ThreadPoolExecutor(max_workers=max(1, WORKERS)) as pool:
        futures = {pool.submit(work, c): c for c in picks}
        for fut in as_completed(futures):
            sym = futures[fut]["symbol"]
            try:
                r = fut.result()
            except GeoBlocked:
                geo_blocked = True
                continue
            except Exception as exc:
                print(f"  {sym}: {exc}", file=sys.stderr)
                continue
            if r:
                # Keep everything and mark viability rather than dropping
                # it. Silently sending nothing looks identical to a broken
                # cron, so the alert always goes out and says what it found.
                r["tradeable"] = not r["blockers"]
                results.append(r)

    if geo_blocked and not results:
        send_telegram("⚠️ <b>Scan blocked</b>\nBinance returned HTTP 451 "
                      "during analysis — runner IP is in a Restricted Location.")
        return 2

    if not results:
        send_telegram(f"⚠️ <b>Scan ran, nothing analysable</b>\n"
                      f"{liquid} pairs passed liquidity+spread but none "
                      f"returned usable candles. Not a crash — check the "
                      f"Actions log if this repeats.")
        print("no analysable results")
        return 0

    # tradeable setups rank above untradeable ones at any score
    results.sort(key=lambda r: (r["tradeable"], r["score"]), reverse=True)
    send_telegram(format_alert(results[:3], len(results), liquid))
    print("sent: " + ", ".join(f"{r['symbol']} {r['score']}" for r in results[:3]))
    return 0


if __name__ == "__main__":
    sys.exit(main())
