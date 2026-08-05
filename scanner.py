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

Read-only. Public endpoints. No API keys. No order-placing code path.

Environment variables:
    TELEGRAM_BOT_TOKEN    required
    TELEGRAM_CHAT_ID      required
    BINANCE_BASE_URL      optional, default https://api.binance.com
    MIN_QUOTE_VOLUME      optional, default 10000000
    MAX_SPREAD_PCT        optional, default 0.12
    MIN_COST_EDGE         optional, default 4.0
    MAX_RISK_PCT          optional, default 4.0   (max stop distance)
    MIN_NEAR_DEPTH        optional, default 25000 (bid depth within 0.5%)
    MAX_ATR_PCT           optional, default 3.0   (per-15m volatility cap)
    SHORTLIST_SIZE        optional, default 25
    TAKER_FEE_PCT         optional, default 0.10
    HOLD_CANDLES          optional, default 12 (12 x 15m = 3h hold)
    EXTRA_EXCLUDE         optional, comma-separated base assets
"""

from __future__ import annotations

import os
import sys
import time
from datetime import datetime, timezone

import requests

BASE_URL = os.environ.get("BINANCE_BASE_URL", "https://api.binance.com").rstrip("/")
FAPI_URL = os.environ.get("BINANCE_FAPI_URL", "https://fapi.binance.com").rstrip("/")
BOT_TOKEN = os.environ.get("TELEGRAM_BOT_TOKEN", "").strip()
CHAT_ID = os.environ.get("TELEGRAM_CHAT_ID", "").strip()

MIN_QUOTE_VOLUME = float(os.environ.get("MIN_QUOTE_VOLUME", 10_000_000))
MAX_SPREAD_PCT = float(os.environ.get("MAX_SPREAD_PCT", 0.12))
MIN_COST_EDGE = float(os.environ.get("MIN_COST_EDGE", 4.0))
# Max distance to the stop. A 19% stop is incompatible with a 30min-4hr
# hold no matter how good the structure looks.
MAX_RISK_PCT = float(os.environ.get("MAX_RISK_PCT", 4.0))
# Minimum resting bid depth within 0.5% of price. Below this you cannot
# exit a position without moving the market against yourself.
MIN_NEAR_DEPTH = float(os.environ.get("MIN_NEAR_DEPTH", 25_000))
# Ceiling on per-candle volatility. Above this it is chaos, not opportunity.
MAX_ATR_PCT = float(os.environ.get("MAX_ATR_PCT", 3.0))
SHORTLIST_SIZE = int(os.environ.get("SHORTLIST_SIZE", 25))
TAKER_FEE_PCT = float(os.environ.get("TAKER_FEE_PCT", 0.10))
# how many 15m candles you expect to hold - 12 = 3 hours
HOLD_CANDLES = int(os.environ.get("HOLD_CANDLES", 12))

TIMEOUT = 20
SESSION = requests.Session()
SESSION.headers.update({"User-Agent": "scalp-screener/1.0"})

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


# --------------------------------------------------------------------------
# Universe
# --------------------------------------------------------------------------

def build_universe():
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
        if t["symbol"] not in valid:
            continue
        try:
            qv = float(t["quoteVolume"])
            if qv < MIN_QUOTE_VOLUME:
                continue
            hi, lo = float(t["highPrice"]), float(t["lowPrice"])
            last = float(t["lastPrice"])
            bid, ask = float(t["bidPrice"]), float(t["askPrice"])
            if hi <= lo or last <= 0 or bid <= 0 or ask <= 0:
                continue

            # SPREAD IS A HARD GATE. This is the single biggest difference
            # from the swing screen. On a 1% target, a 0.2% spread is 20%
            # of the move gone before you start.
            spread_pct = (ask - bid) / last * 100
            if spread_pct > MAX_SPREAD_PCT:
                continue

            out.append({
                "symbol": t["symbol"], "last": last, "quote_volume": qv,
                "spread_pct": spread_pct, "change_pct": float(t["priceChangePercent"]),
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
    raw = api_get(BASE_URL, "/api/v3/klines",
                  {"symbol": symbol, "interval": interval, "limit": limit})
    return [{
        "high": float(k[2]), "low": float(k[3]), "close": float(k[4]),
        "open": float(k[1]), "volume": float(k[5]), "taker_buy": float(k[9]),
    } for k in raw]


def get_book(symbol):
    try:
        d = api_get(BASE_URL, "/api/v3/depth", {"symbol": symbol, "limit": 50})
        bids, asks = d.get("bids", []), d.get("asks", [])
        if not bids or not asks:
            return None, None, None
        bid_usd = sum(float(p) * float(q) for p, q in bids)
        ask_usd = sum(float(p) * float(q) for p, q in asks)
        best_bid, best_ask = float(bids[0][0]), float(asks[0][0])
        ratio = bid_usd / ask_usd if ask_usd > 0 else None
        # depth within 0.5% of price - what you can actually fill against
        mid = (best_bid + best_ask) / 2
        near = sum(float(p) * float(q) for p, q in bids
                   if float(p) >= mid * 0.995)
        return ratio, near, (best_ask - best_bid) / mid * 100
    except Exception:
        return None, None, None


def get_funding(symbol):
    try:
        d = api_get(FAPI_URL, "/fapi/v1/premiumIndex", {"symbol": symbol}, retries=1)
        return float(d["lastFundingRate"])
    except Exception:
        return None


# --------------------------------------------------------------------------
# Analysis
# --------------------------------------------------------------------------

def analyse(symbol, spread_pct, quote_volume):
    # 96 x 15m = 24 hours. The whole relevant history for an intraday trade.
    c15 = get_klines(symbol, "15m", 96)
    if len(c15) < 60:
        return None
    # 60 x 5m = 5 hours, for entry-timing detail
    c5 = get_klines(symbol, "5m", 60)
    if len(c5) < 30:
        return None

    last = c15[-1]["close"]

    # --- squeeze: last 2h vs prior 10h ---------------------------------
    recent, prior = c15[-8:], c15[-48:-8]
    r_rec = sum((k["high"] - k["low"]) / k["close"] for k in recent) / len(recent)
    r_pri = sum((k["high"] - k["low"]) / k["close"] for k in prior) / len(prior)
    contraction = (r_rec / r_pri) if r_pri > 0 else 99.0

    # --- intraday resistance: 3h to 12h ago, excluding the recent 3h ----
    # Same logic as the swing version but on an intraday scale: the level
    # must be one price already failed at, not a high made minutes ago.
    window = c15[-48:-12]
    resistance = max(k["high"] for k in window)
    dist_pct = (resistance - last) / last * 100
    already_broken = dist_pct < 0
    if already_broken:
        dist_pct = 99.0

    support = min(k["low"] for k in c15[-12:])

    # --- ATR on 15m, as a % - the size of move you can realistically get
    trs = []
    for i in range(1, len(c15)):
        h, l, pc = c15[i]["high"], c15[i]["low"], c15[i - 1]["close"]
        trs.append(max(h - l, abs(h - pc), abs(l - pc)))
    atr = sum(trs[-14:]) / 14
    atr_pct = atr / last * 100

    # Fetch the book FIRST. The universe-level spread came from the 24h
    # ticker and can be badly stale - HEI passed a 0.12% gate while its
    # live spread was 0.154%. Cost must be computed from the live figure.
    book_ratio, near_depth, live_spread = get_book(symbol)
    if live_spread is not None and live_spread > 0:
        spread_pct = live_spread

    # --- COST EDGE: expected move vs what it costs to round-trip -------
    # This is the metric that decides whether a scalp is even viable.
    # Round trip = spread crossed twice + taker fee both sides.
    #
    # Expected move is NOT one candle's ATR - you are holding across many
    # candles. Price wanders roughly with the square root of time, so a
    # HOLD_CANDLES-long hold gives about atr * sqrt(HOLD_CANDLES). At the
    # default 12 x 15m (3 hours) that is ~3.5x a single-candle ATR.
    round_trip = spread_pct * 2 + TAKER_FEE_PCT * 2
    expected_move = atr_pct * (HOLD_CANDLES ** 0.5)
    cost_edge = (expected_move / round_trip) if round_trip > 0 else 0.0

    # --- relative volume: is activity picking up RIGHT NOW? ------------
    v_rec = sum(k["volume"] for k in c15[-4:]) / 4
    v_base = sum(k["volume"] for k in c15[-96:-4]) / max(1, len(c15[-96:-4]))
    rvol = (v_rec / v_base) if v_base > 0 else 0.0

    # --- short-window CVD on the 5m series -----------------------------
    seg = c5[-12:]          # last hour
    vol = sum(k["volume"] for k in seg)
    delta = sum(2 * k["taker_buy"] - k["volume"] for k in seg)
    cvd_share = (delta / vol) if vol > 0 else 0.0
    px_chg = ((seg[-1]["close"] - seg[0]["open"]) / seg[0]["open"]
              if seg[0]["open"] > 0 else 0.0)
    absorption = cvd_share > 0.03 and px_chg <= 0.003

    funding = get_funding(symbol)

    # --- score ----------------------------------------------------------
    cvd_conf = _clamp(rvol / 0.80)
    cvd_part = _clamp((cvd_share + 0.04) / 0.16)
    if absorption:
        cvd_part = min(1.0, cvd_part + 0.25)
    cvd_part *= cvd_conf

    parts = {
        # 1.0 at 0.45x or tighter, 0 at 1.10x
        "squeeze": _clamp((1.10 - contraction) / 0.65),
        # intraday: within 1.5% is close. 0 at 4%.
        "proximity": _clamp((4.0 - dist_pct) / 4.0),
        # activity building now. 1.0 at 2x, 0 at 0.7x
        "rvol": _clamp((rvol - 0.70) / 1.30),
        "cvd": cvd_part,
        # Is the move worth the cost? Saturates at 10x - beyond that the
        # extra "edge" is just volatility, and volatility is not free.
        # HEI scored full marks at 64x purely because its ATR was 5.6%
        # per candle, which is chaos rather than opportunity.
        "cost_edge": _clamp((cost_edge - MIN_COST_EDGE) / (10.0 - MIN_COST_EDGE)),
        "book": _clamp(((book_ratio or 1.0) - 1.0) / 1.0),
        "funding": 0.5 if funding is None else _clamp((0.0006 - funding) / 0.0012),
    }
    # Reward a stop you can actually place. Full marks at 1.5% or tighter,
    # zero at MAX_RISK_PCT. A 19% stop is not a scalp.
    risk_now = abs(last - support) / last * 100 if last > 0 else 99.0
    parts["tightness"] = _clamp((MAX_RISK_PCT - risk_now) / (MAX_RISK_PCT - 1.5))

    weights = {"squeeze": 21, "proximity": 16, "rvol": 14, "cvd": 14,
               "cost_edge": 10, "tightness": 12, "book": 8, "funding": 5}
    total = sum(parts[k] * weights[k] for k in weights)
    if already_broken:
        total *= 0.70

    # --- HARD TRADEABILITY GATES ---------------------------------------
    # Structure score is meaningless if you cannot actually take the trade.
    # Each of these disqualifies regardless of how good the setup looks.
    risk_pct = abs(last - support) / last * 100 if last > 0 else 99.0

    blockers = []
    if cost_edge < MIN_COST_EDGE:
        blockers.append(f"cost edge {cost_edge:.1f}x < {MIN_COST_EDGE}x")
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
        "risk_pct": risk_pct, "blockers": blockers,
        "parts": {k: round(parts[k] * weights[k], 1) for k in weights},
        "resistance": resistance, "support": support,
        "already_broken": already_broken, "dist_pct": dist_pct,
        "contraction": contraction, "rvol": rvol, "cvd_share": cvd_share,
        "absorption": absorption, "atr_pct": atr_pct,
        "spread_pct": live_spread if live_spread else spread_pct,
        "cost_edge": cost_edge, "book_ratio": book_ratio,
        "expected_move": expected_move, "round_trip": round_trip,
        "near_depth": near_depth, "funding": funding,
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
         f"<i>{liquid} pairs passed liquidity+spread, {deep} analysed</i>", ""]

    for i, r in enumerate(results, 1):
        p = r["parts"]
        band = ("strong" if r["score"] >= 70 else
                "moderate" if r["score"] >= 55 else
                "weak" if r["score"] >= 40 else "poor")
        L.append(f"<b>{i}. {r['symbol']}</b> — {r['score']}/100 ({band})")
        L.append(f"  price {r['price']:.6g}")
        if r["already_broken"]:
            L.append(f"  ⚠️ already above {r['resistance']:.6g} — move underway")
        else:
            L.append(f"  trigger {r['resistance']:.6g} ({r['dist_pct']:.2f}% away)")
        L.append(f"  stop below {r['support']:.6g} "
                 f"(risk {abs(r['price']-r['support'])/r['price']*100:.2f}%)")
        L.append(f"  squeeze {p['squeeze']:.0f}/24 · near {p['proximity']:.0f}/18 "
                 f"· rvol {p['rvol']:.0f}/16")
        L.append(f"  cvd {p['cvd']:.0f}/16 · cost {p['cost_edge']:.0f}/12 "
                 f"· book {p['book']:.0f}/9 · fund {p['funding']:.0f}/5")
        if r.get("blockers"):
            L.append("  🚫 NOT TRADEABLE — " + "; ".join(r["blockers"]))
        if r["absorption"]:
            L.append("  🔵 buying absorbed on flat price")
        L.append(f"  <i>ATR {r['atr_pct']:.2f}%/15m · exp move ~{r['expected_move']:.2f}% "
                 f"· cost {r['round_trip']:.2f}% · edge {r['cost_edge']:.1f}x</i>")
        L.append(f"  <i>spread {r['spread_pct']:.3f}% (live) · rvol {r['rvol']:.2f}x "
                 f"· cvd {r['cvd_share']:+.1%}</i>")
        if r["near_depth"]:
            L.append(f"  <i>bid depth within 0.5%: ${r['near_depth']:,.0f}</i>")
        L.append("")

    if results and results[0]["score"] < 55:
        L.append("<i>⚠️ Weak field — nothing here is a clean setup.</i>")
    L.append("<i>Intraday structure only, not a prediction. Costs are "
             "modelled at taker fees both sides; limit entries change the "
             "maths. Sizing and risk are yours.</i>")
    return "\n".join(L)


def main():
    try:
        universe = build_universe()
    except GeoBlocked as exc:
        send_telegram(f"⚠️ <b>Scan blocked</b>\n{exc}")
        return 2
    except Exception as exc:
        send_telegram(f"⚠️ <b>Scan failed</b>\n<code>{exc}</code>")
        return 1

    liquid = len(universe)
    picks = shortlist(universe)
    print(f"{liquid} passed liquidity+spread, {len(picks)} shortlisted")
    if not picks:
        send_telegram(
            "\u26a0\ufe0f <b>Scan ran \u2014 quiet market</b>\n"
            f"{liquid} pairs passed liquidity+spread, but none are in the "
            "upper half of their 24h range. Nothing setting up. "
            "This is a real result, not a failure.")
        print("nothing in the upper half of range - sent status message")
        return 0

    results = []
    for c in picks:
        try:
            r = analyse(c["symbol"], c["spread_pct"], c["quote_volume"])
            if r:
                # Keep everything and mark viability rather than dropping it.
                # Silently sending nothing looks identical to a broken cron,
                # so the alert always goes out and says what it found.
                r["tradeable"] = r["cost_edge"] >= MIN_COST_EDGE
                results.append(r)
        except GeoBlocked as exc:
            send_telegram(f"⚠️ <b>Scan blocked</b>\n{exc}")
            return 2
        except Exception as exc:
            print(f"  {c['symbol']}: {exc}", file=sys.stderr)
        time.sleep(0.12)

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
