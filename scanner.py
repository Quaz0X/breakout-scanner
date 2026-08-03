#!/usr/bin/env python3
"""
Hourly Binance alt-coin breakout screener -> Telegram alert.

Scans every actively-trading USDT spot pair on Binance, filters for
liquidity, then deep-analyses the strongest candidates against six
technical criteria. Sends a Telegram message ONLY when something
qualifies (or when a scan fails), so you are not pinged 24 times a day
with "nothing found".

Read-only. Uses public market-data endpoints. No API keys required and
no code path that can place an order.

Environment variables:
    TELEGRAM_BOT_TOKEN   required
    TELEGRAM_CHAT_ID     required
    BINANCE_BASE_URL     optional, default https://api.binance.com
    MIN_QUOTE_VOLUME     optional, default 2000000
    SHORTLIST_SIZE       optional, default 20
    MIN_CRITERIA         optional, default 4
    STATE_FILE           optional, default state.json
    ALERT_COOLDOWN_HOURS optional, default 6
"""

from __future__ import annotations

import json
import os
import sys
import time
from datetime import datetime, timezone

import requests

BASE_URL = os.environ.get("BINANCE_BASE_URL", "https://api.binance.com").rstrip("/")
FAPI_URL = os.environ.get("BINANCE_FAPI_URL", "https://fapi.binance.com").rstrip("/")
BOT_TOKEN = os.environ.get("TELEGRAM_BOT_TOKEN", "").strip()
CHAT_ID = os.environ.get("TELEGRAM_CHAT_ID", "").strip()

MIN_QUOTE_VOLUME = float(os.environ.get("MIN_QUOTE_VOLUME", 2_000_000))
SHORTLIST_SIZE = int(os.environ.get("SHORTLIST_SIZE", 20))
MIN_CRITERIA = int(os.environ.get("MIN_CRITERIA", 4))
STATE_FILE = os.environ.get("STATE_FILE", "state.json")
COOLDOWN_HOURS = float(os.environ.get("ALERT_COOLDOWN_HOURS", 6))

TIMEOUT = 20
SESSION = requests.Session()
SESSION.headers.update({"User-Agent": "breakout-screener/1.0"})

# Excluded from the universe -----------------------------------------------
STABLE_BASES = {
    "USDC", "FDUSD", "TUSD", "BUSD", "DAI", "USDP", "EUR", "GBP", "AEUR",
    "USD1", "PYUSD", "EURI", "XUSD", "USDE",
}
LEVERAGED_MARKERS = ("UPUSDT", "DOWNUSDT", "BULLUSDT", "BEARUSDT")


# --------------------------------------------------------------------------
# HTTP
# --------------------------------------------------------------------------

class GeoBlocked(Exception):
    """Binance returned 451 - the runner's IP is in a Restricted Location."""


def api_get(base: str, path: str, params: dict | None = None, retries: int = 3):
    url = f"{base}{path}"
    last_err = None
    for attempt in range(retries):
        try:
            resp = SESSION.get(url, params=params, timeout=TIMEOUT)
        except requests.RequestException as exc:
            last_err = exc
            time.sleep(2 * (attempt + 1))
            continue

        if resp.status_code == 451:
            raise GeoBlocked(
                "Binance returned HTTP 451 (Unavailable For Legal Reasons). "
                "This runner's IP is in a Restricted Location - see README "
                "for the workarounds."
            )
        if resp.status_code == 429 or resp.status_code == 418:
            wait = int(resp.headers.get("Retry-After", 5 * (attempt + 1)))
            time.sleep(wait)
            last_err = RuntimeError(f"rate limited ({resp.status_code})")
            continue
        if resp.status_code >= 400:
            raise RuntimeError(f"HTTP {resp.status_code} on {path}: {resp.text[:200]}")

        return resp.json()

    raise RuntimeError(f"request failed after {retries} attempts: {last_err}")


# --------------------------------------------------------------------------
# Stage 1-2: build universe, filter by liquidity
# --------------------------------------------------------------------------

def build_universe() -> list[dict]:
    """Every actively trading USDT spot pair, minus stables/leveraged/BTC."""
    info = api_get(BASE_URL, "/api/v3/exchangeInfo")
    valid = set()
    for s in info.get("symbols", []):
        if s.get("status") != "TRADING":
            continue
        if s.get("quoteAsset") != "USDT":
            continue
        base = s.get("baseAsset", "")
        sym = s["symbol"]
        if base in STABLE_BASES or base == "BTC":
            continue
        if any(sym.endswith(m) for m in LEVERAGED_MARKERS):
            continue
        if not s.get("isSpotTradingAllowed", True):
            continue
        valid.add(sym)

    # One call returns 24h stats for every symbol on the exchange.
    all_stats = api_get(BASE_URL, "/api/v3/ticker/24hr")

    out = []
    for t in all_stats:
        if t["symbol"] not in valid:
            continue
        try:
            qv = float(t["quoteVolume"])
            if qv < MIN_QUOTE_VOLUME:
                continue
            high, low = float(t["highPrice"]), float(t["lowPrice"])
            last = float(t["lastPrice"])
            if high <= low or last <= 0:
                continue
            out.append({
                "symbol": t["symbol"],
                "last": last,
                "high24": high,
                "low24": low,
                "change_pct": float(t["priceChangePercent"]),
                "quote_volume": qv,
                # where in the 24h range price sits: 1.0 = at the high
                "range_pos": (last - low) / (high - low),
            })
        except (KeyError, ValueError):
            continue
    return out


def shortlist(candidates: list[dict]) -> list[dict]:
    """Cheap first pass: upper half of 24h range and not falling."""
    primed = [c for c in candidates if c["range_pos"] >= 0.5 and c["change_pct"] > 0]
    primed.sort(key=lambda c: (c["range_pos"], c["change_pct"]), reverse=True)
    return primed[:SHORTLIST_SIZE]


# --------------------------------------------------------------------------
# Stage 3: deep analysis
# --------------------------------------------------------------------------

def get_klines(symbol: str, interval: str, limit: int) -> list[dict]:
    raw = api_get(BASE_URL, "/api/v3/klines",
                  {"symbol": symbol, "interval": interval, "limit": limit})
    return [{
        "open": float(k[1]), "high": float(k[2]), "low": float(k[3]),
        "close": float(k[4]), "volume": float(k[5]),
    } for k in raw]


def get_depth_ratio(symbol: str) -> float | None:
    try:
        d = api_get(BASE_URL, "/api/v3/depth", {"symbol": symbol, "limit": 20})
        bid = sum(float(p) * float(q) for p, q in d.get("bids", []))
        ask = sum(float(p) * float(q) for p, q in d.get("asks", []))
        return (bid / ask) if ask > 0 else None
    except Exception:
        return None


def get_funding(symbol: str) -> float | None:
    try:
        d = api_get(FAPI_URL, "/fapi/v1/premiumIndex", {"symbol": symbol}, retries=1)
        return float(d["lastFundingRate"])
    except Exception:
        return None  # no perpetual for this pair, or fapi unreachable


def analyse(symbol: str) -> dict | None:
    """Score one symbol against the six criteria."""
    c4 = get_klines(symbol, "4h", 90)
    if len(c4) < 40:
        return None

    last = c4[-1]["close"]
    recent, prior = c4[-6:], c4[-36:-6]

    # 1. volatility contraction
    r_recent = sum((k["high"] - k["low"]) / k["close"] for k in recent) / len(recent)
    r_prior = sum((k["high"] - k["low"]) / k["close"] for k in prior) / len(prior)
    contraction = (r_recent / r_prior) if r_prior > 0 else 99.0

    # 2. proximity to resistance
    resistance = max(k["high"] for k in c4[-15:])
    dist_pct = (resistance - last) / last * 100

    # 3. volume holding up
    v_recent = sum(k["volume"] for k in recent) / len(recent)
    v_prior = sum(k["volume"] for k in prior) / len(prior)
    vol_ratio = (v_recent / v_prior) if v_prior > 0 else 0.0

    # 4. order book pressure
    depth = get_depth_ratio(symbol)

    # 5. higher lows across the last 15 candles
    seg = c4[-15:]
    lows = [min(k["low"] for k in seg[i:i + 5]) for i in (0, 5, 10)]
    higher_lows = lows[0] < lows[1] < lows[2]

    # 6. funding not crowded-long
    funding = get_funding(symbol)

    support = min(k["low"] for k in c4[-15:])

    checks = {
        "volatility squeeze": contraction < 0.70,
        "near resistance": dist_pct <= 3.0,
        "volume holding": vol_ratio >= 0.90,
        "bid-heavy book": depth is not None and depth >= 1.20,
        "higher lows": higher_lows,
        "funding not crowded": funding is None or funding <= 0.0005,
    }
    score = sum(1 for v in checks.values() if v)

    return {
        "symbol": symbol,
        "price": last,
        "score": score,
        "checks": checks,
        "resistance": resistance,
        "support": support,
        "contraction": contraction,
        "dist_pct": dist_pct,
        "vol_ratio": vol_ratio,
        "depth": depth,
        "funding": funding,
    }


# --------------------------------------------------------------------------
# State (avoid re-alerting the same coin every hour)
# --------------------------------------------------------------------------

def load_state() -> dict:
    try:
        with open(STATE_FILE) as fh:
            return json.load(fh)
    except Exception:
        return {}


def save_state(state: dict) -> None:
    try:
        with open(STATE_FILE, "w") as fh:
            json.dump(state, fh)
    except Exception as exc:
        print(f"warning: could not write state: {exc}", file=sys.stderr)


def on_cooldown(state: dict, symbol: str, now: float) -> bool:
    last = state.get("alerted", {}).get(symbol)
    return last is not None and (now - last) < COOLDOWN_HOURS * 3600


# --------------------------------------------------------------------------
# Telegram
# --------------------------------------------------------------------------

def send_telegram(text: str) -> None:
    if not BOT_TOKEN or not CHAT_ID:
        print("Telegram not configured; message below:\n" + text)
        return
    url = f"https://api.telegram.org/bot{BOT_TOKEN}/sendMessage"
    resp = SESSION.post(url, timeout=TIMEOUT, json={
        "chat_id": CHAT_ID,
        "text": text,
        "parse_mode": "HTML",
        "disable_web_page_preview": True,
    })
    if resp.status_code >= 400:
        print(f"Telegram error {resp.status_code}: {resp.text[:300]}", file=sys.stderr)


def format_alert(results: list[dict], scanned: int, passed_liquidity: int) -> str:
    stamp = datetime.now(timezone.utc).strftime("%d %b %H:%M UTC")
    lines = [f"<b>Breakout screen</b> — {stamp}",
             f"<i>{passed_liquidity} liquid pairs scanned, {scanned} deep-analysed</i>", ""]

    for r in results:
        hit = [k for k, v in r["checks"].items() if v]
        miss = [k for k, v in r["checks"].items() if not v]
        lines.append(f"<b>{r['symbol']}</b>  ({r['score']}/6)")
        lines.append(f"  price {r['price']:.6g}")
        lines.append(f"  resistance {r['resistance']:.6g} ({r['dist_pct']:.1f}% away)")
        lines.append(f"  invalidation below {r['support']:.6g}")
        lines.append(f"  squeeze {r['contraction']:.2f}x · vol {r['vol_ratio']:.2f}x"
                     + (f" · book {r['depth']:.2f}" if r['depth'] else ""))
        lines.append(f"  ✅ {', '.join(hit)}")
        if miss:
            lines.append(f"  ❌ {', '.join(miss)}")
        lines.append("")

    lines.append("<i>Technical structure only — not advice. "
                 "Setups fail often; size and risk are your call.</i>")
    return "\n".join(lines)


# --------------------------------------------------------------------------

def main() -> int:
    now = time.time()
    try:
        universe = build_universe()
    except GeoBlocked as exc:
        send_telegram(f"⚠️ <b>Scan blocked</b>\n{exc}")
        print(exc, file=sys.stderr)
        return 2
    except Exception as exc:
        send_telegram(f"⚠️ <b>Scan failed</b>\n<code>{exc}</code>")
        print(f"error: {exc}", file=sys.stderr)
        return 1

    passed_liquidity = len(universe)
    picks = shortlist(universe)
    print(f"{passed_liquidity} liquid pairs, {len(picks)} shortlisted")

    if not picks:
        print("nothing in the upper half of its range — no alert sent")
        return 0

    results = []
    for c in picks:
        try:
            r = analyse(c["symbol"])
            if r:
                results.append(r)
        except GeoBlocked as exc:
            send_telegram(f"⚠️ <b>Scan blocked</b>\n{exc}")
            return 2
        except Exception as exc:
            print(f"  {c['symbol']}: {exc}", file=sys.stderr)
        time.sleep(0.15)  # stay well inside Binance rate limits

    qualifying = [r for r in results if r["score"] >= MIN_CRITERIA]
    qualifying.sort(key=lambda r: (r["score"], -r["dist_pct"]), reverse=True)

    state = load_state()
    state.setdefault("alerted", {})
    fresh = [r for r in qualifying if not on_cooldown(state, r["symbol"], now)]

    if fresh:
        top = fresh[:3]
        send_telegram(format_alert(top, len(results), passed_liquidity))
        for r in top:
            state["alerted"][r["symbol"]] = now
        print(f"alerted on: {', '.join(r['symbol'] for r in top)}")
    else:
        best = max((r["score"] for r in results), default=0)
        print(f"no new qualifying setups (best score {best}/6) — no alert sent")

    # prune old cooldown entries
    state["alerted"] = {
        k: v for k, v in state["alerted"].items()
        if now - v < COOLDOWN_HOURS * 3600 * 4
    }
    state["last_run"] = now
    save_state(state)
    return 0


if __name__ == "__main__":
    sys.exit(main())
