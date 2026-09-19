"""
Multi-Coin Paper Trading — Spot-Perp Mean Reversion (Tick-by-Tick WS, Event-Driven)
=================================================================================
ARCHITECTURE:
  Event-driven WS   → runs on real-time Binance @bookTicker (tick-by-tick, 0ms batching)
  On-tick bucketing → rolling mean/std updated reactively when bucket interval elapses (0 sleeping threads)
  Gate check        → runs on every raw WS tick
  Entry fill        → immediate zero-delay execution, VWAP-filled across available book depth
  Exit fill         → immediate zero-delay execution when REVERSION_FRACTION of deviation reverted
  Exit target       → dynamic: exit when abs(curr_dev) <= abs(entry_dev)×(1-REVERSION_FRACTION)
  Timeout exit      → bucket-level and watchdog fallback after max hold
  Net PnL           → VOLUME-WEIGHTED across all coins (by notional traded)
  Sizing            → find_optimal_notional() walks book to find largest profitable size
"""

import json
import threading
import time
import csv
import os
import ssl
import sys
import urllib.request
import certifi
from collections import deque
from datetime import datetime, timezone
import websocket
import numpy as np
from colorama import Fore, Style, init

# Ensure UTF-8 output encoding across all terminals and pipes (Windows compatibility)
if sys.platform == "win32":
    try:
        sys.stdout.reconfigure(encoding="utf-8", errors="replace")
        sys.stderr.reconfigure(encoding="utf-8", errors="replace")
    except Exception:
        pass

init(autoreset=True)

# ══════════════════════════════════════════════════════════════════════════════
# COIN LIST — 47 coins (29 original + 18 from scanner screenshots)
# ══════════════════════════════════════════════════════════════════════════════

# Small- and mid-cap, volatile pairs, the ones that actually carry funding.
# Anything not listed on BOTH Binance spot and USDⓈ-M futures is detected at
# startup by seed_books() and flagged "Not listed"; it simply never trades.
# Every pair below is listed on BOTH Binance spot and USDⓈ-M perpetual futures,
# checked against exchangeInfo on both venues. A coin missing from either leg is
# flagged "Not listed" at startup and simply never trades, so the list is safe to
# grow: the filters decide what is worth trading, not this list.
COINS = [
    # ── Seed set: the cross-exchange funding scan plus hand-picked volatiles ──
    "XTZ", "LSK", "ONE", "AVA",
    "KAT", "SOL", "AVAX", "LINK", "NEAR",
    "SUI", "ADA", "DOGE", "SEI", "INJ", "TIA",
    "OP", "ZK", "STX", "ATOM", "FLOW", "CFX",
    "ASTR", "CELO", "IMX", "THETA", "ICP", "AR",
    "CRV", "LDO", "DYDX", "PENDLE", "EIGEN", "MORPHO",
    "API3", "COMP", "CAKE", "RENDER", "FET", "TAO",
    "ONDO", "PYTH", "JUP", "JTO", "WLD", "RSR",
    "BAND", "SKY", "WIF", "BOME", "PENGU", "NEIRO",
    "HMSTR", "TRUMP", "ORDI", "ARKM", "BLUR", "KAITO",
    "VIRTUAL", "GALA", "AXS", "SAND", "CHZ", "YGG",
    "ALICE", "APE", "ENJ", "SFP", "BICO", "LPT",

    # ── Widened to the rest of the liquid book, ranked by futures turnover ────
    # Deeper books mean a trade can actually be sized; the entry gates still
    # reject anything whose funding does not cover its own spread.
    "ETH", "BTC", "ZEC", "XRP", "UNI", "ENA",
    "G", "BNB", "FIL", "ZAMA", "ARB", "STRK",
    "BCH", "APT", "AAVE", "F", "LTC", "PUMP",
    "SYN", "XLM", "DASH", "ASTER", "牛来", "BANK",
    "DOT", "MARSCOIN", "ZEN", "HBAR", "COTI", "TRX",
    "ETC", "XPL", "POL", "HEI", "SAGA", "EPIC",
    "AERO", "ETHFI", "C", "ESP", "VET", "ZRO",
    "IOST", "STG", "WLFI", "CHIP", "GENIUS", "ALLO",
    "PROM", "PAXG", "MINA", "ALGO", "BERA", "EGLD",
    "VTHO", "GRAM", "RED", "MET", "ACE", "TUT",
    "0G", "HOME", "HEMI", "S", "ROBO", "LA",
    "SOPH", "MITO", "PEOPLE", "ONG", "CELR", "SOLV",
    "SUSHI", "CVC", "ZIL", "GIGGLE", "JST", "RE",
    "XAUT", "REZ", "ENS", "TREE", "SUPER", "DEXE",
    "KAVA", "BIO", "ARK", "MANA", "NIL", "ACH",
    "MMT", "T", "FF", "MEGA", "PROVE", "SKL",
    "IOTA", "TRB", "CATI", "PNUT",
]

# ══════════════════════════════════════════════════════════════════════════════
# CONFIG
# ══════════════════════════════════════════════════════════════════════════════

BUCKET_SIZE_SEC    = 0.5
ROLLING_WIN        = int(os.environ.get("ROLLING_WIN_OVERRIDE", 1000))  # 1000 x 0.5s = 500s default; override via env var for sweeps

# Adaptive bucketing for low-tick coins
# Coins not in this map use BUCKET_SIZE_SEC (0.5s) and ROLLING_WIN (1000)
# Wider bucket = fewer buckets needed but same ~500s total window
BUCKET_TIERS = {
    # (bucket_sec, rolling_win) — rolling_win chosen to keep window ~500s
    # Derived from ROLLING_WIN so ROLLING_WIN_OVERRIDE actually takes effect
    # (defaults are unchanged: 1000 / 250 / 100).
    "fast"   : (0.5,  ROLLING_WIN),                   # default  — liquid coins  >4 ticks/s
    "medium" : (2.0,  max(5, ROLLING_WIN // 4)),      # mid-tier — 1-4 ticks/s
    "slow"   : (5.0,  max(5, ROLLING_WIN // 10)),     # slow     — <1 tick/s
}
# Coins that need wider buckets (identified by low tick rate in practice)
MEDIUM_TIER = {"ZEC", "VIRTUAL", "HBAR", "PIXEL", "DOT", "JTO",
               "PENGU", "NEIRO", "ALLO", "RE", "MEGA", "SEI", "SAND"}
SLOW_TIER   = {"PAXG", "XAUT", "USDC", "L"}

def get_tier(symbol):
    if symbol in SLOW_TIER:
        return BUCKET_TIERS["slow"]
    if symbol in MEDIUM_TIER:
        return BUCKET_TIERS["medium"]
    return BUCKET_TIERS["fast"]
SD_THRESHOLD       = 2.0

# ── Fee tiers ─────────────────────────────────────────────────────────────────
# (spot_maker, spot_taker, perp_maker, perp_taker) in percent, per execution.
# ZERO is the presentation default: bid-ask spread is then the only cost.
FEE_TIERS = {
    "ZERO": (0.0000, 0.0000, 0.0000, 0.0000),
    "VIP0": (0.1000, 0.1000, 0.0200, 0.0500),
    "VIP1": (0.0900, 0.1000, 0.0160, 0.0400),
    "VIP2": (0.0800, 0.1000, 0.0140, 0.0350),
    "VIP3": (0.0420, 0.0600, 0.0120, 0.0320),
    "VIP4": (0.0420, 0.0540, 0.0100, 0.0300),
    "VIP5": (0.0360, 0.0480, 0.0080, 0.0270),
    "VIP6": (0.0300, 0.0420, 0.0060, 0.0250),
    "VIP7": (0.0240, 0.0360, 0.0040, 0.0220),
    "VIP8": (0.0180, 0.0300, 0.0020, 0.0200),
    "VIP9": (0.0120, 0.0240, 0.0000, 0.0170),
}
FEE_TIER = os.environ.get("FEE_TIER", "ZERO").strip().upper()
if FEE_TIER not in FEE_TIERS:
    FEE_TIER = "ZERO"

# Which assumption the pre-trade gates price in. Fills are maker-first, so "maker"
# is the matching expectation; "taker" gates conservatively on the worst case.
FEE_GATE_MODE = os.environ.get("FEE_GATE_MODE", "maker").strip().lower()

def set_fee_tier(tier):
    """Swap the fee tier at runtime (dashboard/stage toggle). Returns the active tier."""
    global FEE_TIER
    t = str(tier).strip().upper()
    if t in FEE_TIERS:
        FEE_TIER = t
    return FEE_TIER

def leg_fee_pct(leg, fill_type):
    """Fee in percent for one execution on one leg."""
    sm, st, pm, pt = FEE_TIERS[FEE_TIER]
    if leg == "spot":
        return sm if fill_type == "maker" else st
    return pm if fill_type == "maker" else pt

def round_trip_fee_pct(fill_type=None):
    """Entry + exit on both legs = 4 executions."""
    ft = fill_type or ("maker" if FEE_GATE_MODE == "maker" else "taker")
    return 2 * (leg_fee_pct("spot", ft) + leg_fee_pct("perp", ft))

# Kept for display only, the live number comes from round_trip_fee_pct().
EXCHANGE_FEE_PCT   = round_trip_fee_pct()

# ── Execution ─────────────────────────────────────────────────────────────────
# Both legs fill immediately after ENTRY_DELAY_SEC / EXIT_DELAY_SEC at whatever
# the book shows, and the fee charged is this, chosen rather than simulated.
#
# The engine used to race for a passive fill: post at the touch, wait, take if
# nobody came. That wait is what turned an LSK stop sized at 0.22% into a 3.16%
# loss, because the perp bid fell 3.4% while we sat there hoping to save a fee.
# Assuming the fee is both simpler and safer, at the cost of being optimistic:
# a real resting order is not guaranteed to fill. Set FILL_FEE_TYPE=taker for
# the conservative view. Every trade records which was used.
FILL_FEE_TYPE      = os.environ.get("FILL_FEE_TYPE", "maker").strip().lower()
if FILL_FEE_TYPE not in ("maker", "taker"):
    FILL_FEE_TYPE = "maker"

REVERSION_FRACTION = 0.90    # exit when this fraction of entry deviation has reverted
                                    # 0.90 = captures borderline trades just above friction
                                    # tune range: 0.85 (faster exit) ↔ 0.95 (max profit, slower)
MIN_NET_PCT        = 0.001   # fallback exit for tiny entries: if 90% reversion still
                                    # can't cover friction, exit at this minimum net profit
MAX_HOLD_SEC       = float(os.environ.get("MAX_HOLD_SEC", 180.0))
POST_RECONNECT_COOLDOWN_SEC = 10.0  # block new entries for N seconds after any reconnect

# ── Funding capture ───────────────────────────────────────────────────────────
# STRATEGY: "spread" = original 2σ mean reversion only
#           "funding" = delta-neutral funding capture only
#           "both" = funding when a stamp is in range, spread otherwise
STRATEGY                  = os.environ.get("STRATEGY", "funding").strip().lower()
# How far ahead of a payment we will enter, as a fraction of that coin's own
# funding cycle. A fixed number of minutes was wildly uneven: intervals here are
# 1h, 4h and 8h, so one hour covered an hourly coin's entire cycle while
# excluding a 4h coin for three quarters of its own, and it refused genuinely
# good setups purely for settling later in the day. The APR gate already prices
# the wait, so this only stops capital being parked a whole cycle early.
FUNDING_ENTRY_WINDOW_FRAC = float(os.environ.get("FUNDING_ENTRY_WINDOW_FRAC", 1.0))
# Absolute ceiling on top of that, for the rare very long interval
FUNDING_ENTRY_WINDOW_SEC  = float(os.environ.get("FUNDING_ENTRY_WINDOW_SEC", 8 * 3600.0))
# Minimum |funding rate| worth entering for, in percent per interval
MIN_FUNDING_PCT           = float(os.environ.get("MIN_FUNDING_PCT", 0.0050))
# Require expected edge to beat friction by this multiple (kills penny trades)
EDGE_FRICTION_MULT        = float(os.environ.get("EDGE_FRICTION_MULT", 1.5))
# Minimum annualised return on capital for a funding trade to be worth the hold
MIN_FUNDING_APR           = float(os.environ.get("MIN_FUNDING_APR", 12.0))
# Fallback only: how long to keep holding after a stamp when the funding feed has
# gone quiet and we cannot re-evaluate. Normally the decision is remade at every
# stamp instead of running down a clock.
FUNDING_EXIT_GRACE_SEC    = float(os.environ.get("FUNDING_EXIT_GRACE_SEC", 900.0))
# Safety ceiling on a funding hold. Not a strategy parameter: it only catches a
# position that has somehow stopped being re-evaluated.
FUNDING_MAX_HOLD_SEC      = float(os.environ.get("FUNDING_MAX_HOLD_SEC", 6 * 3600.0))
DEFAULT_FUNDING_INTERVAL_H = 8.0
# Stop loss, expressed as a multiple of the funding we entered to collect, so it
# scales with the trade: rates across these pairs span two orders of magnitude,
# and a fixed percentage would be far too loose on one coin and too tight on another.
STOP_LOSS_FUNDING_MULT    = float(os.environ.get("STOP_LOSS_FUNDING_MULT", 2.0))
# Floor for the stop, for the case where funding is tiny
STOP_LOSS_MIN_PCT         = float(os.environ.get("STOP_LOSS_MIN_PCT", 0.05))
# After a stop, wait before re-entering the same coin. Without this the engine
# reopens the identical setup on the very next tick and loops on it.
STOP_COOLDOWN_SEC         = float(os.environ.get("STOP_COOLDOWN_SEC", 120.0))
# Below this, an entry deviation is noise and "convergence" is not a meaningful exit
MIN_CONVERGENCE_DEV_PCT   = float(os.environ.get("MIN_CONVERGENCE_DEV_PCT", 0.005))
# Skip coins whose gap routinely travels further than funding could ever pay for.
# The stop sits at 2x the funding we are waiting to collect, so if that distance
# is only a fraction of how far this gap normally moves, the stop is inside the
# coin's ordinary noise and will be hit before the payment arrives. Expressed in
# standard deviations of the coin's own basis: the stop must be at least this
# many away to be worth entering at all.
MIN_STOP_SIGMAS           = float(os.environ.get("MIN_STOP_SIGMAS", 2.0))
# Enough buckets to estimate that volatility without waiting for the full window
MIN_VOL_SAMPLES           = int(os.environ.get("MIN_VOL_SAMPLES", 30))

# Latency simulation: 1ms by default so a fill never lands on the signal tick itself
ENTRY_DELAY_SEC    = float(os.environ.get("ENTRY_DELAY_SEC", 0.001))
EXIT_DELAY_SEC     = float(os.environ.get("EXIT_DELAY_SEC", 0.001))

# Stream mode: "bookTicker" (real-time tick-by-tick, 0ms buffer) or "depth20" (100ms snapshot)
STREAM_TYPE        = os.environ.get("STREAM_TYPE", "bookTicker").strip()

# ── Dynamic sizing ────────────────────────────────────────────────────────────
MIN_NOTIONAL_USD   = 10.0        # never enter below this size
# Cap per trade. Without one, sizing off the whole visible book put $213k into a
# single BTC position while an illiquid alt got $15, so one coin's noise drowned
# out every other result. A flat cap keeps positions comparable.
MAX_NOTIONAL_USD   = float(os.environ.get("MAX_NOTIONAL_USD", 1000.0))
NOTIONAL_STEPS     = 50          # increased steps for finer granularity across wider range
DEPTH_STREAM_MS    = 100         # depth update rate if using depth stream: 100ms or 250ms

_OUTPUT_SUBDIR     = os.environ.get("OUTPUT_SUBDIR", "multi_coin")
_PROJECT_ROOT      = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
OUTPUT_DIR         = os.environ.get("OUTPUT_DIR") or os.path.join(_PROJECT_ROOT, "data", _OUTPUT_SUBDIR)
HOLD_TICKER        = os.environ.get("HOLD_TICKER", "0") == "1"   # per-tick "hold=..." console line (noisy)
STATS_INTERVAL     = 10          # seconds between summary prints

# ══════════════════════════════════════════════════════════════════════════════
# GLOBAL AGGREGATOR — volume-weighted PnL across all coins
# ══════════════════════════════════════════════════════════════════════════════

global_lock = threading.Lock()
global_stats = {
    "total_closed"        : 0,
    "total_profit"        : 0,
    "total_loss"          : 0,
    "sum_net_pnl_x_notl"  : 0.0,   # Σ (net_pnl_pct × notional)
    "sum_notional"         : 0.0,   # Σ notional
    "total_net_pnl_usd"   : 0.0,
}

def record_global_trade(net_pnl_pct, notional_usd, net_pnl_usd):
    with global_lock:
        global_stats["total_closed"]       += 1
        global_stats["sum_net_pnl_x_notl"] += net_pnl_pct * notional_usd
        global_stats["sum_notional"]        += notional_usd
        global_stats["total_net_pnl_usd"]  += net_pnl_usd
        if net_pnl_pct >= 0:
            global_stats["total_profit"] += 1
        else:
            global_stats["total_loss"]   += 1

def get_vwap_net_pnl_pct():
    """Volume-weighted average net PnL% across all closed trades."""
    with global_lock:
        if global_stats["sum_notional"] == 0:
            return 0.0
        return global_stats["sum_net_pnl_x_notl"] / global_stats["sum_notional"]

# ══════════════════════════════════════════════════════════════════════════════
# RUNTIME CONTROL + TRADE FEED (used by the dashboard API)
# ══════════════════════════════════════════════════════════════════════════════

ENTRIES_ENABLED = threading.Event()   # cleared → no NEW entries (open positions still exit)
ENTRIES_ENABLED.set()

ALL_CS       = []      # populated by start_engine()
ENGINE_START = None
DEMO_MODE    = False   # True when ticks come from backend/demo.py, not Binance

TRADE_LOG    = deque(maxlen=5000)     # closed trades, newest last
_trade_lock  = threading.Lock()
_trade_seq   = 0

def _exit_type(reason):
    r = reason.upper()
    if r.startswith("REVERSION"):  return "reversion"
    if r.startswith("MIN PROFIT"): return "min-profit"
    if r.startswith("TIMEOUT"):    return "timeout"
    if r.startswith("WATCHDOG"):   return "watchdog"
    if r.startswith("STOP LOSS"):  return "stop-loss"
    if r.startswith("FUNDING"):    return "funding"
    if r.startswith("NOT WORTH"): return "not-worth-holding"
    return "other"

def record_trade_event(ev):
    global _trade_seq
    with _trade_lock:
        _trade_seq += 1
        TRADE_LOG.append(dict(ev, seq=_trade_seq))

def trades_since(seq=0, limit=500):
    """Closed trades with seq > `seq` (oldest→newest, capped at the newest `limit`) + head seq."""
    with _trade_lock:
        rows = [t for t in TRADE_LOG if t["seq"] > seq]
        head = _trade_seq
    return rows[-limit:], head

# ══════════════════════════════════════════════════════════════════════════════
# FEED HEALTH — one entry per WebSocket connection (dashboard + quote freshness)
# ══════════════════════════════════════════════════════════════════════════════
# bookTicker only pushes when a coin's top of book changes, so a quiet coin on a
# live connection still holds the current quote. Freshness is therefore judged per
# connection: while a connection keeps delivering messages (in order, over TCP),
# every coin on it that has a quote from this connection session is up to date.

FEEDS            = {}     # "spot-ws-0" -> health dict, created by run_combined_ws
FEED_SILENCE_SEC = 10.0   # a connected feed that delivers nothing for this long is dead → reopen it
CLOCK_OFFSET_MS  = 0.0    # Binance server time − local time, for the perp event-time lag
UNLISTED         = {"spot": set(), "perp": set()}   # symbols a venue doesn't list (found by the REST seed)
REST_BOOK_URL    = {"spot": "https://api.binance.com/api/v3/ticker/bookTicker",
                    "perp": "https://fapi.binance.com/fapi/v1/ticker/bookTicker"}
_SSL_CTX         = ssl.create_default_context(cafile=certifi.where())

def _rest_get(url, timeout=10):
    with urllib.request.urlopen(url, context=_SSL_CTX, timeout=timeout) as r:
        return json.load(r)

def sync_clock():
    """Measure the offset to Binance server time so perp feed lag isn't skewed by the local clock."""
    global CLOCK_OFFSET_MS
    try:
        t0 = time.time()
        server_ms = _rest_get("https://fapi.binance.com/fapi/v1/time", timeout=5)["serverTime"]
        t1 = time.time()
        CLOCK_OFFSET_MS = server_ms - (t0 + t1) / 2 * 1000
    except Exception as e:
        print(f"{Fore.YELLOW}clock sync failed ({e}) — perp lag shown against the local clock{Style.RESET_ALL}")

def seed_books(leg, coin_map, opened_at):
    """Fill the top of book from REST for coins with no tick yet on this connection.
    A quiet coin otherwise has no quote until its book next changes (can be minutes).
    Seeds only the book — no tick processing, so no gate check runs on REST data."""
    try:
        rows = _rest_get(REST_BOOK_URL[leg])
    except Exception as e:
        print(f"{Fore.YELLOW}⚠️  {leg} book seed failed: {e} — quiet coins wait for their first tick{Style.RESET_ALL}")
        return
    by_sym = {r["symbol"].lower(): r for r in rows}
    now = time.time()
    unlisted = []
    for sym, cs in coin_map.items():
        r = by_sym.get(sym)
        try:
            b, bq = float(r["bidPrice"]), float(r["bidQty"])
            a, aq = float(r["askPrice"]), float(r["askQty"])
        except (TypeError, KeyError, ValueError):
            b = a = 0.0
        if b <= 0 or a <= 0:                      # not listed / not trading on this venue
            UNLISTED[leg].add(cs.symbol)
            unlisted.append(cs.symbol)
            continue
        UNLISTED[leg].discard(cs.symbol)
        with cs.lock:
            last = getattr(cs, f"{leg}_last_tick")
            if last is not None and last >= opened_at:
                continue                          # a live tick already arrived — newer than this snapshot
            cs.latest[f"{leg}_bids"] = [[b, bq]]
            cs.latest[f"{leg}_asks"] = [[a, aq]]
            cs.latest[f"{leg}_ts"]   = now
            setattr(cs, f"{leg}_last_tick", now)
    if unlisted:
        print(f"{Fore.YELLOW}⚠️  Not listed on {leg}: {' '.join(unlisted)} — these coins can't trade{Style.RESET_ALL}")

def check_feeds(now, prev_msgs):
    """One pass over every feed: message rate + lag for the dashboard, reopen a feed that
    has gone silent, and keep quiet coins' quotes current while their feed is live."""
    for name, f in list(FEEDS.items()):
        n = f["msgs"]
        f["rate"] = n - prev_msgs.get(name, n)
        prev_msgs[name] = n
        if f["lag_n"]:
            f["lag_ms"], f["lag_max_ms"] = f["lag_sum"] / f["lag_n"], f["lag_max"]
            f["lag_sum"], f["lag_n"], f["lag_max"] = 0.0, 0, 0.0
        if f["status"] != "connected" or f["opened_at"] is None:
            continue
        last = f["last_msg"] or f["opened_at"]
        if now - last > FEED_SILENCE_SEC:
            print(f"{Fore.RED}⚠️  {name}: no data for {now - last:.0f}s — reopening{Style.RESET_ALL}")
            f["last_error"], f["last_error_at"] = f"no data for {now - last:.0f}s", now
            ws = f.get("ws")
            if ws is not None:                    # close() can wait on a dead socket; don't block this loop
                threading.Thread(target=ws.close, daemon=True).start()
            continue
        if f["last_msg"] is None:
            continue
        if not f.get("quotes", True):
            continue          # funding feed carries no order book, nothing to freshen
        # every message up to last_msg has been handled, in order, so a coin whose quote came
        # in on this connection session and hasn't changed since is still current as of last_msg
        ts_key, tick_attr, since, fresh = f"{f['leg']}_ts", f"{f['leg']}_last_tick", f["opened_at"], f["last_msg"]
        for cs in f["cs"]:
            with cs.lock:
                lt = getattr(cs, tick_attr)
                if lt is not None and lt >= since and (cs.latest[ts_key] or 0) < fresh:
                    cs.latest[ts_key] = fresh

def feed_monitor():
    sync_clock()
    last_sync = time.time()
    prev_msgs = {}
    while True:
        time.sleep(1.0)
        now = time.time()
        if now - last_sync > 600:
            sync_clock()
            last_sync = now
        try:
            check_feeds(now, prev_msgs)
        except Exception as e:                    # never let the monitor die
            print(f"{Fore.RED}[feed-monitor] {type(e).__name__}: {e}{Style.RESET_ALL}")

# ══════════════════════════════════════════════════════════════════════════════
# FUNDING FEED, one connection carries the funding rate + next stamp for every
# symbol on USDⓈ-M futures (!markPrice@arr). Funding pays to whoever holds the
# position AT the stamp, so both the rate and its countdown drive entry timing.
# ══════════════════════════════════════════════════════════════════════════════

# Funding comes over REST, not WebSocket. The documented !markPrice@arr stream
# opens cleanly here but never delivers a frame, verified against a bookTicker
# control on the same socket (2280 messages vs 0 in six seconds), including when
# both are subscribed together. premiumIndex returns the rate and the next stamp
# for every symbol in a single unauthenticated call, so one poll covers the book.
FUNDING_REST_URL = "https://fapi.binance.com/fapi/v1/premiumIndex"
FUNDING_INFO_URL = "https://fapi.binance.com/fapi/v1/fundingInfo"
FUNDING_POLL_SEC = float(os.environ.get("FUNDING_POLL_SEC", 5.0))

def fetch_funding_intervals(all_cs):
    """Most pairs settle every 8h, but many volatile ones run 4h, which doubles
    how often they pay. fundingInfo only lists symbols with non-default settings,
    so default to 8h and override whatever it returns."""
    by_symbol = {f"{cs.symbol.upper()}USDT": cs for cs in all_cs}
    try:
        rows = _rest_get(FUNDING_INFO_URL)
    except Exception as e:
        print(f"{Fore.YELLOW}⚠️  fundingInfo fetch failed ({e}), assuming {DEFAULT_FUNDING_INTERVAL_H:.0f}h "
              f"for every pair{Style.RESET_ALL}")
        return
    adjusted = []
    for r in rows:
        cs = by_symbol.get(r.get("symbol", ""))
        if cs is None:
            continue
        try:
            hours = float(r.get("fundingIntervalHours", DEFAULT_FUNDING_INTERVAL_H))
        except (TypeError, ValueError):
            continue
        if hours and hours != DEFAULT_FUNDING_INTERVAL_H:
            with cs.lock:
                cs.funding_interval_h = hours
            adjusted.append(f"{cs.symbol}({hours:.0f}h)")
    if adjusted:
        print(f"{Fore.CYAN}ℹ️  Non-8h funding intervals: {' '.join(adjusted)}{Style.RESET_ALL}")

def run_funding_poller(all_cs):
    """Poll premiumIndex for the funding rate and next stamp of every symbol.

    One call covers the whole book, so this stays far inside the futures weight
    budget even at a few seconds per poll. Registered in FEEDS so the dashboard
    shows it alongside the book connections.
    """
    by_symbol = {f"{cs.symbol.upper()}USDT": cs for cs in all_cs}
    name = "funding-rest"
    feed = FEEDS.setdefault(name, {
        "name": name, "leg": "funding", "cs": list(all_cs),
        "coins": [cs.symbol for cs in all_cs], "quotes": False,
        "status": "init", "opened_at": None, "last_msg": None, "msgs": 0, "rate": 0,
        "connects": 0, "errors": 0, "last_error": None, "last_error_at": None,
        "lag_ms": None, "lag_max_ms": None, "lag_sum": 0.0, "lag_n": 0, "lag_max": 0.0, "ws": None,
    })

    backoff = FUNDING_POLL_SEC
    first   = True
    while True:
        try:
            rows = _rest_get(FUNDING_REST_URL)
            now  = time.time()
            if feed["opened_at"] is None:
                feed["opened_at"] = now
                feed["connects"] += 1
            feed["status"], feed["last_msg"] = "connected", now
            feed["msgs"] += 1

            matched = 0
            for r in rows:
                cs = by_symbol.get(r.get("symbol", ""))
                if cs is None:
                    continue
                try:
                    rate = float(r["lastFundingRate"])
                    nxt  = int(r["nextFundingTime"])
                    mark = float(r["markPrice"])
                except (KeyError, TypeError, ValueError):
                    continue
                with cs.lock:
                    cs.funding_rate    = rate
                    cs.next_funding_ms = nxt
                    cs.mark_price      = mark
                    cs.funding_ts      = now
                matched += 1

            if first:
                print(f"{Fore.GREEN}\u2705 funding-rest live \u2014 {matched}/{len(all_cs)} coins "
                      f"(premiumIndex every {FUNDING_POLL_SEC:.0f}s){Style.RESET_ALL}")
                first = False
            backoff = FUNDING_POLL_SEC
        except Exception as e:
            feed["status"] = "retrying"
            feed["errors"] += 1
            feed["last_error"], feed["last_error_at"] = str(e)[:160], time.time()
            print(f"{Fore.YELLOW}\u26a0\ufe0f  funding-rest poll failed: {e}{Style.RESET_ALL}")
            backoff = min(backoff * 2, 30.0)
        time.sleep(backoff)

def funding_view(cs):
    """(rate_pct_per_interval, seconds_to_stamp, interval_hours) or None if we
    have no funding data for this coin yet."""
    with cs.lock:
        rate, nxt, ts, interval = cs.funding_rate, cs.next_funding_ms, cs.funding_ts, cs.funding_interval_h
    if rate is None or nxt is None or ts is None:
        return None
    if time.time() - ts > 60:          # funding feed has gone quiet, don't trust it
        return None
    return rate * 100.0, (nxt / 1000.0 - time.time()), interval

# ══════════════════════════════════════════════════════════════════════════════
# PER-COIN STATE
# ══════════════════════════════════════════════════════════════════════════════

class CoinState:
    def __init__(self, symbol):
        self.symbol       = symbol
        self.lock         = threading.Lock()
        self.bucket_sec, self.rolling_win = get_tier(symbol)
        self.last_bucket_ts = 0.0  # Tracks timestamp of the most recent bucket processing
        self.bucket_lock    = threading.Lock()   # spot + perp WS threads must not run process_bucket concurrently

        self.latest = {
            # L20 order book — list of [price, qty] sorted best→worst
            "spot_bids": [], "spot_asks": [], "spot_ts": None,
            "perp_bids": [], "perp_asks": [], "perp_ts": None,
        }

        # ── Funding state (fed by the !markPrice@arr stream) ─────────────────
        self.funding_rate       = None   # decimal per interval: 0.0001 = 0.01%
        self.next_funding_ms    = None   # ms epoch of the next funding stamp
        self.funding_ts         = None   # when we last heard a funding update
        self.funding_interval_h = DEFAULT_FUNDING_INTERVAL_H
        self.mark_price         = None

        self.buckets       = deque(maxlen=self.rolling_win + 10)
        self.slip_history  = deque(maxlen=500)
        self.spot_ba_hist  = deque(maxlen=self.rolling_win)
        self.perp_ba_hist  = deque(maxlen=self.rolling_win)
        self.spot_twap_buf = []
        self.perp_twap_buf = []

        self.stats = {
            "total_updates"    : 0,
            "total_buckets"    : 0,
            "signals_detected" : 0,
            "blocked_gate2"    : 0,
            "blocked_gate3"    : 0,
            "signals_fired"    : 0,
            "trades_closed"    : 0,
            "trades_profit"    : 0,
            "trades_loss"      : 0,
            "total_net_pnl_pct": 0.0,
            "total_net_pnl_usd": 0.0,
            "start_time"       : time.time(),
            "slip_buffer"      : 0.0,
            "bucket_index"     : 0,
            "live_round_trip"  : 0.0,
        }

        self.bucket_signal = {
            "roll_mean" : None, "roll_std"   : None,
            "upper"     : None, "lower"      : None,
            "round_trip": None, "min_dev"    : None,
            "ready"     : False,
        }

        self.open_position = None
        self.entry_pending = False
        self.exit_pending  = False

        # WS diagnostics
        self.spot_ws_status = "init"   # init | connected | error | retrying
        self.perp_ws_status = "init"
        self.spot_ws_errors = 0
        self.perp_ws_errors = 0
        self.spot_last_tick = None
        self.perp_last_tick = None

        # Reconnect tracking — time each leg went offline (None = never disconnected)
        self.spot_disconnect_time = None
        self.perp_disconnect_time = None
        # Timestamp of the most recent reconnect (spot or perp) — used to block
        # new entries for POST_RECONNECT_COOLDOWN_SEC after any stream comes back
        self.last_reconnect_time  = None
        # when this coin was last stopped out, for the re-entry cooldown
        self.last_stop_time       = None

        # csv_path removed — all trades written to single master CSV

# ── book helpers ──────────────────────────────────────────────────────────────

def _best_bid(snap, leg):
    lvls = snap[f"{leg}_bids"]
    return lvls[0][0] if lvls else None

def _best_ask(snap, leg):
    lvls = snap[f"{leg}_asks"]
    return lvls[0][0] if lvls else None

def _mid(snap, leg):
    b = _best_bid(snap, leg)
    a = _best_ask(snap, leg)
    if b is None or a is None:
        return None
    return (b + a) / 2.0

def vwap_fill(levels, target_qty):
    """Walk order-book levels to fill target_qty.
    levels : [[price, qty], ...] sorted best→worst
    Returns: (avg_fill_price, filled_qty, fully_filled)
    """
    remaining = target_qty
    cost      = 0.0
    filled    = 0.0
    for price, qty in levels:
        take       = min(qty, remaining)
        cost      += take * price
        filled    += take
        remaining -= take
        if remaining <= 0:
            break
    if filled == 0:
        return None, 0.0, False
    return cost / filled, filled, (remaining <= 1e-12)

def find_optimal_notional(snap, direction, roll_mean, round_trip_pct):
    """Walk increasing notional sizes across the L20 book.
    Returns (best_notional, vwap_spot_fill, vwap_perp_fill, entry_slip_pct).
    entry_slip_pct = combined slippage cost both legs vs mid (always >= 0).
    Returns (0, None, None, 0) when no profitable size exists.
    """
    if direction == +1:
        spot_levels = snap["spot_asks"]   # buying spot  → walk asks
        perp_levels = snap["perp_bids"]   # selling perp → walk bids
    else:
        spot_levels = snap["spot_bids"]   # selling spot → walk bids
        perp_levels = snap["perp_asks"]   # buying perp  → walk asks

    if not spot_levels or not perp_levels:
        return 0.0, None, None, 0.0

    spot_mid_ref = _mid(snap, "spot")
    perp_mid_ref = _mid(snap, "perp")
    if spot_mid_ref is None or perp_mid_ref is None:
        return 0.0, None, None, 0.0

    # Total liquidity available across up to 20 levels
    spot_total_usd = sum(p * q for p, q in spot_levels)
    perp_total_usd = sum(p * q for p, q in perp_levels)
    # Bounded by what both sides can actually fill, and by the per-trade cap
    max_available  = min(spot_total_usd, perp_total_usd, MAX_NOTIONAL_USD)

    if max_available < MIN_NOTIONAL_USD:
        return 0.0, None, None, 0.0

    best_notional  = 0.0
    best_spot_fill = None
    best_perp_fill = None
    best_slip      = 0.0

    step = (max_available - MIN_NOTIONAL_USD) / NOTIONAL_STEPS

    for i in range(NOTIONAL_STEPS + 1):
        notional = MIN_NOTIONAL_USD + step * i

        spot_ref_price = spot_levels[0][0]
        perp_ref_price = perp_levels[0][0]
        spot_qty = notional / spot_ref_price
        perp_qty = notional / perp_ref_price

        spot_fill, _, spot_ok = vwap_fill(spot_levels, spot_qty)
        perp_fill, _, perp_ok = vwap_fill(perp_levels, perp_qty)

        if spot_fill is None or perp_fill is None:
            break   # ran out of book depth — larger sizes only worse
        # Note: perp_ok may be False (bookTicker 1-level, partial fill) — still
        # use the fill price since it's the true best bid/ask

        # Slippage cost = how much worse than mid we filled on each leg
        if direction == +1:
            spot_slip_pct = (spot_fill - spot_mid_ref) / spot_mid_ref * 100  # paying above mid
            perp_slip_pct = (perp_mid_ref - perp_fill) / perp_mid_ref * 100  # receiving below mid
        else:
            spot_slip_pct = (spot_mid_ref - spot_fill) / spot_mid_ref * 100
            perp_slip_pct = (perp_fill - perp_mid_ref) / perp_mid_ref * 100

        total_slip_pct = max(0.0, spot_slip_pct) + max(0.0, perp_slip_pct)

        # Deviation available to capture at these fills
        fill_spread    = (perp_fill - spot_fill) / spot_fill * 100
        fill_deviation = fill_spread - roll_mean
        abs_fill_dev   = abs(fill_deviation)

        # Total cost to enter+exit: fees + entry slippage (exit slip estimated same)
        total_cost = round_trip_pct + total_slip_pct

        if abs_fill_dev > total_cost:
            best_notional  = notional
            best_spot_fill = spot_fill
            best_perp_fill = perp_fill
            best_slip      = total_slip_pct
        else:
            break   # deeper sizes only increase slip — stop searching

    return best_notional, best_spot_fill, best_perp_fill, best_slip

def get_exit_vwap(snap, direction, notional):
    """VWAP exit fill prices for a given notional."""
    if direction == +1:
        spot_levels = snap["spot_bids"]   # closing long spot  → sell into bids
        perp_levels = snap["perp_asks"]   # closing short perp → buy asks
    else:
        spot_levels = snap["spot_asks"]
        perp_levels = snap["perp_bids"]

    if not spot_levels or not perp_levels:
        return None, None

    spot_ref = spot_levels[0][0]
    perp_ref = perp_levels[0][0]
    spot_qty = notional / spot_ref
    perp_qty = notional / perp_ref

    spot_fill, _, _ = vwap_fill(spot_levels, spot_qty)
    perp_fill, _, _ = vwap_fill(perp_levels, perp_qty)
    return spot_fill, perp_fill

def _funding_notional(snap, direction):
    """Size a funding trade to everything both legs can actually fill.
    Funding pays on notional, so the spread doesn't have to be profitable on its
    own, take all the liquidity that's there."""
    if direction == +1:
        spot_levels, perp_levels = snap["spot_asks"], snap["perp_bids"]
    else:
        spot_levels, perp_levels = snap["spot_bids"], snap["perp_asks"]
    if not spot_levels or not perp_levels:
        return 0.0
    available = min(sum(p * q for p, q in spot_levels),
                    sum(p * q for p, q in perp_levels),
                    MAX_NOTIONAL_USD)
    return available if available >= MIN_NOTIONAL_USD else 0.0

def max_hold_for(pos):
    """The 180s spread timeout cannot apply to a funding trade: it has to outlive
    its own countdown. Past that, holding is decided at each stamp on whether the
    next payment is still worth the capital, so this is only a safety ceiling."""
    if pos.get("trade_kind") != "funding":
        return MAX_HOLD_SEC
    return max(MAX_HOLD_SEC,
               (pos.get("secs_to_funding") or 0.0) + FUNDING_EXIT_GRACE_SEC,
               FUNDING_MAX_HOLD_SEC)

def accrue_funding(cs):
    """Credit (or debit) funding whenever a stamp passes while we're holding.

    We enter to *receive*, but the rate can flip before it settles, so sign it
    off the live rate rather than assuming we always collect.
      direction +1 (short perp) receives when the rate is positive
      direction -1 (long perp)  receives when the rate is negative
    """
    fv = funding_view(cs)
    with cs.lock:
        pos = cs.open_position
        if pos is None:
            return
        nxt = pos.get("next_funding_ms")
        if nxt is None or time.time() * 1000.0 < nxt:
            return
        rate_pct = fv[0] if fv is not None else pos.get("funding_pct_at_entry", 0.0)
        received = rate_pct if pos["direction"] == +1 else -rate_pct
        pos["funding_collected_pct"] += received
        pos["stamps_crossed"]        += 1
        pos["last_stamp_time"]        = time.time()
        interval_h = cs.funding_interval_h or DEFAULT_FUNDING_INTERVAL_H
        pos["next_funding_ms"] = nxt + interval_h * 3600 * 1000.0
        sym, total = cs.symbol, pos["funding_collected_pct"]
    clr = Fore.GREEN if received >= 0 else Fore.RED
    print(f"\n{clr}  💰 [{sym}] FUNDING settled {received:+.6f}%  "
          f"(cumulative {total:+.6f}%){Style.RESET_ALL}")

# ══════════════════════════════════════════════════════════════════════════════
# EXECUTION
# Both legs cross the book together after the configured latency. The fee is an
# assumption (FILL_FEE_TYPE), not a race that has to be won.
# ══════════════════════════════════════════════════════════════════════════════

def _taker_price(snap, leg, side, notional):
    """VWAP price from crossing the spread and walking the book for `notional`."""
    levels = snap[f"{leg}_asks"] if side == "buy" else snap[f"{leg}_bids"]
    if not levels:
        return None
    ref = levels[0][0]
    if ref <= 0:
        return None
    price, _, _ = vwap_fill(levels, notional / ref)
    return price

def execute_two_leg_fill(cs, direction, notional, phase, allow_cancel=True):
    """Fill both legs at once, at the book we can see right now.

    phase "entry": direction +1 → buy spot / sell perp.
    phase "exit" : the reverse, to flatten.

    The caller has already waited ENTRY_DELAY_SEC / EXIT_DELAY_SEC, so this never
    fills on the tick that triggered the signal. Both legs land together, which
    is also what keeps the pair delta neutral: there is no window where one side
    is on and the other is not.

    Returns (spot_price, perp_price, spot_fee_type, perp_fee_type, elapsed_ms)
    or None when the book is too thin to fill at all.
    """
    if phase == "entry":
        spot_side = "buy" if direction == 1 else "sell"
    else:
        spot_side = "sell" if direction == 1 else "buy"
    perp_side = "sell" if spot_side == "buy" else "buy"

    t0   = time.time()
    snap = get_fill_snap(cs)

    spot_price = _taker_price(snap, "spot", spot_side, notional)
    perp_price = _taker_price(snap, "perp", perp_side, notional)
    if spot_price is None or perp_price is None:
        return None

    return (spot_price, perp_price, FILL_FEE_TYPE, FILL_FEE_TYPE,
            (time.time() - t0) * 1000.0)

def realised_fee_pct(pos, exit_spot_type, exit_perp_type):
    """Actual fee cost of all four executions, given how each one filled."""
    return (leg_fee_pct("spot", pos.get("entry_spot_fill_type", "taker")) +
            leg_fee_pct("perp", pos.get("entry_perp_fill_type", "taker")) +
            leg_fee_pct("spot", exit_spot_type) +
            leg_fee_pct("perp", exit_perp_type))

# ── retained helpers ──────────────────────────────────────────────────────────

def get_fill_snap(cs):
    with cs.lock:
        return {k: v for k, v in cs.latest.items()}

def get_round_trip_pct(cs):
    sba = list(cs.spot_ba_hist)
    pba = list(cs.perp_ba_hist)
    sm  = float(np.mean(sba)) if len(sba) >= 5 else 0.0
    pm  = float(np.mean(pba)) if len(pba) >= 5 else 0.0
    return sm + pm + round_trip_fee_pct()

def basis_volatility(cs):
    """How far this coin's gap normally travels, in percent.

    Deliberately does not wait for the full rolling window the way
    get_rolling_stats does: a risk filter that only switches on after eight
    minutes is not a risk filter. Anything past MIN_VOL_SAMPLES buckets gives a
    usable estimate.
    """
    b = list(cs.buckets)
    if len(b) < MIN_VOL_SAMPLES:
        return None
    spreads = [x["spread_pct"] for x in b[-cs.rolling_win:]]
    return float(np.std(spreads, ddof=1))

def get_rolling_stats(cs):
    if len(cs.buckets) < cs.rolling_win:
        return None, None
    spreads = [b["spread_pct"] for b in list(cs.buckets)[-cs.rolling_win:]]
    return float(np.mean(spreads)), float(np.std(spreads, ddof=1))

def update_slip_buffer(cs):
    """Recompute the 90th-pct slip buffer from collected entry slippage samples.
    Gate 3 stays disabled (slip_buf=0) until we have at least 5 samples,
    which only start collecting after the first 20 signals anyway."""
    with cs.lock:
        n_samples = len(cs.slip_history)
        fired     = cs.stats["signals_fired"]
    if fired < 20 or n_samples < 5:
        # Not enough history yet — Gate 3 is off, same as Gate 2
        with cs.lock:
            cs.stats["slip_buffer"] = 0.0
        return
    buf = float(np.percentile(list(cs.slip_history), 90))
    with cs.lock:
        cs.stats["slip_buffer"] = max(buf, 0.0)

def calc_pnl(pos, exit_spot, exit_perp, round_trip_pct,
             exit_spot_type="taker", exit_perp_type="taker"):
    d  = pos["direction"]
    es = pos["entry_spot_fill"]
    ep = pos["entry_perp_fill"]
    if d == +1:
        spot_leg = (exit_spot - es) / es * 100
        perp_leg = (ep - exit_perp) / ep * 100
    else:
        spot_leg = (es - exit_spot) / es * 100
        perp_leg = (exit_perp - ep) / ep * 100
    gross = spot_leg + perp_leg

    # gross runs entry fill → exit fill, and those are real prices off the book:
    # a crossing fill already paid the spread, a resting fill already earned it.
    # round_trip_pct is the *pre-trade estimate* of that same cost and belongs in
    # the entry gates, not here, subtracting it again charged every trade the
    # spread twice, which on a wide-spread coin stopped positions out at birth.
    fees_pct    = realised_fee_pct(pos, exit_spot_type, exit_perp_type)
    funding_pct = pos.get("funding_collected_pct", 0.0)

    net = gross + funding_pct - fees_pct
    usd = net / 100 * pos["notional_usd"]
    return gross, net, usd

# ══════════════════════════════════════════════════════════════════════════════
# DISPLAY
# ══════════════════════════════════════════════════════════════════════════════

def print_entry(cs, pos, roll_mean, roll_std, upper, lower, min_dev,
                entry_slip_pct, dev_shrink_pct, round_trip_pct,
                signal_spread, signal_deviation, delay_ms):
    clr    = Fore.GREEN if pos["direction"] == 1 else Fore.RED
    dt_str = datetime.now().strftime("%H:%M:%S.%f")[:-3]
    total_slip = entry_slip_pct + dev_shrink_pct
    print(f"\n{clr}{'═'*72}{Style.RESET_ALL}")
    print(f"{clr}  📥 [{cs.symbol}] ENTER [{dt_str}]  →  {pos['action']}{Style.RESET_ALL}")
    print(f"  {'─'*68}")
    print(f"  VWAP fills     : spot={pos['entry_spot_fill']:.6f}  "
          f"perp={pos['entry_perp_fill']:.6f}  (after {delay_ms:.2f}ms delay)")
    print(f"  Notional       : ${pos['notional_usd']:.2f}  "
          f"(L20 book-walk)")
    print(f"  Fill spread    : {pos['entry_spread']:+.6f}%   "
          f"Fill deviation: {pos['entry_deviation']:+.6f}%")
    print(f"  {'─'*68}")
    print(f"  Signal spread  : {signal_spread:+.6f}%   "
          f"Signal deviation: {signal_deviation:+.6f}%")
    print(f"  Rolling mean   : {roll_mean:+.6f}%   std={roll_std:.6f}%")
    print(f"  Band           : [{lower:+.6f}%, {upper:+.6f}%]")
    print(f"  Slippage       : book-walk={entry_slip_pct:.5f}%  "
          f"dev-shrink={dev_shrink_pct:.5f}%  total={total_slip:.5f}%")
    print(f"  Live friction  : {round_trip_pct:.5f}%  "
          f"(spot_ba + perp_ba + {round_trip_fee_pct():.5f}% [{FEE_TIER}])  "
          f"all-in cost={round_trip_pct + total_slip:.5f}%")
    print(f"{clr}{'─'*72}{Style.RESET_ALL}")

def print_hold(cs, pos, gross, net, usd, round_trip_pct, curr_dev=None):
    if not HOLD_TICKER:
        return
    hold_sec = time.time() - pos["entry_time"]
    pnl_clr  = Fore.GREEN if net >= 0 else Fore.RED
    dev_str  = f"  dev:{curr_dev:+.5f}%" if curr_dev is not None else ""
    print(f"  [{cs.symbol}] 📊 [{datetime.now().strftime('%H:%M:%S')}] "
          f"hold={hold_sec:.1f}s  gross={gross:+.5f}%  "
          f"{pnl_clr}net={net:+.5f}%  ${usd:+.3f}  "
          f"friction={round_trip_pct:.5f}%{dev_str}{Style.RESET_ALL}",
          end="\r")

def print_exit(cs, pos, exit_spot, exit_perp, gross, net, usd,
               exit_reason, closed, total_pct, total_usd,
               round_trip_pct, delay_ms):
    clr  = Fore.GREEN if net >= 0 else Fore.RED
    icon = "✅" if net >= 0 else "❌"
    dt_str = datetime.now().strftime("%H:%M:%S.%f")[:-3]
    vw_pct = get_vwap_net_pnl_pct()
    with global_lock:
        g_closed = global_stats["total_closed"]
        g_usd    = global_stats["total_net_pnl_usd"]
        g_wins   = global_stats["total_profit"]
    print(f"\n{clr}  [{cs.symbol}] 📤 EXIT [{dt_str}]  "
          f"hold={(time.time()-pos['entry_time']):.1f}s  reason={exit_reason}{Style.RESET_ALL}")
    print(f"  {'─'*68}")
    print(f"  Exit fills     : spot={exit_spot:.6f}  perp={exit_perp:.6f}  "
          f"(after {delay_ms:.2f}ms delay)")
    print(f"  Entry fills    : spot={pos['entry_spot_fill']:.6f}  "
          f"perp={pos['entry_perp_fill']:.6f}")
    print(f"  Notional       : ${pos['notional_usd']:.2f}")
    print(f"  Gross PnL      : {gross:+.6f}%")
    print(f"  Live friction  : -{round_trip_pct:.6f}%")
    print(f"  {clr}{icon} Net PnL  : {net:+.6f}%   →  {clr}${usd:+.4f}{Style.RESET_ALL}")
    print(f"  {'─'*55}")
    pnl_clr = Fore.GREEN if total_usd >= 0 else Fore.RED
    print(f"  {pnl_clr}[{cs.symbol}] Cumul: {total_pct:+.6f}%  ${total_usd:+.4f} "
          f"over {closed} trades{Style.RESET_ALL}")
    gw_clr = Fore.GREEN if g_usd >= 0 else Fore.RED
    print(f"  {gw_clr}🌐 GLOBAL  VWAP-net: {vw_pct:+.6f}%  ${g_usd:+.4f}  "
          f"{g_closed} trades  (wins={g_wins}){Style.RESET_ALL}\n")

# ══════════════════════════════════════════════════════════════════════════════
# MASTER CSV — single file, all coins, thread-safe append
# ══════════════════════════════════════════════════════════════════════════════

MASTER_CSV_PATH = os.path.join(OUTPUT_DIR, "trades_master.csv")
_csv_lock       = threading.Lock()

MASTER_CSV_FIELDS = [
    "symbol", "entry_dt", "exit_dt", "action", "direction",
    "notional_usd",
    # ── entry fill vs signal deviation ──────────────────────────────────────
    "signal_spread_pct",        # spread at the tick that triggered the signal
    "signal_deviation_pct",     # spread − rolling_mean at signal tick
    "entry_spread_pct",         # spread computed from VWAP fill prices
    "entry_deviation_pct",      # fill_spread − rolling_mean  (what you actually entered)
    "deviation_shrink_pct",     # signal_dev − fill_dev  (edge lost to latency)
    "rolling_mean_pct",         # rolling mean at time of entry
    # ── fill prices ─────────────────────────────────────────────────────────
    "entry_spot_fill", "entry_perp_fill",
    "exit_spot_fill",  "exit_perp_fill",
    # ── slippage & friction ──────────────────────────────────────────────────
    "book_walk_slip_pct",       # L20 VWAP slippage vs mid, both legs combined
    "live_friction_pct",        # rolling bid-ask + exchange fee
    "total_entry_cost_pct",     # book_walk_slip + live_friction
    # ── execution: how each of the four fills landed ─────────────────────────
    "trade_kind",               # "spread" or "funding"
    "entry_spot_fill_type", "entry_perp_fill_type",
    "exit_spot_fill_type",  "exit_perp_fill_type",
    "fee_tier", "realised_fee_pct",
    # ── funding ──────────────────────────────────────────────────────────────
    "funding_pct_at_entry",     # rate seen when we entered
    "funding_collected_pct",    # what actually settled while we held
    "stamps_crossed",
    "convergence_edge_pct",     # basis edge priced at entry (negative = adverse)
    "stop_loss_pct",            # level this trade would have been cut at
    # ── outcome ──────────────────────────────────────────────────────────────
    "hold_sec",
    "gross_pnl_pct", "net_pnl_pct", "net_pnl_usd",
    "best_pnl_pct",  "best_pnl_usd",
    "exit_reason",
]

def write_trade_csv(row):
    """Append one trade row to the master CSV. Creates file + header if needed."""
    with _csv_lock:
        exists = os.path.isfile(MASTER_CSV_PATH)
        with open(MASTER_CSV_PATH, "a", newline="", encoding="utf-8") as f:
            w = csv.DictWriter(f, fieldnames=MASTER_CSV_FIELDS, extrasaction="ignore")
            if not exists:
                w.writeheader()
            w.writerow(row)

# ══════════════════════════════════════════════════════════════════════════════
# ENTRY — immediate fill (zero artificial sleep), book VWAP sizing
# ══════════════════════════════════════════════════════════════════════════════

def execute_entry(cs, direction, signal_spread, signal_deviation,
                  roll_mean, roll_std, upper, lower, min_dev, round_trip_pct,
                  trade_kind="spread", funding_pct=0.0, secs_to_funding=None,
                  convergence_edge=0.0):
    t0 = time.time()
    if ENTRY_DELAY_SEC > 0:
        time.sleep(ENTRY_DELAY_SEC)   # never fill on the signal tick itself
    delay_ms = (time.time() - t0) * 1000

    snap = get_fill_snap(cs)

    # ── Book walk: size the trade ────────────────────────────────────────────
    notional, ref_spot, ref_perp, entry_slip_pct = find_optimal_notional(
        snap, direction, roll_mean, round_trip_pct
    )
    if trade_kind == "funding":
        # Funding pays on notional whether or not the spread alone is profitable,
        # so size off everything both legs can fill. find_optimal_notional stops
        # at the largest spread-profitable size, which is usually far smaller and
        # on many entries is zero, sizing off it would leave most of the
        # funding on the table.
        notional = max(notional, _funding_notional(snap, direction))
    # Both sizing paths get capped here, so neither can slip a whole order book
    # into one position.
    notional = min(notional, MAX_NOTIONAL_USD)

    if notional <= 0:
        with cs.lock:
            cs.entry_pending = False
        return

    # ── Maker-first fill, taker fallback ─────────────────────────────────────
    fill = execute_two_leg_fill(cs, direction, notional, "entry")
    if fill is None:
        with cs.lock:
            cs.entry_pending = False
        return
    entry_spot, entry_perp, spot_fill_type, perp_fill_type, maker_wait_ms = fill

    # Recompute fill spread/deviation from the realised prices
    fill_spread    = (entry_perp - entry_spot) / entry_spot * 100
    fill_deviation = fill_spread - roll_mean

    # Deviation shrink: how much the spread faded between signal tick and fill
    # Positive = signal partially reverted before we got in (adverse latency cost)
    dev_shrink_pct = max(0.0, abs(signal_deviation) - abs(fill_deviation))

    action = "LONG spot / SHORT perp" if direction == 1 else "SHORT spot / LONG perp"
    now    = time.time()

    with cs.lock:
        if cs.open_position is not None:
            cs.entry_pending = False
            return
        cs.stats["signals_fired"] += 1
        fired_so_far = cs.stats["signals_fired"]
        cs.open_position = {
            "direction"          : direction,
            "action"             : action,
            "signal_spread"      : signal_spread,       # spread at the signal tick
            "signal_deviation"   : signal_deviation,    # deviation at signal tick
            "entry_deviation"    : fill_deviation,      # deviation at VWAP fill prices
            "entry_mean"         : roll_mean,
            "entry_spread"       : fill_spread,
            "entry_spot_fill"    : entry_spot,
            "entry_perp_fill"    : entry_perp,
            "entry_slip_pct"     : entry_slip_pct,      # L20 book-walk slippage both legs
            "dev_shrink_pct"     : dev_shrink_pct,      # deviation faded before fill (latency)
            "notional_usd"       : notional,
            "entry_time"         : now,
            "entry_dt"           : datetime.fromtimestamp(now, tz=timezone.utc).isoformat(),
            "best_pnl"           : -999.0,
            "best_pnl_usd"       : -999.0,
            "profit_target_hit"  : False,
            # ── execution ────────────────────────────────────────────────────
            "entry_spot_fill_type": spot_fill_type,
            "entry_perp_fill_type": perp_fill_type,
            "entry_maker_wait_ms" : maker_wait_ms,
            # ── funding ──────────────────────────────────────────────────────
            "trade_kind"          : trade_kind,
            "funding_pct_at_entry": funding_pct,
            "convergence_edge_pct": convergence_edge,
            "secs_to_funding"     : secs_to_funding,
            "stop_loss_pct"       : max(STOP_LOSS_FUNDING_MULT * abs(funding_pct),
                                        STOP_LOSS_MIN_PCT),
            "funding_collected_pct": 0.0,
            "stamps_crossed"      : 0,
            "entry_mark_pct"      : 0.0,   # set just below
            "next_funding_ms"     : (None if secs_to_funding is None
                                     else int((now + secs_to_funding) * 1000)),
        }
        cs.entry_pending = False

    # A freshly opened pair is already down by the cost of unwinding it: we crossed
    # the spread to get in and would cross it again to get out. That is the trade's
    # starting line, not a loss. Recording it lets the stop measure real adverse
    # movement instead of firing on the entry cost the instant we open.
    exit_spot_now, exit_perp_now = get_exit_vwap(get_fill_snap(cs), direction, notional)
    if exit_spot_now is not None and exit_perp_now is not None:
        with cs.lock:
            if cs.open_position is not None:
                _, mark, _ = calc_pnl(cs.open_position, exit_spot_now, exit_perp_now, round_trip_pct)
                cs.open_position["entry_mark_pct"] = mark

    # ── Gate 3 slip buffer: combined latency + book-walk slippage ────────────
    # Gate 3 buffer = 90th percentile of (dev_shrink + book_slip) across past trades.
    if fired_so_far >= 20:
        total_slip = dev_shrink_pct + entry_slip_pct
        with cs.lock:
            cs.slip_history.append(total_slip)

    print_entry(cs, cs.open_position, roll_mean, roll_std, upper, lower,
                min_dev, entry_slip_pct, dev_shrink_pct, round_trip_pct,
                signal_spread, signal_deviation, delay_ms)

# ══════════════════════════════════════════════════════════════════════════════
# EXIT — immediate fill (zero artificial sleep)
# ══════════════════════════════════════════════════════════════════════════════

def execute_exit(cs, pos, exit_reason):
    t0 = time.time()
    if EXIT_DELAY_SEC > 0:
        time.sleep(EXIT_DELAY_SEC)
    delay_ms = (time.time() - t0) * 1000

    with cs.lock:
        round_trip_pct = cs.stats["live_round_trip"]

    fill = execute_two_leg_fill(cs, pos["direction"], pos["notional_usd"],
                                "exit", allow_cancel=False)
    if fill is None:
        with cs.lock:
            cs.exit_pending = False
        return
    exit_spot, exit_perp, exit_spot_type, exit_perp_type, exit_maker_wait_ms = fill

    gross, net, usd = calc_pnl(pos, exit_spot, exit_perp, round_trip_pct,
                               exit_spot_type, exit_perp_type)
    now = time.time()

    with cs.lock:
        if cs.open_position is None:
            # Already closed by another thread (race) — just clean up
            cs.exit_pending = False
            return
        cs.stats["trades_closed"]     += 1
        cs.stats["total_net_pnl_pct"] += net
        cs.stats["total_net_pnl_usd"] += usd
        if net >= 0:
            cs.stats["trades_profit"] += 1
        else:
            cs.stats["trades_loss"]   += 1
        closed        = cs.stats["trades_closed"]
        total_pnl_pct = cs.stats["total_net_pnl_pct"]
        total_pnl_usd = cs.stats["total_net_pnl_usd"]
        cs.open_position = None
        cs.exit_pending  = False

    record_global_trade(net, pos["notional_usd"], usd)
    record_trade_event({
        "symbol"              : cs.symbol,
        "entry_dt"            : pos["entry_dt"],
        "exit_dt"             : datetime.fromtimestamp(now, tz=timezone.utc).isoformat(),
        "action"              : pos["action"],
        "direction"           : pos["direction"],
        "notional_usd"        : round(pos["notional_usd"], 4),
        "hold_sec"            : round(now - pos["entry_time"], 2),
        "gross_pnl_pct"       : round(gross, 6),
        "net_pnl_pct"         : round(net, 6),
        "net_pnl_usd"         : round(usd, 6),
        "entry_deviation_pct" : round(pos["entry_deviation"], 6),
        "book_walk_slip_pct"  : round(pos.get("entry_slip_pct", 0.0), 6),
        "live_friction_pct"   : round(round_trip_pct, 6),
        "entry_spot_fill"     : pos["entry_spot_fill"],
        "entry_perp_fill"     : pos["entry_perp_fill"],
        "exit_spot_fill"      : exit_spot,
        "exit_perp_fill"      : exit_perp,
        "exit_type"           : _exit_type(exit_reason),
        "exit_reason"         : exit_reason,
        "trade_kind"          : pos.get("trade_kind", "spread"),
        "funding_collected_pct": round(pos.get("funding_collected_pct", 0.0), 6),
        "stamps_crossed"      : pos.get("stamps_crossed", 0),
        "realised_fee_pct"    : round(realised_fee_pct(pos, exit_spot_type, exit_perp_type), 6),
        "fee_tier"            : FEE_TIER,
        "fill_fee_type"       : FILL_FEE_TYPE,
    })

    print()
    print_exit(cs, pos, exit_spot, exit_perp, gross, net, usd,
               exit_reason, closed, total_pnl_pct, total_pnl_usd,
               round_trip_pct, delay_ms)

    write_trade_csv({
        "symbol"               : cs.symbol,
        "entry_dt"             : pos["entry_dt"],
        "exit_dt"              : datetime.fromtimestamp(now, tz=timezone.utc).isoformat(),
        "action"               : pos["action"],
        "direction"            : pos["direction"],
        "notional_usd"         : round(pos["notional_usd"], 4),
        # ── signal vs fill deviation ─────────────────────────────────────────
        "signal_spread_pct"    : round(pos.get("signal_spread", 0.0), 8),
        "signal_deviation_pct" : round(pos.get("signal_deviation", 0.0), 8),
        "entry_spread_pct"     : round(pos["entry_spread"], 8),
        "entry_deviation_pct"  : round(pos["entry_deviation"], 8),
        "deviation_shrink_pct" : round(pos.get("dev_shrink_pct", 0.0), 8),
        "rolling_mean_pct"     : round(pos["entry_mean"], 8),
        # ── fill prices ──────────────────────────────────────────────────────
        "entry_spot_fill"      : pos["entry_spot_fill"],
        "entry_perp_fill"      : pos["entry_perp_fill"],
        "exit_spot_fill"       : exit_spot,
        "exit_perp_fill"       : exit_perp,
        # ── slippage & friction ───────────────────────────────────────────────
        "book_walk_slip_pct"   : round(pos.get("entry_slip_pct", 0.0), 8),
        "live_friction_pct"    : round(round_trip_pct, 8),
        "total_entry_cost_pct" : round(round_trip_pct + pos.get("entry_slip_pct", 0.0)
                                       + pos.get("dev_shrink_pct", 0.0), 8),
        # ── execution ─────────────────────────────────────────────────────────
        "trade_kind"           : pos.get("trade_kind", "spread"),
        "entry_spot_fill_type" : pos.get("entry_spot_fill_type", "taker"),
        "entry_perp_fill_type" : pos.get("entry_perp_fill_type", "taker"),
        "exit_spot_fill_type"  : exit_spot_type,
        "exit_perp_fill_type"  : exit_perp_type,
        "fee_tier"             : FEE_TIER,
        "realised_fee_pct"     : round(realised_fee_pct(pos, exit_spot_type, exit_perp_type), 8),
        # ── funding ───────────────────────────────────────────────────────────
        "funding_pct_at_entry" : round(pos.get("funding_pct_at_entry", 0.0), 8),
        "funding_collected_pct": round(pos.get("funding_collected_pct", 0.0), 8),
        "stamps_crossed"       : pos.get("stamps_crossed", 0),
        "convergence_edge_pct" : round(pos.get("convergence_edge_pct", 0.0), 8),
        "stop_loss_pct"        : round(pos.get("stop_loss_pct", 0.0), 8),
        # ── outcome ───────────────────────────────────────────────────────────
        "hold_sec"             : round(now - pos["entry_time"], 2),
        "gross_pnl_pct"        : round(gross, 8),
        "net_pnl_pct"          : round(net, 8),
        "net_pnl_usd"          : round(usd, 6),
        "best_pnl_pct"         : round(pos["best_pnl"], 8),
        "best_pnl_usd"         : round(pos["best_pnl_usd"], 6),
        "exit_reason"          : exit_reason,
    })

# ══════════════════════════════════════════════════════════════════════════════
# TICK-LEVEL GATE CHECK — entry
# ══════════════════════════════════════════════════════════════════════════════

def funding_entry_check(cs, round_trip):
    """Is there a funding stamp worth holding into?

    Decided on the funding payment alone. Convergence is a second earner we take
    whenever it turns up, not a reason to enter: the perp is meant to track spot,
    so any gap closing is upside on top of a trade that already pays for itself.

    Returns (direction, funding_pct, secs_to_stamp) or None.
    """
    fv = funding_view(cs)
    if fv is None:
        return None
    rate_pct, secs_to_stamp, interval_h = fv

    # get_round_trip_pct() reports 0 until it has five bid-ask samples per leg.
    # Zero means not measured yet, not free, and a coin with a 0.16% spread
    # reading as free clears every hurdle below trivially.
    if round_trip <= 0:
        return None

    if abs(rate_pct) < MIN_FUNDING_PCT:
        return None
    window = min(FUNDING_ENTRY_WINDOW_SEC,
                 (interval_h or DEFAULT_FUNDING_INTERVAL_H) * 3600.0 * FUNDING_ENTRY_WINDOW_FRAC)
    if secs_to_stamp <= 0 or secs_to_stamp > window:
        return None

    direction = +1 if rate_pct > 0 else -1

    # Would this coin's ordinary movement knock us out before we get paid? The
    # stop sits 2x the funding away. If that is inside one standard deviation of
    # how far this gap normally travels, being stopped is the base case rather
    # than the exception, and the funding was never really collectable.
    vol = basis_volatility(cs)
    if vol is None or vol <= 0:
        # Unmeasured is not the same as safe.
        return None
    stop_pct = max(STOP_LOSS_FUNDING_MULT * abs(rate_pct), STOP_LOSS_MIN_PCT)
    if (stop_pct / vol) < MIN_STOP_SIGMAS:
        return None

    total_expected = abs(rate_pct) - round_trip
    if total_expected <= 0:
        return None
    # A multiple of friction, not merely above it: an edge that only just clears
    # costs sits inside the error of the cost estimate itself.
    if abs(rate_pct) < EDGE_FRICTION_MULT * round_trip:
        return None

    # Judged as a rate of return, not an absolute. Capital is committed until we
    # exit, which is the stamp plus however long the unwind takes, not just the
    # countdown, or a trade entered seconds before a stamp would look
    # near-infinitely attractive.
    hold_hours = max((secs_to_stamp + FUNDING_EXIT_GRACE_SEC / 2.0) / 3600.0, 1.0 / 60.0)
    apr        = total_expected * (8760.0 / hold_hours)
    if apr < MIN_FUNDING_APR:
        return None

    return direction, rate_pct, secs_to_stamp

def check_entry_on_tick(cs, snap):
    with cs.lock:
        if cs.open_position is not None or cs.entry_pending:
            return
        bs         = cs.bucket_signal.copy()
        round_trip = cs.stats["live_round_trip"]
        last_recon = cs.last_reconnect_time

    if not ENTRIES_ENABLED.is_set():
        return

    if last_recon is not None and (time.time() - last_recon) < POST_RECONNECT_COOLDOWN_SEC:
        return

    # A stop means this setup just went against us. Re-entering it on the next
    # tick is how the engine ends up taking the same losing trade hundreds of
    # times, so sit the cooldown out and let the situation change first.
    with cs.lock:
        last_stop = cs.last_stop_time
    if last_stop is not None and (time.time() - last_stop) < STOP_COOLDOWN_SEC:
        return

    # ── Funding capture ──────────────────────────────────────────────────────
    # Checked ahead of the spread gates and without waiting for the rolling
    # window: the edge here is the funding payment, not the z-score.
    if STRATEGY in ("funding", "both"):
        spot_mid = _mid(snap, "spot")
        perp_mid = _mid(snap, "perp")
        if spot_mid is not None and perp_mid is not None:
            spread_pct = (perp_mid - spot_mid) / spot_mid * 100
            # Convergence is measured against spot itself, not a rolling average
            # of past gaps. The perp is meant to track spot, so zero is the real
            # anchor, and using it lets a coin trade as soon as we have a quote
            # instead of waiting out an eight minute window for a mean.
            roll_mean  = 0.0
            fc = funding_entry_check(cs, round_trip)
            if fc is not None:
                direction, funding_pct, secs_to_stamp = fc
                convergence_edge = spread_pct * direction * REVERSION_FRACTION
                with cs.lock:
                    if cs.open_position is not None or cs.entry_pending:
                        return
                    cs.entry_pending = True
                    cs.stats["signals_detected"] += 1
                threading.Thread(
                    target=execute_entry,
                    args=(cs, direction, spread_pct, spread_pct - roll_mean,
                          roll_mean, bs["roll_std"] or 0.0,
                          bs["upper"] or 0.0, bs["lower"] or 0.0,
                          bs["min_dev"] or 0.0, round_trip),
                    kwargs={"trade_kind": "funding", "funding_pct": funding_pct,
                            "secs_to_funding": secs_to_stamp,
                            "convergence_edge": convergence_edge},
                    daemon=True,
                ).start()
                return

    if STRATEGY == "funding":
        return      # funding-only: never fall through to a spread trade

    if not bs["ready"]:
        return

    spot_mid   = _mid(snap, "spot")
    perp_mid   = _mid(snap, "perp")
    if spot_mid is None or perp_mid is None:
        return

    roll_mean  = bs["roll_mean"];  roll_std   = bs["roll_std"]
    upper      = bs["upper"];      lower      = bs["lower"]
    round_trip = bs["round_trip"]; min_dev    = bs["min_dev"]
    spread_pct = (perp_mid - spot_mid) / spot_mid * 100
    deviation  = spread_pct - roll_mean
    abs_dev    = abs(deviation)

    gate1_long  = spread_pct > upper
    gate1_short = spread_pct < lower
    if not (gate1_long or gate1_short):
        return

    with cs.lock:
        cs.stats["signals_detected"] += 1

    if abs_dev <= round_trip:
        with cs.lock:
            cs.stats["blocked_gate2"] += 1
        return

    if abs_dev <= min_dev:
        with cs.lock:
            cs.stats["blocked_gate3"] += 1
        return

    direction = +1 if gate1_long else -1
    with cs.lock:
        if cs.open_position is not None or cs.entry_pending:
            return
        cs.entry_pending = True

    t = threading.Thread(
        target=execute_entry,
        args=(cs, direction, spread_pct, deviation,
              roll_mean, roll_std, upper, lower, min_dev, round_trip),
        daemon=True
    )
    t.start()

# ══════════════════════════════════════════════════════════════════════════════
# TICK-LEVEL GATE CHECK — exit
# ══════════════════════════════════════════════════════════════════════════════

def check_exit_on_tick(cs, snap):
    with cs.lock:
        pos = cs.open_position
        if pos is None or cs.exit_pending:
            return
        round_trip_pct = cs.stats["live_round_trip"]

    exit_spot, exit_perp = get_exit_vwap(snap, pos["direction"], pos["notional_usd"])
    if None in (exit_spot, exit_perp):
        return

    gross, net, usd = calc_pnl(pos, exit_spot, exit_perp, round_trip_pct)

    with cs.lock:
        if cs.open_position is not None and net > cs.open_position["best_pnl"]:
            cs.open_position["best_pnl"]     = net
            cs.open_position["best_pnl_usd"] = usd

    # ── Dynamic reversion-based exit ────────────────────────────────────────────
    # Compute current deviation from the mean at entry time
    curr_mid_spot = _mid(snap, "spot")
    curr_mid_perp = _mid(snap, "perp")
    if curr_mid_spot is None or curr_mid_perp is None:
        return
    curr_spread    = (curr_mid_perp - curr_mid_spot) / curr_mid_spot * 100
    curr_deviation = curr_spread - pos["entry_mean"]   # vs mean at time of entry

    abs_entry_dev  = abs(pos["entry_deviation"])
    abs_curr_dev   = abs(curr_deviation)

    print_hold(cs, pos, gross, net, usd, round_trip_pct, curr_deviation)

    # ── Funding trades ───────────────────────────────────────────────────────
    # The payment only lands if we're still holding at the stamp, so nothing
    # closes before it. Once it has settled, leave as soon as the basis has
    # converged far enough that funding + convergence is net positive.
    if pos.get("trade_kind") == "funding":
        collected  = pos.get("funding_collected_pct", 0.0)
        stop_pct   = pos.get("stop_loss_pct", STOP_LOSS_MIN_PCT)
        past_stamp = pos.get("stamps_crossed", 0) >= 1

        # Stop loss runs before and after the stamp. If the basis has moved
        # against us by more than the funding was ever going to pay, the reason
        # for holding is gone, waiting for the stamp would only add to it.
        #
        # Measured from where the trade started, not from zero. Every pair opens
        # already down by one round trip of spread, so comparing raw net against
        # the stop fired the instant we opened on any coin whose spread was wider
        # than the stop, then reopened and fired again on the next tick.
        baseline = pos.get("entry_mark_pct", 0.0)
        if (net - baseline) <= -stop_pct:
            with cs.lock:
                if cs.open_position is None or cs.exit_pending:
                    return
                cs.exit_pending = True
                cs.last_stop_time = time.time()
                pos_snap = cs.open_position
            reason = (f"STOP LOSS  moved {net - baseline:+.5f}% against us "
                      f"(limit -{stop_pct:.5f}%)  funding={collected:+.5f}%  "
                      f"dev:{pos['entry_deviation']:+.5f}%→{curr_deviation:+.5f}%")
            threading.Thread(target=execute_exit, args=(cs, pos_snap, reason),
                             daemon=True).start()
            return

        # Nothing else closes before the stamp, the payment is why we are here.
        if not past_stamp:
            return

        # Funding is banked; now let the convergence leg pay out.
        #
        # Which way counts depends on the side we are on, not on distance from
        # zero. A short perp profits as the gap falls, a long perp as it rises.
        # Measuring |gap| shrinking treated a gap running our way as though it
        # were going wrong, and a gap closing against us as though we were
        # winning, which is why a profitable position could read "gap widened".
        entry_gap = pos["entry_deviation"]
        moved_our_way = (entry_gap - curr_deviation) * pos["direction"]
        # A gap only pays us as it closes when it starts on our side. Sitting the
        # wrong way round, closing it costs us, so there is nothing to wait for.
        conv_available = entry_gap * pos["direction"] > 0

        if abs_entry_dev < MIN_CONVERGENCE_DEV_PCT or not conv_available:
            # No convergence to collect: leave as soon as we are in the black.
            converged = net >= 0
        else:
            converged = moved_our_way >= abs_entry_dev * REVERSION_FRACTION

        if converged:
            with cs.lock:
                if cs.open_position is None or cs.exit_pending:
                    return
                cs.open_position["profit_target_hit"] = True
                cs.exit_pending = True
                pos_snap = cs.open_position
            pct_reverted = (moved_our_way / abs_entry_dev * 100
                            if abs_entry_dev > 0 else 100.0)
            reason = (f"FUNDING + CONVERGENCE  funding={collected:+.5f}%  "
                      f"moved our way {pct_reverted:.0f}%  "
                      f"dev:{pos['entry_deviation']:+.5f}%→{curr_deviation:+.5f}%  "
                      f"net={net:+.5f}%")
            threading.Thread(target=execute_exit, args=(cs, pos_snap, reason),
                             daemon=True).start()
            return

        # ── Still holding, gap not closed. Sit for the next payment or leave? ──
        # A clock is the wrong test. What matters is whether the money still to be
        # made from here beats the hurdle over the time it would take to make it,
        # the same question asked at entry. Staying also means collecting again:
        # an hourly pair pays every hour we hold it.
        fv = funding_view(cs)
        if fv is None:
            # Funding feed is quiet, so the call can't be made. Fall back to the
            # grace window rather than holding an un-evaluated position forever.
            since = pos.get("last_stamp_time") or pos["entry_time"]
            if (time.time() - since) <= FUNDING_EXIT_GRACE_SEC:
                return
            forward_edge, forward_apr, secs_next = 0.0, 0.0, None
            why = "no funding data to re-evaluate"
        else:
            next_rate_pct, secs_next, _interval_h = fv
            # Signed by our side: a rate that flipped means we now pay, not collect.
            next_payment = next_rate_pct if pos["direction"] == +1 else -next_rate_pct
            # Convergence still on the table, signed the same way.
            # What is still on the table, signed by our side. Negative means the
            # gap would have to move against us to close, so there is nothing here.
            conv_left    = curr_deviation * pos["direction"] * REVERSION_FRACTION
            forward_edge = next_payment + conv_left
            hold_h       = max((secs_next or 0.0) / 3600.0, 1.0 / 60.0)
            forward_apr  = forward_edge * (8760.0 / hold_h)
            if forward_edge > 0 and forward_apr >= MIN_FUNDING_APR:
                return                      # worth staying for the next one
            why = (f"next pays {next_payment:+.5f}%, convergence left "
                   f"{conv_left:+.5f}%, forward {forward_apr:+.0f}% APR "
                   f"< {MIN_FUNDING_APR:.0f}%")

        with cs.lock:
            if cs.open_position is None or cs.exit_pending:
                return
            cs.exit_pending = True
            pos_snap = cs.open_position
        reason = (f"NOT WORTH HOLDING  {why}  "
                  f"funding={collected:+.5f}% over {pos.get('stamps_crossed', 0)} stamp(s)  "
                  f"net={net:+.5f}%")
        threading.Thread(target=execute_exit, args=(cs, pos_snap, reason),
                         daemon=True).start()
        return

    # Reversion target: exit when REVERSION_FRACTION of entry deviation is gone
    # e.g. entry_dev=0.15%, REVERSION_FRACTION=0.70 → exit when abs_curr_dev <= 0.045%
    reversion_target = abs_entry_dev * (1.0 - REVERSION_FRACTION)

    # net >= 0 already ensures we're profitable after friction — no separate floor needed
    # reversion_target alone drives the exit: wait until REVERSION_FRACTION% has reverted
    exit_threshold   = reversion_target

    # Can 90% reversion cover friction? abs_entry_dev × RF >= RT%
    viable = (abs_entry_dev * REVERSION_FRACTION) >= round_trip_pct

    if viable:
        # Normal case — exit when REVERSION_FRACTION% has reverted
        should_exit = abs_curr_dev <= exit_threshold and net >= 0
        pct_reverted = ((abs_entry_dev - abs_curr_dev) / abs_entry_dev * 100
                        if abs_entry_dev > 0 else 0)
        reason = (f"REVERSION {pct_reverted:.0f}%  "
                  f"dev:{pos['entry_deviation']:+.5f}%→{curr_deviation:+.5f}%  "
                  f"net={net:+.5f}%")
    else:
        # Small entry — 90% reversion still can't cover friction
        # Exit the moment we hit MIN_NET_PCT instead of holding to timeout
        should_exit = net >= MIN_NET_PCT
        reason      = (f"MIN PROFIT EXIT  "
                       f"dev:{pos['entry_deviation']:+.5f}%→{curr_deviation:+.5f}%  "
                       f"net={net:+.5f}%")

    if should_exit:
        with cs.lock:
            if cs.open_position is None or cs.exit_pending:
                return
            cs.open_position["profit_target_hit"] = True
            cs.exit_pending = True
            pos_snap = cs.open_position
        t = threading.Thread(
            target=execute_exit, args=(cs, pos_snap, reason), daemon=True)
        t.start()

# ══════════════════════════════════════════════════════════════════════════════
# COMBINED STREAM WEBSOCKET — 2 threads total (1 spot + 1 perp) for all coins
# Supports both @bookTicker (tick-by-tick, 0ms buffer) and @depth20
# ══════════════════════════════════════════════════════════════════════════════

def _parse_levels(data):
    """Parse bids and asks from either bookTicker or depth20 payloads.
    Returns: (bids, asks) as lists of [float(price), float(qty)]
    """
    # 1. Spot depth20: {"bids": [["price","qty"],...], "asks": [...]}
    if "bids" in data and isinstance(data["bids"], list):
        bids = [[float(p), float(q)] for p, q in data["bids"]]
        asks = [[float(p), float(q)] for p, q in data.get("asks", [])]
        return bids, asks
    # 2. Futures depth20: {"b": [["price","qty"],...], "a": [...]}
    if "b" in data and isinstance(data["b"], list):
        bids = [[float(p), float(q)] for p, q in data["b"]]
        asks = [[float(p), float(q)] for p, q in data.get("a", [])]
        return bids, asks
    # 3. bookTicker (Spot or Futures): {"b": "price", "B": "qty", "a": "price", "A": "qty"}
    if "b" in data and "a" in data:
        try:
            b_p = float(data["b"])
            a_p = float(data["a"])
            b_q = float(data.get("B", 1.0))
            a_q = float(data.get("A", 1.0))
            return [[b_p, b_q]], [[a_p, a_q]]
        except (ValueError, TypeError):
            pass
    return [], []

def process_spot_tick(cs, data):
    bids, asks = _parse_levels(data)
    if not bids or not asks:
        return
    now = time.time()
    trigger_bucket = False
    with cs.lock:
        cs.latest["spot_bids"] = bids
        cs.latest["spot_asks"] = asks
        cs.latest["spot_ts"]   = now
        cs.spot_last_tick      = now
        cs.spot_ws_status      = "connected"
        cs.stats["total_updates"] += 1
        best_bid = bids[0][0];  best_ask = asks[0][0]
        cs.spot_ba_hist.append((best_ask - best_bid) / best_bid * 100)
        cs.spot_twap_buf.append((best_bid + best_ask) / 2.0)

        # Event-driven on-tick bucketing: trigger when bucket interval elapsed
        if (now - cs.last_bucket_ts) >= cs.bucket_sec:
            cs.last_bucket_ts = now
            trigger_bucket = True

        snap = {k: v for k, v in cs.latest.items()}

    if trigger_bucket:
        run_bucket(cs)

    check_exit_on_tick(cs, snap)
    check_entry_on_tick(cs, snap)

def process_perp_tick(cs, data):
    bids, asks = _parse_levels(data)
    if not bids or not asks:
        return
    now = time.time()
    trigger_bucket = False
    with cs.lock:
        cs.latest["perp_bids"] = bids
        cs.latest["perp_asks"] = asks
        cs.latest["perp_ts"]   = now
        cs.perp_last_tick      = now
        cs.perp_ws_status      = "connected"
        cs.stats["total_updates"] += 1
        best_bid = bids[0][0];  best_ask = asks[0][0]
        cs.perp_ba_hist.append((best_ask - best_bid) / best_bid * 100)
        cs.perp_twap_buf.append((best_bid + best_ask) / 2.0)

        # Event-driven on-tick bucketing: trigger when bucket interval elapsed
        if (now - cs.last_bucket_ts) >= cs.bucket_sec:
            cs.last_bucket_ts = now
            trigger_bucket = True

        snap = {k: v for k, v in cs.latest.items()}

    if trigger_bucket:
        run_bucket(cs)

    check_exit_on_tick(cs, snap)
    check_entry_on_tick(cs, snap)

def run_combined_ws(base_url, coin_map, leg, all_cs, name=None):
    """
    Single WS connection carrying one chunk of coins for one venue (spot or perp).
    coin_map: {"btcusdt": CoinState, "ethusdt": CoinState, ...}
    Reconnects automatically on any failure.

    Disconnect/reconnect handling touches ONLY the coins in coin_map. (It used to
    touch all_cs, so one of the 6 connections dropping marked all 144 coins offline,
    paused entries everywhere, and a >10s outage wiped every coin's warm-up.)

    Reconnect behaviour:
      • On disconnect: marks every coin's bucket_signal["ready"] = False immediately
        so no stale signals can fire while the stream is down.
      • On reconnect (on_open): measures the outage gap per coin.
          – Gap <= RECONNECT_SOFT_SEC (10s): stream came back quickly.
            Keep the existing rolling window — just resume ticking.
            The gap is short enough that the spread distribution hasn't
            meaningfully drifted, so re-warming from scratch wastes the
            history we already have.
          – Gap > RECONNECT_SOFT_SEC: outage was long (sleep, network drop).
            Clear buckets, twap buffers, and BA history so the coin
            re-warms on fresh data instead of using stale history as the
            basis for mean/std. Without this, the old mean is wrong and
            every coin looks like a 2σ+ signal the instant ticks resume.
    """
    RECONNECT_SOFT_SEC = 10   # gaps shorter than this → keep existing window

    if STREAM_TYPE == "bookTicker":
        streams = "/".join(f"{sym}@bookTicker" for sym in coin_map)
    elif leg == "perp":
        # Futures /public endpoint supports @depth20@100ms (20 levels, 100ms)
        streams = "/".join(f"{sym}@depth20@100ms" for sym in coin_map)
    else:
        streams = "/".join(f"{sym}@depth20@{DEPTH_STREAM_MS}ms" for sym in coin_map)
    url       = f"{base_url}/stream?streams={streams}"
    processor = process_spot_tick if leg == "spot" else process_perp_tick
    disc_attr = f"{leg}_disconnect_time"   # e.g. "spot_disconnect_time"
    chunk_cs  = list(coin_map.values())    # the coins carried by THIS connection
    name      = name or threading.current_thread().name
    feed      = FEEDS.setdefault(name, {
        "name": name, "leg": leg, "cs": chunk_cs, "coins": [cs.symbol for cs in chunk_cs],
        "status": "init", "opened_at": None, "last_msg": None, "msgs": 0, "rate": 0,
        "connects": 0, "errors": 0, "last_error": None, "last_error_at": None,
        "lag_ms": None, "lag_max_ms": None, "lag_sum": 0.0, "lag_n": 0, "lag_max": 0.0, "ws": None,
    })

    def on_open(ws):
        now = time.time()
        resumed = []
        cleared = []
        feed.update(status="connected", opened_at=now, last_msg=None)
        feed["connects"] += 1

        for cs in chunk_cs:
            with cs.lock:
                setattr(cs, f"{leg}_ws_status", "connected")
                disc_t = getattr(cs, disc_attr)

            if disc_t is None:
                # First-ever connect — normal cold start, nothing to do
                continue

            gap = now - disc_t

            if gap <= RECONNECT_SOFT_SEC:
                # Short outage — stream came back quickly.
                # Just clear the TWAP buffer (stale mid-prices from before
                # the drop shouldn't pollute the next bucket) but keep the
                # rolling window intact so we don't lose warm-up progress.
                with cs.lock:
                    cs.spot_twap_buf.clear()
                    cs.perp_twap_buf.clear()
                    # bucket_signal stays ready — first fresh tick will
                    # immediately re-enable gate checks as normal
                    cs.last_reconnect_time = now   # start entry cooldown
                resumed.append(cs.symbol)
            else:
                # Long outage — the spread distribution has drifted.
                # Clear everything so the coin re-warms on current data.
                with cs.lock:
                    cs.buckets.clear()
                    cs.spot_twap_buf.clear()
                    cs.perp_twap_buf.clear()
                    cs.spot_ba_hist.clear()
                    cs.perp_ba_hist.clear()
                    cs.bucket_signal["ready"] = False
                    cs.last_reconnect_time = now   # start entry cooldown
                cleared.append(f"{cs.symbol}({gap:.0f}s)")

        # Reset disconnect timestamps now that we're back up
        for cs in chunk_cs:
            with cs.lock:
                setattr(cs, disc_attr, None)

        if resumed or cleared:
            print(f"{Fore.GREEN}✅ {name} reconnected "
                  f"({len(coin_map)} coins) — {len(resumed)} resumed (short gap), "
                  f"{len(cleared)} re-warming{Style.RESET_ALL}")
            if cleared:
                print(f"{Fore.YELLOW}   Re-warming: {' '.join(cleared)}{Style.RESET_ALL}")
        else:
            print(f"{Fore.GREEN}✅ {name} connected "
                  f"({len(coin_map)} coins){Style.RESET_ALL}")
        # quiet coins get a quote now instead of whenever their book next changes
        threading.Thread(target=seed_books, args=(leg, coin_map, now), daemon=True).start()

    def _mark_disconnected(ws_event_name):
        """Shared logic for on_error and on_close."""
        now = time.time()
        feed["status"] = "retrying"
        for cs in chunk_cs:
            with cs.lock:
                # Only record the first disconnect event, not subsequent ones
                # while already disconnected
                if getattr(cs, disc_attr) is None:
                    setattr(cs, disc_attr, now)
                # Immediately block signal firing — stale mean is invalid
                cs.bucket_signal["ready"] = False
                setattr(cs, f"{leg}_ws_status", "retrying")

    def _count_error(e):
        feed["errors"] += 1
        feed["last_error"], feed["last_error_at"] = str(e)[:160], time.time()
        for cs in chunk_cs:
            with cs.lock:
                setattr(cs, f"{leg}_ws_errors", getattr(cs, f"{leg}_ws_errors") + 1)

    def on_error(ws, e):
        print(f"{Fore.RED}⚠️  {name} error: {e}{Style.RESET_ALL}")
        _mark_disconnected("error")
        _count_error(e)

    def on_close(ws, code, msg):
        print(f"{Fore.YELLOW}↩️  {name} closed — reconnecting...{Style.RESET_ALL}")
        _mark_disconnected("close")

    def on_message(ws, message):
        now = time.time()
        feed["last_msg"] = now
        feed["msgs"] += 1
        try:
            outer       = json.loads(message)
            stream_name = outer.get("stream", "")
            data        = outer.get("data", outer)
            sym         = stream_name.split("@")[0]   # "btcusdt"
            cs          = coin_map.get(sym)
            if cs is not None:
                ev = data.get("E")                    # perp event time → how far behind real time we are
                if ev:
                    lag = now * 1000 + CLOCK_OFFSET_MS - ev
                    feed["lag_sum"] += lag
                    feed["lag_n"]   += 1
                    if lag > feed["lag_max"]:
                        feed["lag_max"] = lag
                processor(cs, data)
        except Exception as _e:
            print(f"{Fore.RED}[{name}] on_message error: {_e} | raw={message[:80]}{Style.RESET_ALL}")

    backoff = 1.0
    while True:
        started = time.time()
        try:
            ws = websocket.WebSocketApp(
                url,
                on_open=on_open,
                on_message=on_message,
                on_error=on_error,
                on_close=on_close,
            )
            feed["ws"] = ws
            # skip_utf8_validation: websocket-client otherwise checks every message byte-by-byte
            # in pure Python (~20µs/msg, ~25% of the GIL at burst rates). json.loads still
            # rejects invalid UTF-8, in C. on_message then receives bytes, which json.loads takes.
            ws.run_forever(ping_interval=20, ping_timeout=10,
                           sslopt={"ca_certs": certifi.where()}, skip_utf8_validation=True)
        except Exception as e:
            print(f"{Fore.RED}{name} exception: {e}{Style.RESET_ALL}")
            _mark_disconnected("exception")
            _count_error(e)
        if feed["status"] == "connected":             # run_forever returned without a callback
            _mark_disconnected("return")
        # retry fast so a blip stays under RECONNECT_SOFT_SEC and keeps the warm-up;
        # back off (to the original 5s) only if the connection keeps failing
        if time.time() - started > 60:
            backoff = 1.0
        time.sleep(backoff)
        backoff = min(backoff * 2, 5.0)

# ══════════════════════════════════════════════════════════════════════════════
# BUCKET ENGINE
# ══════════════════════════════════════════════════════════════════════════════

def _fire_timeout_exit(cs, pos_snap, hold_sec):
    reason = f"TIMEOUT ({hold_sec:.0f}s)"
    threading.Thread(target=execute_exit, args=(cs, pos_snap, reason), daemon=True).start()


def run_bucket(cs):
    """Run one bucket cycle; skip if another thread is already inside it for this coin
    (spot and perp WS threads both trigger buckets for the same CoinState)."""
    if not cs.bucket_lock.acquire(blocking=False):
        return
    try:
        process_bucket(cs)
        update_slip_buffer(cs)
    finally:
        cs.bucket_lock.release()


def process_bucket(cs):
    # ── Timeout check FIRST — runs even if no ticks arrived this bucket ────────
    now = time.time()
    with cs.lock:
        pos = cs.open_position
        ep  = cs.exit_pending
    if pos is not None and not ep:
        hold_sec = now - pos["entry_time"]
        if hold_sec >= max_hold_for(pos):
            with cs.lock:
                if cs.open_position is not None and not cs.exit_pending:
                    cs.exit_pending = True
                    pos_snap = cs.open_position
                    _fire_timeout_exit(cs, pos_snap, hold_sec)
    # ── Now do the normal bucket data processing ───────────────────────────────
    with cs.lock:
        snap  = {k: v for k, v in cs.latest.items()}
        b_idx = cs.stats["bucket_index"]
        cs.stats["bucket_index"] += 1
        s_buf = cs.spot_twap_buf.copy();  cs.spot_twap_buf.clear()
        p_buf = cs.perp_twap_buf.copy();  cs.perp_twap_buf.clear()

    now = time.time()
    spot_ts = snap["spot_ts"];  perp_ts = snap["perp_ts"]

    # If TWAP buffers are empty this cycle (bucket fired before any new ticks
    # arrived), fall back to best bid/ask mid from the latest book snapshot.
    # This keeps warmup progressing every bucket instead of silently skipping.
    if not s_buf and snap["spot_bids"] and snap["spot_asks"]:
        sb = snap["spot_bids"][0][0]; sa = snap["spot_asks"][0][0]
        s_buf = [(sb + sa) / 2.0]
    if not p_buf and snap["perp_bids"] and snap["perp_asks"]:
        pb = snap["perp_bids"][0][0]; pa = snap["perp_asks"][0][0]
        p_buf = [(pb + pa) / 2.0]

    if not s_buf or not p_buf or spot_ts is None or perp_ts is None:
        return
    stale_limit = max(10.0, cs.bucket_sec * 30)
    if (now - spot_ts) > stale_limit or (now - perp_ts) > stale_limit:
        return

    spot_mid   = float(np.mean(s_buf))   # s_buf contains mid prices from twap_buf
    perp_mid   = float(np.mean(p_buf))
    spread_pct = (perp_mid - spot_mid) / spot_mid * 100

    cs.buckets.append({"ts": now, "spot_mid": spot_mid, "perp_mid": perp_mid,
                        "spread_pct": spread_pct, "b_idx": b_idx})

    with cs.lock:
        cs.stats["total_buckets"] += 1

    round_trip_pct = get_round_trip_pct(cs)
    with cs.lock:
        cs.stats["live_round_trip"] = round_trip_pct

    roll_mean, roll_std = get_rolling_stats(cs)

    if roll_mean is None:
        with cs.lock:
            cs.bucket_signal["ready"] = False
        return

    upper   = roll_mean + SD_THRESHOLD * roll_std
    lower   = roll_mean - SD_THRESHOLD * roll_std
    with cs.lock:
        slip_buf = cs.stats["slip_buffer"]
    min_dev = round_trip_pct + slip_buf

    with cs.lock:
        cs.bucket_signal.update({
            "roll_mean": roll_mean, "roll_std": roll_std,
            "upper": upper, "lower": lower,
            "round_trip": round_trip_pct, "min_dev": min_dev,
            "ready": True,
        })

    # Timeout exit is handled at the top of process_bucket

# ══════════════════════════════════════════════════════════════════════════════
# GLOBAL STATS PRINTER
# ══════════════════════════════════════════════════════════════════════════════

def print_global_stats(all_cs):
    while True:
        time.sleep(STATS_INTERVAL)

        now_str = datetime.now().strftime("%H:%M:%S")
        vw_pct  = get_vwap_net_pnl_pct()
        with global_lock:
            g_closed = global_stats["total_closed"]
            g_wins   = global_stats["total_profit"]
            g_losses = global_stats["total_loss"]
            g_usd    = global_stats["total_net_pnl_usd"]
            g_notl   = global_stats["sum_notional"]

        win_rate = (g_wins / g_closed * 100) if g_closed > 0 else 0.0
        g_clr    = Fore.GREEN if g_usd >= 0 else Fore.RED

        print(f"\n{Fore.CYAN}{'═'*76}")
        print(f"  🌐 GLOBAL STATS [{now_str}]  —  {len(all_cs)} coins  |  "
              f"uptime={( time.time()-all_cs[0].stats['start_time'])/60:.1f}min")
        print(f"  {'─'*72}")
        print(f"  Trades closed  : {g_closed}  (✅ {g_wins}  ❌ {g_losses}  win%={win_rate:.0f}%)")
        print(f"  Total notional : ${g_notl:,.2f} traded")
        print(f"  {g_clr}VWAP Net PnL%  : {vw_pct:+.6f}%  ← volume-weighted across all coins{Style.RESET_ALL}")
        print(f"  {g_clr}Net PnL USD    : ${g_usd:+.4f}{Style.RESET_ALL}")
        print(f"  {'─'*72}")
        print(f"  {'COIN':<8} {'Closed':>7} {'W':>4} {'L':>4} {'Net%':>12} {'Net$':>10} {'RT%':>9}  {'Status':<12} WS")
        print(f"  {'─'*76}")

        now_ts = time.time()
        for cs in sorted(all_cs, key=lambda c: c.stats["signals_fired"], reverse=True):
            with cs.lock:
                closed      = cs.stats["trades_closed"]
                wins        = cs.stats["trades_profit"]
                losses      = cs.stats["trades_loss"]
                net_pct     = cs.stats["total_net_pnl_pct"]
                net_usd     = cs.stats["total_net_pnl_usd"]
                live_rt     = cs.stats["live_round_trip"]
                n_bkts      = cs.stats["total_buckets"]
                pos         = cs.open_position
                ready       = cs.bucket_signal["ready"]
                s_status    = cs.spot_ws_status
                p_status    = cs.perp_ws_status
                s_errors    = cs.spot_ws_errors
                p_errors    = cs.perp_ws_errors
                s_last      = cs.spot_last_tick
                p_last      = cs.perp_last_tick

            warm_pct = min(100, n_bkts / cs.rolling_win * 100)

            # Staleness check — no tick in >30s even though "connected"
            s_age  = f"{now_ts - s_last:.0f}s" if s_last else "never"
            p_age  = f"{now_ts - p_last:.0f}s" if p_last else "never"
            stale  = (s_last is None or p_last is None or
                      (now_ts - s_last) > 30 or (now_ts - p_last) > 30)

            if s_errors > 0 or p_errors > 0:
                ws_tag = (f"{Fore.RED}S:{s_status[:4]}(x{s_errors}) "
                          f"P:{p_status[:4]}(x{p_errors}){Style.RESET_ALL}")
            elif stale:
                ws_tag = f"{Fore.YELLOW}STALE  S:{s_age}  P:{p_age}{Style.RESET_ALL}"
            elif s_status == "connected" and p_status == "connected":
                ws_tag = f"{Fore.GREEN}✅ both live{Style.RESET_ALL}"
            else:
                ws_tag = f"{Fore.YELLOW}S:{s_status}  P:{p_status}{Style.RESET_ALL}"

            tier_lbl = ("🐢" if cs.bucket_sec >= 5.0
                         else "🐇" if cs.bucket_sec <= 0.5
                         else "🦆")
            if not ready:
                status = f"{tier_lbl}⏳ {warm_pct:.0f}%"
            elif pos is not None:
                d_str = "▲" if pos["direction"] == 1 else "▼"
                status = f"{tier_lbl}📊 {d_str} OPEN"
            else:
                status = f"{tier_lbl}⚪ FLAT"

            with cs.lock:
                slip_buf  = cs.stats["slip_buffer"]
                n_slips   = len(cs.slip_history)
                fired     = cs.stats["signals_fired"]
            if fired < 20:
                gate3_tag = f"{Fore.YELLOW}G3:off(<20sig){Style.RESET_ALL}"
            elif n_slips < 5:
                gate3_tag = f"{Fore.YELLOW}G3:warming({n_slips}/5){Style.RESET_ALL}"
            else:
                gate3_tag = f"{Fore.CYAN}G3:+{slip_buf:.5f}% ({n_slips}smp){Style.RESET_ALL}"

            pclr = Fore.GREEN if net_usd >= 0 else (Fore.RED if net_usd < 0 else "")
            print(f"  {cs.symbol:<8} {closed:>7} {wins:>4} {losses:>4} "
                  f"{pclr}{net_pct:>+12.6f}%{Style.RESET_ALL} "
                  f"{pclr}{net_usd:>+10.4f}{Style.RESET_ALL} "
                  f"{live_rt:>8.5f}%  {status:<12} {ws_tag}  {gate3_tag}")

        print(f"{Fore.CYAN}{'═'*76}{Style.RESET_ALL}\n")

# ══════════════════════════════════════════════════════════════════════════════
# MAIN
# ══════════════════════════════════════════════════════════════════════════════

# ══════════════════════════════════════════════════════════════════════════════
# WATCHDOG — checks every second for stuck open positions and fallback bucketing
# ══════════════════════════════════════════════════════════════════════════════

def position_watchdog(all_cs):
    """Independent 1-second loop:
       1) Fallback bucketing trigger for coins with slow tick rates so stats progress.
       2) Forces exit on any position past MAX_HOLD_SEC or recovers missed profit exits.
    """
    while True:
        time.sleep(1)
        now = time.time()
        for cs in all_cs:
            # 1. Fallback bucketing if ticks are slow but connection is active
            # (quote timestamps, not last-tick times: feed_monitor keeps a quiet coin's
            #  quote current while its connection is live)
            trigger_bkt = False
            with cs.lock:
                if (now - cs.last_bucket_ts) >= cs.bucket_sec:
                    s_last = cs.latest["spot_ts"] or 0
                    p_last = cs.latest["perp_ts"] or 0
                    if (now - s_last) < 30 and (now - p_last) < 30:
                        cs.last_bucket_ts = now
                        trigger_bkt = True
            if trigger_bkt:
                run_bucket(cs)

            # 2. Settle funding for any stamp that passed while we were holding
            try:
                accrue_funding(cs)
            except Exception as e:
                print(f"{Fore.RED}[watchdog] funding accrual {cs.symbol}: {e}{Style.RESET_ALL}")

            # 3. Position timeout / watchdog recovery
            with cs.lock:
                pos = cs.open_position
                ep  = cs.exit_pending
            if pos is None or ep:
                continue
            hold_sec          = now - pos["entry_time"]
            profit_target_hit = pos.get("profit_target_hit", False)
            hold_limit        = max_hold_for(pos)

            # Force exit if: past timeout OR profit target was hit but exit failed
            if hold_sec < hold_limit and not profit_target_hit:
                continue

            with cs.lock:
                if cs.open_position is None or cs.exit_pending:
                    continue
                cs.exit_pending = True
                pos_snap = cs.open_position

            if profit_target_hit and hold_sec < hold_limit:
                reason = f"WATCHDOG PROFIT RECOVERY (target was hit, exit had failed)"
                print(f"\n{Fore.GREEN}  ✅ [{cs.symbol}] WATCHDOG recovering missed "
                      f"profit exit — hold={hold_sec:.0f}s{Style.RESET_ALL}")
            else:
                overshoot = hold_sec - hold_limit
                reason = f"WATCHDOG TIMEOUT ({hold_sec:.0f}s, +{overshoot:.0f}s overshoot)"
                print(f"\n{Fore.YELLOW}  ⚠️  [{cs.symbol}] WATCHDOG forcing exit — "
                      f"held {hold_sec:.0f}s (max={hold_limit:.0f}s){Style.RESET_ALL}")

            threading.Thread(
                target=execute_exit,
                args=(cs, pos_snap, reason),
                daemon=True
            ).start()


def _chunk_list(lst, n):
    """Split a list into chunks of at most n items."""
    for i in range(0, len(lst), n):
        yield lst[i:i + n]


COINS_PER_WS = 50   # Binance URL length limit — keep well under 200 streams/conn


def start_engine(console_stats=True, connect_ws=True):
    """Start all WS + watchdog threads and return the list of CoinState. Non-blocking.
    Idempotent: a second call returns the already-running engine.
    connect_ws=False skips the Binance streams (demo mode feeds ticks in itself)."""
    global ALL_CS, ENGINE_START
    if ALL_CS:
        return ALL_CS
    os.makedirs(OUTPUT_DIR, exist_ok=True)

    stream_desc = "real-time tick-by-tick (0ms delay)" if STREAM_TYPE == "bookTicker" else f"depth20@{DEPTH_STREAM_MS}ms"
    entry_desc  = "0ms (Zero-sleep immediate execution)" if ENTRY_DELAY_SEC == 0 else f"{ENTRY_DELAY_SEC*1000:.1f}ms"
    exit_desc   = "0ms (Zero-sleep immediate execution)" if EXIT_DELAY_SEC == 0 else f"{EXIT_DELAY_SEC*1000:.1f}ms"
    strat_desc  = (f"{STRATEGY}  (window {FUNDING_ENTRY_WINDOW_SEC/60:.0f}min, "
                   f"min rate {MIN_FUNDING_PCT:.4f}%, min APR {MIN_FUNDING_APR:.0f}%, "
                   f"edge >= {EDGE_FRICTION_MULT:.1f}x friction)")
    edge_desc   = (f"funding + convergence ({REVERSION_FRACTION*100:.0f}% of deviation), "
                   f"adverse basis allowed when funding covers it")
    stop_desc   = (f"{STOP_LOSS_FUNDING_MULT:.1f}x funding collected "
                   f"(floor {STOP_LOSS_MIN_PCT:.3f}%)")
    fee_maker   = 2 * (leg_fee_pct("spot", "maker") + leg_fee_pct("perp", "maker"))
    fee_taker   = 2 * (leg_fee_pct("spot", "taker") + leg_fee_pct("perp", "taker"))
    fee_desc    = f"{FEE_TIER}  round-trip maker={fee_maker:.5f}%  taker={fee_taker:.5f}%"
    exec_desc   = (f"both legs at market after {ENTRY_DELAY_SEC*1000:.0f}ms, "
                   f"charged at {FILL_FEE_TYPE} fees")

    print(f"""
{Fore.CYAN}{'═'*76}
  Multi-Coin Paper Trading — Spot-Perp Mean Reversion (Tick-by-Tick WS)
  Coins          : {len(COINS)}
  Stream type    : {STREAM_TYPE} ({stream_desc})
  Entry delay    : {entry_desc}
  Exit delay     : {exit_desc}
  Window         : {ROLLING_WIN} x {BUCKET_SIZE_SEC}s = {ROLLING_WIN*BUCKET_SIZE_SEC:.0f}s
  SD gate        : {SD_THRESHOLD}sigma
  Strategy       : {strat_desc}
  Edge           : {edge_desc}
  Stop loss      : {stop_desc}
  Fee tier       : {fee_desc}
  Execution      : {exec_desc}
  Exit mode      : DYNAMIC REVERSION ({REVERSION_FRACTION*100:.0f}% of deviation)
  Max hold       : {MAX_HOLD_SEC:.0f}s
  Bucketing      : Event-driven on-tick (0 sleeping threads)
  Sizing         : Book-walk  min=${MIN_NOTIONAL_USD:.0f}  max=unlimited (liquidity-capped)  steps={NOTIONAL_STEPS}
  Coins/WS conn  : {COINS_PER_WS} (chunked to avoid Binance URL length limit)
  Net PnL shown  : VOLUME-WEIGHTED (notional-weighted avg across all coins)
  Master CSV     : {MASTER_CSV_PATH}
{'═'*76}{Style.RESET_ALL}
""")

    all_cs = [CoinState(sym) for sym in COINS]
    ALL_CS       = all_cs
    ENGINE_START = time.time()

    # Build symbol->CoinState lookup maps
    spot_coin_map = {f"{cs.symbol.lower()}usdt": cs for cs in all_cs}
    perp_coin_map = {f"{cs.symbol.lower()}usdt": cs for cs in all_cs}

    # Split into chunks of COINS_PER_WS to stay within Binance URL length limit.
    # Each chunk gets its own WS thread — all chunks share the same all_cs list so
    # reconnect logic (ready=False, disconnect timestamps) still works correctly.
    spot_chunks = list(_chunk_list(list(spot_coin_map.items()), COINS_PER_WS)) if connect_ws else []
    perp_chunks = list(_chunk_list(list(perp_coin_map.items()), COINS_PER_WS)) if connect_ws else []

    n_spot_ws = len(spot_chunks)
    n_perp_ws = len(perp_chunks)

    for i, chunk in enumerate(spot_chunks):
        chunk_map = dict(chunk)
        threading.Thread(
            target=run_combined_ws,
            args=("wss://stream.binance.com:9443", chunk_map, "spot", all_cs, f"spot-ws-{i}"),
            name=f"spot-ws-{i}",
            daemon=True,
        ).start()

    for i, chunk in enumerate(perp_chunks):
        chunk_map = dict(chunk)
        threading.Thread(
            target=run_combined_ws,
            args=("wss://fstream.binance.com/public", chunk_map, "perp", all_cs, f"perp-ws-{i}"),
            name=f"perp-ws-{i}",
            daemon=True,
        ).start()
    if connect_ws:
        threading.Thread(target=feed_monitor, name="feed-monitor", daemon=True).start()
        if STRATEGY in ("funding", "both"):
            fetch_funding_intervals(all_cs)
            threading.Thread(target=run_funding_poller, args=(all_cs,),
                             name="funding-rest", daemon=True).start()

    if connect_ws:
        print(f"{Fore.GREEN}▶ Streams launched — {len(all_cs)} coins across "
              f"{n_spot_ws} spot WS + {n_perp_ws} perp WS threads{Style.RESET_ALL}")
    else:
        print(f"{Fore.YELLOW}▶ No exchange connection — ticks are injected by the caller (demo mode){Style.RESET_ALL}")
    print(f"{Fore.GREEN}▶ Event-driven on-tick bucketing active (0 sleeping bucket threads){Style.RESET_ALL}\n")

    if console_stats:
        threading.Thread(target=print_global_stats, args=(all_cs,), daemon=True).start()
    threading.Thread(target=position_watchdog, args=(all_cs,), daemon=True).start()

    return all_cs


def main():
    start_engine(console_stats=True)
    try:
        while True:
            time.sleep(60)   # main thread just keeps process alive

    except KeyboardInterrupt:
        vw_pct = get_vwap_net_pnl_pct()
        with global_lock:
            g_closed = global_stats["total_closed"]
            g_wins   = global_stats["total_profit"]
            g_usd    = global_stats["total_net_pnl_usd"]
            g_notl   = global_stats["sum_notional"]
        print(f"\n{Fore.YELLOW}Stopped.{Style.RESET_ALL}")
        print(f"  Coins          : {len(COINS)}")
        print(f"  Trades closed  : {g_closed}  |  Wins: {g_wins}")
        print(f"  Total notional : ${g_notl:,.2f}")
        print(f"  VWAP Net PnL%  : {vw_pct:+.6f}%")
        print(f"  Net PnL USD    : ${g_usd:+.4f}")
        print(f"  Master CSV     : {MASTER_CSV_PATH}")

if __name__ == "__main__":
    main()
