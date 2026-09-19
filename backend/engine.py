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

COINS = [
    # 143 coins — scanner-validated (both spot+perp BA < 0.10%)
    # "币安人生" excluded (non-ASCII ticker); USDC removed (stablecoin — USDCUSDT is pegged, nothing to revert)
    "BTC", "PAXG", "XAUT", "ETH",
    "BNB", "ZEC", "XRP", "XPL", "AAVE",
    "DOGE", "LINK", "HBAR", "POL", "SUI",
    "SOL", "JTO", "AVAX", "PENGU", "TRX",
    "SEI", "ALLO", "VIRTUAL", "WLD", "SAND",
    "INJ", "LTC", "GENIUS", "TAO", "RE",
    "TIA", "SKY", "BCH", "DASH", "XLM",
    "NIGHT", "MEGA", "ONDO", "VET", "NEIRO",
    "ESP", "UNI", "ONT", "ZEN", "JST",
    "2Z", "BOME", "LDO", "GIGGLE", "TRB",
    "BANANAS31", "MORPHO", "GALA", "API3", "PENDLE",
    "LINEA", "SYRUP", "JUP", "EIGEN", "QNT",
    "PYTH", "AIGENSYN", "ICP", "FF", "CFX",
    "ORDI", "WCT", "XTZ", "BICO", "HOME",
    "ZAMA", "S", "CAKE", "DEXE", "ENJ",
    "CHIP", "SCR", "MMT", "CRV", "DYDX",
    "FLOW", "NEAR", "BIO", "AR", "SYN",
    "CHZ", "EDEN", "NXPC", "CGPT", "OPEN",
    "KAVA", "BANANA", "FET", "STX", "TRUMP",
    "HAEDAL", "ATOM", "LPT", "COMP", "MET",
    "VANRY", "RENDER", "KAITO", "OGN", "OPG",
    "YGG", "ASTR", "WIF", "BAND", "ADA",
    "HMSTR", "BLUR", "SFP", "TST", "CELO",
    "CFG", "BARD", "HUMA", "APE", "VELODROME",
    "SENT", "THETA", "IMX", "PORTAL", "BEAMX",
    "BABY", "ZBT", "EUL", "PUMP", "RSR",
    "SAHARA", "ALICE", "FOGO", "ARKM", "BMT",
    "ORCA", "ARK", "VANA", "OP", "ZK",
    "AXS", "AVNT", "CVC", "GAS",
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

EXCHANGE_FEE_PCT   = 0.05865
REVERSION_FRACTION = 0.90    # exit when this fraction of entry deviation has reverted
                                    # 0.90 = captures borderline trades just above friction
                                    # tune range: 0.85 (faster exit) ↔ 0.95 (max profit, slower)
MIN_NET_PCT        = 0.001   # fallback exit for tiny entries: if 90% reversion still
                                    # can't cover friction, exit at this minimum net profit
MAX_HOLD_SEC       = 180.0
POST_RECONNECT_COOLDOWN_SEC = 10.0  # block new entries for N seconds after any reconnect

# Latency simulation: default 0.0s for immediate zero-sleep execution (override via env var if testing artificial latency)
ENTRY_DELAY_SEC    = float(os.environ.get("ENTRY_DELAY_SEC", 0.0))
EXIT_DELAY_SEC     = float(os.environ.get("EXIT_DELAY_SEC", 0.0))

# Stream mode: "bookTicker" (real-time tick-by-tick, 0ms buffer) or "depth20" (100ms snapshot)
STREAM_TYPE        = os.environ.get("STREAM_TYPE", "bookTicker").strip()

# ── Dynamic sizing ────────────────────────────────────────────────────────────
MIN_NOTIONAL_USD   = 10.0        # never enter below this size
MAX_NOTIONAL_USD   = float('inf')  # no cap — book-walk limited only by available liquidity
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
    # Cap at actual book liquidity on both sides — no artificial notional ceiling
    max_available  = min(spot_total_usd, perp_total_usd)

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

# ── retained helpers ──────────────────────────────────────────────────────────

def get_fill_snap(cs):
    with cs.lock:
        return {k: v for k, v in cs.latest.items()}

def get_round_trip_pct(cs):
    sba = list(cs.spot_ba_hist)
    pba = list(cs.perp_ba_hist)
    sm  = float(np.mean(sba)) if len(sba) >= 5 else 0.0
    pm  = float(np.mean(pba)) if len(pba) >= 5 else 0.0
    return sm + pm + EXCHANGE_FEE_PCT

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

def calc_pnl(pos, exit_spot, exit_perp, round_trip_pct):
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
    net   = gross - round_trip_pct
    usd   = net / 100 * pos["notional_usd"]
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
          f"(spot_ba + perp_ba + {EXCHANGE_FEE_PCT}%)  "
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
                  roll_mean, roll_std, upper, lower, min_dev, round_trip_pct):
    t0 = time.time()
    if ENTRY_DELAY_SEC > 0:
        time.sleep(ENTRY_DELAY_SEC)
    delay_ms = (time.time() - t0) * 1000

    snap = get_fill_snap(cs)

    # ── L20 book walk: find optimal notional ─────────────────────────────────
    notional, entry_spot, entry_perp, entry_slip_pct = find_optimal_notional(
        snap, direction, roll_mean, round_trip_pct
    )

    if notional <= 0 or entry_spot is None or entry_perp is None:
        with cs.lock:
            cs.entry_pending = False
        return

    # Recompute fill spread/deviation from the VWAP prices
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
        }
        cs.entry_pending = False

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
    snap = get_fill_snap(cs)
    exit_spot, exit_perp = get_exit_vwap(snap, pos["direction"], pos["notional_usd"])
    if None in (exit_spot, exit_perp):
        with cs.lock:
            cs.exit_pending = False
        return

    gross, net, usd = calc_pnl(pos, exit_spot, exit_perp, round_trip_pct)
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

def check_entry_on_tick(cs, snap):
    with cs.lock:
        if cs.open_position is not None or cs.entry_pending:
            return
        bs         = cs.bucket_signal.copy()
        round_trip = cs.stats["live_round_trip"]
        last_recon = cs.last_reconnect_time

    if not ENTRIES_ENABLED.is_set():
        return

    if not bs["ready"]:
        return

    # Block new entries for POST_RECONNECT_COOLDOWN_SEC after any reconnect —
    # avoids trading on the first few noisy/stale ticks right after a stream
    # outage, before the book and rolling stats have stabilised again.
    if last_recon is not None and (time.time() - last_recon) < POST_RECONNECT_COOLDOWN_SEC:
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
        if hold_sec >= MAX_HOLD_SEC:
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

            # 2. Position timeout / watchdog recovery
            with cs.lock:
                pos = cs.open_position
                ep  = cs.exit_pending
            if pos is None or ep:
                continue
            hold_sec          = now - pos["entry_time"]
            profit_target_hit = pos.get("profit_target_hit", False)

            # Force exit if: past timeout OR profit target was hit but exit failed
            if hold_sec < MAX_HOLD_SEC and not profit_target_hit:
                continue

            with cs.lock:
                if cs.open_position is None or cs.exit_pending:
                    continue
                cs.exit_pending = True
                pos_snap = cs.open_position

            if profit_target_hit and hold_sec < MAX_HOLD_SEC:
                reason = f"WATCHDOG PROFIT RECOVERY (target was hit, exit had failed)"
                print(f"\n{Fore.GREEN}  ✅ [{cs.symbol}] WATCHDOG recovering missed "
                      f"profit exit — hold={hold_sec:.0f}s{Style.RESET_ALL}")
            else:
                overshoot = hold_sec - MAX_HOLD_SEC
                reason = f"WATCHDOG TIMEOUT ({hold_sec:.0f}s, +{overshoot:.0f}s overshoot)"
                print(f"\n{Fore.YELLOW}  ⚠️  [{cs.symbol}] WATCHDOG forcing exit — "
                      f"held {hold_sec:.0f}s (max={MAX_HOLD_SEC:.0f}s){Style.RESET_ALL}")

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

    print(f"""
{Fore.CYAN}{'═'*76}
  Multi-Coin Paper Trading — Spot-Perp Mean Reversion (Tick-by-Tick WS)
  Coins          : {len(COINS)}
  Stream type    : {STREAM_TYPE} ({stream_desc})
  Entry delay    : {entry_desc}
  Exit delay     : {exit_desc}
  Window         : {ROLLING_WIN} x {BUCKET_SIZE_SEC}s = {ROLLING_WIN*BUCKET_SIZE_SEC:.0f}s
  SD gate        : {SD_THRESHOLD}sigma
  Exchange fee   : {EXCHANGE_FEE_PCT:.5f}%
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
