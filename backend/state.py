"""
Read-only view of the running engine for the dashboard.

`build_state()` copies what it needs from each CoinState under that coin's lock
(never holding it while doing maths), so it can't slow the tick path. A single
`Broadcaster` thread builds the state once a second and fans it out to every
SSE client, so N open browser tabs cost the same as one.
"""
import json
import math
import threading
import time

from . import engine as E

STALE_SEC = 30          # grace period after start before a never-ticked leg is called "stale"


def _f(x, nd=6):
    """Round a float; NaN/inf/None → None so the payload is always valid JSON."""
    if x is None:
        return None
    try:
        if not math.isfinite(x):
            return None
    except TypeError:
        return None
    return round(x, nd)


def _spread_pct(latest):
    sm, pm = E._mid(latest, "spot"), E._mid(latest, "perp")
    if sm is None or pm is None:
        return None, sm, pm
    return (pm - sm) / sm * 100, sm, pm


def _coin_row(cs, now, feed_of):
    with cs.lock:
        latest    = dict(cs.latest)                 # book lists are replaced, never mutated
        st        = dict(cs.stats)
        bs        = dict(cs.bucket_signal)
        pos       = dict(cs.open_position) if cs.open_position else None
        s_status  = cs.spot_ws_status
        p_status  = cs.perp_ws_status
        s_last    = cs.spot_last_tick
        p_last    = cs.perp_last_tick
        s_err     = cs.spot_ws_errors
        p_err     = cs.perp_ws_errors
        n_slips   = len(cs.slip_history)
        last_rec  = cs.last_reconnect_time
    n_buckets = len(cs.buckets)
    fv  = E.funding_view(cs)         # (rate_pct, secs_to_stamp, interval_h) or None
    vol = E.basis_volatility(cs)     # how far this coin's gap normally travels
    # How many of this coin's own standard deviations the stop sits away. Under
    # MIN_STOP_SIGMAS the gap routinely travels further than funding can pay for.
    stop_sigmas = None
    if fv and vol and vol > 0:
        _stop = max(E.STOP_LOSS_FUNDING_MULT * abs(fv[0]), E.STOP_LOSS_MIN_PCT)
        stop_sigmas = _stop / vol

    spread, sm, pm = _spread_pct(latest)
    ready = bool(bs["ready"])
    mean, std = bs["roll_mean"], bs["roll_std"]
    z = None
    if ready and spread is not None and std:
        z = (spread - mean) / std

    # quote age = how current our view of the book is. On a live feed the engine keeps a
    # quiet coin's quote current (bookTicker only pushes changes), so this stays ~1s even
    # when the book itself hasn't moved for minutes (that is `*_quiet`).
    s_ts, p_ts = latest["spot_ts"], latest["perp_ts"]
    seen_both = s_ts is not None and p_ts is not None
    unlisted = [leg for leg in ("spot", "perp") if cs.symbol in E.UNLISTED[leg]]
    # "init" = not opened yet (still connecting); only retrying/error mean the stream dropped
    ws_down = s_status in ("retrying", "error") or p_status in ("retrying", "error")
    # Same freshness limit process_bucket() uses before it skips a bucket for stale data.
    stale_limit = max(10.0, cs.bucket_sec * 30)
    started = E.ENGINE_START or now
    if pos is not None:
        state = "open"
    elif unlisted:
        state = "unlisted"                                           # can never trade
    elif ws_down:
        state = "offline"
    elif not seen_both:
        # connected, but a leg has never had a quote: give it a grace period, then call it stale
        state = "connecting" if (now - started) < STALE_SEC else "stale"
    elif (now - s_ts) > stale_limit or (now - p_ts) > stale_limit:
        state = "stale"                                              # its feed has stopped delivering
    elif not ready:
        state = "warming"
    else:
        state = "flat"
    s_feed, p_feed = feed_of.get(("spot", cs.symbol)), feed_of.get(("perp", cs.symbol))

    position = None
    if pos is not None:
        rt = st["live_round_trip"]
        es, ep = E.get_exit_vwap(latest, pos["direction"], pos["notional_usd"])
        gross = net = usd = None
        if es is not None and ep is not None:
            gross, net, usd = E.calc_pnl(pos, es, ep, rt)
        curr_dev = None
        if spread is not None:
            curr_dev = spread - pos["entry_mean"]
        abs_entry = abs(pos["entry_deviation"])
        reverted = None
        if curr_dev is not None and abs_entry > 0:
            reverted = (abs_entry - abs(curr_dev)) / abs_entry * 100
        best = pos.get("best_pnl", -999.0)
        position = {
            "direction"     : pos["direction"],
            "action"        : pos["action"],
            "notional_usd"  : _f(pos["notional_usd"], 2),
            "hold_sec"      : _f(now - pos["entry_time"], 1),
            "entry_dt"      : pos["entry_dt"],
            "entry_dev_pct" : _f(pos["entry_deviation"]),
            "curr_dev_pct"  : _f(curr_dev),
            "reverted_pct"  : _f(reverted, 1),
            "gross_pct"     : _f(gross),
            "net_pct"       : _f(net),
            "net_usd"       : _f(usd, 4),
            "best_net_pct"  : _f(best) if best > -900 else None,
            "max_hold_sec"  : E.max_hold_for(pos),
            "trade_kind"    : pos.get("trade_kind", "spread"),
            "funding_entry_pct"    : _f(pos.get("funding_pct_at_entry", 0.0)),
            "funding_collected_pct": _f(pos.get("funding_collected_pct", 0.0)),
            "stamps_crossed"       : pos.get("stamps_crossed", 0),
            "convergence_edge_pct" : _f(pos.get("convergence_edge_pct", 0.0)),
            "stop_loss_pct"        : _f(pos.get("stop_loss_pct", 0.0)),
            "secs_to_stamp"        : _f(max(0.0, (pos["next_funding_ms"] / 1000.0 - now)), 0)
                                     if pos.get("next_funding_ms") else None,
            "entry_maker_legs"     : sum(1 for t in (pos.get("entry_spot_fill_type"),
                                                     pos.get("entry_perp_fill_type"))
                                         if t == "maker"),
        }

    cooldown = 0.0
    if last_rec is not None:
        cooldown = max(0.0, E.POST_RECONNECT_COOLDOWN_SEC - (now - last_rec))

    return {
        "symbol"        : cs.symbol,
        "state"         : state,
        "tier_sec"      : cs.bucket_sec,
        "warm_pct"      : _f(min(100.0, n_buckets / cs.rolling_win * 100), 1),
        "spread_pct"    : _f(spread),
        "mean_pct"      : _f(mean),
        "std_pct"       : _f(std),
        "z"             : _f(z, 2),
        "rt_pct"        : _f(st["live_round_trip"]),
        "signals"       : st["signals_detected"],
        "blocked_g2"    : st["blocked_gate2"],
        "blocked_g3"    : st["blocked_gate3"],
        "fired"         : st["signals_fired"],
        "closed"        : st["trades_closed"],
        "wins"          : st["trades_profit"],
        "losses"        : st["trades_loss"],
        "net_pct"       : _f(st["total_net_pnl_pct"]),
        "net_usd"       : _f(st["total_net_pnl_usd"], 4),
        "g3_buffer_pct" : _f(st["slip_buffer"]),
        "g3_samples"    : n_slips,
        "funding_pct"   : _f(fv[0]) if fv else None,
        "funding_apr"   : _f(fv[0] * (24.0 / fv[2]) * 365.0, 1) if fv else None,
        "funding_in_sec": _f(max(0.0, fv[1]), 0) if fv else None,
        "funding_ivl_h" : fv[2] if fv else None,
        "basis_vol_pct" : _f(vol),
        "stop_sigmas"   : _f(stop_sigmas, 2),
        "ws_spot"       : s_status,
        "ws_perp"       : p_status,
        "feed_spot"     : s_feed,
        "feed_perp"     : p_feed,
        "unlisted"      : unlisted,
        "spot_age"      : None if s_ts is None else _f(max(0.0, now - s_ts), 1),
        "perp_age"      : None if p_ts is None else _f(max(0.0, now - p_ts), 1),
        "spot_quiet"    : None if s_last is None else _f(max(0.0, now - s_last), 1),
        "perp_quiet"    : None if p_last is None else _f(max(0.0, now - p_last), 1),
        "ws_errors"     : s_err + p_err,
        "cooldown_sec"  : _f(cooldown, 1),
        "updates"       : st["total_updates"],
        "position"      : position,
    }


def _feed_rows(now):
    rows = []
    for name, f in sorted(E.FEEDS.items(), key=lambda kv: (kv[1]["leg"] != "spot", kv[0])):
        up = f["status"] == "connected" and f["opened_at"] is not None
        rows.append({
            "name"          : name,
            "leg"           : f["leg"],
            "status"        : f["status"],
            "coins"         : len(f["coins"]),
            "msg_rate"      : f["rate"],
            "lag_ms"        : _f(f["lag_ms"], 0) if up else None,
            "lag_max_ms"    : _f(f["lag_max_ms"], 0) if up else None,
            "silent_sec"    : _f(now - (f["last_msg"] or f["opened_at"]), 1) if up else None,
            "up_sec"        : _f(now - f["opened_at"], 0) if up else None,
            "reconnects"    : max(0, f["connects"] - 1),
            "errors"        : f["errors"],
            "last_error"    : f["last_error"],
            "last_error_ago": _f(now - f["last_error_at"], 0) if f["last_error_at"] else None,
        })
    return rows


def build_state(prev_updates=None, prev_ts=None):
    now = time.time()
    feeds = _feed_rows(now)
    feed_of = {(f["leg"], sym): name for name, f in list(E.FEEDS.items()) for sym in f["coins"]}
    coins = [_coin_row(cs, now, feed_of) for cs in E.ALL_CS]

    with E.global_lock:
        g = dict(E.global_stats)
    closed = g["total_closed"]

    open_rows = [c for c in coins if c["position"]]
    unreal = sum((c["position"]["net_usd"] or 0.0) for c in open_rows)
    deployed = sum((c["position"]["notional_usd"] or 0.0) for c in open_rows)
    total_updates = sum(c["updates"] for c in coins)

    tick_rate = None
    if prev_updates is not None and prev_ts is not None and now > prev_ts:
        tick_rate = max(0.0, (total_updates - prev_updates) / (now - prev_ts))

    counts = {}
    for c in coins:
        counts[c["state"]] = counts.get(c["state"], 0) + 1

    started = E.ENGINE_START or now
    state = {
        "ts"            : now,
        "uptime_sec"    : _f(now - started, 1),
        "paper"         : True,
        "demo"          : E.DEMO_MODE,
        "entries_enabled": E.ENTRIES_ENABLED.is_set(),
        "trade_head"    : E._trade_seq,
        "global": {
            "closed"        : closed,
            "wins"          : g["total_profit"],
            "losses"        : g["total_loss"],
            "win_rate"      : _f(g["total_profit"] / closed * 100, 1) if closed else None,
            "net_usd"       : _f(g["total_net_pnl_usd"], 4),
            "vwap_net_pct"  : _f(E.get_vwap_net_pnl_pct()),
            "notional"      : _f(g["sum_notional"], 2),
            "open_count"    : len(open_rows),
            "deployed_usd"  : _f(deployed, 2),
            "unrealized_usd": _f(unreal, 4),
            "coins_total"   : len(coins),
            "coins_ready"   : counts.get("flat", 0) + counts.get("open", 0),
            "state_counts"  : counts,
            "ws_spot_up"    : sum(1 for c in coins if c["ws_spot"] == "connected"),
            "ws_perp_up"    : sum(1 for c in coins if c["ws_perp"] == "connected"),
            "feeds_total"   : len(feeds),
            "feeds_up"      : sum(1 for f in feeds if f["status"] == "connected"),
            "feed_lag_ms"   : max((f["lag_ms"] for f in feeds if f["lag_ms"] is not None), default=None),
            "reconnects"    : sum(f["reconnects"] for f in feeds),
            "tick_rate"     : _f(tick_rate, 0),
        },
        "feeds": feeds,
        "config": {
            "stream_type"        : E.STREAM_TYPE,
            "sd_threshold"       : E.SD_THRESHOLD,
            "exchange_fee_pct"   : _f(E.round_trip_fee_pct(), 5),
            # ── funding capture + convergence ────────────────────────────────
            "strategy"           : E.STRATEGY,
            "fee_tier"           : E.FEE_TIER,
            "fee_tiers"          : sorted(E.FEE_TIERS),
            "fee_rt_maker"       : _f(E.round_trip_fee_pct("maker"), 5),
            "fee_rt_taker"       : _f(E.round_trip_fee_pct("taker"), 5),
            "fill_fee_type"      : E.FILL_FEE_TYPE,
            "min_stop_sigmas"    : E.MIN_STOP_SIGMAS,
            "min_funding_pct"    : E.MIN_FUNDING_PCT,
            "min_funding_apr"    : E.MIN_FUNDING_APR,
            "edge_friction_mult" : E.EDGE_FRICTION_MULT,
            "funding_window_sec" : E.FUNDING_ENTRY_WINDOW_SEC,
            "funding_grace_sec"  : E.FUNDING_EXIT_GRACE_SEC,
            "stop_loss_mult"     : E.STOP_LOSS_FUNDING_MULT,
            "stop_loss_min_pct"  : E.STOP_LOSS_MIN_PCT,
            "stop_cooldown_sec"  : E.STOP_COOLDOWN_SEC,
            "reversion_fraction" : E.REVERSION_FRACTION,
            "min_net_pct"        : E.MIN_NET_PCT,
            "max_hold_sec"       : E.MAX_HOLD_SEC,
            "cooldown_sec"       : E.POST_RECONNECT_COOLDOWN_SEC,
            "min_notional_usd"   : E.MIN_NOTIONAL_USD,
            "notional_steps"     : E.NOTIONAL_STEPS,
            "entry_delay_sec"    : E.ENTRY_DELAY_SEC,
            "exit_delay_sec"     : E.EXIT_DELAY_SEC,
            "rolling_win"        : E.ROLLING_WIN,
            "tiers"              : {k: {"bucket_sec": v[0], "window": v[1]}
                                    for k, v in E.BUCKET_TIERS.items()},
            "coins_per_ws"       : E.COINS_PER_WS,
            "master_csv"         : E.MASTER_CSV_PATH,
        },
        "coins": coins,
    }
    return state, total_updates, now


class Broadcaster:
    """Builds the dashboard state once per interval and wakes every SSE listener."""

    def __init__(self, interval=1.0):
        self.interval = interval
        self.cond     = threading.Condition()
        self.version  = 0
        self.state    = None
        self.payload  = "{}"
        self._prev    = (None, None)

    def start(self):
        threading.Thread(target=self._run, name="state-broadcaster", daemon=True).start()
        return self

    def _run(self):
        while True:
            try:
                state, upd, ts = build_state(*self._prev)
                self._prev = (upd, ts)
                payload = json.dumps(state, separators=(",", ":"), allow_nan=False)
                with self.cond:
                    self.version += 1
                    self.state    = state
                    self.payload  = payload
                    self.cond.notify_all()
            except Exception as e:                       # never let the publisher die
                print(f"[state-broadcaster] {type(e).__name__}: {e}")
            time.sleep(self.interval)
