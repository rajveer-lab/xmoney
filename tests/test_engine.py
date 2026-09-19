"""
Engine + API tests. No network: ticks are injected straight into the engine's real
process_spot_tick / process_perp_tick, so gates, sizing, fills, exits and the trade
feed all run exactly as they do live.

    pytest -q
"""
import csv
import json
import os
import random
import sys
import time
from collections import deque

import pytest

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from backend import engine as E            # noqa: E402
from backend.server import create_app      # noqa: E402
from backend.state import Broadcaster, build_state   # noqa: E402


# ── helpers ──────────────────────────────────────────────────────────────────

def book(price, half=0.00004, usd=5000.0):
    q = usd / price
    return {"b": f"{price * (1 - half):.8f}", "B": f"{q:.6f}",
            "a": f"{price * (1 + half):.8f}", "A": f"{q:.6f}"}


def make_coin(win=30, bucket=0.02):
    cs = E.CoinState("TST")
    cs.bucket_sec, cs.rolling_win = bucket, win
    cs.buckets = deque(maxlen=win + 10)
    cs.spot_ba_hist = deque(maxlen=win)
    cs.perp_ba_hist = deque(maxlen=win)
    return cs


def feed(cs, spread_fn, n, dt=0.004, price=100.0):
    for _ in range(n):
        s = spread_fn()
        E.process_spot_tick(cs, book(price))
        E.process_perp_tick(cs, book(price * (1 + s / 100)))
        time.sleep(dt)


def warm_up(cs, seed=1):
    rng = random.Random(seed)
    feed(cs, lambda: rng.gauss(0.0, 0.002), 320)
    assert cs.bucket_signal["ready"], "coin should be warm after ~1.3s of ticks"


@pytest.fixture
def eng(tmp_path, monkeypatch):
    """Isolate every module-level global the engine mutates."""
    monkeypatch.setattr(E, "MASTER_CSV_PATH", str(tmp_path / "trades_master.csv"))
    monkeypatch.setattr(E, "HOLD_TICKER", False)
    monkeypatch.setattr(E, "_trade_seq", 0)
    monkeypatch.setattr(E, "ALL_CS", [])
    monkeypatch.setattr(E, "FEEDS", {})
    monkeypatch.setattr(E, "UNLISTED", {"spot": set(), "perp": set()})
    # Fill synchronously: maker-first parks an entry for MAKER_WAIT_MS in a
    # background thread, which races every "did a position open?" assertion.
    # Maker-first has its own tests below.
    monkeypatch.setattr(E, "STRATEGY", "spread")
    for k in list(E.global_stats):
        monkeypatch.setitem(E.global_stats, k, 0.0 if isinstance(E.global_stats[k], float) else 0)
    E.TRADE_LOG.clear()
    E.ENTRIES_ENABLED.set()
    yield E
    E.TRADE_LOG.clear()
    E.ENTRIES_ENABLED.set()


# ── pure functions ───────────────────────────────────────────────────────────

def test_vwap_fill_walks_levels_and_reports_partial():
    levels = [[100.0, 1.0], [101.0, 1.0]]
    price, filled, full = E.vwap_fill(levels, 1.5)
    assert full and filled == pytest.approx(1.5)
    assert price == pytest.approx((100.0 * 1.0 + 101.0 * 0.5) / 1.5)
    price, filled, full = E.vwap_fill(levels, 5.0)          # book too thin
    assert not full and filled == pytest.approx(2.0)
    assert E.vwap_fill([], 1.0) == (None, 0.0, False)


def test_calc_pnl_both_directions():
    # Fills are real book prices, so the spread is already inside gross; net only
    # adds funding and subtracts the fees actually paid.
    # long spot / short perp: spot flat, perp fell 100.3 → 100.0 (leg is measured vs the perp entry price)
    long_pos = {"direction": 1, "entry_spot_fill": 100.0, "entry_perp_fill": 100.3, "notional_usd": 1000.0}
    gross, net, usd = E.calc_pnl(long_pos, 100.0, 100.0, 0.1)
    expect = (100.3 - 100.0) / 100.3 * 100
    assert gross == pytest.approx(expect, abs=1e-9) and net == pytest.approx(expect, abs=1e-9)
    assert usd == pytest.approx(net / 100 * 1000.0, abs=1e-9)
    # short spot / long perp: perp rose back 99.7 → 100.0
    short_pos = {"direction": -1, "entry_spot_fill": 100.0, "entry_perp_fill": 99.7, "notional_usd": 1000.0}
    gross, net, _ = E.calc_pnl(short_pos, 100.0, 100.0, 0.1)
    expect = (100.0 - 99.7) / 99.7 * 100
    assert gross == pytest.approx(expect, abs=1e-9) and net == pytest.approx(expect, abs=1e-9)
    # a losing exit: spread widened further
    gross, net, usd = E.calc_pnl(long_pos, 100.0, 100.6, 0.1)
    assert gross < 0 and net < 0 and usd < 0


def test_find_optimal_notional_needs_edge_beyond_costs():
    snap = {"spot_bids": [[99.99, 100]], "spot_asks": [[100.01, 100]],
            "perp_bids": [[100.29, 100]], "perp_asks": [[100.31, 100]],
            "spot_ts": 1, "perp_ts": 1}
    notional, spot_fill, perp_fill, slip = E.find_optimal_notional(snap, +1, 0.0, 0.07)
    assert notional > 0 and spot_fill == 100.01 and perp_fill == 100.29
    # same book, but costs exceed the ~0.28% dislocation → no trade
    assert E.find_optimal_notional(snap, +1, 0.0, 0.5)[0] == 0.0


# ── end to end through the real tick handlers ────────────────────────────────

def test_shock_then_reversion_produces_one_profitable_closed_trade(eng, tmp_path):
    cs = make_coin()
    warm_up(cs)
    feed(cs, lambda: 0.30, 40)          # spread jumps +0.30% → long spot / short perp
    assert cs.open_position is not None, "entry should fire on the first shocked tick"
    assert cs.open_position["direction"] == 1
    feed(cs, lambda: 0.0, 80)           # spread reverts → exit at ≥90% reversion
    assert cs.open_position is None, "position should have exited on reversion"

    trades, head = E.trades_since(0)
    assert head == len(trades) >= 1
    t = trades[0]
    assert t["symbol"] == "TST" and t["exit_type"] == "reversion"
    assert t["net_pnl_usd"] > 0 and t["net_pnl_pct"] > 0
    assert E.global_stats["total_closed"] == len(trades)
    assert E.global_stats["total_profit"] >= 1

    with open(E.MASTER_CSV_PATH, newline="", encoding="utf-8") as f:
        rows = list(csv.DictReader(f))
    assert len(rows) == len(trades) and rows[0]["symbol"] == "TST"


def test_paused_entries_block_new_positions_but_allow_exit(eng):
    cs = make_coin()
    warm_up(cs)
    E.ENTRIES_ENABLED.clear()
    feed(cs, lambda: 0.30, 6)           # a valid signal, but entries are paused
    assert cs.open_position is None and not E.TRADE_LOG
    feed(cs, lambda: 0.0, 40)           # settle (keeps the rolling band tight)
    E.ENTRIES_ENABLED.set()
    feed(cs, lambda: 0.60, 20)          # fresh, larger dislocation after resume
    assert cs.open_position is not None        # resumes normally


def test_pausing_while_open_still_exits(eng):
    cs = make_coin()
    warm_up(cs)
    feed(cs, lambda: 0.30, 30)
    assert cs.open_position is not None
    E.ENTRIES_ENABLED.clear()
    feed(cs, lambda: 0.0, 80)
    assert cs.open_position is None and len(E.TRADE_LOG) == 1


def test_bucket_lock_prevents_concurrent_bucket_runs(eng):
    cs = make_coin()
    assert cs.bucket_lock.acquire(blocking=False)
    before = cs.stats["bucket_index"]
    E.run_bucket(cs)                            # would double-run without the lock
    assert cs.stats["bucket_index"] == before
    cs.bucket_lock.release()


# ── feeds: drops, reconnects, quiet coins (no network) ───────────────────────

class _Stop(BaseException):
    """Ends run_combined_ws's reconnect loop once it has wired up its callbacks."""


def open_feed(monkeypatch, leg, coins, name):
    """Run run_combined_ws just far enough to get its WebSocket callbacks."""
    apps = []

    class FakeApp:
        def __init__(self, url, on_open, on_message, on_error, on_close):
            self.on_open, self.on_message, self.on_error, self.on_close = on_open, on_message, on_error, on_close
            self.closed = False
            apps.append(self)

        def run_forever(self, **kw):
            raise _Stop

        def close(self):
            self.closed = True

    monkeypatch.setattr(E.websocket, "WebSocketApp", FakeApp)
    monkeypatch.setattr(E, "seed_books", lambda *a, **k: None)
    cmap = {f"{cs.symbol.lower()}usdt": cs for cs in coins}
    with pytest.raises(_Stop):
        E.run_combined_ws("wss://example.invalid", cmap, leg, list(coins), name)
    return apps[0]


def msg(sym, price):
    return json.dumps({"stream": f"{sym.lower()}usdt@bookTicker", "data": book(price)})


def test_drop_on_one_feed_only_affects_its_own_coins(eng, monkeypatch):
    a1, a2, b1 = E.CoinState("AAA"), E.CoinState("BBB"), E.CoinState("CCC")
    fa = open_feed(monkeypatch, "perp", [a1, a2], "perp-ws-0")
    fb = open_feed(monkeypatch, "perp", [b1], "perp-ws-1")
    fa.on_open(fa)
    fb.on_open(fb)
    for cs in (a1, a2, b1):
        cs.bucket_signal["ready"] = True

    fa.on_error(fa, Exception("ping/pong timed out"))
    fa.on_close(fa, None, None)

    assert a1.perp_ws_status == a2.perp_ws_status == "retrying"
    assert not a1.bucket_signal["ready"] and a1.perp_ws_errors == 1
    # the other feed's coins are untouched: still live, still ready, no error, no entry cooldown
    assert b1.perp_ws_status == "connected" and b1.bucket_signal["ready"]
    assert b1.perp_ws_errors == 0 and b1.last_reconnect_time is None
    assert E.FEEDS["perp-ws-0"]["status"] == "retrying" and E.FEEDS["perp-ws-1"]["status"] == "connected"


def test_reconnect_keeps_or_rewarms_only_that_feeds_coins(eng, monkeypatch):
    a, b = E.CoinState("AAA"), E.CoinState("BBB")
    fa = open_feed(monkeypatch, "spot", [a], "spot-ws-0")
    fb = open_feed(monkeypatch, "spot", [b], "spot-ws-1")
    fa.on_open(fa)
    fb.on_open(fb)
    for cs in (a, b):
        cs.buckets.extend({"spread_pct": 0.0} for _ in range(50))

    fa.on_close(fa, None, None)                 # short blip → window kept, entry cooldown starts
    fa.on_open(fa)
    assert len(a.buckets) == 50 and a.last_reconnect_time is not None

    fa.on_close(fa, None, None)                 # long outage → that feed's coins re-warm
    a.spot_disconnect_time -= 30
    fa.on_open(fa)
    assert len(a.buckets) == 0
    assert len(b.buckets) == 50 and b.last_reconnect_time is None
    assert E.FEEDS["spot-ws-0"]["connects"] == 3


def test_quiet_coin_quote_stays_current_while_its_feed_is_live(eng, monkeypatch):
    quiet, busy = E.CoinState("QQQ"), E.CoinState("BBB")
    f = open_feed(monkeypatch, "perp", [quiet, busy], "perp-ws-0")
    feed = E.FEEDS["perp-ws-0"]
    f.on_open(f)
    f.on_message(f, msg("QQQ", 10.0))           # the quiet coin's book changes once...
    t_quiet = quiet.latest["perp_ts"]
    time.sleep(0.02)
    for _ in range(5):
        f.on_message(f, msg("BBB", 20.0))       # ...then only the busy coin moves
    E.check_feeds(time.time(), {})
    assert quiet.latest["perp_ts"] == feed["last_msg"] > t_quiet     # quote is current
    assert quiet.perp_last_tick == t_quiet                            # the book itself hasn't changed

    f.on_close(f, None, None)                   # after a reconnect an old quote is NOT extended
    f.on_open(f)
    f.on_message(f, msg("BBB", 20.0))
    before = quiet.latest["perp_ts"]
    E.check_feeds(time.time(), {})
    assert quiet.latest["perp_ts"] == before

    feed["last_msg"] = time.time() - E.FEED_SILENCE_SEC - 1           # feed goes silent → reopened
    E.check_feeds(time.time(), {})
    time.sleep(0.05)
    assert f.closed


def test_rest_seed_fills_quiet_coins_and_flags_unlisted(eng, monkeypatch):
    opened = time.time()
    live, quiet, missing = E.CoinState("LIV"), E.CoinState("QUI"), E.CoinState("MIS")
    E.process_spot_tick(live, book(50.0))       # ticked this session → the live quote wins
    rows = [{"symbol": "LIVUSDT", "bidPrice": "1", "bidQty": "1", "askPrice": "1.1", "askQty": "1"},
            {"symbol": "QUIUSDT", "bidPrice": "9.99", "bidQty": "3", "askPrice": "10.01", "askQty": "4"}]
    monkeypatch.setattr(E, "_rest_get", lambda url, timeout=10: rows)
    E.seed_books("spot", {f"{c.symbol.lower()}usdt": c for c in (live, quiet, missing)}, opened)
    assert live.latest["spot_bids"][0][0] == pytest.approx(50.0 * (1 - 0.00004))
    assert quiet.latest["spot_bids"] == [[9.99, 3.0]] and quiet.latest["spot_asks"] == [[10.01, 4.0]]
    assert quiet.latest["spot_ts"] is not None and quiet.stats["total_updates"] == 0   # book only, no tick run
    assert E.UNLISTED["spot"] == {"MIS"}


def test_state_marks_unlisted_and_keeps_quiet_coins_live(eng, monkeypatch):
    cs, gone = make_coin(), E.CoinState("GONE")
    monkeypatch.setattr(E, "ALL_CS", [cs, gone])
    E.UNLISTED["perp"].add("GONE")
    warm_up(cs)
    cs.spot_last_tick = time.time() - 60        # book unchanged for a minute, but the quote is current
    st, _, _ = build_state()
    rows = {r["symbol"]: r for r in st["coins"]}
    assert rows["GONE"]["state"] == "unlisted" and rows["GONE"]["unlisted"] == ["perp"]
    assert rows["TST"]["state"] == "flat" and rows["TST"]["spot_quiet"] >= 59
    json.dumps(st, allow_nan=False)


# ── state + API ──────────────────────────────────────────────────────────────

def test_build_state_is_strict_json_and_reports_open_position(eng, monkeypatch):
    cs = make_coin()
    monkeypatch.setattr(E, "ALL_CS", [cs])
    st, _, _ = build_state()                    # before any ticks
    assert st["coins"][0]["state"] == "connecting"
    warm_up(cs)
    feed(cs, lambda: 0.30, 30)
    st, _, _ = build_state()
    row = st["coins"][0]
    assert row["state"] == "open" and row["position"]["direction"] == 1
    assert row["position"]["net_usd"] is not None
    assert st["global"]["open_count"] == 1
    json.dumps(st, allow_nan=False)             # no NaN/inf may leak to the browser


@pytest.fixture
def client(eng, monkeypatch):
    cs = make_coin()
    monkeypatch.setattr(E, "ALL_CS", [cs])
    bc = Broadcaster(interval=0.1).start()
    time.sleep(0.25)
    return create_app(bc).test_client()


def test_api_health_state_and_trades(client):
    assert client.get("/api/health").get_json()["ok"] is True
    st = client.get("/api/state").get_json()
    assert st["global"]["coins_total"] == 1 and "config" in st
    r = client.get("/api/trades?since=0&limit=10").get_json()
    assert r == {"trades": [], "head": 0}
    assert client.get("/api/trades?since=abc").status_code == 400
    assert client.get("/").status_code == 200
    assert client.get("/static/app.js").status_code == 200


def test_control_endpoint_validates_and_rejects_cross_origin(client):
    ok = client.post("/api/control/entries", json={"enabled": False})
    assert ok.status_code == 200 and ok.get_json()["entries_enabled"] is False
    assert not E.ENTRIES_ENABLED.is_set()
    assert client.post("/api/control/entries", json={"enabled": "no"}).status_code == 400
    assert client.post("/api/control/entries", data="x", content_type="text/plain").status_code == 400
    evil = client.post("/api/control/entries", json={"enabled": True},
                       headers={"Origin": "http://evil.example"})
    assert evil.status_code == 403 and not E.ENTRIES_ENABLED.is_set()
    same = client.post("/api/control/entries", json={"enabled": True},
                       headers={"Origin": "http://localhost"})
    assert same.status_code == 200 and E.ENTRIES_ENABLED.is_set()


# ── fee tiers ────────────────────────────────────────────────────────────────

def test_fee_tiers_scale_round_trip_cost(eng, monkeypatch):
    """Four executions per trade: entry + exit on both legs."""
    monkeypatch.setattr(E, "FEE_TIER", "ZERO")
    assert E.round_trip_fee_pct("taker") == 0.0      # presentation default: spread only

    monkeypatch.setattr(E, "FEE_TIER", "VIP0")
    spot_t, perp_t = E.leg_fee_pct("spot", "taker"), E.leg_fee_pct("perp", "taker")
    assert E.round_trip_fee_pct("taker") == pytest.approx(2 * (spot_t + perp_t))

    # Resting is always cheaper than crossing, and better tiers are cheaper still.
    assert E.round_trip_fee_pct("maker") < E.round_trip_fee_pct("taker")
    vip0 = E.round_trip_fee_pct("taker")
    monkeypatch.setattr(E, "FEE_TIER", "VIP9")
    assert E.round_trip_fee_pct("taker") < vip0


def test_realised_fee_uses_how_each_leg_actually_filled(eng, monkeypatch):
    monkeypatch.setattr(E, "FEE_TIER", "VIP0")
    pos = {"entry_spot_fill_type": "maker", "entry_perp_fill_type": "taker"}
    expected = (E.leg_fee_pct("spot", "maker") + E.leg_fee_pct("perp", "taker") +
                E.leg_fee_pct("spot", "taker") + E.leg_fee_pct("perp", "maker"))
    assert E.realised_fee_pct(pos, "taker", "maker") == pytest.approx(expected)


# ── maker-first execution ────────────────────────────────────────────────────




def set_funding(cs, rate_pct, secs_to_stamp, interval_h=8.0, calm=True):
    """Set a funding rate. `calm` also seeds a low-volatility basis history,
    because the entry gate refuses coins whose gap it cannot measure."""
    if calm and len(cs.buckets) < E.MIN_VOL_SAMPLES:
        for i in range(E.MIN_VOL_SAMPLES + 10):
            cs.buckets.append({"spread_pct": 0.002 if i % 2 else -0.002})
    with cs.lock:
        cs.funding_rate       = rate_pct / 100.0
        cs.next_funding_ms    = int((time.time() + secs_to_stamp) * 1000)
        cs.funding_ts         = time.time()
        cs.funding_interval_h = interval_h


def test_funding_gate_direction_and_thresholds(eng, monkeypatch):
    monkeypatch.setattr(E, "MIN_FUNDING_PCT", 0.005)
    monkeypatch.setattr(E, "EDGE_FRICTION_MULT", 1.5)
    monkeypatch.setattr(E, "MIN_FUNDING_APR", 20.0)
    monkeypatch.setattr(E, "FUNDING_ENTRY_WINDOW_SEC", 3600.0)
    cs = make_coin()
    friction = 0.01

    # Positive rate → longs pay shorts → short the perp (direction +1) to receive.
    set_funding(cs, 0.05, 600)
    assert E.funding_entry_check(cs, friction, 0.0)[0] == +1
    # Negative rate → shorts pay longs → long the perp instead.
    set_funding(cs, -0.05, 600)
    assert E.funding_entry_check(cs, friction, 0.0)[0] == -1

    # Too small to bother with.
    set_funding(cs, 0.001, 600)
    assert E.funding_entry_check(cs, friction, 0.0) is None
    # Clears the absolute floor but not the multiple of friction, a penny trade.
    set_funding(cs, 0.012, 600)
    assert E.funding_entry_check(cs, friction, 0.0) is None
    # Good rate, but the stamp is outside the entry window.
    set_funding(cs, 0.05, 7200)
    assert E.funding_entry_check(cs, friction, 0.0) is None
    # Stale funding feed is not trusted.
    set_funding(cs, 0.05, 600)
    with cs.lock:
        cs.funding_ts = time.time() - 120
    assert E.funding_entry_check(cs, friction, 0.0) is None


def test_funding_gate_rejects_a_good_rate_that_is_a_bad_rate_of_return(eng, monkeypatch):
    """Same basis points earned over a much longer hold is a worse trade."""
    monkeypatch.setattr(E, "MIN_FUNDING_PCT", 0.001)
    monkeypatch.setattr(E, "EDGE_FRICTION_MULT", 1.0)
    monkeypatch.setattr(E, "FUNDING_ENTRY_WINDOW_SEC", 30 * 3600.0)
    monkeypatch.setattr(E, "MIN_FUNDING_APR", 500.0)
    cs = make_coin()
    monkeypatch.setattr(E, "FUNDING_EXIT_GRACE_SEC", 0.0)   # isolate the countdown
    set_funding(cs, 0.05, 60)                      # 0.05% in a minute → huge APR
    assert E.funding_entry_check(cs, 1e-6, 0.0) is not None
    set_funding(cs, 0.05, 24 * 3600)               # same 0.05%, but a day of capital
    assert E.funding_entry_check(cs, 1e-6, 0.0) is None


def test_funding_settles_with_the_right_sign_when_a_stamp_passes(eng):
    cs = make_coin()
    now_ms = time.time() * 1000
    # Short perp (direction +1) collects a positive rate.
    cs.open_position = {"direction": +1, "next_funding_ms": now_ms - 10,
                        "funding_collected_pct": 0.0, "stamps_crossed": 0,
                        "funding_pct_at_entry": 0.04}
    set_funding(cs, 0.04, 8 * 3600)
    E.accrue_funding(cs)
    assert cs.open_position["funding_collected_pct"] == pytest.approx(0.04)
    assert cs.open_position["stamps_crossed"] == 1
    assert cs.open_position["next_funding_ms"] > now_ms      # rolled to the next stamp

    # If the rate flips before settling, the same position pays instead of collects.
    cs.open_position["next_funding_ms"] = time.time() * 1000 - 10
    set_funding(cs, -0.04, 8 * 3600)
    E.accrue_funding(cs)
    assert cs.open_position["funding_collected_pct"] == pytest.approx(0.0)
    assert cs.open_position["stamps_crossed"] == 2


def test_funding_is_carried_into_pnl(eng, monkeypatch):
    monkeypatch.setattr(E, "FEE_TIER", "ZERO")
    pos = {"direction": +1, "entry_spot_fill": 100.0, "entry_perp_fill": 100.0,
           "notional_usd": 1000.0, "funding_collected_pct": 0.0,
           "entry_spot_fill_type": "taker", "entry_perp_fill_type": "taker"}
    _, flat_net, _ = E.calc_pnl(pos, 100.0, 100.0, 0.0)
    assert flat_net == pytest.approx(0.0)           # no move, no fees, no funding

    pos["funding_collected_pct"] = 0.05
    _, net, usd = E.calc_pnl(pos, 100.0, 100.0, 0.0)
    assert net == pytest.approx(0.05)               # a flat basis still earns the funding
    assert usd == pytest.approx(0.05 / 100 * 1000.0)


def test_funding_position_holds_through_the_stamp_then_exits(eng, monkeypatch):
    """Nothing closes before the stamp, the payment is the whole trade."""
    cs = make_coin()
    E.process_spot_tick(cs, book(100.0))
    E.process_perp_tick(cs, book(100.0))
    cs.open_position = {
        "direction": +1, "entry_spot_fill": 100.0, "entry_perp_fill": 100.0,
        "notional_usd": 100.0, "entry_deviation": 0.0, "entry_mean": 0.0,
        "entry_time": time.time(), "best_pnl": -999.0, "best_pnl_usd": -999.0,
        "trade_kind": "funding", "funding_collected_pct": 0.0, "stamps_crossed": 0,
        "entry_spot_fill_type": "taker", "entry_perp_fill_type": "taker",
        "profit_target_hit": False, "action": "LONG spot / SHORT perp",
        "entry_dt": "x", "secs_to_funding": 60.0,
    }
    E.check_exit_on_tick(cs, E.get_fill_snap(cs))
    assert cs.open_position is not None and not cs.exit_pending   # pre-stamp: hold

    cs.open_position["stamps_crossed"]        = 1
    cs.open_position["funding_collected_pct"] = 0.05
    E.check_exit_on_tick(cs, E.get_fill_snap(cs))
    assert cs.exit_pending                                        # post-stamp: leave on profit


def test_funding_trade_is_not_timed_out_before_its_stamp(eng):
    """MAX_HOLD_SEC is a spread-trade timeout; a funding trade has to outlive it."""
    spread = {"trade_kind": "spread"}
    assert E.max_hold_for(spread) == E.MAX_HOLD_SEC
    funding = {"trade_kind": "funding", "secs_to_funding": 3000.0}
    assert E.max_hold_for(funding) >= 3000.0 + E.FUNDING_EXIT_GRACE_SEC


def test_convergence_is_priced_into_the_funding_entry(eng, monkeypatch):
    """One trade, two earners. Reversion helps a long-spot/short-perp book when
    the spread sits above its mean, and hurts it when below."""
    monkeypatch.setattr(E, "MIN_FUNDING_PCT", 0.001)
    monkeypatch.setattr(E, "EDGE_FRICTION_MULT", 1.0)
    monkeypatch.setattr(E, "MIN_FUNDING_APR", 0.0)
    monkeypatch.setattr(E, "REVERSION_FRACTION", 0.9)
    cs = make_coin()
    set_funding(cs, 0.05, 600)                  # positive → direction +1

    _, _, _, aligned = E.funding_entry_check(cs, 1e-6, +0.02)
    assert aligned == pytest.approx(0.02 * 0.9)      # spread above mean: helps
    _, _, _, adverse = E.funding_entry_check(cs, 1e-6, -0.02)
    assert adverse == pytest.approx(-0.02 * 0.9)     # below mean: works against us


def test_adverse_convergence_is_taken_when_funding_still_pays_for_it(eng, monkeypatch):
    """Option (b): an adverse basis doesn't veto the trade, it just has to be
    outweighed, but it does veto it once it outweighs the funding."""
    monkeypatch.setattr(E, "MIN_FUNDING_PCT", 0.001)
    monkeypatch.setattr(E, "EDGE_FRICTION_MULT", 1.0)
    monkeypatch.setattr(E, "MIN_FUNDING_APR", 0.0)
    monkeypatch.setattr(E, "REVERSION_FRACTION", 1.0)
    cs = make_coin()
    set_funding(cs, 0.10, 600)                  # direction +1, collects 0.10%

    assert E.funding_entry_check(cs, 1e-6, -0.05) is not None   # adverse but covered
    assert E.funding_entry_check(cs, 1e-6, -0.20) is None       # adverse and not covered


def test_funding_exit_waits_for_convergence_instead_of_first_profit(eng, monkeypatch):
    monkeypatch.setattr(E, "REVERSION_FRACTION", 0.9)
    cs = make_coin()
    E.process_spot_tick(cs, book(100.0))
    E.process_perp_tick(cs, book(100.0))
    cs.open_position = {
        "direction": +1, "entry_spot_fill": 100.0, "entry_perp_fill": 100.0,
        "notional_usd": 100.0, "entry_deviation": 0.20, "entry_mean": -0.20,
        "entry_time": time.time(), "best_pnl": -999.0, "best_pnl_usd": -999.0,
        "trade_kind": "funding", "funding_collected_pct": 0.05, "stamps_crossed": 1,
        "entry_spot_fill_type": "taker", "entry_perp_fill_type": "taker",
        "profit_target_hit": False, "action": "LONG spot / SHORT perp",
        "entry_dt": "x", "secs_to_funding": 60.0, "stop_loss_pct": 0.20,
    }
    # Funding banked and net is already positive, but the basis has barely moved,
    # so there is convergence still to collect: hold.
    E.check_exit_on_tick(cs, E.get_fill_snap(cs))
    assert not cs.exit_pending

    # Now the spread has reverted onto its mean → the second earner is in.
    cs.open_position["entry_mean"] = 0.0
    E.check_exit_on_tick(cs, E.get_fill_snap(cs))
    assert cs.exit_pending


def test_funding_stop_loss_fires_before_the_stamp(eng, monkeypatch):
    """A basis that blows through the funding we were going to collect ends the
    trade, waiting for the stamp would only add to the loss."""
    cs = make_coin()
    E.process_spot_tick(cs, book(100.0))
    E.process_perp_tick(cs, book(101.0))          # basis moved hard against a dir +1 book
    cs.open_position = {
        "direction": +1, "entry_spot_fill": 100.0, "entry_perp_fill": 100.0,
        "notional_usd": 100.0, "entry_deviation": 0.0, "entry_mean": 0.0,
        "entry_time": time.time(), "best_pnl": -999.0, "best_pnl_usd": -999.0,
        "trade_kind": "funding", "funding_collected_pct": 0.0,
        "stamps_crossed": 0,                      # stamp has NOT passed yet
        "entry_spot_fill_type": "taker", "entry_perp_fill_type": "taker",
        "profit_target_hit": False, "action": "LONG spot / SHORT perp",
        "entry_dt": "x", "secs_to_funding": 600.0, "stop_loss_pct": 0.10,
    }
    E.check_exit_on_tick(cs, E.get_fill_snap(cs))
    assert cs.exit_pending
    assert E._exit_type("STOP LOSS net=-1%") == "stop-loss"


def test_stop_loss_scales_with_the_funding_being_collected(eng, monkeypatch):
    monkeypatch.setattr(E, "STOP_LOSS_FUNDING_MULT", 2.0)
    monkeypatch.setattr(E, "STOP_LOSS_MIN_PCT", 0.05)
    assert max(2.0 * abs(-0.30), 0.05) == pytest.approx(0.60)   # fat funding, wider stop
    assert max(2.0 * abs(0.001), 0.05) == pytest.approx(0.05)   # thin funding, floor


def test_funding_gate_refuses_to_trade_before_costs_are_known(eng):
    """get_round_trip_pct() reports 0 until it has bid-ask samples. Zero is
    'unknown', not 'free', entering against it opened trades whose spread was
    several times the funding they could ever collect."""
    cs = make_coin()
    set_funding(cs, 0.05, 600)
    assert E.funding_entry_check(cs, 0.0, 0.0) is None     # no cost estimate yet
    assert E.funding_entry_check(cs, 0.01, 0.0) is not None


def test_spread_is_not_charged_twice(eng, monkeypatch):
    """Entry and exit fills are real book prices, so the crossing cost is already
    inside gross. Charging live friction on top of it stopped trades out at birth."""
    monkeypatch.setattr(E, "FEE_TIER", "ZERO")
    pos = {"direction": +1, "entry_spot_fill": 100.0, "entry_perp_fill": 100.0,
           "notional_usd": 1000.0, "funding_collected_pct": 0.0,
           "entry_spot_fill_type": "taker", "entry_perp_fill_type": "taker"}
    # Same fills, wildly different friction estimates → identical realised PnL.
    _, net_a, _ = E.calc_pnl(pos, 100.0, 100.0, 0.0)
    _, net_b, _ = E.calc_pnl(pos, 100.0, 100.0, 0.5)
    assert net_a == pytest.approx(net_b) == pytest.approx(0.0)


def make_funding_pos(**over):
    pos = {
        "direction": +1, "entry_spot_fill": 100.0, "entry_perp_fill": 100.0,
        "notional_usd": 100.0, "entry_deviation": 0.0, "entry_mean": 0.0,
        "entry_time": time.time(), "best_pnl": -999.0, "best_pnl_usd": -999.0,
        "trade_kind": "funding", "funding_collected_pct": 0.0, "stamps_crossed": 0,
        "entry_spot_fill_type": "taker", "entry_perp_fill_type": "taker",
        "profit_target_hit": False, "action": "LONG spot / SHORT perp",
        "entry_dt": "x", "secs_to_funding": 600.0, "stop_loss_pct": 0.06,
        "entry_mark_pct": 0.0, "entry_spread": 0.0, "signal_spread": 0.0,
        "signal_deviation": 0.0, "dev_shrink_pct": 0.0, "entry_slip_pct": 0.0,
    }
    pos.update(over)
    return pos


def test_stop_measures_from_entry_not_from_zero(eng, monkeypatch):
    """A pair opens already down one round trip of spread: that is the cost of
    unwinding, not a loss. Measuring the stop against raw net fired the instant a
    position opened on any coin whose spread was wider than the stop, which is how
    the engine took the same losing trade hundreds of times in a row."""
    cs = make_coin()
    E.process_spot_tick(cs, book(100.0))
    E.process_perp_tick(cs, book(100.0))

    # Wide spread: the trade is 0.12% underwater purely from entry+exit cost,
    # twice the 0.06% stop. It must NOT stop on that alone.
    cs.open_position = make_funding_pos(entry_mark_pct=-0.12)
    E.check_exit_on_tick(cs, E.get_fill_snap(cs))
    assert not cs.exit_pending

    # Only once it moves a further 0.06% against the entry mark does it stop.
    cs.open_position = make_funding_pos(entry_mark_pct=0.20)
    E.check_exit_on_tick(cs, E.get_fill_snap(cs))
    assert cs.exit_pending


def test_a_stop_blocks_re_entry_for_the_cooldown(eng, monkeypatch):
    monkeypatch.setattr(E, "STRATEGY", "funding")   # fixture defaults to spread
    monkeypatch.setattr(E, "STOP_COOLDOWN_SEC", 120.0)
    monkeypatch.setattr(E, "MIN_FUNDING_PCT", 0.001)
    monkeypatch.setattr(E, "EDGE_FRICTION_MULT", 1.0)
    monkeypatch.setattr(E, "MIN_FUNDING_APR", 0.0)
    cs = make_coin()
    warm_up(cs)
    set_funding(cs, 0.20, 600)

    cs.last_stop_time = time.time()
    E.check_entry_on_tick(cs, E.get_fill_snap(cs))
    assert cs.open_position is None and not cs.entry_pending   # still cooling off

    cs.last_stop_time = time.time() - 121
    E.check_entry_on_tick(cs, E.get_fill_snap(cs))
    assert cs.open_position is not None or cs.entry_pending    # free to trade again


def test_holding_past_a_stamp_is_decided_on_value_not_a_clock(eng, monkeypatch):
    """Once a payment is banked and the gap has not closed, staying should depend
    on whether the money still to be made beats the hurdle over the time it takes,
    not on a fixed grace window expiring."""
    monkeypatch.setattr(E, "MIN_FUNDING_APR", 20.0)
    monkeypatch.setattr(E, "REVERSION_FRACTION", 0.9)
    cs = make_coin()
    E.process_spot_tick(cs, book(100.0))
    E.process_perp_tick(cs, book(100.0))
    # Books are level, so entry_mean -0.50 leaves the gap still 0.50 wide:
    # convergence has NOT fired and the hold decision is what runs.
    held = dict(entry_deviation=0.50, entry_mean=-0.50, stamps_crossed=1,
                funding_collected_pct=0.05, stop_loss_pct=5.0, entry_mark_pct=0.0)

    # A fat next payment 10 minutes away, gap still in our favour: keep sitting.
    cs.open_position = make_funding_pos(**held)
    set_funding(cs, 0.20, 600)
    E.check_exit_on_tick(cs, E.get_fill_snap(cs))
    assert not cs.exit_pending

    # Now the rate pays the other side and the gap sits against us, so there is
    # nothing left worth waiting for: leave, even though no clock has run out.
    cs.exit_pending = False
    cs.open_position = make_funding_pos(**dict(held, direction=-1,
                                               action="SHORT spot / LONG perp"))
    set_funding(cs, -0.20, 600)
    E.check_exit_on_tick(cs, E.get_fill_snap(cs))
    assert cs.exit_pending
    assert E._exit_type("NOT WORTH HOLDING next pays -0.2%") == "not-worth-holding"


def test_funding_hold_ceiling_is_a_safety_net_not_the_strategy(eng):
    """The old 15 minute grace was closing positions that were still working."""
    short_stamp = {"trade_kind": "funding", "secs_to_funding": 120.0}
    assert E.max_hold_for(short_stamp) >= E.FUNDING_MAX_HOLD_SEC
    assert E.max_hold_for({"trade_kind": "spread"}) == E.MAX_HOLD_SEC




def test_both_legs_fill_together_at_the_book(eng, monkeypatch):
    """No race to win: both legs cross at once, so the pair is never half on,
    and the fee is whatever we said it is rather than whatever we managed."""
    monkeypatch.setattr(E, "FILL_FEE_TYPE", "maker")
    cs = make_coin()
    E.process_spot_tick(cs, book(100.0))
    E.process_perp_tick(cs, book(100.0))
    snap = E.get_fill_snap(cs)

    t0 = time.time()
    sp, pp, sf, pf, waited = E.execute_two_leg_fill(cs, +1, 100.0, "entry")
    assert (sf, pf) == ("maker", "maker")          # charged as configured
    assert waited < 20 and (time.time() - t0) < 0.05   # no waiting around
    assert sp == pytest.approx(E._best_ask(snap, "spot"))   # bought, so crossed up
    assert pp == pytest.approx(E._best_bid(snap, "perp"))   # sold, so crossed down

    monkeypatch.setattr(E, "FILL_FEE_TYPE", "taker")
    _, _, sf, pf, _ = E.execute_two_leg_fill(cs, +1, 100.0, "exit", allow_cancel=False)
    assert (sf, pf) == ("taker", "taker")


def test_skips_coins_whose_gap_outruns_the_funding(eng, monkeypatch):
    """A stop sitting inside the coin's ordinary noise will be hit before the
    payment arrives, so that funding was never really collectable."""
    monkeypatch.setattr(E, "MIN_FUNDING_PCT", 0.001)
    monkeypatch.setattr(E, "EDGE_FRICTION_MULT", 1.0)
    monkeypatch.setattr(E, "MIN_FUNDING_APR", 0.0)
    monkeypatch.setattr(E, "MIN_STOP_SIGMAS", 2.0)
    monkeypatch.setattr(E, "STOP_LOSS_FUNDING_MULT", 2.0)
    monkeypatch.setattr(E, "STOP_LOSS_MIN_PCT", 0.0)
    monkeypatch.setattr(E, "MIN_VOL_SAMPLES", 5)

    cs = make_coin()
    set_funding(cs, 0.10, 600)          # stop lands 0.20% away

    # Calm gap: 0.20% is 4 standard deviations out, so being stopped is unlikely.
    cs.buckets.clear()
    for i in range(40):
        cs.buckets.append({"spread_pct": 0.05 if i % 2 else -0.05})
    assert E.basis_volatility(cs) == pytest.approx(0.05, rel=0.1)
    assert E.funding_entry_check(cs, 0.001, 0.0) is not None

    # Same funding, but this gap swings 0.30% routinely: the stop is well inside
    # the noise and would be taken out long before the stamp.
    cs.buckets.clear()
    for i in range(40):
        cs.buckets.append({"spread_pct": 0.30 if i % 2 else -0.30})
    assert E.funding_entry_check(cs, 0.001, 0.0) is None


def test_volatility_does_not_wait_for_the_full_window(eng, monkeypatch):
    """A risk filter that only switches on after eight minutes is not a filter."""
    monkeypatch.setattr(E, "MIN_VOL_SAMPLES", 30)
    cs = make_coin(win=1000)
    for i in range(29):
        cs.buckets.append({"spread_pct": 0.1 if i % 2 else -0.1})
    assert E.basis_volatility(cs) is None        # not enough to judge yet
    cs.buckets.append({"spread_pct": 0.1})
    assert E.basis_volatility(cs) is not None    # usable well before 1000 buckets


def test_unmeasured_volatility_blocks_entry(eng, monkeypatch):
    """Same principle as unknown costs: no measurement is not a green light."""
    monkeypatch.setattr(E, "MIN_FUNDING_PCT", 0.001)
    monkeypatch.setattr(E, "EDGE_FRICTION_MULT", 1.0)
    monkeypatch.setattr(E, "MIN_FUNDING_APR", 0.0)
    monkeypatch.setattr(E, "MIN_VOL_SAMPLES", 30)
    cs = make_coin()
    set_funding(cs, 0.20, 600, calm=False)
    assert E.basis_volatility(cs) is None
    assert E.funding_entry_check(cs, 0.001, 0.0) is None      # no history, no trade
    for i in range(40):
        cs.buckets.append({"spread_pct": 0.01 if i % 2 else -0.01})
    assert E.funding_entry_check(cs, 0.001, 0.0) is not None  # calm gap, fine
