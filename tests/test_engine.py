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
    # long spot / short perp: spot flat, perp fell 100.3 → 100.0 (leg is measured vs the perp entry price)
    long_pos = {"direction": 1, "entry_spot_fill": 100.0, "entry_perp_fill": 100.3, "notional_usd": 1000.0}
    gross, net, usd = E.calc_pnl(long_pos, 100.0, 100.0, 0.1)
    expect = (100.3 - 100.0) / 100.3 * 100
    assert gross == pytest.approx(expect, abs=1e-9) and net == pytest.approx(expect - 0.1, abs=1e-9)
    assert usd == pytest.approx(net / 100 * 1000.0, abs=1e-9)
    # short spot / long perp: perp rose back 99.7 → 100.0
    short_pos = {"direction": -1, "entry_spot_fill": 100.0, "entry_perp_fill": 99.7, "notional_usd": 1000.0}
    gross, net, _ = E.calc_pnl(short_pos, 100.0, 100.0, 0.1)
    expect = (100.0 - 99.7) / 99.7 * 100
    assert gross == pytest.approx(expect, abs=1e-9) and net == pytest.approx(expect - 0.1, abs=1e-9)
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
