# Spot–Perp Reversion — paper trading dashboard

A live dashboard around your Binance spot-vs-perp mean-reversion strategy. The strategy
itself is unchanged; this project runs it, records every trade, and shows what it is doing.

**No real orders are ever placed.** All fills are simulated against the live order book.

```
open_crypto_strat/
├── run.py                  start engine + dashboard
├── backend/
│   ├── engine.py           your strategy (see "Changes to the engine" below)
│   ├── state.py            read-only snapshot of the engine → JSON, one broadcaster thread
│   ├── server.py           Flask API + SSE stream + static dashboard
│   └── demo.py             synthetic market feed for --demo
├── dashboard/              index.html · styles.css · app.js  (no build step, no CDN)
├── tests/test_engine.py    engine end-to-end + API tests (no network)
└── data/<subdir>/trades_master.csv    written by the engine (created on first trade)
```

## Run it

```bash
pip install -r requirements.txt
python run.py                       # live Binance data, all 143 coins → http://127.0.0.1:8000
```

| Command | What it does |
|---|---|
| `python run.py` | Live data, all coins. Each coin needs ~500 s of ticks before it can trade — the dashboard shows warm-up progress. |
| `python run.py --coins BTC,ETH,SOL` | Only those coins (quick smoke test). |
| `python run.py --demo` | **Synthetic** feed, no exchange connection. Coins are ready in ~20 s and trades appear within a minute, so you can explore the whole UI. Clearly labelled "Demo" in the header and a banner; writes to `data/demo/`, never the real CSV. |
| `python run.py --port 9000` | Different port. |
| `python run.py --console-stats` | Also print the 10-second global table in the terminal. |
| `pytest -q` | Run the tests (~10 s). |

Stop with `Ctrl+C`. State lives in memory: closed trades are kept for the session and every
trade is appended to the CSV, but the dashboard's counters and equity curve start fresh on restart.

### Environment variables

| Variable | Default | Effect |
|---|---|---|
| `STREAM_TYPE` | `bookTicker` | `bookTicker` (top of book, every tick) or `depth20` (20 levels @100 ms). |
| `ROLLING_WIN_OVERRIDE` | `1000` | Rolling window in 0.5 s buckets for fast coins (medium = ¼, slow = 1⁄10). Lower = quicker warm-up, noisier σ. |
| `ENTRY_DELAY_SEC` / `EXIT_DELAY_SEC` | `0` | Optional simulated latency (a real `sleep`). Leave at 0 for tick-by-tick fills. |
| `OUTPUT_DIR` / `OUTPUT_SUBDIR` | `data/` / `multi_coin` | Where `trades_master.csv` goes. |
| `HOLD_TICKER` | `0` | `1` re-enables the per-tick "hold=…" console line (noisy). |
| `HOST` / `PORT` | `127.0.0.1` / `8000` | Bind address. |

## The dashboard

* **Header** – connection state, uptime, ticks/s, WebSocket health, **Pause entries** switch,
  light/dark theme.
* **Net PnL** and KPI tiles – realised PnL, volume-weighted net % per trade, trades won/lost,
  notional traded, open positions, coins ready.
* **Cumulative net PnL by trade** – hover (or use ← → on the focused chart) to inspect any trade.
* **Open positions** – how much of the entry deviation has reverted vs the exit target, current net,
  hold time vs max hold.
* **Coins** – every coin: state (Open / Flat / Warming / Offline / Stale / Not listed), spread,
  **σ from the rolling mean** (gauge marks ±entry threshold), friction, signals, trades, W/L, net,
  **quote age**. Sort by any column, filter by state, search by symbol. Hover *Signals* for the gate
  breakdown; hover *Quote age* for each leg's feed and how long its book has been unchanged.
* **Trades** – newest first, filter by coin/exit type, **Download CSV**.
* **Feeds** – the 6 exchange connections: status, messages/s, lag (perp: Binance event time → handled
  here), uptime, reconnects, last error.
* **Config** – the live strategy parameters (read-only).

**Quote age vs. a quiet book.** `bookTicker` only sends a message when a coin's top of book changes, so a
thin coin can go a minute without one. While its connection keeps delivering messages, that coin's last
quote *is* the current quote, so the engine keeps it fresh (quote age ≈ 1 s) instead of calling the coin
stale. **Offline** means that coin's connection is reconnecting; **Stale** means the connection is up but
delivered no quote for it.

**Pause entries** stops *new* positions only; open positions still exit normally (reversion, timeout).

### API

| | |
|---|---|
| `GET /api/state` | latest snapshot (also pushed every second on the stream) |
| `GET /api/stream` | Server-Sent Events, `event: state` |
| `GET /api/trades?since=<seq>&limit=<n>` | closed trades |
| `GET /api/trades.csv` | the master CSV |
| `GET /api/health` | liveness |
| `POST /api/control/entries` | `{"enabled": true\|false}` — pause/resume new entries |

The server binds to `127.0.0.1` by default. The control endpoint rejects cross-origin browser requests,
but it has no login — don't expose it beyond a trusted network.

## Changes to the engine

`backend/engine.py` is a copy of `20_depth_all_coins_final.py`; the original is untouched. No line of
the gate, sizing, fill or exit calculations was changed (verified with
`diff --strip-trailing-cr 20_depth_all_coins_final.py backend/engine.py` — the only changed lines are the
ones listed below; line endings were normalised from CRLF to LF). Changes:

1. **`start_engine()`** — `main()`'s startup split out so a server can start it without blocking.
   `python backend/engine.py` still works as the console version.
2. **Trade feed** — `record_trade_event()` / `trades_since()`; `execute_exit` publishes each closed trade.
3. **`ENTRIES_ENABLED`** switch, checked at the top of `check_entry_on_tick`.
4. **`run_bucket()` + per-coin `bucket_lock`** — the spot and perp WebSocket threads both trigger
   `process_bucket` for the same coin; without a lock they could run it concurrently and race on the
   rolling window. A second caller now skips instead of doubling up.
5. **`ROLLING_WIN_OVERRIDE` now works** — the tier table was hard-coded, so the variable was dead.
   Defaults are identical (1000 / 250 / 100).
6. **Output dir** — defaults to `open_crypto_strat/data/<subdir>` (was `~/sol_spread/<subdir>`);
   override with `OUTPUT_DIR`.
7. **`HOLD_TICKER`** — the per-tick `print_hold` line is now off by default.
8. **Connection handling (stale / offline fix)** — all in `run_combined_ws` and the new feed monitor:
   * Drops are **scoped to the connection** that dropped. Before, `on_error` / `on_close` / `on_open`
     looped over *all 144* coins, so one of the 6 connections dropping marked every coin offline, added
     an error to every coin, started the 10 s entry cooldown everywhere, and an outage over 10 s **wiped
     every coin's warm-up** (logged only as "connected"). Now only that connection's ≤ 50 coins are
     affected. The 10 s keep-vs-re-warm rule itself is unchanged.
   * **Faster reconnect** — retry after 1 s, backing off to the original 5 s only if it keeps failing,
     so a blip stays under the 10 s re-warm threshold.
   * **Dead-feed detection** — a connection that delivers nothing for 10 s is reopened (the 20 s ping
     + 10 s timeout could take up to 30 s to notice).
   * **Quiet coins stay current** — `feed_monitor` advances a coin's `latest[*_ts]` to its connection's
     last message time while that connection is live and the quote came in on the current session.
     `process_bucket`'s staleness skip and the watchdog's fallback bucketing use those timestamps, so
     thin coins keep warming instead of stalling. `*_last_tick` still records real book changes.
   * **REST seed** — on (re)connect each coin with no tick yet gets its top of book from
     `/ticker/bookTicker` (book only; no gate check runs on it), so quiet coins have a quote at once.
     It also flags coins a venue doesn't list or has halted (VANRY: spot halted, no perp).
   * **`skip_utf8_validation=True`** — websocket-client was validating every message's UTF-8 in pure
     Python (~20 µs/msg, about a quarter of the GIL at burst rates); `json.loads` still validates, in C.
9. **Coin list** — USDC removed (stablecoin; USDCUSDT is pegged, so there is no spread to revert).
   143 coins. USDT is the quote currency of every pair, not a coin in the list.

## Things to know about the strategy (not changed)

* In **`bookTicker`** mode the book is one level deep, so trade size is capped by the top-of-book
  quantity and there is no book-walk slippage. `depth20` gives real depth-based sizing.
* Fills are computed against the tick that triggered the signal — zero simulated latency, so results
  are optimistic compared with a real system that has network + matching delay.
* Spot and perp legs are assumed to fill together at the same instant.
* The `ALLO`, `MEGA`, … symbols in the coin list must exist on **both** spot and USDⓈ-M futures;
  a coin missing or halted on one side shows as **Not listed** and never trades (currently VANRY).
* The whole feed runs in one Python process, so all 6 connections share one core for Python work. It
  normally runs at 30–50 % with ~0.2–0.35 s of network lag. Market bursts (~13k msgs/s seen) push lag to
  about 1 s, and it recovers. If the Feeds tab shows lag climbing past a few seconds, the next step is
  splitting the feeds across processes.
