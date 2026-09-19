# xmoney — funding capture with convergence

A delta-neutral strategy that gets paid twice for taking no view on the price, with a live
dashboard around it.

Every perpetual futures contract pays a fee every few hours to whichever side is out of
favour. We hold the real coin and the perpetual in equal size and opposite directions, so
the coin's own price cancels out entirely, and collect that fee. While we wait, the gap
between the two prices tends to close, which pays us a second time.

**No real orders are ever placed.** All fills are simulated against the live order book.

```
xmoney/
├── run.py                  start one engine + dashboard
├── compare.py              run several fee tiers side by side, each its own book
├── backend/
│   ├── engine.py           strategy, feeds, execution, accounting
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
python run.py --coins LSK,ONE,XTZ,SOL,SEI,WIF,TIA,ORDI
```

| Command | What it does |
|---|---|
| `python run.py` | Live Binance data, all 172 coins → http://127.0.0.1:8001 |
| `python run.py --coins LSK,ONE,XTZ` | Only those coins. |
| `python compare.py` | **Four engines at once** — ZERO, VIP0, VIP5, VIP9 — on ports 8100-8103, each an independent book taking its own trades. One window per tier. |
| `python run.py --demo` | Synthetic feed, no exchange connection. Labelled "Demo"; writes to `data/demo/`, never the real CSV. |
| `python run.py --port 9000` | Different port. |
| `pytest -q` | Run the tests (~12 s). |

Stop with `Ctrl+C`. Closed trades are kept for the session and appended to the CSV, but the
dashboard's counters and equity curve start fresh on restart.

## How a trade works

**Entry.** Every gate has to pass. Each one exists because of something that went wrong
without it.

| Gate | Rule |
|---|---|
| Funding is real | `\|rate\| ≥ MIN_FUNDING_PCT`, feed under 60 s old |
| A payment is due soon | within `FUNDING_ENTRY_WINDOW_FRAC` of that coin's own funding cycle |
| Costs are known | `friction > 0`. Zero means *not measured yet*, not *free* |
| The gap doesn't outrun the funding | `stop ÷ basis_volatility ≥ MIN_STOP_SIGMAS`. Unmeasured also blocks |
| The edge beats friction by a margin | `funding ≥ EDGE_FRICTION_MULT × friction`. Entry is decided on funding alone |
| It's worth the capital | annualised return `≥ MIN_FUNDING_APR` |
| Not still cooling off | `STOP_COOLDOWN_SEC` since the last stop on this coin |

Direction follows the sign: a positive rate means longs pay shorts, so we short the perp; a
negative rate means the reverse. Either way we are on the receiving side, which is why the
dashboard treats both as income rather than colouring negatives as a loss.

Convergence is measured against **spot itself**, not a rolling average of past gaps: the perp
is meant to track spot, so zero is the real anchor. That also means a coin can trade as soon
as it has a quote and a volatility estimate, rather than waiting out an eight minute window.
Convergence is upside we take when it appears, never a reason to enter.

**Execution.** Both legs cross the book together, one millisecond after the signal, so a
fill never lands on the tick that produced it and the pair is never half on. The fee is
`FILL_FEE_TYPE` — chosen, not raced for. The engine used to post passively and wait up to
200 ms for a better price; that wait turned a stop sized at 0.22% into a 3.16% loss,
because the perp bid fell 3.4% while it waited.

**Exit**, in priority order:

1. **Stop loss** — the gap moved `STOP_LOSS_FUNDING_MULT × funding` against us, measured
   from where the trade started rather than from zero. A pair opens already down one round
   trip of spread; comparing raw PnL against the stop fired the instant we opened on any
   coin whose spread was wider than the stop, then reopened and fired again next tick.
2. **Before the payment, nothing else closes it.** The payment is the reason we are here.
3. **Convergence done** — the gap has moved `REVERSION_FRACTION` of its starting size **in
   our favour**. Which way counts depends on the side we are on: a short perp profits as the
   gap falls, a long perp as it rises. When the gap starts on the wrong side, closing it
   *costs* us, so there is no second payday and the trade rests on the funding alone.
4. **Otherwise, decided at each stamp, not on a clock.** Is the next payment still in our
   favour, and does the return from here still clear the hurdle? If yes we stay and collect
   again — an hourly pair pays every hour we hold it. If no, we leave.

**Accounting.** `net = gross + funding − fees`. Gross is the price move between our fills
and already contains the spread crossed both ways, so it is negative on almost every trade:
a cost, not the result. Friction belongs in the entry gate, not charged again afterwards;
subtracting it twice made every position look a full spread worse than it was.

## Fee tiers

`ZERO` through `VIP9`, applied per leg across all four executions, switchable live from the
Strategy tab or `POST /api/control/fee_tier` — every gate, fill and PnL picks it up on the
next tick.

`compare.py` runs several tiers **simultaneously** as independent books. They see the same
market and disagree about which trades are worth taking, which makes the breakeven tier
visible: at the funding levels these coins carry, ZERO and VIP9 trade while VIP0 and VIP5
refuse, because the fee alone exceeds the edge.

## Environment variables

| Variable | Default | Effect |
|---|---|---|
| `STRATEGY` | `funding` | `funding`, `spread` (the original 2σ mean reversion), or `both`. |
| `FEE_TIER` | `ZERO` | `ZERO` or `VIP0`–`VIP9`. |
| `FILL_FEE_TYPE` | `maker` | Fee charged on every fill. `taker` for the conservative view. |
| `MIN_FUNDING_PCT` | `0.0050` | Smallest rate worth entering for. |
| `MIN_FUNDING_APR` | `12.0` | Minimum annualised return, at entry and at every stamp. |
| `EDGE_FRICTION_MULT` | `1.5` | Edge must beat friction by this multiple, not merely exceed it. |
| `MIN_STOP_SIGMAS` | `2.0` | Skip coins whose gap routinely travels further than funding can pay for. |
| `MIN_VOL_SAMPLES` | `30` | Buckets needed before that volatility can be judged. |
| `FUNDING_ENTRY_WINDOW_FRAC` | `1.0` | Entry window as a fraction of each coin's own funding cycle. |
| `FUNDING_ENTRY_WINDOW_SEC` | `28800` | Absolute ceiling on that window. |
| `REVERSION_FRACTION` | `0.90` | How much of the gap we expect to close, and the exit trigger. |
| `STOP_LOSS_FUNDING_MULT` | `2.0` | Stop distance as a multiple of the funding being collected. |
| `STOP_LOSS_MIN_PCT` | `0.05` | Floor on that, for when funding is tiny. |
| `STOP_COOLDOWN_SEC` | `120` | Wait before re-entering a coin we were just stopped on. |
| `FUNDING_MAX_HOLD_SEC` | `21600` | Safety ceiling only; holding is decided at each stamp. |
| `ENTRY_DELAY_SEC` / `EXIT_DELAY_SEC` | `0.001` | Latency before a fill, so it never lands on the signal tick. |
| `ROLLING_WIN_OVERRIDE` | `1000` | Buckets defining the mean the gap reverts to (× 0.5 s = 500 s). Lower for a faster warm-up and a noisier mean. |
| `STREAM_TYPE` | `bookTicker` | `bookTicker` (top of book, every tick) or `depth20` (20 levels @100 ms). |
| `OUTPUT_DIR` / `OUTPUT_SUBDIR` | `data/` / `multi_coin` | Where `trades_master.csv` goes. |
| `HOLD_TICKER` | `0` | `1` re-enables the per-tick "hold=…" console line (noisy). |
| `HOST` / `PORT` | `127.0.0.1` / `8000` | Bind address. |

## The dashboard

* **Header** — connection state, uptime, ticks/s, feed health, **Pause entries**, theme.
* **KPI tiles** — realised PnL, volume-weighted net % per trade, won/lost, notional,
  open positions, funding tracked.
* **Open positions** — how much of the gap has closed against the exit target, current net,
  hold time, and the countdown to the next payment.
* **Coins** — funding rate, what it annualises to, countdown to the next payment, the gap
  and how far it sits from its mean, friction, and **Stop σ**: how many of that coin's own
  standard deviations the stop sits away. Under the threshold the coin is skipped and the
  cell turns red. Hover anything for the reasoning in words.
* **Trades** — gross, the funding that bridges it, net, and why it exited. Gross is left
  neutral because it is a component rather than a verdict; net carries the colour. Hover net
  for the arithmetic. **Download CSV**.
* **Strategy** — four clickable steps explaining the idea in plain language, a chart of one
  trade, and the live fee-tier switch.
* **Feeds** — the exchange connections, messages/s, lag, reconnects, last error.
* **Config** — the live parameters, read-only.

**Quote age vs. a quiet book.** `bookTicker` only sends a message when a coin's top of book
changes, so a thin coin can go a minute without one. While its connection keeps delivering
messages, that coin's last quote *is* the current quote, so the engine keeps it fresh
instead of calling the coin stale. **Offline** means that coin's connection is reconnecting;
**Stale** means the connection is up but delivered no quote for it.

## Data feeds

| What | Source | Notes |
|---|---|---|
| Spot book | `stream.binance.com` bookTicker | tick by tick, top of book only |
| Perp book | `fstream.binance.com` bookTicker | same |
| Funding rate + next stamp | `fapi/v1/premiumIndex` REST | every 5 s, all symbols in one call |
| Funding interval | `fapi/v1/fundingInfo` REST | at startup; 8 h default, many pairs 4 h, LSK and ONE hourly |

**Funding is REST, not WebSocket.** The documented `!markPrice@arr` stream opens cleanly and
then never delivers a frame — verified against a bookTicker control on the same socket,
2280 messages against 0 in six seconds, including when both were subscribed together.
`premiumIndex` covers every symbol in one unauthenticated call.

A coin missing from either leg is flagged **Not listed** at startup and never trades.

## What this does not model

- Fills assume the visible top of book. In `bookTicker` mode there is only one level, so
  size is capped by it and there is no depth-walking slippage. `depth20` gives real depth.
- The fee type is an assumption, not a simulated race. A resting order is not guaranteed to
  fill; `FILL_FEE_TYPE=taker` is the conservative reading.
- Both legs are assumed to fill at the same instant.
- Shorting spot is treated as costless. A real margin short pays a borrow fee.
- Funding is credited from the live rate at the stamp, which can differ from the rate seen
  at entry. If it flips, the trade pays instead of collecting, and that is recorded as such.
- Trade size is capped per position, not against a shared pot of capital. Several positions
  can be open at once with no portfolio-level limit on total exposure.
