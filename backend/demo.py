"""
Synthetic market feed for `python run.py --demo`.

It does NOT touch the strategy: it only produces bookTicker-style ticks and hands them to
the engine's real `process_spot_tick` / `process_perp_tick`, so entry gates, book-walk
sizing, fills, reversion exits, timeouts and the trade log all run exactly as they do live.

The simulated perp-minus-spot spread is Ornstein-Uhlenbeck around a slowly-set mean with
occasional shocks (0.08-0.30%) big enough to clear round-trip costs, so trades happen within
a minute or two instead of hours. ~30% of shocks are regime shifts that do not revert, which
produces timeouts and losing trades. run.py also shortens MAX_HOLD_SEC in demo mode so those
timeouts show up quickly. All numbers are synthetic and are flagged as demo data everywhere
in the dashboard.
"""
import math
import random
import threading
import time

from . import engine as E

DEMO_COINS = {          # symbol: reference price
    "BTC": 65000.0, "ETH": 3200.0, "SOL": 150.0, "XRP": 0.62,
    "DOGE": 0.16, "ADA": 0.45, "LINK": 15.0, "AVAX": 36.0,
}
TICK_DT   = 0.05        # seconds between simulated ticks per coin (20/s per leg)
HALF_SPR  = 0.00004     # half bid-ask spread, as a fraction of price (0.004%)


def _book_tick(price, notional):
    q = notional / price
    return {"b": f"{price * (1 - HALF_SPR):.10f}", "B": f"{q:.6f}",
            "a": f"{price * (1 + HALF_SPR):.10f}", "A": f"{q:.6f}"}


def _run_coin(cs, base_price, seed):
    rng = random.Random(seed)
    price  = base_price
    mean   = rng.uniform(-0.03, 0.03)               # long-run spread, %
    spread = mean
    tau    = 8.0                                     # reversion time constant, s
    a      = 1.0 - math.exp(-TICK_DT / tau)
    next_shock = time.time() + rng.uniform(6, 20)
    while True:
        price *= math.exp(rng.gauss(0.0, 0.00004))
        spread += (mean - spread) * a + rng.gauss(0.0, 0.0012)
        now = time.time()
        if now >= next_shock:
            jump = rng.choice((-1, 1)) * rng.uniform(0.08, 0.30)
            spread += jump
            if rng.random() < 0.3:
                # regime shift: the spread's centre moves with the shock, so it does NOT revert to
                # the level the position was entered against → exercises timeouts and losing trades
                mean += 0.7 * jump
                if abs(mean) > 0.2:
                    mean *= 0.4
            next_shock = now + rng.uniform(25, 70)
        depth = rng.uniform(1500, 15000)             # top-of-book USD notional
        E.process_spot_tick(cs, _book_tick(price, depth))
        E.process_perp_tick(cs, _book_tick(price * (1 + spread / 100), depth * rng.uniform(0.6, 1.4)))
        time.sleep(TICK_DT)


def start_feed():
    """Spawn one feeder thread per coin in E.ALL_CS (call after E.start_engine)."""
    for i, cs in enumerate(E.ALL_CS):
        base = DEMO_COINS.get(cs.symbol, 10.0)
        threading.Thread(target=_run_coin, args=(cs, base, 1000 + i),
                         name=f"demo-{cs.symbol}", daemon=True).start()
