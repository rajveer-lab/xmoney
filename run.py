"""
Start the strategy engine and the dashboard server.

    python run.py                          # all 143 coins, live Binance data, http://127.0.0.1:8000
    python run.py --coins BTC,ETH,SOL      # subset (quick smoke test)
    python run.py --demo                   # synthetic feed, no exchange connection, trades within ~a minute
    python run.py --port 9000 --console-stats

Environment (all optional): STREAM_TYPE=bookTicker|depth20, ROLLING_WIN_OVERRIDE,
ENTRY_DELAY_SEC, EXIT_DELAY_SEC, OUTPUT_DIR, OUTPUT_SUBDIR, HOLD_TICKER=1, HOST, PORT.
"""
import argparse
import os
import sys

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

# The engine reads these at import time, so demo defaults must be set before importing it.
# A short window makes coins ready in ~20s; a separate output dir keeps synthetic trades
# out of the real trades_master.csv.
if "--demo" in sys.argv:
    os.environ.setdefault("ROLLING_WIN_OVERRIDE", "40")
    os.environ.setdefault("OUTPUT_SUBDIR", "demo")

from backend import engine as E                     # noqa: E402
from backend import demo                            # noqa: E402
from backend.server import create_app               # noqa: E402
from backend.state import Broadcaster               # noqa: E402


def main():
    ap = argparse.ArgumentParser(description="Spot-perp mean-reversion paper trader + dashboard")
    ap.add_argument("--host", default=os.environ.get("HOST", "127.0.0.1"),
                    help="bind address (default 127.0.0.1; the dashboard has a control endpoint, "
                         "so only widen this on a trusted network)")
    ap.add_argument("--port", type=int, default=int(os.environ.get("PORT", 8000)))
    ap.add_argument("--coins", default=os.environ.get("COINS", ""),
                    help="comma-separated symbols to trade instead of the full list, e.g. BTC,ETH")
    ap.add_argument("--demo", action="store_true",
                    help="drive the engine with a synthetic feed instead of Binance (clearly labelled in the UI)")
    ap.add_argument("--console-stats", action="store_true",
                    help="also print the 10s global stats table to the console")
    args = ap.parse_args()

    if args.demo:
        E.DEMO_MODE = True
        E.COINS = list(demo.DEMO_COINS)
        E.MAX_HOLD_SEC = 45.0          # demo pace: timeouts (and their losses) appear within a minute
    elif args.coins.strip():
        E.COINS = [c.strip().upper() for c in args.coins.split(",") if c.strip()]

    E.start_engine(console_stats=args.console_stats, connect_ws=not args.demo)
    if args.demo:
        demo.start_feed()
    bc = Broadcaster(interval=1.0).start()
    app = create_app(bc)

    print(f"\n  Dashboard → http://{args.host}:{args.port}   (Ctrl+C to stop)"
          + ("   [DEMO — synthetic data]" if args.demo else "") + "\n")
    try:
        app.run(host=args.host, port=args.port, threaded=True, debug=False, use_reloader=False)
    except KeyboardInterrupt:
        pass


if __name__ == "__main__":
    main()
