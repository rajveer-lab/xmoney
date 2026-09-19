#!/usr/bin/env python3
"""
Run the same strategy at several fee tiers at once, side by side.

Each tier gets its own engine, its own feeds and its own trade log, so the books
are genuinely independent: a ZERO-fee book will take trades a VIP0 book refuses,
because the entry gate prices that tier's fees into the friction it has to beat.
Nothing is shared between them except the market itself.

    python compare.py
    python compare.py --tiers ZERO,VIP0,VIP5,VIP9 --coins LSK,ONE,XTZ,SOL

Opens one grid page with every tier's live dashboard side by side. Ctrl+C stops
all of them.
"""
import argparse
import http.server
import os
import signal
import socketserver
import subprocess
import sys
import threading
import time
import webbrowser

ROOT = os.path.dirname(os.path.abspath(__file__))

GRID_HTML = """<!doctype html>
<meta charset="utf-8">
<title>xmoney — same strategy, different fee tiers</title>
<style>
  :root {{ color-scheme: dark; }}
  * {{ box-sizing: border-box; }}
  body {{ margin: 0; background: #0d0d0d; color: #fff;
         font: 14px system-ui, -apple-system, "Segoe UI", sans-serif; }}
  header {{ padding: 10px 16px; border-bottom: 1px solid rgba(255,255,255,.12);
            display: flex; align-items: baseline; gap: 12px; flex-wrap: wrap; }}
  h1 {{ font-size: 15px; margin: 0; }}
  header p {{ margin: 0; color: #a9a7a1; font-size: 13px; }}
  .grid {{ display: grid; grid-template-columns: repeat({cols}, 1fr);
           gap: 10px; padding: 10px; height: calc(100vh - 46px); }}
  .pane {{ display: flex; flex-direction: column; min-height: 0;
           border: 1px solid rgba(255,255,255,.14); border-radius: 10px; overflow: hidden; }}
  .bar {{ padding: 7px 12px; background: #1a1a19; border-bottom: 1px solid rgba(255,255,255,.12);
          display: flex; align-items: baseline; gap: 8px; }}
  .tier {{ font-weight: 700; letter-spacing: .02em; }}
  .cost {{ color: #a9a7a1; font-size: 12px; font-variant-numeric: tabular-nums; }}
  .free {{ color: #4ade80; }}
  iframe {{ flex: 1; border: 0; width: 100%; background: #0d0d0d; }}
</style>
<header>
  <h1>Same strategy, four fee tiers, running at once</h1>
  <p>Each pane is an independent book. They see the same market and disagree about
     which trades are worth taking, because each prices its own fees into the hurdle.</p>
</header>
<div class="grid">{panes}</div>
"""

PANE = """  <div class="pane">
    <div class="bar"><span class="tier {cls}">{tier}</span>
      <span class="cost">{cost}</span></div>
    <iframe src="http://127.0.0.1:{port}" title="{tier} dashboard"></iframe>
  </div>
"""


def tier_cost(tier):
    """Round-trip taker cost for a tier, read from the engine's own table."""
    sys.path.insert(0, ROOT)
    from backend import engine as E
    if tier not in E.FEE_TIERS:
        return "unknown tier"
    sm, st, pm, pt = E.FEE_TIERS[tier]
    if st == 0 and pt == 0:
        return "spread only — no fees"
    return f"round trip {2 * (st + pt):.3f}% taker · {2 * (sm + pm):.3f}% maker"


def start_engine(tier, port, coins, extra_env):
    env = dict(os.environ)
    env["FEE_TIER"] = tier
    # separate trade log per tier, so the CSVs never mix
    env["OUTPUT_SUBDIR"] = f"compare_{tier.lower()}"
    env.update(extra_env)
    cmd = [sys.executable, os.path.join(ROOT, "run.py"), "--port", str(port)]
    if coins:
        cmd += ["--coins", coins]
    log = open(os.path.join(ROOT, f"compare_{tier.lower()}.log"), "w")
    return subprocess.Popen(cmd, cwd=ROOT, env=env, stdout=log, stderr=subprocess.STDOUT)


def serve_grid(port, tiers, ports):
    panes = "".join(
        PANE.format(tier=t, port=p, cost=tier_cost(t),
                    cls="free" if t == "ZERO" else "")
        for t, p in zip(tiers, ports)
    )
    cols = 2 if len(tiers) > 2 else len(tiers)
    html = GRID_HTML.format(panes=panes, cols=cols).encode()

    class Handler(http.server.BaseHTTPRequestHandler):
        def do_GET(self):
            self.send_response(200)
            self.send_header("Content-Type", "text/html; charset=utf-8")
            self.send_header("Content-Length", str(len(html)))
            self.end_headers()
            self.wfile.write(html)

        def log_message(self, *a):
            pass

    socketserver.TCPServer.allow_reuse_address = True
    srv = socketserver.TCPServer(("127.0.0.1", port), Handler)
    threading.Thread(target=srv.serve_forever, daemon=True).start()
    return srv


def main():
    ap = argparse.ArgumentParser(description="Run the strategy at several fee tiers side by side.")
    ap.add_argument("--tiers", default="ZERO,VIP0,VIP5,VIP9",
                    help="comma separated, in ZERO / VIP0..VIP9 (default: ZERO,VIP0,VIP5,VIP9)")
    ap.add_argument("--coins", default="", help="comma separated, e.g. LSK,ONE,XTZ,SOL")
    ap.add_argument("--base-port", type=int, default=8100, help="first engine port (default 8100)")
    ap.add_argument("--grid-port", type=int, default=8099, help="grid page port (default 8099)")
    ap.add_argument("--no-open", action="store_true", help="don't open any browser window")
    ap.add_argument("--grid", action="store_true",
                    help="also open the single-page grid (needs a browser that allows framing)")
    args = ap.parse_args()

    tiers = [t.strip().upper() for t in args.tiers.split(",") if t.strip()]
    ports = [args.base_port + i for i in range(len(tiers))]

    procs = []
    for tier, port in zip(tiers, ports):
        procs.append(start_engine(tier, port, args.coins, {}))
        print(f"  {tier:<5} → http://127.0.0.1:{port}   ({tier_cost(tier)})")
        time.sleep(0.6)          # stagger the WS handshakes

    srv = serve_grid(args.grid_port, tiers, ports)
    grid_url = f"http://127.0.0.1:{args.grid_port}"
    print(f"\n  One window per tier opens automatically.")
    print(f"  Grid view (only if your browser allows framing) → {grid_url}")
    print("  Each coin still needs its warm-up before it can trade. Ctrl+C stops everything.\n")

    if not args.no_open:
        # A window per tier is the reliable view — some browsers refuse to frame
        # localhost pages at all, which leaves the grid blank with no error.
        def open_windows():
            for port in ports:
                webbrowser.open_new(f"http://127.0.0.1:{port}")
                time.sleep(0.4)
            if args.grid:
                webbrowser.open_new(grid_url)
        threading.Timer(2.5, open_windows).start()

    def stop(*_):
        print("\nstopping…")
        for p in procs:
            p.terminate()
        for p in procs:
            try:
                p.wait(timeout=5)
            except subprocess.TimeoutExpired:
                p.kill()
        srv.shutdown()
        sys.exit(0)

    signal.signal(signal.SIGINT, stop)
    signal.signal(signal.SIGTERM, stop)
    while True:
        time.sleep(1)
        for tier, p in zip(tiers, procs):
            if p.poll() is not None:
                print(f"  ⚠️  {tier} engine exited (code {p.returncode}) — see compare_{tier.lower()}.log")
                procs.remove(p)
                tiers.remove(tier)
                break


if __name__ == "__main__":
    main()
