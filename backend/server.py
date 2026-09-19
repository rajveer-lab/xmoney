"""
Flask API + static dashboard.

  GET  /                       dashboard
  GET  /api/health             liveness + engine/broadcaster status
  GET  /api/state              latest state snapshot (same payload the SSE stream sends)
  GET  /api/stream             Server-Sent Events: `event: state` once per second
  GET  /api/trades             closed trades  ?since=<seq>&limit=<n>
  GET  /api/trades.csv         the master CSV written by the engine
  POST /api/control/entries    {"enabled": true|false}  pause / resume NEW entries
"""
import logging
import os
from urllib.parse import urlparse

from flask import Flask, Response, abort, jsonify, request, send_file, send_from_directory, stream_with_context

from . import engine as E
from .state import Broadcaster

DASH_DIR = os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))), "dashboard")


def create_app(bc: Broadcaster) -> Flask:
    app = Flask(__name__, static_folder=None)
    logging.getLogger("werkzeug").setLevel(logging.WARNING)

    @app.after_request
    def _no_cache(resp):
        resp.headers["Cache-Control"] = "no-cache"
        return resp

    # ── dashboard ────────────────────────────────────────────────────────────
    @app.get("/")
    def index():
        return send_from_directory(DASH_DIR, "index.html")

    @app.get("/static/<path:name>")
    def static_files(name):
        return send_from_directory(DASH_DIR, name)

    # ── read API ─────────────────────────────────────────────────────────────
    @app.get("/api/health")
    def health():
        return jsonify(ok=True, engine_started=bool(E.ALL_CS), coins=len(E.ALL_CS),
                       state_version=bc.version, entries_enabled=E.ENTRIES_ENABLED.is_set())

    @app.get("/api/state")
    def state():
        with bc.cond:
            payload = bc.payload
        return Response(payload, mimetype="application/json")

    @app.get("/api/stream")
    def stream():
        def gen():
            last = 0
            yield "retry: 2000\n\n"
            while True:
                with bc.cond:
                    bc.cond.wait_for(lambda: bc.version != last, timeout=15)
                    ver, payload = bc.version, bc.payload
                if ver != last:
                    last = ver
                    yield f"id: {ver}\nevent: state\ndata: {payload}\n\n"
                else:
                    yield ": keepalive\n\n"
        return Response(stream_with_context(gen()), mimetype="text/event-stream",
                        headers={"Cache-Control": "no-cache", "X-Accel-Buffering": "no"})

    @app.get("/api/trades")
    def trades():
        try:
            since = max(0, int(request.args.get("since", 0)))
            limit = min(5000, max(1, int(request.args.get("limit", 500))))
        except ValueError:
            abort(400, "since/limit must be integers")
        rows, head = E.trades_since(since, limit)
        return jsonify(trades=rows, head=head)

    @app.get("/api/trades.csv")
    def trades_csv():
        if not os.path.isfile(E.MASTER_CSV_PATH):
            abort(404, "no trades recorded yet")
        return send_file(E.MASTER_CSV_PATH, mimetype="text/csv", as_attachment=True,
                         download_name="trades_master.csv")

    # ── control ──────────────────────────────────────────────────────────────
    @app.post("/api/control/entries")
    def control_entries():
        # Block cross-site POSTs (a random web page must not be able to flip this).
        origin = request.headers.get("Origin")
        if origin and urlparse(origin).netloc != request.host:
            abort(403, "cross-origin request refused")
        body = request.get_json(silent=True)
        if not isinstance(body, dict) or not isinstance(body.get("enabled"), bool):
            abort(400, 'expected JSON body {"enabled": true|false}')
        if body["enabled"]:
            E.ENTRIES_ENABLED.set()
        else:
            E.ENTRIES_ENABLED.clear()
        print(f"[control] new entries {'ENABLED' if body['enabled'] else 'PAUSED'}")
        return jsonify(entries_enabled=E.ENTRIES_ENABLED.is_set())

    @app.post("/api/control/fee_tier")
    def control_fee_tier():
        """Swap the fee tier live. Every gate, fill and PnL picks it up on the
        next tick, so profitability can be shown at ZERO through VIP9 without a
        restart."""
        origin = request.headers.get("Origin")
        if origin and urlparse(origin).netloc != request.host:
            abort(403, "cross-origin request refused")
        body = request.get_json(silent=True)
        tier = (body or {}).get("tier")
        if not isinstance(tier, str) or tier.strip().upper() not in E.FEE_TIERS:
            abort(400, f'expected {{"tier": one of {sorted(E.FEE_TIERS)}}}')
        active = E.set_fee_tier(tier)
        print(f"[control] fee tier -> {active}")
        return jsonify(fee_tier=active,
                       round_trip_maker=E.round_trip_fee_pct("maker"),
                       round_trip_taker=E.round_trip_fee_pct("taker"))

    @app.errorhandler(400)
    @app.errorhandler(403)
    @app.errorhandler(404)
    def _err(e):
        return jsonify(error=e.description), e.code

    return app
