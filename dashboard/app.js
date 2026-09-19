"use strict";
(() => {
  // ── helpers ───────────────────────────────────────────────────────────────
  const $ = (sel, root = document) => root.querySelector(sel);
  const MINUS = "−";

  function el(tag, cls, text) {
    const e = document.createElement(tag);
    if (cls) e.className = cls;
    if (text != null) e.textContent = text;          // labels are data → textContent only
    return e;
  }
  function setText(node, txt) { if (node.textContent !== txt) node.textContent = txt; }
  function setCls(node, cls) { if (node.className !== cls) node.className = cls; }

  const isNum = (n) => typeof n === "number" && Number.isFinite(n);
  const sign = (n) => (n > 0 ? "+" : n < 0 ? MINUS : "");
  const signCls = (n) => (!isNum(n) || n === 0 ? "" : n > 0 ? "pos" : "neg");

  function fmtUsd(n, signed = true) {
    if (!isNum(n)) return "–";
    const a = Math.abs(n);
    const d = a >= 100 ? 2 : a >= 1 ? 3 : 4;
    const s = "$" + a.toLocaleString("en-US", { minimumFractionDigits: d, maximumFractionDigits: d });
    return (signed ? sign(n) : (n < 0 ? MINUS : "")) + s;
  }
  function fmtPct(n, d = 4, signed = true) {
    if (!isNum(n)) return "–";
    const s = Math.abs(n).toFixed(d) + "%";
    return (signed ? sign(n) : (n < 0 ? MINUS : "")) + s;
  }
  function fmtNum(n, d = 2) {
    if (!isNum(n)) return "–";
    return (n < 0 ? MINUS : "") + Math.abs(n).toFixed(d);
  }
  function fmtInt(n) { return isNum(n) ? n.toLocaleString("en-US") : "–"; }
  function fmtDur(sec) {
    if (!isNum(sec)) return "–";
    sec = Math.round(sec);
    if (sec < 60) return sec + "s";
    if (sec < 3600) return Math.floor(sec / 60) + "m " + String(sec % 60).padStart(2, "0") + "s";
    return Math.floor(sec / 3600) + "h " + String(Math.floor((sec % 3600) / 60)).padStart(2, "0") + "m";
  }
  function fmtClock(iso) {
    const d = new Date(iso);
    return isNaN(d) ? "–" : d.toLocaleTimeString("en-GB", { hour12: false });
  }
  function fmtDateTime(iso) {
    const d = new Date(iso);
    return isNaN(d) ? "–" : d.toLocaleString("en-GB", { hour12: false });
  }
  const css = (name) => getComputedStyle(document.documentElement).getPropertyValue(name).trim();

  // ── state ─────────────────────────────────────────────────────────────────
  const S = {
    state: null,
    trades: [],
    lastSeq: 0,
    syncing: false,
    sort: { key: "state", dir: 1 },
    filter: "all",
    q: "",
    tq: "",
    texit: "",
    tab: "coins",
    lastMsg: 0,
    cfgKey: "",
  };

  // ── theme ─────────────────────────────────────────────────────────────────
  const themeBtn = $("#theme");
  function currentTheme() {
    const t = document.documentElement.dataset.theme;
    if (t === "dark" || t === "light") return t;
    return window.matchMedia("(prefers-color-scheme: dark)").matches ? "dark" : "light";
  }
  function applyTheme(t, persist) {
    document.documentElement.dataset.theme = t;
    themeBtn.textContent = t === "dark" ? "Light" : "Dark";
    themeBtn.setAttribute("aria-label", "Switch to " + (t === "dark" ? "light" : "dark") + " theme");
    if (persist) { try { localStorage.setItem("theme", t); } catch (_) { /* private mode */ } }
    drawChart();
  }
  (function initTheme() {
    let saved = null;
    try { saved = localStorage.getItem("theme"); } catch (_) { /* ignore */ }
    if (saved === "dark" || saved === "light") document.documentElement.dataset.theme = saved;
    themeBtn.textContent = currentTheme() === "dark" ? "Light" : "Dark";
  })();
  themeBtn.addEventListener("click", () => applyTheme(currentTheme() === "dark" ? "light" : "dark", true));
  window.matchMedia("(prefers-color-scheme: dark)").addEventListener("change", () => {
    themeBtn.textContent = currentTheme() === "dark" ? "Light" : "Dark";
    drawChart();
  });

  // ── connection (SSE) ──────────────────────────────────────────────────────
  const connEl = $("#conn"), connText = $("#conn-text");
  function setConn(state, text) {
    connEl.dataset.state = state;
    setText(connText, text);
  }
  function connect() {
    const es = new EventSource("/api/stream");
    es.addEventListener("state", (ev) => {
      let st;
      try { st = JSON.parse(ev.data); } catch (_) { return; }
      S.lastMsg = Date.now();
      setConn("live", "Live");
      onState(st);
    });
    es.onerror = () => setConn("down", "Reconnecting…");     // EventSource retries by itself
  }
  setInterval(() => {
    if (S.lastMsg && Date.now() - S.lastMsg > 6000 && connEl.dataset.state === "live") {
      setConn("down", "No data");
    }
  }, 2000);

  // ── main render ───────────────────────────────────────────────────────────
  function onState(st) {
    S.state = st;
    renderHeader(st);
    renderKpis(st);
    renderOpen(st);
    renderCoins(st);
    renderFeeds(st);
    renderConfig(st);
    if (st.trade_head !== S.lastSeq) syncTrades(st.trade_head);
  }

  function renderHeader(st) {
    const g = st.global, c = st.config;
    const parts = [
      "Uptime " + fmtDur(st.uptime_sec),
      (isNum(g.tick_rate) ? fmtInt(g.tick_rate) : "–") + " ticks/s",
      g.feeds_total ? "Feeds " + g.feeds_up + "/" + g.feeds_total : "Demo feed",
      ...(isNum(g.feed_lag_ms) ? ["Lag " + fmtInt(Math.round(g.feed_lag_ms)) + " ms"] : []),
      c.stream_type,
    ];
    const sub = $("#subline");
    sub.textContent = "";
    parts.forEach((p, i) => {
      if (i) sub.appendChild(el("span", "sep", "·"));
      sub.appendChild(el("span", "num", p));
    });

    const pause = $("#pause");
    pause.disabled = false;
    pause.dataset.paused = String(!st.entries_enabled);
    setText(pause, st.entries_enabled ? "Pause entries" : "Resume entries");

    const badge = $("#badge");
    setText(badge, st.demo ? "Demo · synthetic data" : "Paper");
    badge.title = st.demo ? "Prices are simulated — no exchange connection" : "No real orders are placed";

    // banners
    const b = [];
    if (st.demo) {
      b.push(["info", "Demo mode", "Prices and spreads are simulated. The strategy code is real; the market is not."]);
    }
    if (!st.entries_enabled) {
      b.push(["", "Entries paused", "No new positions will open. Open positions still exit normally."]);
    }
    const sc = g.state_counts || {};
    const warming = (sc.warming || 0) + (sc.connecting || 0);
    if (warming > 0) {
      const t = c.tiers.fast;
      b.push(["info", "Warming up",
        g.coins_ready + " of " + g.coins_total + " coins have a full rolling window. Each coin needs about " +
        fmtDur(t.window * t.bucket_sec) + " of live data before it can trade."]);
    }
    const down = (st.feeds || []).filter((f) => f.status !== "connected");
    if (down.length && st.uptime_sec > 15) {
      b.push(["", "Reconnecting " + down.map((f) => f.name).join(", "),
        down.reduce((n, f) => n + f.coins, 0) + " coins on " + (down.length > 1 ? "these feeds" : "this feed") +
        " are paused until it reopens (usually a few seconds). All other coins keep trading."]);
    }
    const nofeed = (sc.stale || 0) + (sc.offline || 0);
    if (nofeed > 0) {
      b.push(["", nofeed + " coin" + (nofeed > 1 ? "s" : "") + " with no fresh quote",
        (sc.offline || 0) + " offline (feed reconnecting) · " + (sc.stale || 0) +
        " stale (feed delivered no quote). Warm-up pauses and new entries are blocked until quotes resume."]);
    }
    if (sc.unlisted) {
      const names = st.coins.filter((x) => x.state === "unlisted").map((x) => x.symbol + " (no " + x.unlisted.join("/") + ")");
      b.push(["info", "Not listed on both venues", names.join(", ") + " — can never trade. Remove from COINS in engine.py."]);
    }
    if (S.flash) b.push(["error", "Error", S.flash]);
    const key = JSON.stringify(b);
    if (key !== S.bannerKey) {
      S.bannerKey = key;
      const host = $("#banners");
      host.textContent = "";
      b.forEach(([cls, head, body]) => {
        const d = el("div", "banner " + cls);
        d.appendChild(el("strong", null, head));
        d.appendChild(el("span", null, body));
        host.appendChild(d);
      });
    }
  }

  function renderKpis(st) {
    const g = st.global;
    const net = $("#k-net");
    net.textContent = "";
    if (isNum(g.net_usd) && g.net_usd !== 0) {
      net.appendChild(el("span", "glyph " + signCls(g.net_usd), g.net_usd > 0 ? "▲" : "▼"));
    }
    net.appendChild(el("span", signCls(g.net_usd), g.closed ? fmtUsd(g.net_usd) : "$0.00"));
    setText($("#k-vwap"), g.closed ? fmtPct(g.vwap_net_pct) : "–");
    setCls($("#k-vwap"), "num " + (g.closed ? signCls(g.vwap_net_pct) : ""));
    setText($("#k-unreal"), fmtUsd(g.unrealized_usd));
    setCls($("#k-unreal"), "num " + signCls(g.unrealized_usd));

    setText($("#k-trades"), fmtInt(g.closed));
    setText($("#k-wl"), g.closed
      ? g.wins + " won · " + g.losses + " lost · " + fmtNum(g.win_rate, 0) + "% win"
      : "No closed trades yet");
    setText($("#k-notional"), "$" + Math.round(g.notional || 0).toLocaleString("en-US"));
    setText($("#k-open"), fmtInt(g.open_count));
    setText($("#k-deployed"), g.open_count ? fmtUsd(g.deployed_usd, false) + " deployed" : "Flat");
    setText($("#k-ready"), g.coins_ready + "/" + g.coins_total);
    const sc = g.state_counts || {};
    setText($("#k-ready-sub"), (sc.warming || 0) + (sc.connecting || 0) + " warming · " +
      ((sc.stale || 0) + (sc.offline || 0) + (sc.unlisted || 0)) + " no feed");

    document.title = (g.closed ? fmtUsd(g.net_usd) + " · " : "") + "Spot-Perp Reversion";
  }

  // ── open positions ────────────────────────────────────────────────────────
  function renderOpen(st) {
    const host = $("#open-list");
    const open = st.coins.filter((c) => c.position).sort((a, b) => b.position.hold_sec - a.position.hold_sec);
    setText($("#open-aside"), open.length ? open.length + " open" : "");
    host.textContent = "";
    if (!open.length) {
      host.appendChild(el("p", "empty", st.entries_enabled
        ? "No open positions. Entries fire when a spread moves beyond the σ band and clears costs."
        : "No open positions. Entries are paused."));
      return;
    }
    const target = st.config.reversion_fraction * 100;
    for (const c of open) {
      const p = c.position;
      const item = el("div", "pos-item");
      const top = el("div", "pos-top");
      const left = el("div");
      left.appendChild(el("span", "pos-sym", c.symbol));
      left.appendChild(el("span", "pos-side", p.direction === 1 ? "Long spot / short perp" : "Short spot / long perp"));
      top.appendChild(left);
      top.appendChild(el("span", "pos-pnl num " + signCls(p.net_usd), fmtUsd(p.net_usd)));
      item.appendChild(top);

      const meter = el("div", "meter");
      meter.setAttribute("role", "img");
      meter.setAttribute("aria-label", "Deviation reverted " + fmtNum(p.reverted_pct, 0) + " percent, target " + target.toFixed(0));
      const fill = el("i");
      fill.style.width = Math.max(0, Math.min(100, p.reverted_pct || 0)) + "%";
      const mark = el("b");
      mark.style.left = "calc(" + Math.min(100, target) + "% - 1px)";
      meter.appendChild(fill);
      meter.appendChild(mark);
      item.appendChild(meter);

      item.appendChild(el("div", "pos-meta num",
        "Reverted " + fmtNum(p.reverted_pct, 0) + "% of " + target.toFixed(0) + "% · net " + fmtPct(p.net_pct) +
        " · held " + fmtDur(p.hold_sec) + " / " + fmtDur(p.max_hold_sec) + " · " + fmtUsd(p.notional_usd, false)));
      host.appendChild(item);
    }
  }

  // ── coins table ───────────────────────────────────────────────────────────
  const STATE_ORDER = { open: 0, flat: 1, warming: 2, connecting: 3, stale: 4, offline: 5, unlisted: 6 };
  const STATE_LABEL = { open: "Open", flat: "Flat", warming: "Warming", connecting: "Connecting", stale: "Stale",
    offline: "Offline", unlisted: "Not listed" };
  const NOFEED = new Set(["stale", "offline", "unlisted"]);
  const quoteAge = (c) => (isNum(c.spot_age) && isNum(c.perp_age) ? Math.max(c.spot_age, c.perp_age) : null);

  const COLS = [
    { key: "symbol", label: "Coin", cls: "sym", val: (c) => c.symbol, str: true },
    { key: "state", label: "State", val: (c) => STATE_ORDER[c.state] ?? 9 },
    { key: "spread", label: "Spread %", r: true, val: (c) => c.spread_pct },
    { key: "z", label: "σ from mean", r: true, val: (c) => (isNum(c.z) ? Math.abs(c.z) : null) },
    { key: "rt", label: "Friction %", r: true, val: (c) => c.rt_pct },
    { key: "signals", label: "Signals", r: true, val: (c) => c.signals },
    { key: "closed", label: "Trades", r: true, val: (c) => c.closed },
    { key: "wl", label: "W / L", r: true, val: (c) => (c.closed ? c.wins / c.closed : null) },
    { key: "net_usd", label: "Net $", r: true, val: (c) => c.net_usd },
    { key: "net_pct", label: "Net %", r: true, val: (c) => c.net_pct },
    { key: "feed", label: "Quote age", r: true, val: quoteAge },
  ];

  const rowMap = new Map();
  const head = $("#coin-head"), body = $("#coin-body");

  COLS.forEach((col) => {
    const th = el("th", (col.r ? "r " : "") + "sortable", col.label);
    th.tabIndex = 0;
    th.dataset.key = col.key;
    th.setAttribute("aria-sort", "none");
    const go = () => {
      if (S.sort.key === col.key) S.sort.dir *= -1;
      else S.sort = { key: col.key, dir: col.str || col.key === "state" ? 1 : -1 };
      if (S.state) renderCoins(S.state);
    };
    th.addEventListener("click", go);
    th.addEventListener("keydown", (e) => { if (e.key === "Enter" || e.key === " ") { e.preventDefault(); go(); } });
    head.appendChild(th);
  });

  function makeRow(sym) {
    const tr = el("tr");
    const cells = {};
    COLS.forEach((col) => {
      const td = el("td", (col.r ? "r " : "") + "num " + (col.cls || ""));
      cells[col.key] = td;
      tr.appendChild(td);
    });
    // composite cells
    const pill = el("span", "pill"); pill.appendChild(el("i")); pill.appendChild(el("span"));
    cells.state.textContent = ""; cells.state.appendChild(pill);
    const zc = el("div", "zcell");
    const zv = el("span", "zval");
    const gauge = el("div", "gauge");
    const mid = el("span", "mid"); mid.style.left = "50%";
    const tl = el("span", "th"), tr2 = el("span", "th");
    const mk = el("span", "m");
    gauge.append(tl, mid, tr2, mk);
    zc.append(zv, gauge);
    cells.z.textContent = ""; cells.z.appendChild(zc);
    const row = { tr, cells, pill, pillText: pill.lastChild, zv, gauge, tl, tr2, mk, sym };
    tr.dataset.sym = sym;
    return row;
  }

  function updateRow(row, c, sd) {
    const k = row.cells;
    setText(k.symbol, c.symbol);

    row.pill.dataset.s = c.state;
    setText(row.pillText, c.state === "warming" ? "Warming " + fmtNum(c.warm_pct, 0) + "%" : STATE_LABEL[c.state] || c.state);
    if (c.state === "open" && c.position) {
      setText(row.pillText, (c.position.direction === 1 ? "▲ " : "▼ ") + "Open");
    }
    row.tr.title = c.cooldown_sec > 0 ? "Post-reconnect cooldown: " + c.cooldown_sec + "s" : "";
    if (c.state === "stale") {
      row.tr.title = "Feed is up but no current quote (spot " + (c.spot_age == null ? "none" : c.spot_age + "s old") +
        ", perp " + (c.perp_age == null ? "none" : c.perp_age + "s old") + ").";
    } else if (c.state === "offline") {
      row.tr.title = "Reconnecting " + [c.ws_spot !== "connected" ? c.feed_spot : null, c.ws_perp !== "connected" ? c.feed_perp : null]
        .filter(Boolean).join(" and ") + " — resumes automatically.";
    } else if (c.state === "unlisted") {
      row.tr.title = "Not listed on Binance " + c.unlisted.join(" and ") + " — this coin can never trade.";
    }

    setText(k.spread, fmtPct(c.spread_pct, 4));
    setText(row.zv, isNum(c.z) ? sign(c.z) + Math.abs(c.z).toFixed(2) : "–");
    // gauge: ±3σ scale, ticks at ±sd threshold
    const pos = (v) => ((Math.max(-3, Math.min(3, v)) + 3) / 6 * 100) + "%";
    row.tl.style.left = pos(-sd); row.tr2.style.left = pos(sd);
    if (isNum(c.z)) {
      row.mk.style.display = "";
      row.mk.style.left = pos(c.z);
      setCls(row.mk, "m on" + (Math.abs(c.z) >= sd ? " hit" : ""));
    } else {
      row.mk.style.display = "none";
    }
    setText(k.rt, isNum(c.rt_pct) && c.rt_pct > 0 ? fmtPct(c.rt_pct, 4, false) : "–");
    setText(k.signals, fmtInt(c.signals));
    k.signals.title = "Detected " + c.signals + " · blocked by cost gate " + c.blocked_g2 +
      " · by slippage gate " + c.blocked_g3 + " · entered " + c.fired;
    setText(k.closed, fmtInt(c.closed));
    setText(k.wl, c.closed ? c.wins + " / " + c.losses : "–");
    setText(k.net_usd, c.closed ? fmtUsd(c.net_usd) : "–");
    setCls(k.net_usd, "r num " + (c.closed ? signCls(c.net_usd) : "dim"));
    setText(k.net_pct, c.closed ? fmtPct(c.net_pct) : "–");
    setCls(k.net_pct, "r num " + (c.closed ? signCls(c.net_pct) : "dim"));
    const age = quoteAge(c);
    setText(k.feed, age == null ? "–" : age.toFixed(1) + "s");
    const side = (leg, feed, status, a, quiet) => leg + " via " + (feed || "–") + " (" + status + "): quote " +
      (a == null ? "none" : a + "s old") + (quiet == null ? "" : ", book last changed " + quiet + "s ago");
    k.feed.title = side("spot", c.feed_spot, c.ws_spot, c.spot_age, c.spot_quiet) + "\n" +
      side("perp", c.feed_perp, c.ws_perp, c.perp_age, c.perp_quiet) +
      (c.ws_errors ? "\nfeed errors this session: " + c.ws_errors : "");
  }

  function renderChips(st) {
    const sc = st.global.state_counts || {};
    const items = [["all", "All", st.coins.length], ["open", "Open", sc.open || 0], ["flat", "Flat", sc.flat || 0],
      ["warming", "Warming", (sc.warming || 0) + (sc.connecting || 0)],
      ["nofeed", "No feed", (sc.stale || 0) + (sc.offline || 0) + (sc.unlisted || 0)]];
    const host = $("#chips");
    if (!host.children.length) {
      items.forEach(([key]) => {
        const b = el("button", "chip");
        b.type = "button"; b.dataset.key = key;
        b.addEventListener("click", () => { S.filter = key; if (S.state) renderCoins(S.state); });
        host.appendChild(b);
      });
    }
    items.forEach(([key, label, n], i) => {
      const b = host.children[i];
      setText(b, label + " " + n);
      b.setAttribute("aria-pressed", String(S.filter === key));
    });
  }

  function renderCoins(st) {
    renderChips(st);
    setText($("#n-coins"), String(st.coins.length));
    const sd = st.config.sd_threshold;

    // header sort indicators
    [...head.children].forEach((th) => th.setAttribute("aria-sort",
      th.dataset.key === S.sort.key ? (S.sort.dir > 0 ? "ascending" : "descending") : "none"));

    const col = COLS.find((c) => c.key === S.sort.key) || COLS[1];
    const q = S.q.trim().toUpperCase();
    const list = st.coins.slice().sort((a, b) => {
      const va = col.val(a), vb = col.val(b);
      const na = va == null, nb = vb == null;
      if (na !== nb) return na ? 1 : -1;                       // nulls last, either direction
      const d = na ? 0 : (col.str ? String(va).localeCompare(String(vb)) : va - vb);
      if (d !== 0) return d * S.sort.dir;
      return a.symbol.localeCompare(b.symbol);                 // stable tiebreak, always A→Z
    });

    let shown = 0;
    list.forEach((c, i) => {
      let row = rowMap.get(c.symbol);
      if (!row) { row = makeRow(c.symbol); rowMap.set(c.symbol, row); }
      updateRow(row, c, sd);
      const stateMatch = S.filter === "all" || c.state === S.filter ||
        (S.filter === "warming" && c.state === "connecting") ||
        (S.filter === "nofeed" && NOFEED.has(c.state));
      const visible = stateMatch && (!q || c.symbol.includes(q));
      row.tr.hidden = !visible;
      if (visible) shown++;
      if (body.children[i] !== row.tr) body.insertBefore(row.tr, body.children[i] || null);
    });
    setText($("#coin-count"), "Showing " + shown + " of " + st.coins.length);
  }

  // ── feeds ─────────────────────────────────────────────────────────────────
  const FEED_STATUS = { connected: "Live", retrying: "Reconnecting", error: "Error", init: "Connecting" };
  function renderFeeds(st) {
    const feeds = st.feeds || [];
    const up = feeds.filter((f) => f.status === "connected").length;
    setText($("#n-feeds"), feeds.length ? up + "/" + feeds.length : "");
    $("#feed-empty").hidden = feeds.length > 0;
    const tb = $("#feed-body");
    const frag = document.createDocumentFragment();
    for (const f of feeds) {
      const tr = el("tr");
      const cell = (txt, cls) => { const td = el("td", cls || "", txt); tr.appendChild(td); return td; };
      cell(f.name, "sym");
      const st2 = cell("");
      const pill = el("span", "pill"); pill.dataset.s = f.status;
      pill.appendChild(el("i")); pill.appendChild(el("span", null, FEED_STATUS[f.status] || f.status));
      st2.appendChild(pill);
      cell(fmtInt(f.coins), "r num");
      cell(fmtInt(f.msg_rate), "r num");
      const lag = cell(f.leg === "perp" ? (isNum(f.lag_ms) ? fmtInt(f.lag_ms) + " ms" : "–") : "n/a", "r num");
      if (isNum(f.lag_max_ms)) lag.title = "worst in the last second: " + fmtInt(f.lag_max_ms) + " ms";
      cell(isNum(f.up_sec) ? fmtDur(f.up_sec) : "–", "r num");
      cell(fmtInt(f.reconnects), "r num");
      cell(f.last_error ? f.last_error + (isNum(f.last_error_ago) ? " (" + fmtDur(f.last_error_ago) + " ago)" : "") : "–", "err");
      frag.appendChild(tr);
    }
    tb.textContent = "";
    tb.appendChild(frag);
  }

  // ── config ────────────────────────────────────────────────────────────────
  function renderConfig(st) {
    const c = st.config;
    const key = JSON.stringify(c);
    if (key === S.cfgKey) return;
    S.cfgKey = key;
    const t = c.tiers;
    const rows = [
      ["Stream", c.stream_type === "bookTicker" ? "bookTicker (top of book, per tick)" : c.stream_type],
      ["Entry gate", c.sd_threshold + "σ from rolling mean, then cost + slippage gates"],
      ["Exchange fee (round trip)", c.exchange_fee_pct + "%"],
      ["Exit", fmtNum(c.reversion_fraction * 100, 0) + "% deviation reverted (net ≥ 0), else ≥ " + c.min_net_pct + "% net"],
      ["Max hold", c.max_hold_sec + "s"],
      ["Reconnect cooldown", c.cooldown_sec + "s"],
      ["Min notional", "$" + c.min_notional_usd + " · " + c.notional_steps + " sizing steps · no max (liquidity-capped)"],
      ["Simulated latency", c.entry_delay_sec === 0 && c.exit_delay_sec === 0 ? "None — fills at the triggering tick"
        : "entry " + c.entry_delay_sec * 1000 + "ms · exit " + c.exit_delay_sec * 1000 + "ms"],
      ["Rolling window", "fast " + t.fast.window + "×" + t.fast.bucket_sec + "s · medium " + t.medium.window + "×" + t.medium.bucket_sec +
        "s · slow " + t.slow.window + "×" + t.slow.bucket_sec + "s"],
      ["Coins per WebSocket", String(c.coins_per_ws)],
      ["Trade log", c.master_csv],
    ];
    const dl = $("#cfg");
    dl.textContent = "";
    rows.forEach(([k, v]) => {
      const d = el("div");
      d.appendChild(el("dt", null, k));
      d.appendChild(el("dd", null, v));
      dl.appendChild(d);
    });
  }

  // ── trades ────────────────────────────────────────────────────────────────
  async function syncTrades(head) {
    if (S.syncing) return;
    if (head < S.lastSeq) { S.trades = []; S.lastSeq = 0; }   // engine restarted
    if (head === S.lastSeq) return;
    S.syncing = true;
    try {
      const r = await fetch("/api/trades?since=" + S.lastSeq + "&limit=5000");
      if (!r.ok) throw new Error("HTTP " + r.status);
      const j = await r.json();
      if (j.trades.length) {
        S.trades.push(...j.trades);
        if (S.trades.length > 5000) S.trades = S.trades.slice(-5000);
        S.lastSeq = j.trades[j.trades.length - 1].seq;
      } else {
        S.lastSeq = j.head;
      }
      renderTrades();
      drawChart();
    } catch (e) {
      console.warn("trade sync failed:", e);
    } finally {
      S.syncing = false;
    }
  }

  const SIDE = { 1: "Spot long", "-1": "Spot short" };
  function renderTrades() {
    setText($("#n-trades"), S.trades.length ? String(S.trades.length) : "");
    const q = S.tq.trim().toUpperCase();
    const rows = [];
    for (let i = S.trades.length - 1; i >= 0 && rows.length < 300; i--) {
      const t = S.trades[i];
      if (q && !t.symbol.includes(q)) continue;
      if (S.texit && t.exit_type !== S.texit) continue;
      rows.push(t);
    }
    const tb = $("#trade-body");
    const frag = document.createDocumentFragment();
    for (const t of rows) {
      const tr = el("tr");
      const cell = (txt, cls) => { const td = el("td", cls || "", txt); tr.appendChild(td); return td; };
      cell(fmtClock(t.exit_dt), "num").title = fmtDateTime(t.exit_dt);
      cell(t.symbol, "sym");
      cell(SIDE[t.direction] || "–").title = t.action;
      cell(fmtUsd(t.notional_usd, false), "r num");
      cell(fmtDur(t.hold_sec), "r num");
      cell(fmtPct(t.gross_pnl_pct), "r num " + signCls(t.gross_pnl_pct));
      cell(fmtPct(t.net_pnl_pct), "r num " + signCls(t.net_pnl_pct));
      cell(fmtUsd(t.net_pnl_usd), "r num " + signCls(t.net_pnl_usd));
      cell(t.exit_type).title = t.exit_reason;
      cell(fmtPct(t.book_walk_slip_pct, 4, false), "r num");
      frag.appendChild(tr);
    }
    tb.textContent = "";
    tb.appendChild(frag);
    $("#trade-empty").hidden = rows.length > 0;
    $("#trade-empty").textContent = S.trades.length ? "No trades match the filter." : "No closed trades yet.";
    const total = S.trades.length;
    setText($("#trade-count"), rows.length < total ? "Showing " + rows.length + " of " + total : total + " trades");
  }

  // ── equity chart ──────────────────────────────────────────────────────────
  const SVGNS = "http://www.w3.org/2000/svg";
  const chartEl = $("#chart");
  let chartState = null;   // { pts, x(i), y(v), idx }

  function svg(tag, attrs, parent) {
    const n = document.createElementNS(SVGNS, tag);
    for (const k in attrs) n.setAttribute(k, attrs[k]);
    if (parent) parent.appendChild(n);
    return n;
  }
  function niceTicks(lo, hi, target = 4) {
    if (lo === hi) { lo -= 1; hi += 1; }
    const raw = (hi - lo) / target;
    const mag = Math.pow(10, Math.floor(Math.log10(raw)));
    const n = raw / mag;
    const step = (n < 1.5 ? 1 : n < 3.5 ? 2 : n < 7.5 ? 5 : 10) * mag;
    const out = [];
    for (let v = Math.ceil(lo / step) * step; v <= hi + step * 1e-9; v += step) out.push(Math.abs(v) < step * 1e-9 ? 0 : v);
    return { ticks: out, step };
  }
  function axisUsd(v, step) {
    const d = step >= 1 ? 0 : step >= 0.1 ? 1 : step >= 0.01 ? 2 : step >= 0.001 ? 3 : 4;
    return (v < 0 ? MINUS : "") + "$" + Math.abs(v).toLocaleString("en-US", { minimumFractionDigits: d, maximumFractionDigits: d });
  }

  function drawChart() {
    const W = Math.max(280, Math.floor(chartEl.clientWidth));
    const H = Math.max(200, Math.floor(chartEl.clientHeight));
    chartEl.textContent = "";
    chartState = null;

    const cSeries = css("--series"), cSurface = css("--surface"), cGrid = css("--grid"),
      cBase = css("--baseline"), cMuted = css("--muted"), cInk2 = css("--ink-2");

    // cumulative series: point i = state after trade i (i=0 → start at 0)
    const cum = [0];
    for (const t of S.trades) cum.push(cum[cum.length - 1] + (t.net_pnl_usd || 0));
    const n = S.trades.length;

    const m = { l: 62, r: 76, t: 12, b: 28 };
    const iw = W - m.l - m.r, ih = H - m.t - m.b;
    let lo = Math.min(0, ...cum), hi = Math.max(0, ...cum);
    if (lo === hi) { lo = -0.5; hi = 0.5; }
    const pad = (hi - lo) * 0.08;
    lo -= pad; hi += pad;
    const { ticks, step } = niceTicks(lo, hi, 4);
    const x = (i) => m.l + (n === 0 ? 0 : (i / Math.max(1, n)) * iw);
    const y = (v) => m.t + (1 - (v - lo) / (hi - lo)) * ih;

    const root = svg("svg", { viewBox: `0 0 ${W} ${H}`, width: W, height: H, role: "img", tabindex: 0,
      "aria-label": n ? `Cumulative net PnL after ${n} trades, currently ${fmtUsd(cum[n])}. Use left and right arrow keys to inspect trades.`
        : "Cumulative net PnL chart. No closed trades yet." }, chartEl);

    // gridlines + y ticks (hairline, solid, recessive)
    ticks.forEach((v) => {
      svg("line", { x1: m.l, x2: W - m.r, y1: y(v), y2: y(v), stroke: v === 0 ? cBase : cGrid, "stroke-width": 1 }, root);
      const tx = svg("text", { x: m.l - 8, y: y(v) + 4, "text-anchor": "end", fill: cMuted, "font-size": 11 }, root);
      tx.textContent = axisUsd(v, step);
    });
    // x ticks: trade numbers
    const xt = niceTicks(0, Math.max(1, n), 5);
    xt.ticks.filter((v) => Number.isInteger(v) && v >= 0 && v <= n).forEach((v) => {
      const tx = svg("text", { x: x(v), y: H - 8, "text-anchor": v === 0 ? "start" : "middle", fill: cMuted, "font-size": 11 }, root);
      tx.textContent = v === 0 ? "Start" : "#" + v;
    });

    if (n === 0) {
      const d = el("div", "chart-empty", "No closed trades yet. The curve builds as positions exit.");
      chartEl.appendChild(d);
      return;
    }

    // area wash (10%) between the curve and the zero line, then the 2px line
    const pts = cum.map((v, i) => [x(i), y(v)]);
    const line = pts.map((p, i) => (i ? "L" : "M") + p[0].toFixed(1) + " " + p[1].toFixed(1)).join(" ");
    const y0 = y(0);
    svg("path", { d: `${line} L${pts[n][0].toFixed(1)} ${y0.toFixed(1)} L${pts[0][0].toFixed(1)} ${y0.toFixed(1)} Z`,
      fill: cSeries, "fill-opacity": 0.10, stroke: "none" }, root);
    svg("path", { d: line, fill: "none", stroke: cSeries, "stroke-width": 2, "stroke-linejoin": "round", "stroke-linecap": "round" }, root);

    // end dot with surface ring + direct end label (value in text ink, never the series colour)
    const last = pts[n];
    svg("circle", { cx: last[0], cy: last[1], r: 5, fill: cSeries, stroke: cSurface, "stroke-width": 2 }, root);
    const lab = svg("text", { x: last[0] + 10, y: last[1] + 4, fill: css("--ink"), "font-size": 12, "font-weight": 650 }, root);
    lab.textContent = fmtUsd(cum[n]);

    // hover layer
    const cross = svg("line", { y1: m.t, y2: m.t + ih, stroke: cInk2, "stroke-width": 1, visibility: "hidden" }, root);
    const hdot = svg("circle", { r: 5, fill: cSeries, stroke: cSurface, "stroke-width": 2, visibility: "hidden" }, root);
    const tip = el("div", "tip"); tip.hidden = true;
    chartEl.appendChild(tip);
    const hit = svg("rect", { x: m.l, y: m.t, width: iw, height: ih, fill: "transparent" }, root);

    chartState = { n, cum, x, y, tip, cross, hdot, m, W, H, iw };
    const show = (i) => {
      i = Math.max(0, Math.min(n, i));
      chartState.idx = i;
      const px = x(i), py = y(cum[i]);
      cross.setAttribute("x1", px); cross.setAttribute("x2", px); cross.setAttribute("visibility", "visible");
      hdot.setAttribute("cx", px); hdot.setAttribute("cy", py); hdot.setAttribute("visibility", "visible");
      tip.textContent = "";
      const t = i > 0 ? S.trades[i - 1] : null;
      tip.appendChild(el("div", "t-h", i === 0 ? "Start" : "Trade #" + i + " · " + t.symbol + " · " + fmtClock(t.exit_dt)));
      if (t) {
        const r1 = el("div", "t-r");
        r1.appendChild(el("span", "k", "This trade"));
        r1.appendChild(el("span", "v num " + signCls(t.net_pnl_usd), fmtUsd(t.net_pnl_usd)));
        tip.appendChild(r1);
      }
      const r2 = el("div", "t-r");
      const k2 = el("span", "k"); k2.appendChild(el("span", "key")); k2.appendChild(document.createTextNode("Cumulative"));
      r2.appendChild(k2);
      r2.appendChild(el("span", "v num " + signCls(cum[i]), fmtUsd(cum[i])));
      tip.appendChild(r2);
      tip.hidden = false;
      const tw = tip.offsetWidth, th = tip.offsetHeight;
      let left = px + 14; if (left + tw > W) left = px - tw - 14;
      let top = py - th - 10; if (top < 0) top = py + 12;
      tip.style.left = Math.max(0, left) + "px"; tip.style.top = Math.max(0, top) + "px";
    };
    const hide = () => { cross.setAttribute("visibility", "hidden"); hdot.setAttribute("visibility", "hidden"); tip.hidden = true; };
    const idxAt = (ev) => {
      const r = hit.getBoundingClientRect();
      const f = (ev.clientX - r.left) / r.width;
      return Math.round(f * n);
    };
    hit.addEventListener("pointermove", (ev) => show(idxAt(ev)));
    hit.addEventListener("pointerleave", hide);
    root.addEventListener("keydown", (ev) => {
      if (ev.key === "ArrowLeft" || ev.key === "ArrowRight") {
        ev.preventDefault();
        show((chartState.idx == null ? n : chartState.idx) + (ev.key === "ArrowRight" ? 1 : -1));
      } else if (ev.key === "Escape") hide();
    });
    root.addEventListener("blur", hide);
  }
  new ResizeObserver(() => drawChart()).observe(chartEl);

  // ── tabs & filters ────────────────────────────────────────────────────────
  function selectTab(name, focus) {
    S.tab = name;
    document.querySelectorAll(".tab").forEach((t) => {
      const on = t.dataset.tab === name;
      t.setAttribute("aria-selected", String(on));
      t.tabIndex = on ? 0 : -1;
      if (on && focus) t.focus();
    });
    document.querySelectorAll(".panel").forEach((p) => { p.hidden = p.id !== "panel-" + name; });
  }
  document.querySelectorAll(".tab").forEach((t) => {
    t.addEventListener("click", () => selectTab(t.dataset.tab));
    t.addEventListener("keydown", (e) => {
      const tabs = [...document.querySelectorAll(".tab")];
      const i = tabs.indexOf(t);
      if (e.key === "ArrowRight") { e.preventDefault(); selectTab(tabs[(i + 1) % tabs.length].dataset.tab, true); }
      if (e.key === "ArrowLeft")  { e.preventDefault(); selectTab(tabs[(i + tabs.length - 1) % tabs.length].dataset.tab, true); }
    });
  });
  $("#to-table").addEventListener("click", () => { selectTab("trades", true); });
  $("#q").addEventListener("input", (e) => { S.q = e.target.value; if (S.state) renderCoins(S.state); });
  $("#tq").addEventListener("input", (e) => { S.tq = e.target.value; renderTrades(); });
  $("#texit").addEventListener("change", (e) => { S.texit = e.target.value; renderTrades(); });

  // ── controls ──────────────────────────────────────────────────────────────
  $("#pause").addEventListener("click", async () => {
    if (!S.state) return;
    const btn = $("#pause");
    const want = !S.state.entries_enabled;
    btn.disabled = true;
    try {
      const r = await fetch("/api/control/entries", {
        method: "POST", headers: { "Content-Type": "application/json" }, body: JSON.stringify({ enabled: want }),
      });
      if (!r.ok) throw new Error("HTTP " + r.status);
      const j = await r.json();
      S.state.entries_enabled = j.entries_enabled;
      S.flash = null;
      renderHeader(S.state);
    } catch (e) {
      S.flash = "Could not change entry state (" + e.message + ").";
      renderHeader(S.state);
      setTimeout(() => { S.flash = null; if (S.state) renderHeader(S.state); }, 6000);
    } finally {
      btn.disabled = false;
    }
  });

  // ── boot ──────────────────────────────────────────────────────────────────
  selectTab("coins");
  drawChart();
  renderTrades();
  connect();
})();
