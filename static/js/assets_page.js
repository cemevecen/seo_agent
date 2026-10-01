/**
 * /assets — doviz.com piyasa serileri (Android chart/table/KPI UX).
 */
(function () {
  "use strict";

  var cfg = window.SEO_ASSETS_PAGE || {};
  var ALL_SERIES = Array.isArray(cfg.series) ? cfg.series : [];
  var DEFAULTS = Array.isArray(cfg.defaults) ? cfg.defaults.slice() : [];
  var dataMin = String(cfg.dataMin || "").slice(0, 10);
  var dataMax = String(cfg.dataMax || "").slice(0, 10);
  var CATEGORY_LABELS = cfg.categories || {
    gold: "Altın",
    silver: "Gümüş",
    commodity: "Emtia",
    fx: "Döviz",
    index: "Endeks",
    crypto: "Kripto",
    equity: "Hisse",
    other: "Diğer",
  };
  var CATEGORY_ORDER = ["gold", "silver", "commodity", "fx", "index", "crypto", "equity", "other"];

  var PALETTE = [
    "#1d4ed8", "#059669", "#dc2626", "#7c3aed", "#ea580c",
    "#0891b2", "#db2777", "#65a30d", "#4f46e5", "#0f766e",
    "#b45309", "#0369a1", "#be123c", "#15803d", "#6d28d9",
  ];

  var el = {
    start: document.getElementById("as-start"),
    end: document.getElementById("as-end"),
    preset: document.getElementById("as-preset"),
    breakdown: document.getElementById("as-breakdown"),
    compare: document.getElementById("as-compare-mode"),
    run: document.getElementById("as-run"),
    runLabel: document.getElementById("as-run-label"),
    loading: document.getElementById("as-loading"),
    kpiGrid: document.getElementById("as-kpi-grid"),
    chart: document.getElementById("as-chart"),
    chartWrap: document.getElementById("as-chart-wrap"),
    tableShell: document.getElementById("as-table-shell"),
    table: document.getElementById("as-table"),
    thead: document.getElementById("as-thead-row"),
    tbody: document.getElementById("as-tbody"),
    legend: document.getElementById("as-legend"),
    compareRange: document.getElementById("as-compare-range"),
    sync: document.getElementById("as-sync"),
    metricTrigger: document.getElementById("as-metric-trigger"),
    metricLabel: document.getElementById("as-metric-label"),
    metricList: document.getElementById("as-metric-list"),
    metricScroll: document.getElementById("as-metric-list-scroll"),
    tip: document.getElementById("as-tooltip"),
    tipTitle: document.getElementById("as-tip-title"),
    tipBody: document.getElementById("as-tip-body"),
    chartStyleRoot: document.getElementById("as-chart-style"),
  };

  var selected = DEFAULTS.length
    ? DEFAULTS.slice()
    : ALL_SERIES.slice(0, 10).map(function (s) { return s.key; });
  var chartStyle = "area";
  try {
    var storedStyle = localStorage.getItem("asChartStyle");
    if (storedStyle === "line" || storedStyle === "area" || storedStyle === "bar") chartStyle = storedStyle;
  } catch (_) {}
  var lastPayload = null;
  var lastComparePayload = null;
  var lastCompareWin = null;
  var focusSeriesKey = null;
  var seriesByKey = {};
  var _kpiSparkGradSeq = 0;
  ALL_SERIES.forEach(function (s) { seriesByKey[s.key] = s; });

  function pad(n) { return n < 10 ? "0" + n : String(n); }
  function iso(d) {
    return d.getFullYear() + "-" + pad(d.getMonth() + 1) + "-" + pad(d.getDate());
  }
  function parseIso(s) {
    var p = String(s || "").slice(0, 10).split("-");
    if (p.length !== 3) return null;
    return new Date(+p[0], +p[1] - 1, +p[2]);
  }
  function addDays(d, n) {
    var x = new Date(d.getTime());
    x.setDate(x.getDate() + n);
    return x;
  }
  function esc(s) {
    return String(s == null ? "" : s)
      .replace(/&/g, "&amp;")
      .replace(/</g, "&lt;")
      .replace(/>/g, "&gt;")
      .replace(/"/g, "&quot;");
  }
  function fmtNum(v) {
    if (v == null || v === "") return "—";
    var n = typeof v === "number" ? v : Number(v);
    if (!Number.isFinite(n)) return "—";
    if (n === 0) return "0";
    var abs = Math.abs(n);
    var sign = n < 0 ? "-" : "";
    // PEPE / SHIB gibi mikro fiyatlar: 0,0000… kesilmesin → 3,42e-6
    if (abs < 1e-4) {
      var exp = Math.floor(Math.log10(abs));
      var mant = abs / Math.pow(10, exp);
      var mantR = Math.round(mant * 1000) / 1000;
      if (mantR >= 10) {
        mantR /= 10;
        exp += 1;
      }
      var mantStr = mantR.toLocaleString("tr-TR", {
        maximumFractionDigits: 3,
        minimumFractionDigits: 0,
      });
      return sign + mantStr + "e" + exp;
    }
    if (abs < 0.01) {
      return n.toLocaleString("tr-TR", { maximumFractionDigits: 6 });
    }
    if (abs < 1) {
      return n.toLocaleString("tr-TR", { maximumFractionDigits: 4 });
    }
    if (abs < 1000) {
      return n.toLocaleString("tr-TR", { maximumFractionDigits: 4 });
    }
    return n.toLocaleString("tr-TR", { maximumFractionDigits: 2 });
  }
  /** Hücre title / tooltip — tam hassasiyet */
  function fmtNumFull(v) {
    if (v == null || v === "") return "—";
    var n = typeof v === "number" ? v : Number(v);
    if (!Number.isFinite(n)) return "—";
    if (n === 0) return "0";
    var abs = Math.abs(n);
    var digits = abs >= 1 ? 6 : abs >= 1e-4 ? 8 : 12;
    return n.toLocaleString("tr-TR", {
      maximumFractionDigits: digits,
      maximumSignificantDigits: 10,
    });
  }
  function colorFor(key, idx) {
    var i = typeof idx === "number" ? idx : selected.indexOf(key);
    return PALETTE[(i >= 0 ? i : 0) % PALETTE.length];
  }
  function shortLabel(label, key) {
    var s = String(label || key || "");
    if (s.length <= 14) return s;
    return s.slice(0, 12) + "…";
  }

  function syncDateBoundsAttrs() {
    if (el.start) {
      if (dataMin) el.start.setAttribute("data-min", dataMin);
      if (dataMax) el.start.setAttribute("data-max", dataMax);
      if (dataMin) el.start.min = dataMin;
      if (dataMax) el.start.max = dataMax;
    }
    if (el.end) {
      if (dataMin) el.end.setAttribute("data-min", dataMin);
      if (dataMax) el.end.setAttribute("data-max", dataMax);
      if (dataMin) el.end.min = dataMin;
      if (dataMax) el.end.max = dataMax;
    }
  }

  function rememberDataRange(payload) {
    var dr = payload && payload.data_range;
    if (!dr) return;
    if (dr.min) dataMin = String(dr.min).slice(0, 10);
    if (dr.max) dataMax = String(dr.max).slice(0, 10);
    syncDateBoundsAttrs();
  }

  function applyPreset(opts) {
    opts = opts || {};
    var p = el.preset ? el.preset.value : "30d";
    if (p === "custom") return;
    var today = new Date();
    today.setHours(12, 0, 0, 0);
    var anchor = dataMax ? (parseIso(dataMax) || today) : today;
    if (anchor > today) anchor = today;
    var startD;
    var endD = new Date(anchor.getTime());

    if (p === "all") {
      if (el.start) el.start.value = dataMin || "";
      if (el.end) el.end.value = dataMax || iso(today);
      if (!opts.skipRun) run();
      return;
    }
    if (p === "today") {
      var day = iso(today);
      if (el.start) el.start.value = day;
      if (el.end) el.end.value = day;
      if (!opts.skipRun) run();
      return;
    }
    if (p === "yesterday") {
      var y = addDays(today, -1);
      var yIso = iso(y);
      if (el.start) el.start.value = yIso;
      if (el.end) el.end.value = yIso;
      if (!opts.skipRun) run();
      return;
    }
    if (p === "this_month") {
      startD = new Date(anchor.getFullYear(), anchor.getMonth(), 1);
      endD = new Date(anchor.getTime());
    } else if (p === "last_month") {
      startD = new Date(anchor.getFullYear(), anchor.getMonth() - 1, 1);
      endD = new Date(anchor.getFullYear(), anchor.getMonth(), 0);
    } else if (p === "ytd") {
      startD = new Date(anchor.getFullYear(), 0, 1);
      endD = new Date(anchor.getTime());
    } else if (p === "last_year") {
      var ly = anchor.getFullYear() - 1;
      startD = new Date(ly, 0, 1);
      endD = new Date(ly, 11, 31);
    } else if (p === "last_year_h1") {
      var ly1 = anchor.getFullYear() - 1;
      startD = new Date(ly1, 0, 1);
      endD = new Date(ly1, 5, 30);
    } else if (p === "last_year_h2") {
      var ly2 = anchor.getFullYear() - 1;
      startD = new Date(ly2, 6, 1);
      endD = new Date(ly2, 11, 31);
    } else if (/^\d+y$/.test(p)) {
      // Last N years (inclusive calendar-day window; 1y ≈ 365 gün)
      var nYears = parseInt(p, 10) || 1;
      endD = new Date(anchor.getTime());
      startD = addDays(endD, -(365 * nYears - 1));
    } else {
      var daysMap = { "7d": 7, "14d": 14, "30d": 30, "60d": 60, "90d": 90, "180d": 180, "6m": 180 };
      var days = daysMap[p] || parseInt(p, 10) || 30;
      endD = new Date(anchor.getTime());
      startD = addDays(endD, -(days - 1));
    }

    // Depo sınırları içinde tut
    if (dataMin) {
      var lo = parseIso(dataMin);
      if (lo && startD < lo) startD = lo;
    }
    if (dataMax) {
      var hi = parseIso(dataMax);
      if (hi && endD > hi) endD = hi;
    }
    if (startD > endD) startD = new Date(endD.getTime());

    if (el.start) el.start.value = iso(startD);
    if (el.end) el.end.value = iso(endD);
    if (!opts.skipRun) run();
  }

  function setLoading(on) {
    if (!el.loading) return;
    el.loading.classList.toggle("hidden", !on);
    el.loading.classList.toggle("flex", !!on);
    el.loading.setAttribute("aria-busy", on ? "true" : "false");
    if (el.run) el.run.disabled = !!on;
    if (el.runLabel) el.runLabel.textContent = on ? "Loading…" : "Apply";
  }

  function updateMetricTrigger() {
    if (!el.metricLabel) return;
    var n = selected.length;
    if (!n) el.metricLabel.textContent = "Select";
    else if (n === 1) {
      var s = seriesByKey[selected[0]];
      el.metricLabel.textContent = (s && s.label) || selected[0];
    } else el.metricLabel.textContent = n + " assets selected";
  }

  function iconHtml(spec, cls) {
    var url = (spec && spec.icon_url) || "";
    if (!url) return "";
    return '<img class="' + (cls || "as-icon") + '" src="' + esc(url) + '" alt="" width="20" height="20" loading="lazy" decoding="async" referrerpolicy="no-referrer" onerror="this.classList.add(\'is-missing\')" />';
  }

  function buildMetricList() {
    if (!el.metricScroll) return;
    var byCat = {};
    ALL_SERIES.forEach(function (s) {
      var c = s.category || "other";
      if (!byCat[c]) byCat[c] = [];
      byCat[c].push(s);
    });
    var html = "";
    html += '<button type="button" class="as-metric-opt" data-as-clear="1">' +
      '<span class="w-4 text-center">' + (!selected.length ? "✓" : "") + "</span>Clear selection</button>";
    CATEGORY_ORDER.forEach(function (cat) {
      var items = byCat[cat];
      if (!items || !items.length) return;
      html += '<div class="as-metric-group">' + esc(CATEGORY_LABELS[cat] || cat) + "</div>";
      items.forEach(function (s) {
        var on = selected.indexOf(s.key) >= 0;
        html += '<button type="button" role="option" aria-selected="' + on + '" data-as-key="' + esc(s.key) + '" class="as-metric-opt' + (on ? " is-on" : "") + '">' +
          '<span class="w-4 text-center">' + (on ? "✓" : "") + "</span>" +
          iconHtml(s, "as-icon as-icon--sm") +
          '<span class="min-w-0 flex-1 truncate">' + esc(s.label || s.key) + "</span>" +
          '<span class="text-[10px] font-medium text-slate-400">' + esc(s.unit || "") + "</span></button>";
      });
    });
    el.metricScroll.innerHTML = html;
  }

  function positionMetricDropdown() {
    if (!el.metricList || !el.metricTrigger) return;
    var r = el.metricTrigger.getBoundingClientRect();
    var maxW = Math.min(420, window.innerWidth - 16);
    var w = Math.max(r.width, Math.min(maxW, 320));
    var left = Math.min(Math.max(8, r.left), window.innerWidth - w - 8);
    var spaceBelow = window.innerHeight - r.bottom - 12;
    var spaceAbove = r.top - 12;
    var maxH = Math.min(360, Math.max(spaceBelow, spaceAbove, 140));
    var top = r.bottom + 4;
    if (spaceBelow < 160 && spaceAbove > spaceBelow) top = Math.max(8, r.top - maxH - 4);
    el.metricList.style.left = left + "px";
    el.metricList.style.top = top + "px";
    el.metricList.style.width = w + "px";
    el.metricList.style.maxHeight = maxH + "px";
  }

  function openMetricList(on) {
    if (!el.metricList || !el.metricTrigger) return;
    if (on) {
      if (el.metricList.parentElement !== document.body) {
        document.body.appendChild(el.metricList);
      }
      buildMetricList();
      positionMetricDropdown();
      el.metricList.classList.remove("hidden");
      el.metricTrigger.setAttribute("aria-expanded", "true");
    } else {
      el.metricList.classList.add("hidden");
      el.metricTrigger.setAttribute("aria-expanded", "false");
    }
  }

  function compareMode() {
    return (el.compare && el.compare.value) || "";
  }

  function compareBounds(start, end, mode) {
    if (!mode) return null;
    if (window.SeoPeriodCompare && SeoPeriodCompare.bounds) {
      return SeoPeriodCompare.bounds(start, end, mode);
    }
    var a = parseIso(start);
    var b = parseIso(end);
    if (!a || !b) return null;
    if (mode === "previous_year") {
      a.setFullYear(a.getFullYear() - 1);
      b.setFullYear(b.getFullYear() - 1);
      return { start: iso(a), end: iso(b), mode: mode };
    }
    if (mode === "previous_period") {
      var days = Math.round((b - a) / 86400000) + 1;
      var end2 = addDays(a, -1);
      var start2 = addDays(end2, -(days - 1));
      return { start: iso(start2), end: iso(end2), mode: mode };
    }
    return null;
  }

  function fetchOverlay(start, end, keys) {
    var p = new URLSearchParams();
    p.set("start", start);
    p.set("end", end);
    if (keys && keys.length) p.set("series", keys.join(","));
    return fetch("/api/market-quotes/overlay?" + p.toString(), {
      credentials: "same-origin",
      cache: "no-store",
    }).then(function (r) {
      if (!r.ok) throw new Error("HTTP " + r.status);
      return r.json();
    });
  }

  /** Points as {key, value} for SeoPeriodCompare / heat grid. */
  function pointsOf(payload, key) {
    var block = payload && payload.series && payload.series[key];
    var out = [];
    ((block && block.by_date) || []).forEach(function (pt) {
      if (!pt || pt.close == null || pt.close === "") return;
      var v = Number(pt.close);
      if (!Number.isFinite(v)) return;
      out.push({ key: String(pt.date).slice(0, 10), value: v });
    });
    return out;
  }

  function aggregate(points, breakdown) {
    if (breakdown === "day" || !breakdown) return points.slice();
    var buckets = {};
    var order = [];
    points.forEach(function (pt) {
      var d = parseIso(pt.key);
      if (!d) return;
      var key;
      if (breakdown === "week") {
        var day = (d.getDay() + 6) % 7;
        var mon = addDays(d, -day);
        key = iso(mon);
      } else {
        key = d.getFullYear() + "-" + pad(d.getMonth() + 1) + "-01";
      }
      if (!buckets[key]) {
        buckets[key] = { key: key, sum: 0, n: 0 };
        order.push(key);
      }
      buckets[key].sum += pt.value;
      buckets[key].n += 1;
    });
    return order.map(function (k) {
      var b = buckets[k];
      return { key: b.key, value: b.sum / b.n };
    });
  }

  function lastClose(points) {
    if (!points || !points.length) return null;
    return points[points.length - 1].value;
  }

  function normalizeSeries(points) {
    if (!points.length) return [];
    var vals = points.map(function (p) { return p.value; });
    var lo = Math.min.apply(null, vals);
    var hi = Math.max.apply(null, vals);
    var span = hi - lo;
    if (span <= 0) {
      return points.map(function (p) { return { key: p.key, value: p.value, norm: 50 }; });
    }
    return points.map(function (p) {
      return { key: p.key, value: p.value, norm: ((p.value - lo) / span) * 100 };
    });
  }

  function seriesSparkSvg(series, color, w, h) {
    var ptsMeta = [];
    (series || []).forEach(function (r) {
      var n = Number(r && r.value);
      if (!Number.isFinite(n)) return;
      ptsMeta.push({ key: r.key, value: n });
    });
    if (ptsMeta.length < 2) {
      return '<div class="metric-kpi-spark" style="opacity:.35" aria-hidden="true"></div>';
    }
    var vals = ptsMeta.map(function (p) { return p.value; });
    var min = Math.min.apply(null, vals);
    var max = Math.max.apply(null, vals);
    var span = (max - min) || 1;
    var coords = vals.map(function (v, i) {
      var x = (i / (vals.length - 1)) * w;
      var y = h - ((v - min) / span) * (h - 4) - 2;
      return [x, y];
    });
    var line = coords.map(function (p) { return p[0].toFixed(1) + "," + p[1].toFixed(1); }).join(" ");
    var area =
      "M" + coords[0][0].toFixed(1) + " " + h.toFixed(1) +
      " L" + coords.map(function (p) { return p[0].toFixed(1) + " " + p[1].toFixed(1); }).join(" L") +
      " L" + coords[coords.length - 1][0].toFixed(1) + " " + h.toFixed(1) + " Z";
    _kpiSparkGradSeq += 1;
    var gid = "as-kpi-spark-grad-" + _kpiSparkGradSeq;
    var metaJson = esc(JSON.stringify(ptsMeta.map(function (p) {
      return { k: p.key, v: p.value };
    })));
    return (
      '<svg class="metric-kpi-spark" viewBox="0 0 ' + w + " " + h + '" preserveAspectRatio="none" ' +
        'data-as-spark="1" data-as-spark-pts="' + metaJson + '" data-as-spark-w="' + w + '" role="img" aria-label="Sparkline">' +
        "<defs>" +
          '<linearGradient id="' + gid + '" x1="0" y1="0" x2="0" y2="1">' +
            '<stop offset="0%" stop-color="' + color + '" stop-opacity="0.38"></stop>' +
            '<stop offset="55%" stop-color="' + color + '" stop-opacity="0.10"></stop>' +
            '<stop offset="100%" stop-color="' + color + '" stop-opacity="0"></stop>' +
          "</linearGradient>" +
        "</defs>" +
        '<path d="' + area + '" fill="url(#' + gid + ')"></path>' +
        '<polyline points="' + line + '" fill="none" stroke="' + color +
          '" stroke-width="1.35" stroke-linecap="round" stroke-linejoin="round" vector-effect="non-scaling-stroke"></polyline>' +
        '<line class="as-spark-cursor" x1="0" y1="0" x2="0" y2="' + h +
          '" stroke="' + color + '" stroke-width="1" stroke-opacity="0" vector-effect="non-scaling-stroke"></line>' +
      "</svg>"
    );
  }

  function applyKpiLayout(n) {
    if (!el.kpiGrid) return;
    el.kpiGrid.className = "metric-kpi-grid metric-kpi-grid--ss2";
    el.kpiGrid.style.removeProperty("grid-template-columns");
    var cols = n >= 9 ? Math.ceil(n / 2) : Math.max(n, 1);
    el.kpiGrid.style.setProperty("--kpi-n", String(Math.max(cols, 1)));
    el.kpiGrid.setAttribute("data-kpi-count", String(n));
    var density = n <= 4 ? "roomy" : n <= 8 ? "normal" : n <= 14 ? "dense" : "packed";
    el.kpiGrid.setAttribute("data-kpi-density", density);
  }

  var _kpiFitObs = null;
  var _kpiFitRaf = 0;
  function fitOneKpiText(node, minPx, maxPx) {
    if (!node) return;
    node.style.removeProperty("font-size");
    var avail = node.clientWidth;
    if (avail < 4) return;
    var csMax = parseFloat(window.getComputedStyle(node).fontSize) || 12;
    var lo = Math.max(5, minPx || 5);
    var hi = Math.max(lo, maxPx != null ? maxPx : csMax);
    hi = Math.min(hi, Math.max(lo, csMax));
    var best = lo;
    node.style.whiteSpace = "nowrap";
    node.style.overflow = "hidden";
    node.style.textOverflow = "ellipsis";
    node.style.maxWidth = "100%";
    for (var i = 0; i < 16; i++) {
      var mid = (lo + hi) / 2;
      node.style.fontSize = mid + "px";
      if (node.scrollWidth <= node.clientWidth + 0.75) {
        best = mid;
        lo = mid;
      } else {
        hi = mid;
      }
    }
    node.style.fontSize = best + "px";
  }
  function fitMetricKpiValues(root) {
    if (!root) return;
    root.querySelectorAll(".metric-kpi-ss2").forEach(function (card) {
      var w = card.clientWidth || 0;
      if (w < 8) return;
      var val = card.querySelector(".metric-kpi-ss2-value");
      var dlt = card.querySelector(".metric-kpi-ss2-delta");
      var cmp = card.querySelector(".metric-kpi-ss2-cmp");
      var metrics = card.querySelector(".metric-kpi-ss2-metrics");
      var chip = card.querySelector(".metric-kpi-ss2-head .metric-kpi-chip");
      var availVal = (metrics && metrics.clientWidth) || Math.max(8, w * 0.3);
      var scale = Math.min(1, Math.max(0.42, w / 150));
      fitOneKpiText(val, 5.5, Math.min(14, Math.max(7, availVal * 0.2 * scale + 4)));
      fitOneKpiText(dlt, 5.5, Math.min(18, Math.max(7.5, availVal * 0.26 * scale + 4)));
      if (cmp) fitOneKpiText(cmp, 5, Math.min(12, Math.max(6.5, availVal * 0.16 * scale + 3)));
      if (chip) fitOneKpiText(chip, 5, Math.min(11, Math.max(6, w * 0.075)));
      card.querySelectorAll(".metric-kpi-ss2-panel > div").forEach(function (cell) {
        var cellW = cell.clientWidth || Math.max(8, w * 0.45);
        var kicker = cell.querySelector(".metric-kpi-kicker");
        var strong = cell.querySelector("strong");
        fitOneKpiText(kicker, 4.5, Math.min(9, Math.max(5, cellW * 0.13)));
        fitOneKpiText(strong, 5, Math.min(13, Math.max(6, cellW * 0.19)));
      });
    });
  }
  function bindMetricKpiFit(root) {
    if (_kpiFitRaf) {
      cancelAnimationFrame(_kpiFitRaf);
      _kpiFitRaf = 0;
    }
    if (_kpiFitObs) {
      _kpiFitObs.disconnect();
      _kpiFitObs = null;
    }
    if (!root) return;
    function run() {
      _kpiFitRaf = 0;
      fitMetricKpiValues(root);
    }
    _kpiFitRaf = requestAnimationFrame(function () {
      _kpiFitRaf = requestAnimationFrame(run);
    });
    if (typeof ResizeObserver === "undefined") return;
    _kpiFitObs = new ResizeObserver(function () {
      if (_kpiFitRaf) cancelAnimationFrame(_kpiFitRaf);
      _kpiFitRaf = requestAnimationFrame(run);
    });
    _kpiFitObs.observe(root);
    root.querySelectorAll(".metric-kpi-ss2").forEach(function (card) {
      _kpiFitObs.observe(card);
    });
  }

  function seriesStats(series) {
    var vals = [];
    (series || []).forEach(function (r) {
      if (!r || r.value == null) return;
      var n = Number(r.value);
      if (!Number.isFinite(n)) return;
      vals.push(n);
    });
    if (!vals.length) {
      return { n: 0, avg: null, sum: null, min: null, max: null, last: null };
    }
    var sum = 0;
    var min = vals[0];
    var max = vals[0];
    for (var i = 0; i < vals.length; i++) {
      sum += vals[i];
      if (vals[i] < min) min = vals[i];
      if (vals[i] > max) max = vals[i];
    }
    return {
      n: vals.length,
      avg: sum / vals.length,
      sum: sum,
      min: min,
      max: max,
      last: vals[vals.length - 1],
    };
  }

  function buildComparePack(curPts, prevPts, mode, win) {
    if (!mode || !win || !window.SeoPeriodCompare) return null;
    var curPack = {
      series: curPts,
      total: lastClose(curPts),
      total_mode: "last",
    };
    var prevPack = {
      series: prevPts || [],
      total: lastClose(prevPts),
      total_mode: "last",
    };
    return SeoPeriodCompare.fromSeries(curPack, prevPack, mode, win);
  }

  function seriesDeltaPct(series) {
    var vals = [];
    (series || []).forEach(function (r) {
      var n = Number(r && r.value);
      if (Number.isFinite(n)) vals.push(n);
    });
    if (vals.length < 2 || !vals[0]) return null;
    return Math.round(((vals[vals.length - 1] - vals[0]) / Math.abs(vals[0])) * 1000) / 10;
  }

  function setChartFocus(metricKey) {
    focusSeriesKey = metricKey || null;
    if (el.kpiGrid) {
      el.kpiGrid.querySelectorAll("[data-metric-kpi-card]").forEach(function (c) {
        c.classList.toggle("is-chart-focus", !!focusSeriesKey && c.getAttribute("data-metric") === focusSeriesKey);
      });
    }
    if (el.chart) {
      el.chart.classList.toggle("is-series-focus", !!focusSeriesKey);
      el.chart.querySelectorAll(".chart-series-g").forEach(function (g) {
        g.classList.toggle("is-focus", !!focusSeriesKey && g.getAttribute("data-chart-series") === focusSeriesKey);
      });
    }
  }

  function renderKpis(payload, comparePayload, breakdown) {
    if (!el.kpiGrid) return;
    if (!selected.length) {
      el.kpiGrid.className = "grid grid-cols-1 gap-3";
      el.kpiGrid.style.removeProperty("--kpi-n");
      el.kpiGrid.innerHTML = '<p class="text-xs text-slate-400">Varlık seçin.</p>';
      return;
    }
    applyKpiLayout(selected.length);
    var mode = compareMode();
    var win = lastCompareWin;
    el.kpiGrid.innerHTML = selected.map(function (key, idx) {
      var spec = seriesByKey[key] || { label: key, unit: "" };
      // Ana % = seçili aralık dönem getirisi (ilk → son kapanış). COMPARE açıksa ayrı rozet.
      var daily = pointsOf(payload, key);
      var sparkPts = aggregate(daily, breakdown);
      var cur = lastClose(daily);
      var prevDaily = comparePayload ? pointsOf(comparePayload, key) : [];
      var color = colorFor(key, idx);
      var spark = seriesSparkSvg(sparkPts, color, 220, 56);
      var label = spec.label || key;
      var dlt = seriesDeltaPct(daily);
      var dltCls = dlt == null ? "is-flat" : (dlt >= 0 ? "is-up" : "is-down");
      var dltTxt = dlt == null ? "—" : ((dlt >= 0 ? "↑ " : "↓ ") + Math.abs(dlt).toFixed(1) + "%");
      var dltTitle = "Period return (first → last close in selected range)";
      var cmpHtml = "";
      if (mode && window.SeoPeriodCompare) {
        var cmp = buildComparePack(daily, prevDaily, mode, win);
        var kpiCmp = SeoPeriodCompare.kpiText(cmp);
        var cmpShort = mode === "previous_year" ? "vs LY" : "vs prev";
        cmpHtml =
          '<p class="metric-kpi-ss2-cmp ' + kpiCmp.cls + '" title="' + esc(kpiCmp.title || "") + '">' +
          esc(cmpShort + " " + kpiCmp.text) +
          "</p>";
      }
      var src = spec.source_url || "#";
      var info =
        '<a class="metric-kpi-info" href="' + esc(src) + '" target="_blank" rel="noopener" title="Kaynak" aria-label="' +
        esc(label) + ' kaynak" onclick="event.stopPropagation()">i</a>';
      var chipInner = iconHtml(spec, "as-icon") + esc(label);
      var st = seriesStats(daily);
      // Fiyat serisi: Total = son kapanış (Android market total_mode=last ile aynı)
      var totalTxt = fmtNum(st.last);
      var openCls = focusSeriesKey === key ? " is-chart-focus is-open" : " is-open";
      return (
        '<article class="metric-kpi-ss2' + openCls +
          '" style="--kpi-color:' + color + '" data-metric-kpi-card data-metric="' + esc(key) + '">' +
          '<div class="metric-kpi-ss2-head">' +
            '<span class="metric-kpi-chip" title="' + esc(label) + '">' + chipInner + "</span>" +
            info +
          "</div>" +
          '<div class="metric-kpi-ss2-main">' +
            '<div class="metric-kpi-ss2-metrics">' +
              '<p class="metric-kpi-ss2-value" title="' + esc(fmtNumFull(cur)) + '">' + esc(fmtNum(cur)) + "</p>" +
              '<p class="metric-kpi-ss2-delta ' + dltCls + '" title="' + esc(dltTitle) + '">' + esc(dltTxt) + "</p>" +
              cmpHtml +
            "</div>" +
            '<div class="metric-kpi-ss2-spark">' + spark + "</div>" +
          "</div>" +
          '<div class="metric-kpi-ss2-panel">' +
            '<div><p class="metric-kpi-kicker">Total</p><strong>' + esc(totalTxt) + "</strong></div>" +
            '<div><p class="metric-kpi-kicker">Ortalama</p><strong>' + esc(fmtNum(st.avg)) + "</strong></div>" +
            '<div><p class="metric-kpi-kicker">Min</p><strong>' + esc(fmtNum(st.min)) + "</strong></div>" +
            '<div><p class="metric-kpi-kicker">Max</p><strong>' + esc(fmtNum(st.max)) + "</strong></div>" +
            '<div><p class="metric-kpi-kicker">Last</p><strong>' + esc(fmtNum(st.last)) + "</strong></div>" +
            '<div><p class="metric-kpi-kicker">Nokta</p><strong>' + esc(String(st.n || 0)) + "</strong></div>" +
          "</div>" +
          '<button type="button" class="metric-kpi-ss2-expand" data-metric-kpi-expand aria-expanded="true" aria-label="Close details">' +
            '<svg class="metric-kpi-chev" width="12" height="12" viewBox="0 0 12 12" fill="none" aria-hidden="true"><path d="M3 4.5 L6 7.5 L9 4.5" stroke="currentColor" stroke-width="1.5" stroke-linecap="round" stroke-linejoin="round"/></svg>' +
          "</button>" +
        "</article>"
      );
    }).join("");
    bindMetricKpiFit(el.kpiGrid);
    bindSparkHovers(el.kpiGrid);
  }

  var _sparkTipEl = null;
  function ensureSparkTip() {
    if (_sparkTipEl && _sparkTipEl.isConnected) return _sparkTipEl;
    _sparkTipEl = document.createElement("div");
    _sparkTipEl.id = "as-spark-tip";
    _sparkTipEl.className = "as-spark-tip hidden";
    _sparkTipEl.setAttribute("role", "tooltip");
    document.body.appendChild(_sparkTipEl);
    return _sparkTipEl;
  }
  function hideSparkTip() {
    var tip = _sparkTipEl;
    if (!tip) return;
    tip.classList.add("hidden");
    tip.innerHTML = "";
  }
  function showSparkTip(ev, svg, pts, idx) {
    if (!pts || !pts.length || idx < 0 || idx >= pts.length) return;
    var tip = ensureSparkTip();
    var pt = pts[idx];
    var dateLabel = pt.k || "";
    if (window.SeoMetricTableUx && SeoMetricTableUx.formatTableDateKey) {
      dateLabel = SeoMetricTableUx.formatTableDateKey(pt.k) || pt.k;
    }
    tip.innerHTML =
      '<p class="as-spark-tip__d">' + esc(dateLabel) + "</p>" +
      '<p class="as-spark-tip__v" title="' + esc(fmtNumFull(pt.v)) + '">' + esc(fmtNum(pt.v)) + "</p>";
    tip.classList.remove("hidden");
    var pad = 10;
    var tw = tip.offsetWidth || 96;
    var th = tip.offsetHeight || 44;
    var left = ev.clientX - tw / 2;
    var top = ev.clientY - th - 14;
    if (top < pad) top = ev.clientY + 16;
    left = Math.max(pad, Math.min(window.innerWidth - tw - pad, left));
    top = Math.max(pad, Math.min(window.innerHeight - th - pad, top));
    tip.style.left = left + "px";
    tip.style.top = top + "px";
    var cursor = svg.querySelector(".as-spark-cursor");
    var w = Number(svg.getAttribute("data-as-spark-w")) || 220;
    if (cursor && pts.length > 1) {
      var x = (idx / (pts.length - 1)) * w;
      cursor.setAttribute("x1", String(x));
      cursor.setAttribute("x2", String(x));
      cursor.setAttribute("stroke-opacity", "0.55");
    }
  }
  function bindSparkHovers(root) {
    if (!root || root._asSparkBound) return;
    root._asSparkBound = true;
    function clearCursors() {
      root.querySelectorAll(".as-spark-cursor").forEach(function (ln) {
        ln.setAttribute("stroke-opacity", "0");
      });
    }
    root.addEventListener("mousemove", function (ev) {
      var svg = ev.target && ev.target.closest ? ev.target.closest("svg[data-as-spark]") : null;
      if (!svg || !root.contains(svg)) {
        hideSparkTip();
        clearCursors();
        return;
      }
      var raw = svg.getAttribute("data-as-spark-pts") || "[]";
      var pts;
      try { pts = JSON.parse(raw); } catch (e) { pts = []; }
      if (!pts.length) return;
      var rect = svg.getBoundingClientRect();
      if (!(rect.width > 1)) return;
      var ratio = (ev.clientX - rect.left) / rect.width;
      var idx = Math.round(Math.max(0, Math.min(1, ratio)) * (pts.length - 1));
      root.querySelectorAll("svg[data-as-spark] .as-spark-cursor").forEach(function (ln) {
        if (!svg.contains(ln)) ln.setAttribute("stroke-opacity", "0");
      });
      showSparkTip(ev, svg, pts, idx);
    });
    root.addEventListener("mouseleave", function () {
      hideSparkTip();
      clearCursors();
    });
  }

  function unionKeys(seriesMap) {
    var set = {};
    Object.keys(seriesMap).forEach(function (k) {
      seriesMap[k].forEach(function (p) { set[p.key] = 1; });
    });
    return Object.keys(set).sort();
  }

  function renderLegend() {
    if (!el.legend) return;
    el.legend.innerHTML = selected.map(function (key, idx) {
      var spec = seriesByKey[key] || { label: key };
      var c = colorFor(key, idx);
      return '<span class="inline-flex items-center gap-1.5">' +
        iconHtml(spec, "as-icon as-icon--xs") +
        '<span class="h-2 w-2 rounded-full" style="background:' + c + '"></span>' +
        esc(spec.label || key) + "</span>";
    }).join("");
  }

  function syncChartStyleButtons() {
    if (!el.chartStyleRoot) return;
    Array.prototype.forEach.call(
      el.chartStyleRoot.querySelectorAll("[data-as-chart-style]"),
      function (btn) {
        var on = btn.getAttribute("data-as-chart-style") === chartStyle;
        btn.classList.toggle("is-active", on);
        btn.setAttribute("aria-pressed", on ? "true" : "false");
      }
    );
  }

  function setChartStyle(style) {
    if (style !== "line" && style !== "area" && style !== "bar") return;
    chartStyle = style;
    try { localStorage.setItem("asChartStyle", style); } catch (_) {}
    syncChartStyleButtons();
    refreshViews();
  }

  function renderChart(payload, breakdown) {
    if (!el.chart) return;
    var seriesMap = {};
    selected.forEach(function (key) {
      seriesMap[key] = normalizeSeries(aggregate(pointsOf(payload, key), breakdown));
    });
    var dates = unionKeys(seriesMap);
    var W = 720, H = 260;
    var padL = 48, padR = 20, padT = 18, padB = 36;
    var plotW = W - padL - padR;
    var plotH = H - padT - padB;
    var clipId = "as-plot-clip";

    function xOf(i) {
      if (dates.length <= 1) return padL + plotW / 2;
      return padL + (i / (dates.length - 1)) * plotW;
    }
    function yOf(norm) {
      var n = Math.max(0, Math.min(100, Number(norm) || 0));
      return padT + (1 - n / 100) * plotH;
    }

    var grid = "";
    for (var g = 0; g <= 4; g++) {
      var gy = yOf(g * 25);
      grid += '<line class="pa-grid" x1="' + padL + '" y1="' + gy + '" x2="' + (W - padR) + '" y2="' + gy + '" stroke="#e2e8f0" stroke-width="1"/>';
      grid += '<text class="pa-axis-label pa-axis-label--y" x="' + (padL - 8) + '" y="' + (gy + 3) + '" text-anchor="end" fill="#94a3b8">' + (g * 25) + "%</text>";
    }
    var xLabels = "";
    var labelCount = Math.min(6, dates.length);
    for (var li = 0; li < labelCount; li++) {
      var di = labelCount === 1 ? 0 : Math.round((li / (labelCount - 1)) * (dates.length - 1));
      xLabels += '<text class="pa-axis-label pa-axis-label--x" x="' + xOf(di) + '" y="' + (H - 10) + '" text-anchor="middle" fill="#94a3b8">' + esc(dates[di]) + "</text>";
    }

    var paths = "";
    var style = chartStyle || "area";
    selected.forEach(function (key, idx) {
      var pts = seriesMap[key];
      if (!pts.length) return;
      var byDate = {};
      pts.forEach(function (p) { byDate[p.key] = p; });
      var color = colorFor(key, idx);
      var focusCls = focusSeriesKey === key ? " is-focus" : "";
      var inner = "";
      if (style === "bar") {
        var barW = Math.max(2, (plotW / Math.max(dates.length, 1)) * 0.55 / Math.max(selected.length, 1));
        dates.forEach(function (d, i) {
          var p = byDate[d];
          if (!p) return;
          var x = xOf(i) - (selected.length * barW) / 2 + idx * barW;
          var y = yOf(p.norm);
          var h = padT + plotH - y;
          inner += '<rect x="' + x.toFixed(1) + '" y="' + y.toFixed(1) + '" width="' + barW.toFixed(1) + '" height="' + Math.max(0, h).toFixed(1) + '" fill="' + color + '" fill-opacity="0.78" rx="1"/>';
        });
      } else {
        var coords = [];
        dates.forEach(function (d, i) {
          var p = byDate[d];
          if (!p) return;
          coords.push({ x: xOf(i), y: yOf(p.norm) });
        });
        if (coords.length) {
          if (style === "area" && coords.length >= 2) {
            var areaD =
              "M" + coords[0].x.toFixed(1) + " " + (padT + plotH).toFixed(1) +
              " L" + coords.map(function (c) { return c.x.toFixed(1) + " " + c.y.toFixed(1); }).join(" L") +
              " L" + coords[coords.length - 1].x.toFixed(1) + " " + (padT + plotH).toFixed(1) + " Z";
            inner += '<path d="' + areaD + '" fill="' + color + '" fill-opacity="0.16"/>';
          }
          var linePts = coords.map(function (c) { return c.x.toFixed(1) + "," + c.y.toFixed(1); }).join(" ");
          inner += '<polyline points="' + linePts + '" fill="none" stroke="' + color + '" stroke-width="0.6" stroke-linecap="round" stroke-linejoin="round"/>';
        }
      }
      paths += '<g class="chart-series-g' + focusCls + '" data-chart-series="' + esc(key) + '">' + inner + "</g>";
    });

    var defs = '<defs><clipPath id="' + clipId + '">' +
      '<rect x="' + padL + '" y="' + padT + '" width="' + plotW + '" height="' + plotH + '"/>' +
      "</clipPath></defs>";
    var plotFrame = '<rect class="pa-plot-frame" x="' + padL + '" y="' + padT + '" width="' + plotW + '" height="' + plotH +
      '" fill="none" stroke="#e2e8f0" stroke-width="1" rx="2"/>';

    el.chart.setAttribute("viewBox", "0 0 " + W + " " + H);
    el.chart.classList.toggle("is-series-focus", !!focusSeriesKey);
    el.chart.innerHTML = defs + grid + plotFrame +
      '<g clip-path="url(#' + clipId + ')">' + paths + "</g>" +
      xLabels +
      '<rect id="as-hit" x="' + padL + '" y="' + padT + '" width="' + plotW + '" height="' + plotH + '" fill="transparent"/>';

    var hit = document.getElementById("as-hit");
    if (hit) {
      hit.addEventListener("mousemove", function (ev) {
        if (!dates.length) return;
        var rect = el.chart.getBoundingClientRect();
        var mx = ((ev.clientX - rect.left) / rect.width) * W;
        var ratio = (mx - padL) / plotW;
        var idx = Math.round(Math.max(0, Math.min(1, ratio)) * (dates.length - 1));
        var d = dates[idx];
        showTip(ev, d, seriesMap);
      });
      hit.addEventListener("mouseleave", function (ev) {
        // Tip pointer-events:auto — imleç tip'e geçerken hemen kapatma
        if (el.tip && ev.relatedTarget && el.tip.contains(ev.relatedTarget)) return;
        scheduleHideTip();
      });
    }
    if (typeof window.paSyncChartLayout === "function") window.paSyncChartLayout();
  }

  var tipHideTimer = null;
  var tipPointerInside = false;

  function cancelHideTip() {
    if (tipHideTimer) {
      clearTimeout(tipHideTimer);
      tipHideTimer = null;
    }
  }

  function scheduleHideTip() {
    cancelHideTip();
    tipHideTimer = setTimeout(function () {
      tipHideTimer = null;
      if (!tipPointerInside) hideTip();
    }, 160);
  }

  function showTip(ev, dateKey, seriesMap) {
    if (!el.tip || !el.chartWrap) return;
    cancelHideTip();
    el.tip.classList.remove("hidden");
    if (el.tipTitle) el.tipTitle.textContent = dateKey;
    var lines = selected.map(function (key, idx) {
      var pts = seriesMap[key] || [];
      var found = null;
      for (var i = 0; i < pts.length; i++) if (pts[i].key === dateKey) { found = pts[i]; break; }
      var spec = seriesByKey[key] || { label: key };
      var c = colorFor(key, idx);
      return '<div class="flex items-center gap-2">' +
        iconHtml(spec, "as-icon as-icon--xs") +
        '<span class="h-2 w-2 rounded-full" style="background:' + c + '"></span>' +
        '<span class="flex-1 truncate">' + esc(spec.label || key) + '</span>' +
        '<span class="tabular-nums font-bold" title="' +
          esc(found ? fmtNumFull(found.value) : "") + '">' +
          (found ? fmtNum(found.value) : "—") + "</span></div>";
    });
    var dateChanged = el.tip.getAttribute("data-tip-date") !== String(dateKey);
    if (el.tipBody) el.tipBody.innerHTML = lines.join("");
    if (dateChanged) {
      el.tip.setAttribute("data-tip-date", String(dateKey));
      el.tip.scrollTop = 0;
    }

    // Android ile aynı: ölçüldükten sonra imlecin üstüne / kenarlara yasla;
    // sabit 120px varsayımı uzun listelerde popup'ı grafik altına taşıyordu.
    var wrapRect = el.chartWrap.getBoundingClientRect();
    var pad = 8;
    var maxTipW = Math.max(160, Math.min(360, wrapRect.width - pad * 2));
    var maxTipH = Math.max(64, wrapRect.height - pad * 2);
    el.tip.style.width = "max-content";
    el.tip.style.maxWidth = maxTipW + "px";
    el.tip.style.maxHeight = maxTipH + "px";
    el.tip.style.overflowY = "auto";
    el.tip.style.overscrollBehavior = "contain";
    el.tip.style.pointerEvents = "auto";
    el.tip.style.transform = "none";
    var tipW = Math.min(maxTipW, Math.max(el.tip.offsetWidth || 0, 140));
    var tipH = Math.min(maxTipH, Math.max(el.tip.offsetHeight || 0, 48));
    // Tip üzerinde scroll ederken pozisyonu sabitle (mousemove ile zıplamasın)
    if (tipPointerInside) {
      return;
    }
    var x = ev.clientX - wrapRect.left;
    var y = ev.clientY - wrapRect.top;
    var left = x - tipW / 2;
    var top = y - tipH - 14;
    if (top < pad) top = Math.min(wrapRect.height - tipH - pad, y + 18);
    left = Math.max(pad, Math.min(wrapRect.width - tipW - pad, left));
    top = Math.max(pad, Math.min(Math.max(pad, wrapRect.height - tipH - pad), top));
    el.tip.style.left = left + "px";
    el.tip.style.top = top + "px";
  }
  function hideTip() {
    cancelHideTip();
    tipPointerInside = false;
    if (el.tip) {
      el.tip.classList.add("hidden");
      el.tip.removeAttribute("data-tip-date");
    }
  }

  function fitDataList(rowCount) {
    var shell = el.tableShell;
    if (!shell || !window.SeoResizableDataList) return;
    window.SeoResizableDataList.bind(shell);
    requestAnimationFrame(function () {
      requestAnimationFrame(function () {
        window.SeoResizableDataList.fit(shell, rowCount || 0);
        if (window.SeoMetricTableUx && window.SeoMetricTableUx.equalizeTableBox) {
          window.SeoMetricTableUx.equalizeTableBox(el.table);
        }
      });
    });
  }

  function renderTable(payload, comparePayload, breakdown) {
    if (!el.thead || !el.tbody) return;
    var ux = window.SeoMetricTableUx;
    if (ux) ux.injectStyles();
    var mode = compareMode();
    var win = lastCompareWin;
    var seriesMap = {};
    selected.forEach(function (key) {
      seriesMap[key] = aggregate(pointsOf(payload, key), breakdown);
    });
    var keys = unionKeys(seriesMap);
    var fmtTableKey = ux && ux.formatTableDateKey
      ? ux.formatTableDateKey
      : function (k) { return k; };

    var colItems = selected.map(function (key, idx) {
      var spec = seriesByKey[key] || { label: key };
      var pts = seriesMap[key] || [];
      var map = {};
      pts.forEach(function (p) {
        if (!p || p.key == null) return;
        var nv = Number(p.value);
        if (!Number.isFinite(nv)) return;
        map[String(p.key)] = nv;
      });
      var prevPts = comparePayload ? aggregate(pointsOf(comparePayload, key), breakdown) : [];
      var compare = mode && window.SeoPeriodCompare
        ? buildComparePack(pts, prevPts, mode, win)
        : null;
      return {
        key: "m:" + key,
        label: spec.label || key,
        shortLabel: shortLabel(spec.label, key),
        color: colorFor(key, idx),
        metric: "market:" + key,
        map: map,
        series: pts.map(function (p) {
          return { key: String(p.key), value: Number(p.value) };
        }),
        compare: compare,
        assetKey: key,
      };
    });

    if (ux) {
      var preferred = ux.readJson("as-table-col-order", []);
      // Eski / bozuk order kayıtlarını m: prefix ile hizala
      preferred = (preferred || []).map(function (k) {
        var s = String(k || "");
        if (!s) return s;
        if (s.indexOf("m:") === 0 || s.indexOf("o:") === 0 || s.indexOf(":dlt") >= 0) return s;
        return "m:" + s;
      });
      var orderedKeys = ux.orderKeys(preferred, colItems.map(function (c) { return c.key; }));
      var byKey = Object.create(null);
      colItems.forEach(function (c) { byKey[c.key] = c; });
      colItems = orderedKeys.map(function (k) { return byKey[k]; }).filter(Boolean);
    }

    var gridCols = colItems.slice();
    if (mode && window.SeoPeriodCompare && (!ux || ux.isCompareColsEnabled())) {
      gridCols = SeoPeriodCompare.appendDeltaColumns(gridCols);
    }
    // Render öncesi map'leri series'ten yeniden kur (kolon sırası / stale map)
    gridCols.forEach(function (col) {
      if (!col || col.isDelta) return;
      var m = {};
      (col.series || []).forEach(function (p) {
        if (!p || p.key == null) return;
        var nv = Number(p.value);
        if (!Number.isFinite(nv)) return;
        m[String(p.key)] = nv;
      });
      col.map = m;
    });
    if (ux && ux.attachWeekendCarry) ux.attachWeekendCarry(gridCols, keys);

    var thRemoveCls =
      'inline-flex h-3.5 w-3.5 shrink-0 items-center justify-center rounded text-[10px] leading-none text-slate-400 hover:bg-rose-100 hover:text-rose-600 dark:text-zinc-500 dark:hover:bg-rose-950/50 dark:hover:text-rose-300';
    var pinOn = ux && ux.isPinEnabled();
    var stickyTop = pinOn ? " mtux-sticky-top" : "";

    function refreshTable() {
      if (!lastPayload) return;
      renderTable(lastPayload, lastComparePayload, el.breakdown ? el.breakdown.value : "day");
    }

    var tableEl = el.table;
    var gridRes = ux && tableEl
      ? ux.renderHeatGrid({
          shell: el.tableShell,
          tableEl: tableEl,
          theadRow: el.thead,
          tbody: el.tbody,
          keys: keys,
          colItems: gridCols,
          esc: esc,
          fmtKey: fmtTableKey,
          fmtVal: function (v, col) {
            if (col && col.isDelta) {
              return v == null ? "—" : ((v >= 0 ? "+" : "") + Number(v).toFixed(1) + "%");
            }
            return v == null ? "—" : fmtNum(v);
          },
          breakdownLabel: "Date",
          averageLabel: "Average",
          onRefresh: refreshTable,
          compareCols: true,
          bindInteractive: {
            widthsKey: "as-table-col-widths",
            orderKey: "as-table-col-order",
            onOrderChange: refreshTable,
          },
          renderStandardHeaderCell: function (col) {
            var assetKey = col.assetKey || String(col.metric || "").replace(/^market:/, "");
            return (
              '<th class="mtux-th px-1 py-2 font-bold tabular-nums sm:px-1.5' + stickyTop + '" data-mtux-key="' + esc(col.key) +
                '" style="color:' + esc(col.color) + '" title="' + esc(col.label || "") + '">' +
                '<span class="mtux-th-label">' +
                  '<span class="mtux-th-text">' + esc(col.shortLabel) + "</span>" +
                  (col.isDelta
                    ? ""
                    : '<button type="button" data-as-col-remove="' + esc(assetKey) + '" class="' + thRemoveCls +
                      '" title="Remove column" aria-label="' + esc(col.shortLabel) + ' remove column">×</button>') +
                "</span>" +
              "</th>"
            );
          },
        })
      : { rowCount: 0 };

    if (!ux) {
      el.thead.innerHTML = '<th class="px-2 py-2 font-bold">Date</th>' + selected.map(function (key) {
        var spec = seriesByKey[key] || { label: key };
        return '<th class="px-2 py-2 font-bold tabular-nums">' + esc(spec.label || key) + "</th>";
      }).join("");
      el.tbody.innerHTML = keys.slice().reverse().map(function (d) {
        return "<tr><td class=\"px-2 py-1.5 tabular-nums text-slate-500\">" + esc(d) + "</td>" +
          selected.map(function (key) {
            var pts = seriesMap[key] || [];
            var found = null;
            for (var i = 0; i < pts.length; i++) if (pts[i].key === d) { found = pts[i]; break; }
            return '<td class="px-2 py-1.5 tabular-nums font-semibold">' + (found ? fmtNum(found.value) : "—") + "</td>";
          }).join("") + "</tr>";
      }).join("");
      fitDataList(keys.length + 1);
      return;
    }

    if (!gridRes.rowCount && (!colItems.length || !keys.length)) {
      fitDataList(0);
      return;
    }
    fitDataList(gridRes.rowCount || keys.length + 1);
    if (ux && ux.fitTextToWidth && el.table) {
      requestAnimationFrame(function () {
        ux.fitTextToWidth(el.table, "td.mtux-heat-cell", { minPx: 7 });
      });
    }
  }

  function refreshViews() {
    if (!lastPayload) return;
    var breakdown = el.breakdown ? el.breakdown.value : "day";
    renderKpis(lastPayload, lastComparePayload, breakdown);
    renderLegend();
    renderChart(lastPayload, breakdown);
    renderTable(lastPayload, lastComparePayload, breakdown);
  }

  function run() {
    if (!selected.length) {
      updateMetricTrigger();
      if (el.kpiGrid) {
        el.kpiGrid.className = "grid grid-cols-1 gap-3";
        el.kpiGrid.innerHTML = '<p class="text-xs text-slate-400">Varlık seçin.</p>';
      }
      return;
    }
    var start = el.start && el.start.value;
    var end = el.end && el.end.value;
    if (!start || !end) applyPreset();
    start = el.start.value;
    end = el.end.value;
    setLoading(true);
    var mode = compareMode();
    var cmp = mode ? compareBounds(start, end, mode) : null;
    lastCompareWin = cmp;
    if (el.compareRange) {
      if (cmp) {
        var label = window.SeoPeriodCompare ? SeoPeriodCompare.label(mode) : (mode === "previous_year" ? "Previous year" : "Previous period");
        el.compareRange.textContent = label + ": " + cmp.start + " → " + cmp.end;
      } else {
        el.compareRange.textContent = "";
      }
    }
    var main = fetchOverlay(start, end, selected);
    var side = cmp ? fetchOverlay(cmp.start, cmp.end, selected) : Promise.resolve(null);
    Promise.all([main, side]).then(function (pair) {
      lastPayload = pair[0];
      lastComparePayload = pair[1];
      rememberDataRange(lastPayload);
      if (lastComparePayload) rememberDataRange(lastComparePayload);
      if (el.sync && lastPayload && lastPayload.synced_at) {
        el.sync.textContent = "Last sync · " + String(lastPayload.synced_at).replace("T", " ").slice(0, 19);
      }
      refreshViews();
    }).catch(function (err) {
      if (el.kpiGrid) {
        el.kpiGrid.className = "grid grid-cols-1 gap-3";
        el.kpiGrid.innerHTML = '<p class="text-xs text-red-600">Veri yüklenemedi: ' +
          esc(String((err && err.message) || err)) + "</p>";
      }
    }).then(function () { setLoading(false); });
  }

  // Events
  if (el.preset) el.preset.addEventListener("change", function () { applyPreset(); });
  if (el.start) {
    el.start.addEventListener("change", function () {
      if (el.preset) el.preset.value = "custom";
    });
  }
  if (el.end) {
    el.end.addEventListener("change", function () {
      if (el.preset) el.preset.value = "custom";
    });
  }
  if (el.run) el.run.addEventListener("click", run);
  if (el.breakdown) el.breakdown.addEventListener("change", refreshViews);
  if (el.compare) el.compare.addEventListener("change", run);
  if (el.tip) {
    el.tip.addEventListener("mouseenter", function () {
      tipPointerInside = true;
      cancelHideTip();
    });
    el.tip.addEventListener("mouseleave", function (ev) {
      tipPointerInside = false;
      // Grafiğe geri dönüyorsa tip açık kalsın; mousemove yeniler
      if (el.chart && ev.relatedTarget && el.chart.contains(ev.relatedTarget)) {
        cancelHideTip();
        return;
      }
      scheduleHideTip();
    });
    // Popup üzerindeyken tekerlek sayfaya gitmesin — tip scroll
    el.tip.addEventListener(
      "wheel",
      function (ev) {
        tipPointerInside = true;
        cancelHideTip();
        var node = el.tip;
        var before = node.scrollTop;
        node.scrollTop += ev.deltaY;
        // Her durumda sayfa kaymasını engelle (uçta da)
        ev.preventDefault();
        ev.stopPropagation();
        if (node.scrollTop === before && ev.deltaY !== 0) {
          // Uçta: yine de sayfayı kaydırma (kullanıcı popup'ta)
        }
      },
      { passive: false }
    );
  }
  if (el.metricTrigger) {
    el.metricTrigger.addEventListener("click", function (e) {
      e.stopPropagation();
      var open = el.metricList && !el.metricList.classList.contains("hidden");
      openMetricList(!open);
    });
  }
  if (el.metricList) {
    el.metricList.addEventListener("click", function (e) {
      var btn = e.target.closest("[data-as-key], [data-as-clear]");
      if (!btn) return;
      // Liste rebuild e.target'i DOM'dan düşürür; bubble document'e gidince
      // "dışarı tık" sanılıp dropdown kapanıyordu — çoklu seçim için durdur.
      e.preventDefault();
      e.stopPropagation();
      if (btn.getAttribute("data-as-clear")) {
        selected = [];
      } else {
        var key = btn.getAttribute("data-as-key");
        var i = selected.indexOf(key);
        if (i >= 0) selected.splice(i, 1);
        else selected.push(key);
      }
      updateMetricTrigger();
      var scrollTop = el.metricScroll ? el.metricScroll.scrollTop : 0;
      buildMetricList();
      if (el.metricScroll) el.metricScroll.scrollTop = scrollTop;
    });
  }
  document.addEventListener("click", function (e) {
    if (!el.metricList || el.metricList.classList.contains("hidden")) return;
    if (el.metricTrigger && el.metricTrigger.contains(e.target)) return;
    if (el.metricList.contains(e.target)) return;
    openMetricList(false);
  });
  window.addEventListener("resize", function () {
    if (el.metricList && !el.metricList.classList.contains("hidden")) positionMetricDropdown();
  });

  if (el.chartStyleRoot) {
    el.chartStyleRoot.addEventListener("click", function (ev) {
      var btn = ev.target && ev.target.closest ? ev.target.closest("[data-as-chart-style]") : null;
      if (!btn) return;
      setChartStyle(btn.getAttribute("data-as-chart-style") || "area");
    });
  }

  if (el.kpiGrid) {
    el.kpiGrid.addEventListener("click", function (ev) {
      if (ev.target.closest && ev.target.closest("a.metric-kpi-info")) return;
      var expand = ev.target.closest ? ev.target.closest("[data-metric-kpi-expand]") : null;
      if (expand) {
        ev.preventDefault();
        ev.stopPropagation();
        var expCard = expand.closest("[data-metric-kpi-card]");
        if (!expCard || !el.kpiGrid.contains(expCard)) return;
        var open = expCard.classList.toggle("is-open");
        expand.setAttribute("aria-expanded", open ? "true" : "false");
        expand.setAttribute("aria-label", open ? "Close details" : "Open details");
        return;
      }
      var card = ev.target.closest ? ev.target.closest("[data-metric-kpi-card]") : null;
      if (!card) return;
      var key = card.getAttribute("data-metric");
      setChartFocus(focusSeriesKey === key ? null : key);
    });
  }

  document.addEventListener("click", function (ev) {
    var rm = ev.target.closest ? ev.target.closest("[data-as-col-remove]") : null;
    if (!rm) return;
    ev.preventDefault();
    var key = rm.getAttribute("data-as-col-remove");
    var i = selected.indexOf(key);
    if (i >= 0) {
      selected.splice(i, 1);
      if (focusSeriesKey === key) focusSeriesKey = null;
      updateMetricTrigger();
      run();
    }
  });

  syncDateBoundsAttrs();
  syncChartStyleButtons();
  applyPreset({ skipRun: true });
  updateMetricTrigger();
  run();
})();
