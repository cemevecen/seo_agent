/**
 * /assets — doviz.com piyasa serileri (Android chart/table/KPI UX).
 */
(function () {
  "use strict";

  var cfg = window.SEO_ASSETS_PAGE || {};
  var ALL_SERIES = Array.isArray(cfg.series) ? cfg.series : [];
  var DEFAULTS = Array.isArray(cfg.defaults) ? cfg.defaults.slice() : [];
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
    if (v == null || !Number.isFinite(v)) return "—";
    var abs = Math.abs(v);
    var opts;
    if (abs >= 1000) opts = { maximumFractionDigits: 2 };
    else if (abs >= 1) opts = { maximumFractionDigits: 4 };
    else opts = { maximumFractionDigits: 8 };
    return v.toLocaleString("tr-TR", opts);
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

  function applyPreset() {
    var today = new Date();
    today.setHours(0, 0, 0, 0);
    var end = today;
    var start;
    var p = el.preset ? el.preset.value : "90";
    if (p === "since2025") {
      start = new Date(2025, 0, 1);
    } else {
      var days = parseInt(p, 10) || 90;
      start = addDays(end, -(days - 1));
    }
    if (el.start) el.start.value = iso(start);
    if (el.end) el.end.value = iso(end);
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
    var vals = [];
    (series || []).forEach(function (r) {
      var n = Number(r && r.value);
      if (Number.isFinite(n)) vals.push(n);
    });
    if (vals.length < 2) {
      return '<div class="metric-kpi-spark" style="opacity:.35" aria-hidden="true"></div>';
    }
    var min = Math.min.apply(null, vals);
    var max = Math.max.apply(null, vals);
    var span = (max - min) || 1;
    var pts = vals.map(function (v, i) {
      var x = (i / (vals.length - 1)) * w;
      var y = h - ((v - min) / span) * (h - 4) - 2;
      return [x, y];
    });
    var line = pts.map(function (p) { return p[0].toFixed(1) + "," + p[1].toFixed(1); }).join(" ");
    var area =
      "M" + pts[0][0].toFixed(1) + " " + h.toFixed(1) +
      " L" + pts.map(function (p) { return p[0].toFixed(1) + " " + p[1].toFixed(1); }).join(" L") +
      " L" + pts[pts.length - 1][0].toFixed(1) + " " + h.toFixed(1) + " Z";
    _kpiSparkGradSeq += 1;
    var gid = "as-kpi-spark-grad-" + _kpiSparkGradSeq;
    return (
      '<svg class="metric-kpi-spark" viewBox="0 0 ' + w + " " + h + '" preserveAspectRatio="none" aria-hidden="true">' +
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
      "</svg>"
    );
  }

  function applyKpiLayout(n) {
    if (!el.kpiGrid) return;
    el.kpiGrid.className = "metric-kpi-grid metric-kpi-grid--ss2";
    el.kpiGrid.style.removeProperty("grid-template-columns");
    var cols = n >= 9 ? Math.ceil(n / 2) : Math.max(n, 1);
    el.kpiGrid.style.setProperty("--kpi-n", String(Math.max(cols, 1)));
  }

  var _kpiFitObs = null;
  var _kpiFitRaf = 0;
  function fitOneKpiText(node, minPx, maxPx) {
    if (!node) return;
    var avail = node.clientWidth;
    if (avail < 8) return;
    var lo = minPx;
    var hi = Math.max(minPx, maxPx);
    var best = minPx;
    node.style.whiteSpace = "nowrap";
    node.style.overflow = "hidden";
    node.style.textOverflow = "ellipsis";
    for (var i = 0; i < 14; i++) {
      var mid = (lo + hi) / 2;
      node.style.fontSize = mid + "px";
      if (node.scrollWidth <= avail + 0.75) {
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
      var metrics = card.querySelector(".metric-kpi-ss2-metrics");
      var availVal = (metrics && metrics.clientWidth) || Math.max(8, w * 0.3);
      fitOneKpiText(val, Math.max(8, w * 0.045), Math.min(11.5, Math.max(9, availVal * 0.17)));
      fitOneKpiText(dlt, Math.max(14, w * 0.056), 18);
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
      var pts = aggregate(pointsOf(payload, key), breakdown);
      var cur = lastClose(pts);
      var prevPts = comparePayload ? aggregate(pointsOf(comparePayload, key), breakdown) : [];
      var color = colorFor(key, idx);
      var spark = seriesSparkSvg(pts, color, 220, 56);
      var label = spec.label || key;
      var dlt = seriesDeltaPct(pts);
      var dltCls = dlt == null ? "is-flat" : (dlt >= 0 ? "is-up" : "is-down");
      var dltTxt = dlt == null ? "—" : ((dlt >= 0 ? "↑ " : "↓ ") + Math.abs(dlt).toFixed(1) + "%");
      var dltTitle = "Period change";
      if (mode && window.SeoPeriodCompare) {
        var cmp = buildComparePack(pts, prevPts, mode, win);
        var kpiCmp = SeoPeriodCompare.kpiText(cmp);
        dltTxt = kpiCmp.text;
        dltCls = kpiCmp.cls;
        dltTitle = kpiCmp.title || "";
      }
      var src = spec.source_url || "#";
      var info =
        '<a class="metric-kpi-info" href="' + esc(src) + '" target="_blank" rel="noopener" title="Kaynak" aria-label="' +
        esc(label) + ' kaynak" onclick="event.stopPropagation()">i</a>';
      var chipInner = iconHtml(spec, "as-icon") + esc(label);
      return (
        '<article class="metric-kpi-ss2' + (focusSeriesKey === key ? " is-chart-focus" : "") +
          '" style="--kpi-color:' + color + '" data-metric-kpi-card data-metric="' + esc(key) + '">' +
          '<div class="metric-kpi-ss2-head">' +
            '<span class="metric-kpi-chip" title="' + esc(label) + '">' + chipInner + "</span>" +
            info +
          "</div>" +
          '<div class="metric-kpi-ss2-main">' +
            '<div class="metric-kpi-ss2-metrics">' +
              '<p class="metric-kpi-ss2-value" title="' + esc(fmtNum(cur)) + '">' + esc(fmtNum(cur)) + "</p>" +
              '<p class="metric-kpi-ss2-delta ' + dltCls + '" title="' + esc(dltTitle) + '">' + esc(dltTxt) + "</p>" +
            "</div>" +
            '<div class="metric-kpi-ss2-spark">' + spark + "</div>" +
          "</div>" +
        "</article>"
      );
    }).join("");
    bindMetricKpiFit(el.kpiGrid);
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
          inner += '<polyline points="' + linePts + '" fill="none" stroke="' + color + '" stroke-width="2" stroke-linecap="round" stroke-linejoin="round"/>';
        }
      }
      paths += '<g class="chart-series-g' + focusCls + '" data-chart-series="' + esc(key) + '">' + inner + "</g>";
    });

    var defs = '<defs><clipPath id="' + clipId + '">' +
      '<rect x="' + padL + '" y="' + padT + '" width="' + plotW + '" height="' + plotH + '"/>' +
      "</clipPath></defs>";
    var plotFrame = '<rect x="' + padL + '" y="' + padT + '" width="' + plotW + '" height="' + plotH +
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
      hit.addEventListener("mouseleave", hideTip);
    }
    if (typeof window.paSyncChartLayout === "function") window.paSyncChartLayout();
  }

  function showTip(ev, dateKey, seriesMap) {
    if (!el.tip) return;
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
        '<span class="tabular-nums font-bold">' + (found ? fmtNum(found.value) : "—") + "</span></div>";
    });
    if (el.tipBody) el.tipBody.innerHTML = lines.join("");
    var wrap = el.chartWrap.getBoundingClientRect();
    var left = ev.clientX - wrap.left + 12;
    var top = ev.clientY - wrap.top + 12;
    if (left + 200 > wrap.width) left = Math.max(8, wrap.width - 220);
    if (top + 120 > wrap.height) top = Math.max(8, wrap.height - 140);
    el.tip.style.left = left + "px";
    el.tip.style.top = top + "px";
  }
  function hideTip() {
    if (el.tip) el.tip.classList.add("hidden");
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
      pts.forEach(function (p) { map[p.key] = p.value; });
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
        series: pts,
        compare: compare,
        assetKey: key,
      };
    });

    if (ux) {
      var preferred = ux.readJson("as-table-col-order", []);
      var orderedKeys = ux.orderKeys(preferred, colItems.map(function (c) { return c.key; }));
      var byKey = {};
      colItems.forEach(function (c) { byKey[c.key] = c; });
      colItems = orderedKeys.map(function (k) { return byKey[k]; }).filter(Boolean);
    }

    var gridCols = colItems.slice();
    if (mode && window.SeoPeriodCompare && (!ux || ux.isCompareColsEnabled())) {
      gridCols = SeoPeriodCompare.appendDeltaColumns(gridCols);
    }
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
  if (el.preset) el.preset.addEventListener("change", applyPreset);
  if (el.run) el.run.addEventListener("click", run);
  if (el.breakdown) el.breakdown.addEventListener("change", refreshViews);
  if (el.compare) el.compare.addEventListener("change", run);
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
      e.preventDefault();
      if (btn.getAttribute("data-as-clear")) {
        selected = [];
      } else {
        var key = btn.getAttribute("data-as-key");
        var i = selected.indexOf(key);
        if (i >= 0) selected.splice(i, 1);
        else selected.push(key);
      }
      updateMetricTrigger();
      buildMetricList();
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

  syncChartStyleButtons();
  applyPreset();
  updateMetricTrigger();
  run();
})();
