/**
 * Sinemalar Datas tab — Empower X-Data (web / mweb), project=sinemalar.
 * Prefix: sd-
 * KPI cards + heat table mirror Android Metrik UX (without editing android.html).
 */
(function (global) {
  "use strict";

  var PROJECT = "sinemalar";
  var OVERLAY_ID = "sd-cross-metric-overlay-root";
  var DEFAULT_METRICS = ["xdata:sessions", "xdata:active1DayUsers", "xdata:usdSpent"];
  var COLORS = ["#2563EB", "#10B981", "#F59E0B", "#EF4444", "#8B5CF6", "#06B6D4", "#F97316", "#EC4899"];
  var OVERLAY_COLORS = ["#F59E0B", "#A855F7", "#14B8A6", "#F43F5E", "#64748B"];
  /** Android/iOS ile aynı sabit SVG koordinat yüksekliği — yükseklik/sıkıştırma pa_chart_height.js ile. */
  var CHART_VIEW_H = 260;
  var AXIS_LABEL_FONT = 10;

  var state = {
    platform: "web",
    selected: DEFAULT_METRICS.slice(),
    seriesByMetric: {},
    overlaySeries: [],
    compareSeries: {},
    dates: [],
    chartStyle: "area",
    loading: false,
    labelByKey: {},
    focusedMetric: null,
    /** Legend ile gizlenen seriler (key → true). Legend’da kalır; chart/KPI/table’dan düşer. */
    legendMuted: {},
  };

  var _kpiSparkGradSeq = 0;
  var _kpiFitObs = null;
  var _kpiFitRaf = 0;
  var _kpiEventsBound = false;
  var _legendBound = false;

  function $(id) {
    return document.getElementById(id);
  }

  function esc(s) {
    return String(s == null ? "" : s)
      .replace(/&/g, "&amp;")
      .replace(/</g, "&lt;")
      .replace(/>/g, "&gt;")
      .replace(/"/g, "&quot;");
  }

  function pad2(n) {
    return n < 10 ? "0" + n : String(n);
  }

  function iso(d) {
    return d.getFullYear() + "-" + pad2(d.getMonth() + 1) + "-" + pad2(d.getDate());
  }

  function parseIso(s) {
    var p = String(s || "").split("-");
    if (p.length < 3) return null;
    return new Date(Number(p[0]), Number(p[1]) - 1, Number(p[2]));
  }

  function today() {
    var d = new Date();
    return new Date(d.getFullYear(), d.getMonth(), d.getDate());
  }

  function addDays(d, n) {
    return new Date(d.getFullYear(), d.getMonth(), d.getDate() + n);
  }

  function peerPlatform(p) {
    return p === "web" ? "mweb" : "web";
  }

  function peerLabel(p) {
    return p === "web" ? "Mobile Web metrics" : "Web metrics";
  }

  function platformLabel(p) {
    return p === "mweb" ? "Mobile Web" : "Web";
  }

  function metricLabel(key) {
    if (state.labelByKey[key]) return state.labelByKey[key];
    if (global.PlayMetricOverlay && PlayMetricOverlay.metricLabel) {
      return PlayMetricOverlay.metricLabel(key);
    }
    return String(key || "").replace(/^xdata:/, "");
  }

  function shortMetricLabel(label, key) {
    var s = String(label || key || "");
    if (s.length <= 18) return s;
    return s.slice(0, 16) + "…";
  }

  function isAvgMetric(key) {
    var avg = global.SD_XDATA_AVG_KEYS || [];
    return avg.indexOf(key) >= 0;
  }

  function fmtNum(v, key) {
    if (v == null || !isFinite(Number(v))) return "—";
    var n = Number(v);
    var abs = Math.abs(n);
    if (isAvgMetric(key) || (abs > 0 && abs < 10)) {
      return n.toLocaleString(undefined, { maximumFractionDigits: 2 });
    }
    if (abs >= 1000) {
      return n.toLocaleString(undefined, { maximumFractionDigits: 0 });
    }
    return n.toLocaleString(undefined, { maximumFractionDigits: 1 });
  }

  function buildLabelIndex() {
    state.labelByKey = {};
    var cat = $("sd-metric-catalog");
    if (!cat) return;
    Array.prototype.forEach.call(cat.options, function (opt) {
      if (opt.value) state.labelByKey[opt.value] = opt.textContent.trim();
    });
    var pack = global.SEO_XDATA_METRIC_OPTIONS || {};
    ["web", "mweb"].forEach(function (plat) {
      (pack[plat] || []).forEach(function (o) {
        if (o && o.value) state.labelByKey[o.value] = o.label || o.value;
      });
    });
  }

  function syncMetricCatalog() {
    var pack = global.SEO_XDATA_METRIC_OPTIONS || {};
    var opts = pack[state.platform] || pack.web || [];
    var group = $("sd-metric-optgroup");
    var cat = $("sd-metric-catalog");
    if (!group || !cat) return;
    group.innerHTML = "";
    opts.forEach(function (o) {
      if (!o || !o.value) return;
      var opt = document.createElement("option");
      opt.value = o.value;
      opt.textContent = o.label || o.value;
      group.appendChild(opt);
    });
    buildLabelIndex();
    renderMetricList();
    updateMetricTriggerLabel();
  }

  function selectedMetrics() {
    return state.selected.filter(Boolean);
  }

  function isLegendMuted(key) {
    return !!(state.legendMuted && state.legendMuted[key]);
  }

  function pruneLegendMuted(validKeys) {
    var ok = {};
    (validKeys || []).forEach(function (k) {
      ok[k] = true;
    });
    Object.keys(state.legendMuted || {}).forEach(function (k) {
      if (!ok[k]) delete state.legendMuted[k];
    });
  }

  function visibleSeriesList(seriesList) {
    return (seriesList || []).filter(function (s) {
      return s && !isLegendMuted(s.key);
    });
  }

  function toggleLegendMuted(key) {
    if (!key) return;
    var turningOff = !state.legendMuted[key];
    if (turningOff) {
      // Son görünür seriyi kapatma (iOS/Android legend davranışı)
      var preview = Object.assign({}, state.legendMuted);
      preview[key] = true;
      if (String(key).indexOf("ov:") !== 0 && String(key).indexOf(":prev") < 0) {
        preview[key + ":prev"] = true;
      }
      var all = collectChartSeries();
      var stillVisible = all.filter(function (s) {
        return s && !preview[s.key];
      });
      if (!stillVisible.length && all.length) return;
    }
    if (state.legendMuted[key]) delete state.legendMuted[key];
    else state.legendMuted[key] = true;
    // primary metrik kapanınca prev dönem çizgisi de aynı kalsın
    if (String(key).indexOf("ov:") !== 0 && String(key).indexOf(":prev") < 0) {
      var prevKey = key + ":prev";
      if (state.legendMuted[key]) state.legendMuted[prevKey] = true;
      else delete state.legendMuted[prevKey];
    }
    renderMetricKpis();
    renderChart();
    renderTable();
  }

  function bindLegendEvents() {
    var host = $("sd-legend");
    if (!host || _legendBound) return;
    _legendBound = true;
    host.addEventListener("click", function (ev) {
      var btn = ev.target && ev.target.closest ? ev.target.closest("[data-sd-legend-key]") : null;
      if (!btn || !host.contains(btn)) return;
      ev.preventDefault();
      toggleLegendMuted(btn.getAttribute("data-sd-legend-key"));
    });
  }

  function updateMetricTriggerLabel() {
    var el = $("sd-metric-label");
    if (!el) return;
    var keys = selectedMetrics();
    if (!keys.length) {
      el.textContent = "Select metrics";
      return;
    }
    if (keys.length === 1) {
      el.textContent = metricLabel(keys[0]);
      return;
    }
    el.textContent = keys.length + " metrics · " + metricLabel(keys[0]);
  }

  function renderMetricList() {
    var scroll = $("sd-metric-list-scroll");
    var cat = $("sd-metric-catalog");
    if (!scroll || !cat) return;
    var selected = {};
    state.selected.forEach(function (k) {
      selected[k] = true;
    });
    var html = "";
    html +=
      '<button type="button" role="option" aria-selected="' +
      (!state.selected.length ? "true" : "false") +
      '" data-sd-metric-clear="1" class="flex w-full items-center gap-2 px-2.5 py-1.5 text-left text-xs ' +
      (!state.selected.length
        ? "bg-emerald-50 font-semibold text-emerald-800 dark:bg-emerald-950/40 dark:text-emerald-100"
        : "font-medium text-slate-700 hover:bg-slate-100 dark:text-zinc-200 dark:hover:bg-zinc-900") +
      '">' +
      '<span class="inline-flex w-3.5 shrink-0 justify-center text-[12px] text-emerald-600 dark:text-emerald-400" aria-hidden="true">' +
      (!state.selected.length ? "✓" : "") +
      "</span>" +
      '<span class="min-w-0 flex-1 truncate">Clear</span></button>';

    Array.prototype.forEach.call(cat.options, function (opt) {
      if (!opt.value) return;
      var on = !!selected[opt.value];
      var selIdx = state.selected.indexOf(opt.value);
      var swatch =
        on && selIdx >= 0
          ? '<span class="sd-metric-swatch" style="background:' +
            COLORS[selIdx % COLORS.length] +
            '" aria-hidden="true"></span>'
          : '<span class="sd-metric-swatch" style="background:transparent" aria-hidden="true"></span>';
      html +=
        '<button type="button" role="option" aria-selected="' +
        (on ? "true" : "false") +
        '" data-sd-metric-pick="' +
        esc(opt.value) +
        '" class="flex w-full items-center gap-2 px-2.5 py-1.5 text-left text-xs ' +
        (on
          ? "font-semibold text-emerald-800 dark:text-emerald-100"
          : "font-medium text-slate-700 hover:bg-slate-100 dark:text-zinc-200 dark:hover:bg-zinc-900") +
        '">' +
        '<span class="inline-flex w-3.5 shrink-0 justify-center text-[12px] text-emerald-600 dark:text-emerald-400" aria-hidden="true">' +
        (on ? "✓" : "") +
        "</span>" +
        swatch +
        '<span class="min-w-0 flex-1 truncate">' +
        esc(opt.textContent || opt.value) +
        "</span></button>";
    });
    scroll.innerHTML = html;
  }

  var _reloadTimer = 0;
  var _tableRemoveBound = false;
  var TH_REMOVE_CLS =
    "inline-flex h-3.5 w-3.5 shrink-0 items-center justify-center rounded text-[10px] leading-none text-slate-400 hover:bg-rose-100 hover:text-rose-600 dark:text-zinc-500 dark:hover:bg-rose-950/50 dark:hover:text-rose-300";

  function scheduleReload() {
    if (_reloadTimer) global.clearTimeout(_reloadTimer);
    _reloadTimer = global.setTimeout(function () {
      _reloadTimer = 0;
      runLoad();
    }, 60);
  }

  function uncheckOverlayMetric(metricKey) {
    if (!metricKey) return false;
    var root = $(OVERLAY_ID);
    if (!root) return false;
    var panel =
      root.querySelector("[data-play-metric-overlay-panel]") ||
      document.querySelector('[data-play-metric-overlay-for="' + OVERLAY_ID + '"]');
    if (!panel) return false;
    var cb = panel.querySelector('input[type="checkbox"][value="' + metricKey.replace(/"/g, "") + '"]');
    if (!cb || !cb.checked) return false;
    cb.checked = false;
    cb.dispatchEvent(new Event("change", { bubbles: true }));
    return true;
  }

  function removeSeriesFromUi(colKey, opts) {
    opts = opts || {};
    if (opts.overlay || String(colKey || "").indexOf("ov:") === 0) {
      var ovMetric = opts.overlayMetric || String(colKey || "").replace(/^ov:/, "");
      if (uncheckOverlayMetric(ovMetric)) return;
      // checkbox bulunamazsa state'ten düş
      state.overlaySeries = (state.overlaySeries || []).filter(function (ov) {
        return String(ov.metric || "") !== String(ovMetric);
      });
      renderMetricKpis();
      renderChart();
      renderTable();
      return;
    }
    var key = opts.metric || colKey;
    var idx = state.selected.indexOf(key);
    if (idx < 0) return;
    if (state.selected.length <= 1) {
      setStatus("Keep at least one metric.");
      return;
    }
    state.selected.splice(idx, 1);
    delete state.seriesByMetric[key];
    delete state.compareSeries[key];
    renderMetricList();
    updateMetricTriggerLabel();
    scheduleReload();
  }

  function metricRemoveButtonHtml(col) {
    if (!col) return "";
    var isOv = !!col.overlay;
    var metric = isOv ? col.overlayMetric || String(col.key || "").replace(/^ov:/, "") : col.metric || col.key;
    return (
      '<button type="button" class="' +
      TH_REMOVE_CLS +
      '" data-sd-col-remove="1" data-sd-remove-kind="' +
      (isOv ? "overlay" : "metric") +
      '" data-sd-remove-key="' +
      esc(col.key || "") +
      '" data-sd-remove-metric="' +
      esc(metric || "") +
      '" title="Remove" aria-label="' +
      esc((col.shortLabel || col.label || col.key || "metric") + " remove") +
      '">×</button>'
    );
  }

  function bindTableRemove() {
    var shell = $("sd-table-shell");
    if (!shell || _tableRemoveBound) return;
    _tableRemoveBound = true;
    shell.addEventListener("click", function (ev) {
      var btn = ev.target && ev.target.closest ? ev.target.closest("[data-sd-col-remove]") : null;
      if (!btn || !shell.contains(btn)) return;
      ev.preventDefault();
      ev.stopPropagation();
      removeSeriesFromUi(btn.getAttribute("data-sd-remove-key"), {
        overlay: btn.getAttribute("data-sd-remove-kind") === "overlay",
        overlayMetric: btn.getAttribute("data-sd-remove-metric") || "",
        metric: btn.getAttribute("data-sd-remove-metric") || "",
      });
    });
  }

  function toggleMetric(key) {
    if (!key) return;
    var idx = state.selected.indexOf(key);
    if (idx >= 0) {
      if (state.selected.length <= 1) {
        setStatus("Keep at least one metric.");
        return;
      }
      state.selected.splice(idx, 1);
      delete state.seriesByMetric[key];
      delete state.compareSeries[key];
    } else {
      state.selected.push(key);
    }
    renderMetricList();
    updateMetricTriggerLabel();
    scheduleReload();
  }

  function clearMetrics() {
    state.selected = [DEFAULT_METRICS[0]];
    Object.keys(state.seriesByMetric || {}).forEach(function (k) {
      if (state.selected.indexOf(k) < 0) delete state.seriesByMetric[k];
    });
    Object.keys(state.compareSeries || {}).forEach(function (k) {
      if (state.selected.indexOf(k) < 0) delete state.compareSeries[k];
    });
    renderMetricList();
    updateMetricTriggerLabel();
    scheduleReload();
  }

  function positionMetricList() {
    var trigger = $("sd-metric-trigger");
    var list = $("sd-metric-list");
    if (!trigger || !list || list.classList.contains("hidden")) return;
    var r = trigger.getBoundingClientRect();
    list.style.position = "fixed";
    list.style.left = Math.max(8, r.left) + "px";
    list.style.top = r.bottom + 4 + "px";
    list.style.width = Math.max(r.width, 220) + "px";
    list.style.zIndex = "10050";
  }

  function openMetricList() {
    var trigger = $("sd-metric-trigger");
    var list = $("sd-metric-list");
    if (!trigger || !list) return;
    if (list.parentElement !== document.body) {
      document.body.appendChild(list);
      list.classList.add("sd-metric-list--portal");
    }
    list.classList.remove("hidden");
    trigger.setAttribute("aria-expanded", "true");
    positionMetricList();
  }

  function closeMetricList() {
    var trigger = $("sd-metric-trigger");
    var list = $("sd-metric-list");
    var dd = document.querySelector(".sd-metric-dd");
    if (!list) return;
    list.classList.add("hidden");
    if (dd && list.parentElement === document.body) {
      dd.appendChild(list);
      list.classList.remove("sd-metric-list--portal");
    }
    if (trigger) trigger.setAttribute("aria-expanded", "false");
  }

  function applyPreset(preset) {
    if (preset === "custom") return;
    var end = today();
    var start = end;
    if (preset === "ytd") {
      start = new Date(end.getFullYear(), 0, 1);
    } else {
      var days = parseInt(preset, 10) || 28;
      // Complete days: end = yesterday (align with Android)
      end = addDays(today(), -1);
      start = addDays(end, -(days - 1));
    }
    var s = $("sd-start");
    var e = $("sd-end");
    if (s) s.value = iso(start);
    if (e) e.value = iso(end);
  }

  function rangeDays() {
    var s = parseIso(($("sd-start") || {}).value);
    var e = parseIso(($("sd-end") || {}).value);
    if (!s || !e || e < s) return 0;
    return Math.round((e - s) / 86400000) + 1;
  }

  function previousRange() {
    var s = parseIso(($("sd-start") || {}).value);
    var e = parseIso(($("sd-end") || {}).value);
    if (!s || !e) return null;
    var days = rangeDays();
    var pe = addDays(s, -1);
    var ps = addDays(pe, -(days - 1));
    return { start: iso(ps), end: iso(pe) };
  }

  function setLoading(on) {
    state.loading = !!on;
    var el = $("sd-loading");
    var btn = $("sd-run");
    if (el) {
      el.classList.toggle("hidden", !on);
      el.setAttribute("aria-busy", on ? "true" : "false");
    }
    if (btn) btn.disabled = !!on;
  }

  function setStatus(msg) {
    var el = $("sd-status");
    if (el) el.textContent = msg || "";
  }

  async function fetchSeries(metric, platform, start, end) {
    var qs = new URLSearchParams({
      project: PROJECT,
      platform: platform,
      metric: metric,
      start: start || "",
      end: end || "",
    });
    var r = await fetch("/api/empower-intel/series?" + qs.toString(), {
      credentials: "same-origin",
      cache: "no-store",
    });
    var data = await r.json().catch(function () {
      return {};
    });
    if (!r.ok) throw new Error(data.detail || data.message || "HTTP " + r.status);
    return {
      label: data.label || metricLabel(metric),
      series: data.series || [],
      metric: metric,
      platform: platform,
    };
  }

  function valueMap(series) {
    var m = {};
    (series || []).forEach(function (pt) {
      if (!pt || pt.key == null) return;
      var k = String(pt.key).slice(0, 10);
      m[k] = Number(pt.value);
    });
    return m;
  }

  function unionDates(maps) {
    var set = {};
    maps.forEach(function (m) {
      Object.keys(m || {}).forEach(function (k) {
        set[k] = 1;
      });
    });
    return Object.keys(set).sort();
  }

  function syncChartLayout() {
    if (typeof global.paSyncChartLayout === "function") {
      global.paSyncChartLayout();
    }
  }

  function syncToggleGroup(root, attr, value) {
    if (!root) return;
    Array.prototype.forEach.call(root.querySelectorAll("[" + attr + "]"), function (btn) {
      var on = btn.getAttribute(attr) === value;
      btn.classList.toggle("is-active", on);
      btn.setAttribute("aria-pressed", on ? "true" : "false");
    });
  }

  function syncChartStyleUi() {
    var root = $("sd-chart-style");
    if (!root) return;
    Array.prototype.forEach.call(root.querySelectorAll("[data-sd-chart-style]"), function (btn) {
      var on = btn.getAttribute("data-sd-chart-style") === state.chartStyle;
      btn.classList.toggle("is-active", on);
      btn.setAttribute("aria-pressed", on ? "true" : "false");
    });
  }

  function syncPlatformUi() {
    var root = $("sd-platform-toggle");
    if (!root) return;
    Array.prototype.forEach.call(root.querySelectorAll("[data-sd-platform]"), function (btn) {
      btn.classList.toggle("is-active", btn.getAttribute("data-sd-platform") === state.platform);
    });
    var page = $("sd-datas-root");
    if (page) page.setAttribute("data-platform", state.platform);
  }

  function updateOverlayPeer() {
    var peer = peerPlatform(state.platform);
    if (!global.PlayMetricOverlay) return;
    PlayMetricOverlay.setPlatform(OVERLAY_ID, peer);
    PlayMetricOverlay.setLabelPrefix(OVERLAY_ID, peerLabel(state.platform));
  }

  function seriesStats(series) {
    var vals = [];
    (series || []).forEach(function (r) {
      if (!r || r.value == null) return;
      var n = Number(r.value);
      if (!isFinite(n)) return;
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

  function seriesDeltaPct(series) {
    var vals = [];
    (series || []).forEach(function (r) {
      var n = Number(r && r.value);
      if (isFinite(n)) vals.push(n);
    });
    if (vals.length < 2 || !vals[0]) return null;
    return Math.round(((vals[vals.length - 1] - vals[0]) / Math.abs(vals[0])) * 1000) / 10;
  }

  function seriesSparkSvg(series, color, w, h) {
    var vals = [];
    (series || []).forEach(function (r) {
      var n = Number(r && r.value);
      if (isFinite(n)) vals.push(n);
    });
    if (vals.length < 2) {
      return '<div class="metric-kpi-spark" style="opacity:.35" aria-hidden="true"></div>';
    }
    var min = Math.min.apply(null, vals);
    var max = Math.max.apply(null, vals);
    var span = max - min || 1;
    var pts = vals.map(function (v, i) {
      var x = (i / (vals.length - 1)) * w;
      var y = h - ((v - min) / span) * (h - 4) - 2;
      return [x, y];
    });
    var line = pts
      .map(function (p) {
        return p[0].toFixed(1) + "," + p[1].toFixed(1);
      })
      .join(" ");
    var area =
      "M" +
      pts[0][0].toFixed(1) +
      " " +
      h.toFixed(1) +
      " L" +
      pts
        .map(function (p) {
          return p[0].toFixed(1) + " " + p[1].toFixed(1);
        })
        .join(" L") +
      " L" +
      pts[pts.length - 1][0].toFixed(1) +
      " " +
      h.toFixed(1) +
      " Z";
    _kpiSparkGradSeq += 1;
    var gid = "sd-kpi-spark-grad-" + _kpiSparkGradSeq;
    return (
      '<svg class="metric-kpi-spark" viewBox="0 0 ' +
      w +
      " " +
      h +
      '" preserveAspectRatio="none" aria-hidden="true">' +
      "<defs>" +
      '<linearGradient id="' +
      gid +
      '" x1="0" y1="0" x2="0" y2="1">' +
      '<stop offset="0%" stop-color="' +
      color +
      '" stop-opacity="0.38"></stop>' +
      '<stop offset="55%" stop-color="' +
      color +
      '" stop-opacity="0.14"></stop>' +
      '<stop offset="100%" stop-color="' +
      color +
      '" stop-opacity="0.02"></stop>' +
      "</linearGradient>" +
      "</defs>" +
      '<path d="' +
      area +
      '" fill="url(#' +
      gid +
      ')"></path>' +
      '<polyline points="' +
      line +
      '" fill="none" stroke="' +
      color +
      '" stroke-width="1.35" stroke-linecap="round" stroke-linejoin="round" vector-effect="non-scaling-stroke"></polyline>' +
      "</svg>"
    );
  }

  function applyKpiLayout(el, n) {
    if (!el) return;
    el.className = "metric-kpi-grid metric-kpi-grid--ss2";
    el.style.removeProperty("grid-template-columns");
    var cols = n >= 9 ? Math.ceil(n / 2) : Math.max(n, 1);
    el.style.setProperty("--kpi-n", String(Math.max(cols, 1)));
  }

  function fitOneKpiText(el, minPx, maxPx) {
    if (!el) return;
    var avail = el.clientWidth;
    if (avail < 8) return;
    var lo = minPx;
    var hi = Math.max(minPx, maxPx);
    var best = minPx;
    el.style.whiteSpace = "nowrap";
    el.style.overflow = "hidden";
    el.style.textOverflow = "ellipsis";
    for (var i = 0; i < 14; i++) {
      var mid = (lo + hi) / 2;
      el.style.fontSize = mid + "px";
      if (el.scrollWidth <= avail + 0.75) {
        best = mid;
        lo = mid;
      } else {
        hi = mid;
      }
    }
    el.style.fontSize = best + "px";
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
      fitOneKpiText(dlt, Math.max(7, w * 0.028), Math.min(9, Math.max(7, availVal * 0.08)));
      card.querySelectorAll(".metric-kpi-ss2-summary strong").forEach(function (el) {
        fitOneKpiText(el, 8, Math.min(12, Math.max(9, w * 0.055)));
      });
    });
  }

  function unbindMetricKpiFit() {
    if (_kpiFitRaf) {
      cancelAnimationFrame(_kpiFitRaf);
      _kpiFitRaf = 0;
    }
    if (_kpiFitObs) {
      _kpiFitObs.disconnect();
      _kpiFitObs = null;
    }
  }

  function bindMetricKpiFit(root) {
    unbindMetricKpiFit();
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

  function setChartSeriesFocus(metric) {
    state.focusedMetric = metric || null;
    applyChartSeriesFocus();
  }

  function applyChartSeriesFocus() {
    var chart = $("sd-chart");
    var host = $("sd-metric-kpis");
    var metric = state.focusedMetric;
    if (chart) {
      var groups = chart.querySelectorAll(".chart-series-g");
      var has = false;
      groups.forEach(function (g) {
        var on = !!metric && g.getAttribute("data-metric") === metric;
        g.classList.toggle("is-focus", on);
        if (on) has = true;
      });
      chart.classList.toggle("is-series-focus", has);
      if (has) {
        var focused = chart.querySelector(".chart-series-g.is-focus");
        var hit = chart.querySelector("[data-sd-idx]");
        if (focused) {
          if (hit) chart.insertBefore(focused, hit);
          else chart.appendChild(focused);
        }
      }
    }
    if (host) {
      host.querySelectorAll("[data-metric-kpi-card]").forEach(function (c) {
        c.classList.toggle("is-chart-focus", !!metric && c.getAttribute("data-metric") === metric);
      });
    }
  }

  function wrapChartSeriesG(metric, innerHtml) {
    if (!innerHtml) return "";
    return (
      '<g class="chart-series-g" data-metric="' + esc(metric || "") + '">' + innerHtml + "</g>"
    );
  }

  function renderMetricKpis() {
    var host = $("sd-metric-kpis");
    if (!host) return;
    var keys = selectedMetrics().filter(function (key) {
      return !isLegendMuted(key);
    });
    if (!keys.length) {
      unbindMetricKpiFit();
      host.innerHTML = "";
      host.style.removeProperty("--kpi-n");
      return;
    }
    applyKpiLayout(host, keys.length);
    host.innerHTML = keys
      .map(function (key, i) {
        var pack = state.seriesByMetric[key] || { series: [], label: metricLabel(key), metric: key };
        var color = COLORS[i % COLORS.length];
        var st = seriesStats(pack.series);
        var avg = st.avg;
        var avgTxt = fmtNum(avg, key);
        var totalVal = isAvgMetric(key) ? avgTxt : fmtNum(st.sum, key);
        var label = pack.label || metricLabel(key);
        var spark = seriesSparkSvg(pack.series, color, 220, 56);
        var dlt = seriesDeltaPct(pack.series);
        var dltCls = dlt == null ? "is-flat" : dlt >= 0 ? "is-up" : "is-down";
        var dltTxt = dlt == null ? "—" : (dlt >= 0 ? "↑ " : "↓ ") + Math.abs(dlt).toFixed(1) + "%";
        var tip = label + " · average + total over selected range";
        return (
          '<article class="metric-kpi-ss2" style="--kpi-color:' +
          color +
          '" data-metric-kpi-card data-metric="' +
          esc(key) +
          '">' +
          '<div class="metric-kpi-ss2-head">' +
          '<span class="metric-kpi-chip" title="' +
          esc(label) +
          '">' +
          esc(label) +
          "</span>" +
          '<button type="button" class="metric-kpi-info" data-sd-metric-info aria-label="' +
          esc(label) +
          ' info">i<span class="metric-kpi-tip" role="tooltip">' +
          esc(tip) +
          "</span></button>" +
          "</div>" +
          '<div class="metric-kpi-ss2-main">' +
          '<div class="metric-kpi-ss2-metrics">' +
          '<p class="metric-kpi-ss2-value" title="Average: ' +
          esc(avgTxt) +
          '">' +
          esc(avgTxt) +
          "</p>" +
          '<p class="metric-kpi-ss2-delta ' +
          dltCls +
          '">' +
          esc(dltTxt) +
          "</p>" +
          "</div>" +
          '<div class="metric-kpi-ss2-spark">' +
          spark +
          "</div>" +
          "</div>" +
          '<div class="metric-kpi-ss2-summary" aria-label="Average and total">' +
          '<div><p class="metric-kpi-kicker">Average</p><strong title="' +
          esc(avgTxt) +
          '">' +
          esc(avgTxt) +
          "</strong></div>" +
          '<div><p class="metric-kpi-kicker">Total</p><strong title="' +
          esc(totalVal) +
          '">' +
          esc(totalVal) +
          "</strong></div>" +
          "</div>" +
          '<div class="metric-kpi-ss2-panel">' +
          '<div><p class="metric-kpi-kicker">Total</p><strong>' +
          esc(totalVal) +
          "</strong></div>" +
          '<div><p class="metric-kpi-kicker">Average</p><strong>' +
          esc(avgTxt) +
          "</strong></div>" +
          '<div><p class="metric-kpi-kicker">Min</p><strong>' +
          esc(fmtNum(st.min, key)) +
          "</strong></div>" +
          '<div><p class="metric-kpi-kicker">Max</p><strong>' +
          esc(fmtNum(st.max, key)) +
          "</strong></div>" +
          '<div><p class="metric-kpi-kicker">Last</p><strong>' +
          esc(fmtNum(st.last, key)) +
          "</strong></div>" +
          '<div><p class="metric-kpi-kicker">Points</p><strong>' +
          esc(String(st.n || 0)) +
          "</strong></div>" +
          "</div>" +
          '<button type="button" class="metric-kpi-ss2-expand" data-metric-kpi-expand aria-expanded="false" aria-label="Open details">' +
          '<svg class="metric-kpi-chev" width="12" height="12" viewBox="0 0 12 12" fill="none" aria-hidden="true"><path d="M3 4.5 L6 7.5 L9 4.5" stroke="currentColor" stroke-width="1.5" stroke-linecap="round" stroke-linejoin="round"/></svg>' +
          "</button>" +
          "</article>"
        );
      })
      .join("");
    bindMetricKpiFit(host);
    applyChartSeriesFocus();
  }

  function bindMetricKpiEvents() {
    var host = $("sd-metric-kpis");
    if (!host || _kpiEventsBound) return;
    _kpiEventsBound = true;
    host.addEventListener("pointerover", function (ev) {
      var card = ev.target && ev.target.closest ? ev.target.closest("[data-metric-kpi-card]") : null;
      if (!card || !host.contains(card)) return;
      if (ev.relatedTarget && card.contains(ev.relatedTarget)) return;
      setChartSeriesFocus(card.getAttribute("data-metric"));
    });
    host.addEventListener("pointerout", function (ev) {
      var card = ev.target && ev.target.closest ? ev.target.closest("[data-metric-kpi-card]") : null;
      if (!card || !host.contains(card)) return;
      var to =
        ev.relatedTarget && ev.relatedTarget.closest
          ? ev.relatedTarget.closest("[data-metric-kpi-card]")
          : null;
      if (to && host.contains(to)) {
        if (to !== card) setChartSeriesFocus(to.getAttribute("data-metric"));
        return;
      }
      setChartSeriesFocus(null);
    });
    host.addEventListener("click", function (ev) {
      if (ev.target && ev.target.closest && ev.target.closest("[data-sd-metric-info]")) {
        ev.preventDefault();
        ev.stopPropagation();
        return;
      }
      var expand = ev.target && ev.target.closest ? ev.target.closest("[data-metric-kpi-expand]") : null;
      if (!expand) return;
      ev.preventDefault();
      ev.stopPropagation();
      var card = expand.closest("[data-metric-kpi-card]");
      if (!card || !host.contains(card)) return;
      var open = !card.classList.contains("is-open");
      host.querySelectorAll("[data-metric-kpi-card].is-open").forEach(function (other) {
        if (other === card) return;
        other.classList.remove("is-open");
        var btn = other.querySelector("[data-metric-kpi-expand]");
        if (btn) {
          btn.setAttribute("aria-expanded", "false");
          btn.setAttribute("aria-label", "Open details");
        }
      });
      card.classList.toggle("is-open", open);
      expand.setAttribute("aria-expanded", open ? "true" : "false");
      expand.setAttribute("aria-label", open ? "Close details" : "Open details");
    });
  }

  function collectChartSeries() {
    var out = [];
    selectedMetrics().forEach(function (key, i) {
      var pack = state.seriesByMetric[key];
      if (!pack) return;
      out.push({
        key: key,
        label: pack.label || metricLabel(key),
        map: valueMap(pack.series),
        color: COLORS[i % COLORS.length],
        overlay: false,
        dashed: false,
      });
      if (state.compareSeries[key]) {
        out.push({
          key: key + ":prev",
          label: (pack.label || metricLabel(key)) + " · prev",
          map: valueMap(state.compareSeries[key].series),
          color: COLORS[i % COLORS.length],
          overlay: false,
          dashed: true,
        });
      }
    });
    (state.overlaySeries || []).forEach(function (ov, i) {
      out.push({
        key: "ov:" + (ov.metric || i),
        label: (ov.label || ov.metric || "Overlay") + " · " + platformLabel(ov.platform || peerPlatform(state.platform)),
        map: valueMap(ov.series),
        color: OVERLAY_COLORS[i % OVERLAY_COLORS.length],
        overlay: true,
        overlayMetric: ov.metric || "",
        dashed: false,
      });
    });
    return out;
  }

  function renderLegend(seriesList) {
    var host = $("sd-legend");
    if (!host) return;
    if (!seriesList.length) {
      host.innerHTML = '<span class="text-slate-400">No series</span>';
      return;
    }
    host.innerHTML = seriesList
      .map(function (s) {
        var muted = isLegendMuted(s.key);
        var color = muted ? "#94a3b8" : s.color;
        var mark = s.dashed
          ? '<span class="inline-block h-0.5 w-3 border-t-2 border-dashed" style="border-color:' +
            color +
            '"></span>'
          : '<span class="h-2 w-2 rounded-full' +
            (muted ? " ring-1 ring-slate-300 dark:ring-zinc-600" : "") +
            '" style="background:' +
            color +
            '"></span>';
        return (
          '<button type="button" data-sd-legend-key="' +
          esc(s.key) +
          '" aria-pressed="' +
          (muted ? "false" : "true") +
          '" class="sd-legend-item cursor-pointer inline-flex items-center gap-1.5 rounded-md px-1 py-0.5 transition focus:outline-none focus-visible:ring-2 focus-visible:ring-slate-300 dark:focus-visible:ring-zinc-600 ' +
          (muted
            ? "is-off opacity-45 text-slate-400 hover:opacity-70 hover:text-slate-600 dark:text-zinc-500 dark:hover:text-zinc-300"
            : "hover:bg-slate-100 hover:text-slate-800 dark:hover:bg-zinc-800 dark:hover:text-zinc-100") +
          '" title="' +
          esc(s.label) +
          (muted ? " — show on chart" : " — hide from chart") +
          '">' +
          mark +
          '<span class="' +
          (muted ? "line-through decoration-slate-300 dark:decoration-zinc-600" : "") +
          '">' +
          esc(s.label) +
          "</span></button>"
        );
      })
      .join("");
  }

  function niceMax(v) {
    if (!isFinite(v) || v <= 0) return 1;
    var exp = Math.pow(10, Math.floor(Math.log10(v)));
    var n = v / exp;
    var nice = n <= 1 ? 1 : n <= 2 ? 2 : n <= 5 ? 5 : 10;
    return nice * exp;
  }

  function renderChart() {
    var svg = $("sd-chart");
    var tip = $("sd-tooltip");
    if (!svg) return;
    var seriesAll = collectChartSeries();
    pruneLegendMuted(
      seriesAll.map(function (s) {
        return s.key;
      })
    );
    renderLegend(seriesAll);
    var seriesList = visibleSeriesList(seriesAll);
    var maps = seriesList.map(function (s) {
      return s.map;
    });
    var dates = unionDates(maps);
    state.dates = dates;
    var h = CHART_VIEW_H;
    var w = 720;
    var padL = 48;
    var padR = 12;
    var padT = 12;
    var padB = 28;
    var plotW = w - padL - padR;
    var plotH = h - padT - padB;
    svg.setAttribute("viewBox", "0 0 " + w + " " + h);

    if (!dates.length || !seriesList.length) {
      var emptyMsg =
        seriesAll.length && !seriesList.length
          ? "Turn on at least one metric in the legend"
          : "No data for this range";
      svg.innerHTML =
        '<text class="pa-axis-label" x="50%" y="50%" text-anchor="middle" fill="#94a3b8" font-size="' +
        AXIS_LABEL_FONT +
        '">' +
        emptyMsg +
        "</text>";
      if (tip) tip.classList.add("hidden");
      syncChartLayout();
      return;
    }

    var allVals = [];
    seriesList.forEach(function (s) {
      dates.forEach(function (d) {
        var v = s.map[d];
        if (v != null && isFinite(v)) allVals.push(v);
      });
    });
    var yMax = niceMax(Math.max.apply(null, allVals.concat([0])));
    var yMin = 0;
    if (allVals.length && Math.min.apply(null, allVals) < 0) {
      yMin = Math.min.apply(null, allVals);
    }

    function xAt(i) {
      if (dates.length === 1) return padL + plotW / 2;
      return padL + (i / (dates.length - 1)) * plotW;
    }
    function yAt(v) {
      var span = yMax - yMin || 1;
      return padT + plotH - ((v - yMin) / span) * plotH;
    }

    var grid = "";
    for (var g = 0; g <= 4; g++) {
      var gy = padT + (plotH * g) / 4;
      var gv = yMax - ((yMax - yMin) * g) / 4;
      grid +=
        '<line x1="' +
        padL +
        '" y1="' +
        gy +
        '" x2="' +
        (w - padR) +
        '" y2="' +
        gy +
        '" stroke="rgba(148,163,184,0.25)" stroke-width="1"/>';
      grid +=
        '<text class="pa-axis-label pa-axis-label--y" x="' +
        (padL - 6) +
        '" y="' +
        (gy + 3) +
        '" text-anchor="end" fill="#94a3b8" font-size="' +
        AXIS_LABEL_FONT +
        '">' +
        fmtNum(gv, "") +
        "</text>";
    }

    var xLabels = "";
    var labelStep = Math.max(1, Math.ceil(dates.length / 6));
    dates.forEach(function (d, i) {
      if (i % labelStep !== 0 && i !== dates.length - 1) return;
      xLabels +=
        '<text class="pa-axis-label pa-axis-label--x" x="' +
        xAt(i) +
        '" y="' +
        (h - 8) +
        '" text-anchor="middle" fill="#94a3b8" font-size="' +
        AXIS_LABEL_FONT +
        '">' +
        d.slice(5) +
        "</text>";
    });

    var paths = "";
    seriesList.forEach(function (s) {
      var pts = [];
      dates.forEach(function (d, i) {
        var v = s.map[d];
        if (v == null || !isFinite(v)) {
          pts.push(null);
          return;
        }
        pts.push({ x: xAt(i), y: yAt(v), v: v, d: d });
      });
      var inner = "";
      if (state.chartStyle === "bar" && !s.dashed) {
        var barW = Math.max(2, (plotW / Math.max(dates.length, 1)) * 0.55);
        pts.forEach(function (p) {
          if (!p) return;
          inner +=
            '<rect x="' +
            (p.x - barW / 2) +
            '" y="' +
            p.y +
            '" width="' +
            barW +
            '" height="' +
            Math.max(0, yAt(0) - p.y) +
            '" fill="' +
            s.color +
            '" fill-opacity="' +
            (s.overlay ? "0.45" : "0.7") +
            '"/>';
        });
        paths += wrapChartSeriesG(s.key, inner);
        return;
      }
      var dPath = "";
      var areaD = "";
      var started = false;
      pts.forEach(function (p) {
        if (!p) {
          started = false;
          return;
        }
        dPath += (started ? " L " : "M ") + p.x.toFixed(1) + " " + p.y.toFixed(1);
        if (!started) areaD = "M " + p.x.toFixed(1) + " " + yAt(0).toFixed(1) + " L " + p.x.toFixed(1) + " " + p.y.toFixed(1);
        else areaD += " L " + p.x.toFixed(1) + " " + p.y.toFixed(1);
        started = true;
      });
      var lastPt = null;
      for (var k = pts.length - 1; k >= 0; k--) {
        if (pts[k]) {
          lastPt = pts[k];
          break;
        }
      }
      if (state.chartStyle === "area" && !s.dashed && lastPt && areaD) {
        areaD += " L " + lastPt.x.toFixed(1) + " " + yAt(0).toFixed(1) + " Z";
        inner +=
          '<path d="' +
          areaD +
          '" fill="' +
          s.color +
          '" fill-opacity="' +
          (s.overlay ? "0.14" : "0.18") +
          '"/>';
      }
      if (dPath) {
        inner +=
          '<path d="' +
          dPath +
          '" fill="none" stroke="' +
          s.color +
          '" stroke-width="' +
          (s.overlay ? "1.6" : "2") +
          '"' +
          (s.dashed ? ' stroke-dasharray="4 3"' : "") +
          ' stroke-linecap="round" stroke-linejoin="round"/>';
      }
      paths += wrapChartSeriesG(s.key, inner);
    });

    var hit = "";
    dates.forEach(function (d, i) {
      hit +=
        '<rect data-sd-idx="' +
        i +
        '" x="' +
        (xAt(i) - plotW / Math.max(dates.length, 1) / 2) +
        '" y="' +
        padT +
        '" width="' +
        Math.max(4, plotW / Math.max(dates.length, 1)) +
        '" height="' +
        plotH +
        '" fill="transparent"/>';
    });

    svg.innerHTML = grid + xLabels + paths + hit;
    applyChartSeriesFocus();
    syncChartLayout();

    svg.querySelectorAll("[data-sd-idx]").forEach(function (el) {
      el.addEventListener("mousemove", function (ev) {
        var idx = Number(el.getAttribute("data-sd-idx"));
        showTip(ev, idx, seriesList, dates);
      });
      el.addEventListener("mouseleave", function () {
        if (tip) tip.classList.add("hidden");
      });
    });
  }

  function showTip(ev, idx, seriesList, dates) {
    var tip = $("sd-tooltip");
    var title = $("sd-tip-title");
    var body = $("sd-tip-body");
    var wrap = $("sd-chart-wrap");
    if (!tip || !title || !body || !wrap || !dates[idx]) return;
    title.textContent = dates[idx];
    body.innerHTML = seriesList
      .filter(function (s) {
        return !isLegendMuted(s.key);
      })
      .map(function (s) {
        var v = s.map[dates[idx]];
        return (
          '<div class="flex items-center justify-between gap-4">' +
          '<span class="inline-flex items-center gap-1.5"><span class="h-2 w-2 rounded-full" style="background:' +
          s.color +
          '"></span>' +
          s.label +
          "</span>" +
          '<strong class="tabular-nums">' +
          fmtNum(v, s.key) +
          "</strong></div>"
        );
      })
      .join("");
    tip.classList.remove("hidden");
    var rect = wrap.getBoundingClientRect();
    var x = ev.clientX - rect.left + 12;
    var y = ev.clientY - rect.top + 12;
    if (x + tip.offsetWidth > rect.width - 8) x = Math.max(8, rect.width - tip.offsetWidth - 8);
    if (y + tip.offsetHeight > rect.height - 8) y = Math.max(8, rect.height - tip.offsetHeight - 8);
    tip.style.left = x + "px";
    tip.style.top = y + "px";
  }

  function renderTable() {
    var thead = $("sd-thead-row");
    var tbody = $("sd-table");
    var shell = $("sd-table-shell");
    if (!thead || !tbody) return;
    var seriesList = collectChartSeries().filter(function (s) {
      return !s.dashed && !isLegendMuted(s.key);
    });
    var maps = seriesList.map(function (s) {
      return s.map;
    });
    var keys = unionDates(maps);
    state.dates = keys;

    var ux = global.SeoMetricTableUx;
    if (ux && typeof ux.renderHeatGrid === "function") {
      var colItems = seriesList.map(function (s) {
        return {
          key: s.key,
          label: s.label,
          shortLabel: shortMetricLabel(s.label, s.key),
          color: s.color,
          map: s.map,
          metric: s.overlay ? s.overlayMetric || s.key : s.key,
          overlay: !!s.overlay,
          overlayMetric: s.overlayMetric || "",
        };
      });
      if (typeof ux.orderKeys === "function") {
        var preferred = typeof ux.readJson === "function" ? ux.readJson("sd-table-col-order", []) : [];
        var orderedKeys = ux.orderKeys(
          preferred,
          colItems.map(function (c) {
            return c.key;
          })
        );
        var byKey = {};
        colItems.forEach(function (c) {
          byKey[c.key] = c;
        });
        colItems = orderedKeys
          .map(function (k) {
            return byKey[k];
          })
          .filter(Boolean);
      }
      var tableEl = tbody.closest("table");
      var fmtKey =
        ux.formatTableDateKey ||
        function (k) {
          return k;
        };
      var pinOn = typeof ux.isPinEnabled === "function" ? ux.isPinEnabled() : false;
      var stickyTop = pinOn ? " mtux-sticky-top" : "";
      ux.renderHeatGrid({
        shell: shell,
        tableEl: tableEl,
        theadRow: thead,
        tbody: tbody,
        keys: keys,
        colItems: colItems,
        esc: esc,
        fmtKey: fmtKey,
        fmtVal: function (v, col) {
          return fmtNum(v, (col && (col.metric || col.key)) || "");
        },
        breakdownLabel: "Date",
        averageLabel: "Average",
        showTotal: true,
        totalLabel: "Total",
        computeTotal: function (map, dayKeys, col) {
          var metric = (col && (col.metric || col.key)) || "";
          var sum = 0;
          var n = 0;
          (dayKeys || []).forEach(function (k) {
            var v = map[k];
            if (v == null || !Number.isFinite(v)) return;
            sum += v;
            n += 1;
          });
          if (!n) return null;
          return isAvgMetric(metric) ? sum / n : sum;
        },
        onRefresh: renderTable,
        bindInteractive: {
          widthsKey: "sd-table-col-widths",
          orderKey: "sd-table-col-order",
          onOrderChange: renderTable,
        },
        renderStandardHeaderCell: function (col) {
          return (
            '<th class="mtux-th px-1 py-2 font-bold tabular-nums sm:px-1.5' +
            stickyTop +
            '" data-mtux-key="' +
            esc(col.key) +
            '" style="color:' +
            esc(col.color || "#2563eb") +
            '" title="' +
            esc(col.label || "") +
            '">' +
            '<span class="mtux-th-label">' +
            '<span class="mtux-th-text">' +
            esc(col.shortLabel || col.label || col.key) +
            "</span>" +
            metricRemoveButtonHtml(col) +
            "</span></th>"
          );
        },
        renderTransposedMetricLabel: function (col) {
          return (
            '<span class="mtux-metric-row-label">' +
            '<span class="mtux-metric-dot" style="background:' +
            esc(col.color || "#2563eb") +
            '"></span>' +
            '<span class="mtux-metric-row-text">' +
            esc(col.shortLabel || col.label || col.key) +
            "</span>" +
            metricRemoveButtonHtml(col) +
            "</span>"
          );
        },
      });
      bindTableRemove();
      return;
    }

    // Fallback without mtux
    thead.innerHTML =
      '<th class="px-2 py-2 font-bold sm:px-3">Date</th>' +
      seriesList
        .map(function (s) {
          return (
            '<th class="px-2 py-2 font-bold tabular-nums sm:px-3" title="' +
            esc(s.label) +
            '">' +
            esc(s.label) +
            "</th>"
          );
        })
        .join("");
    if (!keys.length) {
      tbody.innerHTML =
        '<tr><td class="px-3 py-6 text-center text-slate-400" colspan="' +
        (seriesList.length + 1) +
        '">No rows</td></tr>';
      return;
    }
    tbody.innerHTML = keys
      .slice()
      .reverse()
      .map(function (d) {
        return (
          "<tr>" +
          '<td class="px-2 py-1.5 font-medium tabular-nums sm:px-3">' +
          esc(d) +
          "</td>" +
          seriesList
            .map(function (s) {
              return (
                '<td class="px-2 py-1.5 tabular-nums sm:px-3">' + fmtNum(s.map[d], s.key) + "</td>"
              );
            })
            .join("") +
          "</tr>"
        );
      })
      .join("");
  }

  async function loadOverlay() {
    state.overlaySeries = [];
    if (!global.PlayMetricOverlay || !PlayMetricOverlay.fetchSelectedSeries) return;
    var start = ($("sd-start") || {}).value || "";
    var end = ($("sd-end") || {}).value || "";
    var peer = peerPlatform(state.platform);
    try {
      var rows = await PlayMetricOverlay.fetchSelectedSeries(OVERLAY_ID, start, end, peer);
      state.overlaySeries = rows || [];
    } catch (e) {
      state.overlaySeries = [];
    }
  }

  async function runLoad() {
    var start = ($("sd-start") || {}).value || "";
    var end = ($("sd-end") || {}).value || "";
    var keys = selectedMetrics();
    if (!keys.length) {
      setStatus("Select at least one metric.");
      return;
    }
    setLoading(true);
    setStatus("Loading " + platformLabel(state.platform) + "…");
    state.seriesByMetric = {};
    state.compareSeries = {};
    try {
      var tasks = keys.map(function (key) {
        return fetchSeries(key, state.platform, start, end).then(function (pack) {
          state.seriesByMetric[key] = pack;
        });
      });
      var compareOn = ($("sd-compare") || {}).checked;
      var prev = compareOn ? previousRange() : null;
      if (prev) {
        keys.forEach(function (key) {
          tasks.push(
            fetchSeries(key, state.platform, prev.start, prev.end)
              .then(function (pack) {
                state.compareSeries[key] = pack;
              })
              .catch(function () {
                /* ignore compare gaps */
              })
          );
        });
      }
      tasks.push(loadOverlay());
      await Promise.all(tasks);
      renderMetricKpis();
      renderChart();
      renderTable();
      var n = state.dates.length;
      setStatus(
        platformLabel(state.platform) +
          " · " +
          keys.length +
          " metrics · " +
          n +
          " days" +
          (state.overlaySeries.length ? " · " + state.overlaySeries.length + " overlay" : "")
      );
    } catch (e) {
      setStatus((e && e.message) || "Load failed");
      renderMetricKpis();
      renderChart();
      renderTable();
    } finally {
      setLoading(false);
    }
  }

  function wireUi() {
    applyPreset("28");
    buildLabelIndex();
    state.selected = DEFAULT_METRICS.filter(function (k) {
      return !!state.labelByKey[k];
    });
    if (!state.selected.length) {
      var cat = $("sd-metric-catalog");
      if (cat) {
        Array.prototype.forEach.call(cat.options, function (opt) {
          if (opt.value && state.selected.length < 3) state.selected.push(opt.value);
        });
      }
    }
    renderMetricList();
    updateMetricTriggerLabel();
    syncPlatformUi();
    syncChartStyleUi();
    syncChartLayout();
    bindMetricKpiEvents();
    bindLegendEvents();

    var preset = $("sd-preset");
    if (preset) {
      if (!preset.value) preset.value = "28";
      preset.addEventListener("change", function () {
        if (preset.value === "custom") return;
        applyPreset(preset.value);
        runLoad();
      });
    }
    ["sd-start", "sd-end"].forEach(function (id) {
      var el = $(id);
      if (!el) return;
      el.addEventListener("change", function () {
        if (preset) preset.value = "custom";
      });
    });

    var trigger = $("sd-metric-trigger");
    var list = $("sd-metric-list");
    if (trigger && list) {
      trigger.addEventListener("click", function (ev) {
        ev.preventDefault();
        ev.stopPropagation();
        if (list.classList.contains("hidden")) openMetricList();
        else closeMetricList();
      });
      list.addEventListener("click", function (ev) {
        ev.stopPropagation();
        var clearBtn = ev.target.closest ? ev.target.closest("[data-sd-metric-clear]") : null;
        if (clearBtn) {
          ev.preventDefault();
          clearMetrics();
          return;
        }
        var pick = ev.target.closest ? ev.target.closest("[data-sd-metric-pick]") : null;
        if (!pick) return;
        ev.preventDefault();
        toggleMetric(pick.getAttribute("data-sd-metric-pick"));
      });
      document.addEventListener("click", function (ev) {
        if (list.classList.contains("hidden")) return;
        if (list.contains(ev.target) || trigger.contains(ev.target)) return;
        closeMetricList();
      });
      global.addEventListener("resize", positionMetricList);
      global.addEventListener("scroll", positionMetricList, true);
    }

    var platRoot = $("sd-platform-toggle");
    if (platRoot) {
      platRoot.addEventListener("click", function (ev) {
        var btn = ev.target.closest("[data-sd-platform]");
        if (!btn) return;
        var p = btn.getAttribute("data-sd-platform");
        if (p !== "web" && p !== "mweb") return;
        if (state.platform === p) return;
        state.platform = p;
        syncPlatformUi();
        syncMetricCatalog();
        updateOverlayPeer();
        runLoad();
      });
    }

    var run = $("sd-run");
    if (run) run.addEventListener("click", runLoad);

    var sRoot = $("sd-chart-style");
    if (sRoot) {
      sRoot.addEventListener("click", function (ev) {
        var btn = ev.target.closest("[data-sd-chart-style]");
        if (!btn) return;
        state.chartStyle = btn.getAttribute("data-sd-chart-style") || "area";
        syncChartStyleUi();
        renderChart();
      });
    }

    var compare = $("sd-compare");
    if (compare) compare.addEventListener("change", runLoad);

    global.sdOnCrossMetricOverlayChange = function () {
      loadOverlay().then(function () {
        renderChart();
        renderTable();
        setStatus(
          platformLabel(state.platform) +
            (state.overlaySeries.length ? " · " + state.overlaySeries.length + " overlay" : "")
        );
      });
    };

    function bindOverlay() {
      if (!global.PlayMetricOverlay) return false;
      PlayMetricOverlay.bindWhenReady(OVERLAY_ID, global.sdOnCrossMetricOverlayChange);
      updateOverlayPeer();
      return true;
    }
    if (!bindOverlay()) {
      var n = 0;
      var t = global.setInterval(function () {
        n += 1;
        if (bindOverlay() || n > 40) global.clearInterval(t);
      }, 100);
    }
  }

  function boot() {
    if (!$("sd-datas-root")) return;
    wireUi();
    runLoad();
  }

  if (document.readyState === "loading") {
    document.addEventListener("DOMContentLoaded", boot);
  } else {
    boot();
  }
})(window);
