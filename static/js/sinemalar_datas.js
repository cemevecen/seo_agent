/**
 * Sinemalar Datas tab — Empower X-Data (web / mweb), project=sinemalar.
 * Prefix: sd-
 */
(function (global) {
  "use strict";

  var PROJECT = "sinemalar";
  var OVERLAY_ID = "sd-cross-metric-overlay-root";
  var DEFAULT_METRICS = ["xdata:sessions", "xdata:active1DayUsers", "xdata:usdSpent"];
  var COLORS = ["#2563EB", "#10B981", "#F59E0B", "#EF4444", "#8B5CF6", "#06B6D4", "#F97316", "#EC4899"];
  var OVERLAY_COLORS = ["#F59E0B", "#A855F7", "#14B8A6", "#F43F5E", "#64748B"];
  var HEIGHT_BASE = { "1": 260, "2": 200, "3": 150 };
  var COMPRESS_DIV = { "1": 1, "2": 1.28, "3": 1.62 };

  var state = {
    platform: "web",
    selected: DEFAULT_METRICS.slice(),
    seriesByMetric: {},
    overlaySeries: [],
    compareSeries: {},
    dates: [],
    chartStyle: "area",
    chartHeight: "2",
    chartCompress: "1",
    loading: false,
    labelByKey: {},
  };

  function $(id) {
    return document.getElementById(id);
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
    var html = "";
    Array.prototype.forEach.call(cat.options, function (opt) {
      if (!opt.value) return;
      var checked = state.selected.indexOf(opt.value) >= 0;
      html +=
        '<label class="sd-metric-opt' +
        (checked ? " is-checked" : "") +
        '">' +
        '<input type="checkbox" value="' +
        opt.value.replace(/"/g, "&quot;") +
        '"' +
        (checked ? " checked" : "") +
        " />" +
        "<span>" +
        (opt.textContent || opt.value) +
        "</span></label>";
    });
    scroll.innerHTML = html;
    scroll.querySelectorAll("input[type=checkbox]").forEach(function (cb) {
      cb.addEventListener("change", function () {
        var key = cb.value;
        var idx = state.selected.indexOf(key);
        if (cb.checked && idx < 0) state.selected.push(key);
        if (!cb.checked && idx >= 0) state.selected.splice(idx, 1);
        if (!state.selected.length) state.selected = DEFAULT_METRICS.slice(0, 1);
        renderMetricList();
        updateMetricTriggerLabel();
      });
    });
  }

  function positionMetricList() {
    var trigger = $("sd-metric-trigger");
    var list = $("sd-metric-list");
    if (!trigger || !list || list.classList.contains("hidden")) return;
    var r = trigger.getBoundingClientRect();
    list.style.left = Math.max(8, r.left) + "px";
    list.style.top = r.bottom + 4 + "px";
    list.style.width = Math.max(r.width, 220) + "px";
  }

  function applyPreset(preset) {
    var end = today();
    var start = end;
    if (preset === "7") start = addDays(end, -6);
    else if (preset === "28") start = addDays(end, -27);
    else if (preset === "90") start = addDays(end, -89);
    else if (preset === "ytd") start = new Date(end.getFullYear(), 0, 1);
    else return;
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

  function chartPxHeight() {
    var base = HEIGHT_BASE[state.chartHeight] || 200;
    var div = COMPRESS_DIV[state.chartCompress] || 1;
    return Math.max(72, Math.round(base / div));
  }

  function applyChartHeight() {
    var wrap = $("sd-chart-wrap");
    var svg = $("sd-chart");
    var h = chartPxHeight();
    if (wrap) {
      wrap.style.height = h + "px";
      wrap.setAttribute("data-chart-height", state.chartHeight);
      wrap.setAttribute("data-chart-compress", state.chartCompress);
    }
    if (svg) svg.setAttribute("viewBox", "0 0 720 " + h);
    syncToggleGroup($("sd-chart-height"), "data-chart-height", state.chartHeight);
    syncToggleGroup($("sd-chart-compress"), "data-chart-compress", state.chartCompress);
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

  function sparkPath(values, w, h) {
    var nums = values.filter(function (v) {
      return v != null && isFinite(v);
    });
    if (nums.length < 2) return "";
    var min = Math.min.apply(null, nums);
    var max = Math.max.apply(null, nums);
    var span = max - min || 1;
    var step = w / Math.max(1, values.length - 1);
    var d = "";
    values.forEach(function (v, i) {
      if (v == null || !isFinite(v)) return;
      var x = i * step;
      var y = h - ((v - min) / span) * (h - 4) - 2;
      d += (d ? " L " : "M ") + x.toFixed(1) + " " + y.toFixed(1);
    });
    return d;
  }

  function renderSparks() {
    var host = $("sd-sparks");
    if (!host) return;
    var keys = selectedMetrics();
    if (!keys.length) {
      host.innerHTML = "";
      return;
    }
    var html = "";
    keys.forEach(function (key, i) {
      var pack = state.seriesByMetric[key] || { series: [] };
      var vals = (pack.series || []).map(function (pt) {
        return pt && isFinite(Number(pt.value)) ? Number(pt.value) : null;
      });
      var last = null;
      for (var j = vals.length - 1; j >= 0; j--) {
        if (vals[j] != null) {
          last = vals[j];
          break;
        }
      }
      var color = COLORS[i % COLORS.length];
      var path = sparkPath(vals, 160, 36);
      html +=
        '<div class="sd-spark-card">' +
        '<div class="flex items-start justify-between gap-2">' +
        '<div class="min-w-0">' +
        '<p class="truncate text-[10px] font-bold uppercase tracking-wide text-slate-500 dark:text-zinc-400">' +
        metricLabel(key) +
        "</p>" +
        '<p class="mt-1 text-xl font-extrabold tabular-nums text-slate-900 dark:text-zinc-50">' +
        fmtNum(last, key) +
        "</p>" +
        "</div>" +
        '<span class="mt-1 inline-block h-2 w-2 shrink-0 rounded-full" style="background:' +
        color +
        '"></span></div>' +
        '<svg class="sd-spark-svg mt-2" viewBox="0 0 160 36" preserveAspectRatio="none">' +
        (path
          ? '<path d="' +
            path +
            '" fill="none" stroke="' +
            color +
            '" stroke-width="1.6" stroke-linecap="round" stroke-linejoin="round"/>'
          : "") +
        "</svg></div>";
    });
    host.innerHTML = html;
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
        var mark = s.dashed
          ? '<span class="inline-block h-0.5 w-3 border-t-2 border-dashed" style="border-color:' +
            s.color +
            '"></span>'
          : '<span class="inline-block h-2 w-2 rounded-full" style="background:' + s.color + '"></span>';
        return (
          '<span class="inline-flex items-center gap-1.5">' +
          mark +
          "<span>" +
          s.label +
          "</span></span>"
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
    applyChartHeight();
    var seriesList = collectChartSeries();
    renderLegend(seriesList);
    var maps = seriesList.map(function (s) {
      return s.map;
    });
    var dates = unionDates(maps);
    state.dates = dates;
    var h = chartPxHeight();
    var w = 720;
    var padL = 48;
    var padR = 12;
    var padT = 12;
    var padB = 28;
    var plotW = w - padL - padR;
    var plotH = h - padT - padB;

    if (!dates.length || !seriesList.length) {
      svg.innerHTML =
        '<text x="50%" y="50%" text-anchor="middle" fill="#94a3b8" font-size="12">No data for this range</text>';
      if (tip) tip.classList.add("hidden");
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
        '<text x="' +
        (padL - 6) +
        '" y="' +
        (gy + 3) +
        '" text-anchor="end" fill="#94a3b8" font-size="10">' +
        fmtNum(gv, "") +
        "</text>";
    }

    var xLabels = "";
    var labelStep = Math.max(1, Math.ceil(dates.length / 6));
    dates.forEach(function (d, i) {
      if (i % labelStep !== 0 && i !== dates.length - 1) return;
      xLabels +=
        '<text x="' +
        xAt(i) +
        '" y="' +
        (h - 8) +
        '" text-anchor="middle" fill="#94a3b8" font-size="10">' +
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
      if (state.chartStyle === "bar" && !s.dashed) {
        var barW = Math.max(2, (plotW / Math.max(dates.length, 1)) * 0.55);
        pts.forEach(function (p) {
          if (!p) return;
          paths +=
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
      if (state.chartStyle === "area" && !s.dashed && !s.overlay && lastPt && areaD) {
        areaD += " L " + lastPt.x.toFixed(1) + " " + yAt(0).toFixed(1) + " Z";
        paths += '<path d="' + areaD + '" fill="' + s.color + '" fill-opacity="0.18"/>';
      }
      if (dPath) {
        paths +=
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
      .map(function (s) {
        var v = s.map[dates[idx]];
        return (
          '<div class="flex items-center justify-between gap-4">' +
          '<span class="inline-flex items-center gap-1.5"><span class="h-2 w-2 rounded-full" style="background:' +
          s.color +
          '"></span>' +
          s.label +
          "</span>" +
          "<strong class=\"tabular-nums\">" +
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
    if (!thead || !tbody) return;
    var seriesList = collectChartSeries().filter(function (s) {
      return !s.dashed;
    });
    var maps = seriesList.map(function (s) {
      return s.map;
    });
    var dates = unionDates(maps).slice().reverse();
    var th =
      '<th class="sd-sticky-col px-2 py-2 font-bold sm:px-3">Date</th>' +
      seriesList
        .map(function (s) {
          return (
            '<th class="px-2 py-2 font-bold tabular-nums sm:px-3" title="' +
            s.label +
            '">' +
            s.label +
            "</th>"
          );
        })
        .join("");
    thead.innerHTML = th;
    if (!dates.length) {
      tbody.innerHTML =
        '<tr><td class="px-3 py-6 text-center text-slate-400" colspan="' +
        (seriesList.length + 1) +
        '">No rows</td></tr>';
      return;
    }
    tbody.innerHTML = dates
      .map(function (d) {
        return (
          "<tr>" +
          '<td class="sd-sticky-col px-2 py-1.5 font-medium tabular-nums sm:px-3">' +
          d +
          "</td>" +
          seriesList
            .map(function (s) {
              return (
                '<td class="px-2 py-1.5 tabular-nums sm:px-3">' +
                fmtNum(s.map[d], s.key) +
                "</td>"
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
      renderSparks();
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
      renderSparks();
      renderChart();
      renderTable();
    } finally {
      setLoading(false);
    }
  }

  function wireUi() {
    applyPreset("7");
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
    applyChartHeight();

    var preset = $("sd-preset");
    if (preset) {
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
        list.classList.toggle("hidden");
        trigger.setAttribute("aria-expanded", list.classList.contains("hidden") ? "false" : "true");
        positionMetricList();
      });
      document.addEventListener("click", function (ev) {
        if (list.classList.contains("hidden")) return;
        if (list.contains(ev.target) || trigger.contains(ev.target)) return;
        list.classList.add("hidden");
        trigger.setAttribute("aria-expanded", "false");
      });
      global.addEventListener("resize", positionMetricList);
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

    var hRoot = $("sd-chart-height");
    if (hRoot) {
      hRoot.addEventListener("click", function (ev) {
        var btn = ev.target.closest("[data-chart-height]");
        if (!btn) return;
        state.chartHeight = btn.getAttribute("data-chart-height") || "2";
        applyChartHeight();
        renderChart();
      });
    }
    var cRoot = $("sd-chart-compress");
    if (cRoot) {
      cRoot.addEventListener("click", function (ev) {
        var btn = ev.target.closest("[data-chart-compress]");
        if (!btn) return;
        state.chartCompress = btn.getAttribute("data-chart-compress") || "1";
        applyChartHeight();
        renderChart();
      });
    }
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
