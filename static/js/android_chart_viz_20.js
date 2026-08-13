(function () {
  "use strict";

  var root = document.getElementById("pa-viz20-list");
  if (!root) return;

  var metaCache = null;
  var stateById = {};

  function th() {
    return window.seoPlotlyTheme
      ? window.seoPlotlyTheme()
      : { paper: "#fff", plot: "#fff", text: "#334155", grid: "#e2e8f0", legend: "#64748b", tick: "#64748b" };
  }

  function plotCfg() {
    return { responsive: true, displayModeBar: false, displaylogo: false };
  }

  function baseLayout(extra) {
    var t = th();
    var lo = {
      paper_bgcolor: t.paper,
      plot_bgcolor: t.plot,
      font: { color: t.text, size: 11, family: "ui-sans-serif, system-ui, sans-serif" },
      margin: { l: 48, r: 24, t: 36, b: 48 },
      legend: { orientation: "h", y: 1.12, x: 0, font: { color: t.legend, size: 10 } },
      xaxis: { gridcolor: t.grid, tickfont: { color: t.tick }, zerolinecolor: t.grid },
      yaxis: { gridcolor: t.grid, tickfont: { color: t.tick }, zerolinecolor: t.grid },
    };
    if (window.seoPlotlyTimeSeriesLayout) {
      lo = window.seoPlotlyTimeSeriesLayout(lo);
    }
    if (extra) {
      Object.keys(extra).forEach(function (k) {
        lo[k] = extra[k];
      });
    }
    return lo;
  }

  function whenPlotly(cb) {
    if (window.Plotly) {
      cb();
      return;
    }
    var n = 0;
    var iv = setInterval(function () {
      n += 1;
      if (window.Plotly || n > 80) {
        clearInterval(iv);
        if (window.Plotly) cb();
      }
    }, 75);
  }

  function esc(s) {
    return String(s == null ? "" : s)
      .replace(/&/g, "&amp;")
      .replace(/</g, "&lt;")
      .replace(/>/g, "&gt;")
      .replace(/"/g, "&quot;");
  }

  function isoToday() {
    return new Date().toISOString().slice(0, 10);
  }

  function isoDaysAgo(n) {
    var d = new Date();
    d.setDate(d.getDate() - n);
    return d.toISOString().slice(0, 10);
  }

  function metricOptions(meta) {
    var opts = [];
    (meta.play_metrics || []).forEach(function (m) {
      opts.push({ v: m, l: m });
    });
    (meta.ga4_metrics || []).forEach(function (m) {
      opts.push({ v: "ga4:" + m, l: "GA4 · " + m });
    });
    (meta.virgul_metrics || []).forEach(function (m) {
      opts.push({ v: "virgul:" + m, l: "Virgül · " + m });
    });
    return opts;
  }

  function selectHtml(id, label, options, value) {
    var h =
      '<label class="flex flex-col gap-0.5 text-[10px] font-semibold uppercase tracking-wide text-slate-500 dark:text-zinc-400">' +
      esc(label) +
      '<select class="pa-viz20-ctrl rounded-md border border-slate-200 bg-white px-2 py-1 text-xs text-slate-800 dark:border-zinc-700 dark:bg-zinc-900 dark:text-zinc-100" data-ctrl="' +
      esc(id) +
      '">';
    options.forEach(function (o) {
      var v = typeof o === "string" ? o : o.v;
      var l = typeof o === "string" ? o : o.l || o.v;
      h +=
        '<option value="' +
        esc(v) +
        '"' +
        (String(v) === String(value) ? " selected" : "") +
        ">" +
        esc(l) +
        "</option>";
    });
    return h + "</select></label>";
  }

  function inputHtml(id, label, type, value) {
    return (
      '<label class="flex flex-col gap-0.5 text-[10px] font-semibold uppercase tracking-wide text-slate-500 dark:text-zinc-400">' +
      esc(label) +
      '<input type="' +
      esc(type) +
      '" class="pa-viz20-ctrl rounded-md border border-slate-200 bg-white px-2 py-1 text-xs tabular-nums text-slate-800 dark:border-zinc-700 dark:bg-zinc-900 dark:text-zinc-100" data-ctrl="' +
      esc(id) +
      '" value="' +
      esc(value) +
      '"/></label>'
    );
  }

  var HORIZON_METRICS_MAX = 6;

  function metricListFor(details) {
    if (!details) return null;
    var id = details.id;
    if (id) {
      var portaled = document.body.querySelector(
        '.pa-viz20-metric-list[data-viz20-details-id="' + id.replace(/"/g, '\\"') + '"]'
      );
      if (portaled) return portaled;
    }
    return details.querySelector(".pa-viz20-metric-list");
  }

  function metricListScrollFor(details) {
    var list = metricListFor(details);
    return list ? list.querySelector(".pa-viz20-metric-list-scroll") : null;
  }

  function metricDdFor(details) {
    return details ? details.querySelector(".pa-viz20-metric-dd") : null;
  }

  function clearMetricListPosition(list) {
    if (!list) return;
    list.style.position = "";
    list.style.left = "";
    list.style.top = "";
    list.style.width = "";
    list.style.maxHeight = "";
    list.style.zIndex = "";
  }

  function parseMetricsCsv(s) {
    return String(s || "")
      .split(",")
      .map(function (x) {
        return x.trim();
      })
      .filter(Boolean);
  }

  function metricsCsv(arr) {
    return (arr || []).join(",");
  }

  function getPaMetricCatalog() {
    return document.getElementById("pa-metric-catalog");
  }

  function viz20MetricLabel(key, meta) {
    if (!key) return "";
    var cat = getPaMetricCatalog();
    if (cat) {
      var opt = cat.querySelector('option[value="' + String(key).replace(/"/g, '\\"') + '"]');
      if (opt) return String(opt.textContent || key).trim();
    }
    var mo = metricOptions(meta || metaCache || {});
    for (var i = 0; i < mo.length; i++) {
      if (mo[i].v === key) return mo[i].l;
    }
    return key;
  }

  function updateViz20MetricsTrigger(details, meta) {
    var labelEl = details.querySelector(".pa-viz20-metric-label");
    var hidden = details.querySelector('[data-ctrl="metrics"]');
    if (!labelEl || !hidden) return;
    var selected = parseMetricsCsv(hidden.value);
    if (!selected.length) {
      labelEl.textContent = "Seçin";
      return;
    }
    if (selected.length === 1) {
      labelEl.textContent = viz20MetricLabel(selected[0], meta);
      return;
    }
    labelEl.textContent = selected.length + " metrik seçili";
  }

  function positionViz20MetricDropdown(details) {
    var trigger = details.querySelector(".pa-viz20-metric-trigger");
    var list = metricListFor(details);
    if (!trigger || !list || list.classList.contains("hidden")) return;
    var r = trigger.getBoundingClientRect();
    var maxW = Math.min(380, window.innerWidth - 16);
    var w = Math.max(r.width, Math.min(maxW, 300));
    var left = Math.min(Math.max(8, r.left), window.innerWidth - w - 8);
    var spaceBelow = window.innerHeight - r.bottom - 12;
    var spaceAbove = r.top - 12;
    var maxH = Math.min(320, Math.max(spaceBelow, spaceAbove, 140));
    var top = r.bottom + 4;
    if (spaceBelow < 160 && spaceAbove > spaceBelow) {
      top = Math.max(8, r.top - maxH - 4);
    }
    list.style.position = "fixed";
    list.style.left = left + "px";
    list.style.top = top + "px";
    list.style.width = w + "px";
    list.style.maxHeight = maxH + "px";
    list.style.zIndex = "220";
  }

  function mountViz20MetricListPortal(details, on) {
    var dd = metricDdFor(details);
    var list = metricListFor(details);
    if (!dd || !list) return;
    if (on) {
      if (list.parentElement !== document.body) document.body.appendChild(list);
    } else if (list.parentElement !== dd) {
      dd.appendChild(list);
      clearMetricListPosition(list);
    }
  }

  function setViz20MetricDropdownOpen(details, meta, on) {
    var trigger = details.querySelector(".pa-viz20-metric-trigger");
    var list = metricListFor(details);
    if (!trigger || !list) return;
    if (on) {
      fillViz20MetricsList(details, meta);
      mountViz20MetricListPortal(details, true);
      positionViz20MetricDropdown(details);
      list.classList.remove("hidden");
      trigger.setAttribute("aria-expanded", "true");
    } else {
      list.classList.add("hidden");
      trigger.setAttribute("aria-expanded", "false");
      mountViz20MetricListPortal(details, false);
    }
  }

  function fillViz20MetricsList(details, meta) {
    var listScroll = metricListScrollFor(details);
    var hidden = details.querySelector('[data-ctrl="metrics"]');
    if (!listScroll || !hidden) return;
    var scrollY = listScroll.scrollTop;
    var selected = parseMetricsCsv(hidden.value);
    var selectedMap = {};
    selected.forEach(function (k) {
      selectedMap[k] = true;
    });
    var html =
      '<button type="button" role="option" aria-selected="' +
      (!selected.length ? "true" : "false") +
      '" data-viz20-metric-clear="1" class="flex w-full items-center gap-2 px-2.5 py-1.5 text-left text-xs ' +
      (!selected.length
        ? "bg-sky-50 font-semibold text-sky-800 dark:bg-sky-950/40 dark:text-sky-100"
        : "font-medium text-slate-700 hover:bg-slate-100 dark:text-zinc-200 dark:hover:bg-zinc-900") +
      '">' +
      '<span class="inline-flex w-3.5 shrink-0 justify-center text-[12px] text-sky-600 dark:text-sky-400" aria-hidden="true">' +
      (!selected.length ? "✓" : "") +
      "</span>" +
      '<span class="min-w-0 flex-1 truncate normal-case">Seçin</span></button>';
    var cat = getPaMetricCatalog();
    if (cat) {
      Array.prototype.forEach.call(cat.children, function (node) {
        if (node.tagName !== "OPTGROUP") return;
        html +=
          '<div class="px-2.5 py-1 text-[9px] font-bold uppercase tracking-wide text-slate-400 dark:text-zinc-500">' +
          esc(node.label) +
          "</div>";
        Array.prototype.forEach.call(node.children, function (opt) {
          if (!opt.value) return;
          var on = !!selectedMap[opt.value];
          html +=
            '<div class="pa-viz20-metric-row' +
            (on ? " is-on" : "") +
            '">' +
            '<button type="button" role="option" aria-selected="' +
            (on ? "true" : "false") +
            '" data-viz20-metric-pick="' +
            esc(opt.value) +
            '" class="flex min-w-0 flex-1 items-center gap-2 px-2.5 py-1.5 text-left text-xs normal-case ' +
            (on
              ? "bg-transparent font-semibold text-sky-800 dark:text-sky-100"
              : "font-medium text-slate-700 hover:bg-slate-100 dark:text-zinc-200 dark:hover:bg-zinc-900") +
            '">' +
            '<span class="inline-flex w-3.5 shrink-0 justify-center text-[12px] text-sky-600 dark:text-sky-400" aria-hidden="true">' +
            (on ? "✓" : "") +
            "</span>" +
            '<span class="min-w-0 flex-1 truncate">' +
            esc(String(opt.textContent || opt.value).trim()) +
            "</span></button></div>";
        });
      });
    } else {
      metricOptions(meta).forEach(function (o) {
        var on = !!selectedMap[o.v];
        html +=
          '<div class="pa-viz20-metric-row' +
          (on ? " is-on" : "") +
          '">' +
          '<button type="button" role="option" aria-selected="' +
          (on ? "true" : "false") +
          '" data-viz20-metric-pick="' +
          esc(o.v) +
          '" class="flex min-w-0 flex-1 items-center gap-2 px-2.5 py-1.5 text-left text-xs normal-case ' +
          (on
            ? "bg-transparent font-semibold text-sky-800 dark:text-sky-100"
            : "font-medium text-slate-700 hover:bg-slate-100 dark:text-zinc-200 dark:hover:bg-zinc-900") +
          '">' +
          '<span class="inline-flex w-3.5 shrink-0 justify-center text-[12px] text-sky-600 dark:text-sky-400" aria-hidden="true">' +
          (on ? "✓" : "") +
          "</span>" +
          '<span class="min-w-0 flex-1 truncate">' +
          esc(o.l) +
          "</span></button></div>";
      });
    }
    listScroll.innerHTML = html;
    listScroll.scrollTop = scrollY;
    updateViz20MetricsTrigger(details, meta);
  }

  function toggleViz20Metric(details, meta, key) {
    var hidden = details.querySelector('[data-ctrl="metrics"]');
    if (!hidden) return;
    if (!key) {
      hidden.value = "";
      fillViz20MetricsList(details, meta);
      if (details.open) loadViz(details);
      return;
    }
    var arr = parseMetricsCsv(hidden.value);
    var idx = arr.indexOf(key);
    if (idx >= 0) {
      arr.splice(idx, 1);
    } else {
      if (arr.length >= HORIZON_METRICS_MAX) return;
      arr.push(key);
    }
    hidden.value = metricsCsv(arr);
    fillViz20MetricsList(details, meta);
    if (details.open) loadViz(details);
  }

  function viz20MetricEventInside(details, t) {
    if (!t || !t.closest) return false;
    var wrap = details.querySelector(".pa-viz20-metrics-wrap");
    if (wrap && wrap.contains(t)) return true;
    var list = metricListFor(details);
    if (list && list.contains(t)) return true;
    return false;
  }

  function toggleViz20MetricDropdown(details, meta) {
    var list = metricListFor(details);
    if (!list) return;
    var open = list.classList.contains("hidden");
    root.querySelectorAll("details.pa-viz20-drop").forEach(function (d) {
      if (d !== details) setViz20MetricDropdownOpen(d, metaCache, false);
    });
    setViz20MetricDropdownOpen(details, meta, !open);
  }

  function metricsMultiSelectHtml(meta, metricsValue) {
    var selected = parseMetricsCsv(metricsValue);
    var triggerLabel = "Seçin";
    if (selected.length === 1) {
      triggerLabel = viz20MetricLabel(selected[0], meta);
    } else if (selected.length > 1) {
      triggerLabel = selected.length + " metrik seçili";
    }
    return (
      '<div class="pa-viz20-metrics-wrap flex flex-col gap-0.5 text-[10px] font-semibold uppercase tracking-wide text-slate-500 dark:text-zinc-400">' +
      '<span class="pa-viz20-metrics-title">Metrikler</span>' +
      '<div class="pa-viz20-metric-dd relative min-w-[11rem]">' +
      '<button type="button" class="pa-viz20-metric-trigger" aria-haspopup="listbox" aria-expanded="false">' +
      '<span class="pa-viz20-metric-label min-w-0 flex-1 truncate normal-case">' +
      esc(triggerLabel) +
      "</span>" +
      '<svg class="h-3 w-3 shrink-0 opacity-60" viewBox="0 0 20 20" fill="currentColor" aria-hidden="true"><path fill-rule="evenodd" d="M5.23 7.21a.75.75 0 011.06.02L10 10.94l3.71-3.71a.75.75 0 111.06 1.06l-4.24 4.25a.75.75 0 01-1.06 0L5.21 8.29a.75.75 0 01.02-1.08z" clip-rule="evenodd"/></svg>' +
      "</button>" +
      '<div class="pa-viz20-metric-list hidden overflow-hidden rounded-xl border border-slate-300 bg-white shadow-lg dark:border-zinc-600 dark:bg-zinc-950" role="listbox" aria-multiselectable="true" data-viz20-portal="1">' +
      '<div class="pa-viz20-metric-list-scroll py-1"></div>' +
      "</div>" +
      '<input type="hidden" class="pa-viz20-ctrl" data-ctrl="metrics" value="' +
      esc(metricsValue) +
      '"/>' +
      "</div></div>"
    );
  }

  function isoDate(d) {
    var y = d.getFullYear();
    var m = String(d.getMonth() + 1).padStart(2, "0");
    var day = String(d.getDate()).padStart(2, "0");
    return y + "-" + m + "-" + day;
  }

  var DATE_PRESETS = [
    { v: "yesterday", l: "Yesterday" },
    { v: "3", l: "Last 3 days" },
    { v: "7", l: "Last 1 week" },
    { v: "14", l: "Last 2 weeks" },
    { v: "30", l: "Last 1 month" },
    { v: "90", l: "Last 3 months" },
    { v: "180", l: "Last 6 months" },
    { v: "270", l: "Last 9 months" },
    { v: "365", l: "Last 1 year" },
  ];

  function pagePreset() {
    var el = document.getElementById("pa-preset");
    return el && el.value ? el.value : "30";
  }

  function applyDatePreset(raw) {
    var end = new Date();
    var start = new Date();
    if (raw === "yesterday") {
      end.setDate(end.getDate() - 1);
      start = new Date(end.getTime());
    } else {
      var days = parseInt(raw, 10) || 30;
      start.setDate(end.getDate() - (days - 1));
    }
    return { start: isoDate(start), end: isoDate(end) };
  }

  function pageDateRange() {
    var s = document.getElementById("pa-start");
    var e = document.getElementById("pa-end");
    if (s && s.value && e && e.value) return { start: s.value, end: e.value };
    return null;
  }

  var CHART_HEIGHTS = { "1": 220, "2": 340, "3": 480 };
  var DEFAULT_HEIGHT_TIER = "2";
  var HEIGHT_STORAGE_PREFIX = "paViz20H:";

  function readHeightTier(vizId) {
    try {
      var v = localStorage.getItem(HEIGHT_STORAGE_PREFIX + vizId);
      if (v === "1" || v === "2" || v === "3") return v;
    } catch (_) {}
    return DEFAULT_HEIGHT_TIER;
  }

  function storeHeightTier(vizId, tier) {
    try {
      localStorage.setItem(HEIGHT_STORAGE_PREFIX + vizId, tier);
    } catch (_) {}
  }

  function heightPx(tier) {
    return CHART_HEIGHTS[tier] || CHART_HEIGHTS[DEFAULT_HEIGHT_TIER];
  }

  function heightControlsHtml(vizId) {
    var tier = readHeightTier(vizId);
    var labels = { "1": "Alçak", "2": "Orta", "3": "Yüksek" };
    var btns = ["1", "2", "3"]
      .map(function (t) {
        var on = tier === t;
        return (
          '<button type="button" class="pa-viz20-h-btn' +
          (on ? " is-active" : "") +
          '" data-height-tier="' +
          t +
          '" aria-pressed="' +
          (on ? "true" : "false") +
          '">' +
          labels[t] +
          "</button>"
        );
      })
      .join("");
    return (
      '<div class="pa-viz20-height" role="group" aria-label="Grafik yüksekliği">' +
      '<span class="pa-viz20-height-label">Yükseklik</span>' +
      '<div class="pa-viz20-height-btns">' +
      btns +
      "</div></div>"
    );
  }

  function applyChartShellHeight(chartEl, tier) {
    if (!chartEl) return;
    var px = heightPx(tier);
    chartEl.style.minHeight = px + "px";
    chartEl.style.height = px + "px";
    chartEl.setAttribute("data-height-tier", tier);
  }

  function defaultParams(viz, meta) {
    var dr = meta.default_range || {};
    var pageDr = pageDateRange();
    var p = {
      start: (pageDr && pageDr.start) || dr.start || isoDaysAgo(27),
      end: (pageDr && pageDr.end) || dr.end || isoToday(),
      preset: pagePreset(),
      metric: "crashes",
      metric_left: "ga4:sessions",
      metric_right: "virgul:net_revenue",
      metrics: "anrs,crashes,dau,ga4:sessions",
      etype: "CRASH",
      limit: "15",
    };
    if (viz.id === "timeline") p.metric = "crashes";
    if (viz.id === "control") p.metric = "crashes";
    return p;
  }

  function controlsHtml(viz, meta, params) {
    var c = viz.controls || [];
    var mo = metricOptions(meta);
    var dims = (meta.dims || []).map(function (d) {
      return { v: d, l: d };
    });
    var parts = [];
    var hasDates = c.indexOf("start") >= 0 && c.indexOf("end") >= 0;
    if (c.indexOf("start") >= 0) parts.push(inputHtml("start", "Start", "date", params.start));
    if (c.indexOf("end") >= 0) parts.push(inputHtml("end", "End", "date", params.end));
    if (hasDates) parts.push(selectHtml("preset", "Preset", DATE_PRESETS, params.preset || "30"));
    if (c.indexOf("metric") >= 0) parts.push(selectHtml("metric", "Metrik", mo, params.metric));
    if (c.indexOf("dim") >= 0) parts.push(selectHtml("dim", "Kırılım", dims, params.dim));
    if (c.indexOf("metric_left") >= 0) parts.push(selectHtml("metric_left", "Sol eksen", mo, params.metric_left));
    if (c.indexOf("metric_right") >= 0) parts.push(selectHtml("metric_right", "Sağ eksen", mo, params.metric_right));
    if (c.indexOf("metrics") >= 0) {
      parts.push(metricsMultiSelectHtml(meta, params.metrics));
    }
    if (c.indexOf("etype") >= 0) {
      parts.push(
        selectHtml("etype", "Issue tipi", [
          { v: "CRASH", l: "Crash" },
          { v: "ANR", l: "ANR" },
        ], params.etype)
      );
    }
    if (c.indexOf("limit") >= 0) parts.push(inputHtml("limit", "Limit", "number", params.limit));
    if (!parts.length) {
      parts.push('<span class="text-[11px] text-slate-500 dark:text-zinc-400">Canlı vitals / stability verisi</span>');
    }
    return (
      '<div class="mb-3 flex flex-wrap items-end gap-2">' +
      parts.join("") +
      heightControlsHtml(viz.id) +
      '<button type="button" class="pa-viz20-refresh shrink-0 rounded-md bg-slate-900 px-3 py-1.5 text-[11px] font-semibold text-white hover:bg-slate-800 dark:bg-zinc-100 dark:text-zinc-900 dark:hover:bg-white">Yenile</button>' +
      "</div>"
    );
  }

  function readParams(details) {
    var p = stateById[details.id] || {};
    details.querySelectorAll(".pa-viz20-ctrl").forEach(function (el) {
      var k = el.getAttribute("data-ctrl");
      if (k) p[k] = el.value;
    });
    stateById[details.id] = p;
    return p;
  }

  function qsParams(p) {
    var q = new URLSearchParams();
    ["start", "end", "metric", "dim", "metric_left", "metric_right", "metrics", "etype", "limit"].forEach(function (k) {
      if (p[k] != null && p[k] !== "") q.set(k, p[k]);
    });
    return q;
  }

  function renderTable(el, table) {
    if (!el || !table || !table.columns || !table.columns.length) {
      if (el) el.innerHTML = '<p class="text-xs text-slate-400">Tablo verisi yok</p>';
      return;
    }
    var h =
      '<div class="mt-3 max-h-48 overflow-auto rounded-lg border border-slate-200 dark:border-zinc-700"><table class="w-full text-left text-[11px]"><thead class="sticky top-0 bg-slate-100 dark:bg-zinc-800"><tr>';
    table.columns.forEach(function (c) {
      h += '<th class="px-2 py-1.5 font-bold text-slate-600 dark:text-zinc-300">' + esc(c) + "</th>";
    });
    h += "</tr></thead><tbody>";
    (table.rows || []).forEach(function (row, ri) {
      h += '<tr class="' + (ri % 2 ? "bg-slate-50/80 dark:bg-zinc-900/40" : "") + '">';
      row.forEach(function (cell) {
        h += '<td class="px-2 py-1 tabular-nums text-slate-700 dark:text-zinc-300">' + esc(cell) + "</td>";
      });
      h += "</tr>";
    });
    el.innerHTML = h + "</tbody></table></div>";
  }

  function normalizeDay(x) {
    return String(x == null ? "" : x).slice(0, 10);
  }

  function timelineReleaseHoverText(rel) {
    if (!rel) return "";
    return (
      "<br><span style=\"color:#22c55e\">● Release</span><br>" +
      "Sürüm: " +
      esc(rel.version || "—") +
      "<br>Versiyon: " +
      esc(rel.version_code || "—")
    );
  }

  function plotChart(chartEl, payload, heightPxVal) {
    var c = payload.chart || {};
    var type = c.type;
    var traces = [];
    var layout = baseLayout({ title: { text: "", font: { size: 12 } }, height: heightPxVal });

    if (type === "funnel") {
      traces = [
        {
          type: "funnel",
          y: c.labels || [],
          x: c.values || [],
          textinfo: "value+percent initial",
          marker: { color: ["#3b82f6", "#22c55e", "#f97316"] },
        },
      ];
      layout.funnelmode = "stack";
    } else if (type === "waterfall") {
      traces = [
        {
          type: "waterfall",
          x: c.labels || [],
          y: c.values || [],
          measure: c.measure || [],
          connector: { line: { color: "#94a3b8" } },
        },
      ];
    } else if (type === "heatmap") {
      traces = [
        {
          type: "heatmap",
          x: c.x || [],
          y: c.y || [],
          z: c.z || [],
          colorscale: "Blues",
          hoverongaps: false,
        },
      ];
      layout.xaxis.title = c.matrix ? "Dönem" : "Hafta";
      layout.yaxis.title = c.matrix ? "Metrik" : c.cohort ? "Cohort" : "Gün";
    } else if (type === "treemap") {
      traces = [
        {
          type: "treemap",
          labels: c.labels || [],
          parents: c.parents || [],
          values: c.values || [],
          textinfo: "label+value",
        },
      ];
      layout.margin = { l: 8, r: 8, t: 24, b: 8 };
    } else if (type === "bump") {
      (c.traces || []).forEach(function (tr, i) {
        traces.push({
          type: "scatter",
          mode: "lines+markers",
          name: tr.name,
          x: tr.x,
          y: tr.y,
          line: { width: 2 },
        });
      });
      layout.yaxis.autorange = "reversed";
      layout.yaxis.title = "Rank";
    } else if (type === "combo") {
      var left = c.left || {};
      var right = c.right || {};
      traces.push({
        type: "bar",
        name: left.label || "Left",
        x: (left.series || []).map(function (r) {
          return r.key;
        }),
        y: (left.series || []).map(function (r) {
          return r.value;
        }),
        marker: { color: "#22c55e", opacity: 0.55 },
        yaxis: "y",
      });
      traces.push({
        type: "scatter",
        mode: "lines",
        name: right.label || "Right",
        x: (right.series || []).map(function (r) {
          return r.key;
        }),
        y: (right.series || []).map(function (r) {
          return r.value;
        }),
        line: { color: "#f97316", width: 2 },
        yaxis: "y2",
      });
      layout.yaxis2 = { overlaying: "y", side: "right", gridcolor: "rgba(0,0,0,0)" };
    } else if (type === "stacked100") {
      (c.traces || []).forEach(function (tr) {
        traces.push({
          type: "scatter",
          mode: "lines",
          stackgroup: "one",
          groupnorm: "percent",
          name: tr.name,
          x: tr.x,
          y: tr.y,
          fill: "tonexty",
        });
      });
      layout.yaxis.tickformat = ".0%";
    } else if (type === "boxplot") {
      (c.traces || []).forEach(function (tr) {
        traces.push({ type: "box", name: tr.name, y: tr.y, boxpoints: "outliers" });
      });
    } else if (type === "scatter") {
      var pts = c.points || [];
      traces.push({
        type: "scatter",
        mode: "markers+text",
        text: pts.map(function (p) {
          return p.name;
        }),
        textposition: "top center",
        x: pts.map(function (p) {
          return p.x;
        }),
        y: pts.map(function (p) {
          return p.y;
        }),
        marker: {
          size: pts.map(function (p) {
            return Math.sqrt(p.size || 1) * 2;
          }),
          sizemode: "diameter",
          opacity: 0.65,
        },
      });
      layout.xaxis.title = "ANR %";
      layout.yaxis.title = "Crash %";
    } else if (type === "calendar") {
      var series = c.series || [];
      traces.push({
        type: "bar",
        x: series.map(function (r) {
          return r.key;
        }),
        y: series.map(function (r) {
          return r.value;
        }),
        marker: {
          color: series.map(function (r) {
            return r.value;
          }),
          colorscale: "Greens",
          showscale: true,
        },
      });
      layout.xaxis.title = "Tarih";
    } else if (type === "sankey") {
      var nodes = c.nodes || [];
      var links = c.links || [];
      traces.push({
        type: "sankey",
        node: { label: nodes, pad: 12, thickness: 16 },
        link: {
          source: links.map(function (l) {
            return nodes.indexOf(l.source);
          }),
          target: links.map(function (l) {
            return nodes.indexOf(l.target);
          }),
          value: links.map(function (l) {
            return l.value;
          }),
        },
      });
      layout.margin = { l: 16, r: 16, t: 16, b: 16 };
    } else if (type === "horizon") {
      (c.traces || []).forEach(function (tr, i) {
        traces.push({
          type: "scatter",
          mode: "lines",
          fill: "tozeroy",
          name: tr.name,
          x: tr.x,
          y: tr.y,
          opacity: 0.85,
          yaxis: i === 0 ? "y" : "y" + (i + 1),
        });
        if (i > 0) {
          layout["yaxis" + (i + 1)] = {
            overlaying: "y",
            side: "right",
            showgrid: false,
            visible: false,
          };
        }
      });
    } else if (type === "bar") {
      if (c.horizontal) {
        traces.push({
          type: "bar",
          orientation: "h",
          y: c.labels || [],
          x: c.values || [],
          marker: { color: "#ef4444", opacity: 0.65 },
        });
      } else {
        traces.push({
          type: "bar",
          x: c.labels || [],
          y: c.values || [],
          marker: { color: "#ef4444", opacity: 0.65 },
        });
      }
    } else if (type === "marimekko") {
      var cols = c.columns || [];
      var cumX = 0;
      var totalW = cols.reduce(function (s, col) {
        return s + (col.width || 0);
      }, 0) || 1;
      cols.forEach(function (col) {
        var w = (col.width || 0) / totalW;
        var y0 = 0;
        (col.segments || []).forEach(function (seg, si) {
          var h = seg.share || 0;
          traces.push({
            type: "bar",
            name: col.label + " · " + seg.label,
            x: [col.label],
            y: [h * 100],
            offsetgroup: col.label,
            marker: { opacity: 0.55 + si * 0.1 },
            showlegend: false,
          });
        });
        cumX += w;
      });
      layout.barmode = "stack";
      layout.yaxis.title = "OS pay %";
    } else if (type === "control") {
      traces.push({
        type: "scatter",
        mode: "lines+markers",
        name: "Günlük",
        x: c.x,
        y: c.y,
        line: { color: "#22c55e" },
      });
      if (c.alarms && c.alarms.length) {
        traces.push({
          type: "scatter",
          mode: "markers",
          name: "Alarm",
          x: c.alarms.map(function (i) {
            return c.x[i];
          }),
          y: c.alarms.map(function (i) {
            return c.y[i];
          }),
          marker: { color: "#ef4444", size: 9 },
        });
      }
      layout.shapes = [
        { type: "line", xref: "paper", x0: 0, x1: 1, y0: c.ucl, y1: c.ucl, line: { dash: "dash", color: "#ef4444" } },
        { type: "line", xref: "paper", x0: 0, x1: 1, y0: c.lcl, y1: c.lcl, line: { dash: "dash", color: "#ef4444" } },
        { type: "line", xref: "paper", x0: 0, x1: 1, y0: c.mean, y1: c.mean, line: { color: "#64748b" } },
      ];
    } else if (type === "pareto") {
      traces.push({
        type: "bar",
        name: "Events",
        x: c.labels,
        y: c.values,
        marker: { color: "#3b82f6", opacity: 0.55 },
      });
      traces.push({
        type: "scatter",
        mode: "lines+markers",
        name: "Kümülatif %",
        x: c.labels,
        y: c.cumulative,
        yaxis: "y2",
        line: { color: "#f97316", width: 2 },
      });
      layout.yaxis2 = { overlaying: "y", side: "right", range: [0, 105], title: "Cum %" };
    } else if (type === "multiples") {
      var panels = c.panels || [];
      panels.forEach(function (pn, i) {
        var ax = i === 0 ? "" : i + 1;
        traces.push({
          type: "scatter",
          mode: "lines",
          name: pn.title,
          x: pn.x,
          y: pn.y,
          xaxis: "x" + (i + 1),
          yaxis: "y" + (i + 1),
        });
      });
      layout.grid = { rows: 2, columns: 2, pattern: "independent" };
      layout.height = heightPxVal;
      panels.forEach(function (pn, i) {
        var n = i + 1;
        layout["xaxis" + n] = { title: pn.title, anchor: "y" + n };
        layout["yaxis" + n] = { title: "", anchor: "x" + n };
      });
    } else if (type === "timeline") {
      var series = c.series || [];
      var xs = series.map(function (r) {
        return r.key;
      });
      var ys = series.map(function (r) {
        return r.value;
      });
      var releaseByDate = {};
      (c.releases || []).forEach(function (rel) {
        var d = normalizeDay(rel.date);
        if (d) releaseByDate[d] = rel;
      });
      var maxY = ys.reduce(function (m, v) {
        return Math.max(m, Number(v) || 0);
      }, 0);
      if (!maxY) maxY = 1;
      traces.push({
        type: "scatter",
        mode: "lines",
        name: "Metrik",
        x: xs,
        y: ys,
        line: { color: "#3b82f6" },
        text: xs.map(function (x) {
          return timelineReleaseHoverText(releaseByDate[normalizeDay(x)]);
        }),
        hovertemplate: "%{y}<br>%{x}%{text}<extra></extra>",
      });
      (c.spikes || []).forEach(function (sp) {
        layout.shapes = layout.shapes || [];
        layout.shapes.push({
          type: "line",
          x0: sp.date,
          x1: sp.date,
          y0: 0,
          y1: 1,
          yref: "paper",
          line: { color: "#ef4444", dash: "dot" },
        });
      });
      var orphanRelX = [];
      var orphanRelY = [];
      var orphanRelCustom = [];
      (c.releases || []).forEach(function (rel) {
        var day = normalizeDay(rel.date);
        if (!day) return;
        layout.shapes = layout.shapes || [];
        layout.shapes.push({
          type: "line",
          x0: day,
          x1: day,
          y0: 0,
          y1: 1,
          yref: "paper",
          line: { color: "#22c55e", dash: "dot", width: 1 },
        });
        orphanRelX.push(day);
        orphanRelY.push(maxY * 0.92);
        orphanRelCustom.push([rel.version || "—", rel.version_code || "—", day]);
      });
      if (orphanRelX.length) {
        traces.push({
          type: "scatter",
          mode: "markers",
          name: "Release",
          x: orphanRelX,
          y: orphanRelY,
          marker: {
            size: 9,
            color: "#22c55e",
            symbol: "diamond",
            line: { width: 1, color: "#ffffff" },
          },
          customdata: orphanRelCustom,
          hovertemplate:
            "<b>Release</b><br>Sürüm: %{customdata[0]}<br>Versiyon: %{customdata[1]}<br>%{customdata[2]}<extra></extra>",
          showlegend: false,
        });
      }
    }

    if (!traces.length) {
      chartEl.innerHTML = '<p class="p-4 text-center text-xs text-slate-400">Grafik verisi yok</p>';
      return;
    }

    whenPlotly(function () {
      Plotly.newPlot(chartEl, traces, layout, plotCfg());
    });
  }

  function setStatus(details, msg, isErr) {
    var el = details.querySelector(".pa-viz20-status");
    if (!el) return;
    el.textContent = msg || "";
    el.className =
      "pa-viz20-status text-[11px] " +
      (isErr ? "text-rose-600 dark:text-rose-400" : "text-slate-500 dark:text-zinc-400");
  }

  function loadViz(details) {
    var vizId = details.getAttribute("data-viz-id");
    if (!vizId) return;
    var params = readParams(details);
    var chartEl = details.querySelector(".pa-viz20-chart");
    var tableEl = details.querySelector(".pa-viz20-table");
    if (!chartEl) return;
    var hTier = readHeightTier(vizId);
    applyChartShellHeight(chartEl, hTier);
    setStatus(details, "Yükleniyor…");
    chartEl.innerHTML =
      '<div class="flex h-48 items-center justify-center text-xs text-slate-400">Veri çekiliyor…</div>';
    fetch("/api/play-analytics/viz20/" + encodeURIComponent(vizId) + "?" + qsParams(params).toString(), {
      credentials: "same-origin",
      cache: "no-store",
    })
      .then(function (r) {
        return r.json().then(function (j) {
          return { ok: r.ok, body: j };
        });
      })
      .then(function (res) {
        var body = res.body || {};
        if (!res.ok || body.ok === false) {
          chartEl.innerHTML = "";
          setStatus(details, body.message || "Veri alınamadı", true);
          renderTable(tableEl, body.table);
          return;
        }
        setStatus(details, body.params ? JSON.stringify(body.params).replace(/[{}"]/g, "").replace(/,/g, " · ") : "");
        try {
          plotChart(chartEl, body, heightPx(hTier));
          renderTable(tableEl, body.table);
        } catch (plotErr) {
          chartEl.innerHTML = "";
          setStatus(details, (plotErr && plotErr.message) || "Grafik çizilemedi", true);
        }
      })
      .catch(function (err) {
        chartEl.innerHTML = "";
        setStatus(details, (err && err.message) || "Ağ hatası", true);
      });
  }

  function bindViz20MetricsDropdown(details, meta) {
    var trigger = details.querySelector(".pa-viz20-metric-trigger");
    var list = metricListFor(details);
    var wrap = details.querySelector(".pa-viz20-metrics-wrap");
    if (!trigger || !list) return;
    if (details.id) list.setAttribute("data-viz20-details-id", details.id);
    fillViz20MetricsList(details, meta);
    if (trigger.getAttribute("data-viz20-metric-bound") === "1") return;
    trigger.setAttribute("data-viz20-metric-bound", "1");

    function openToggle(ev) {
      ev.preventDefault();
      ev.stopPropagation();
      toggleViz20MetricDropdown(details, meta);
    }

    trigger.addEventListener("click", openToggle);
    trigger.addEventListener("keydown", function (ev) {
      if (ev.key === "Enter" || ev.key === " ") {
        openToggle(ev);
      }
    });
    if (wrap) {
      wrap.addEventListener("click", function (ev) {
        if (ev.target.closest(".pa-viz20-metric-trigger")) return;
        if (ev.target.closest(".pa-viz20-metric-list")) return;
        openToggle(ev);
      });
    }
    list.addEventListener("click", function (ev) {
      var clearBtn = ev.target.closest("[data-viz20-metric-clear]");
      if (clearBtn) {
        ev.preventDefault();
        ev.stopPropagation();
        toggleViz20Metric(details, meta, "");
        return;
      }
      var pick = ev.target.closest("[data-viz20-metric-pick]");
      if (!pick) return;
      ev.preventDefault();
      ev.stopPropagation();
      toggleViz20Metric(details, meta, pick.getAttribute("data-viz20-metric-pick"));
    });
    list.addEventListener("mousedown", function (ev) {
      ev.stopPropagation();
    });
  }

  function bindDetails(details, meta) {
    bindViz20MetricsDropdown(details, meta);
    details.addEventListener("toggle", function () {
      setViz20MetricDropdownOpen(details, meta, false);
      if (details.open && !details.getAttribute("data-loaded")) {
        details.setAttribute("data-loaded", "1");
        loadViz(details);
      }
    });
    var refresh = details.querySelector(".pa-viz20-refresh");
    if (refresh) {
      refresh.addEventListener("click", function () {
        loadViz(details);
      });
    }
    details.querySelectorAll(".pa-viz20-ctrl").forEach(function (el) {
      el.addEventListener("change", function () {
        if (el.getAttribute("data-ctrl") === "preset") {
          var range = applyDatePreset(el.value);
          var startEl = details.querySelector('[data-ctrl="start"]');
          var endEl = details.querySelector('[data-ctrl="end"]');
          if (startEl) startEl.value = range.start;
          if (endEl) endEl.value = range.end;
        }
        if (details.open) loadViz(details);
      });
    });
    details.querySelectorAll(".pa-viz20-h-btn").forEach(function (btn) {
      btn.addEventListener("click", function () {
        var tier = btn.getAttribute("data-height-tier") || DEFAULT_HEIGHT_TIER;
        var vizId = details.getAttribute("data-viz-id");
        if (vizId) storeHeightTier(vizId, tier);
        details.querySelectorAll(".pa-viz20-h-btn").forEach(function (b) {
          var on = b === btn;
          b.classList.toggle("is-active", on);
          b.setAttribute("aria-pressed", on ? "true" : "false");
        });
        var chartEl = details.querySelector(".pa-viz20-chart");
        applyChartShellHeight(chartEl, tier);
        if (chartEl && chartEl.data && window.Plotly) {
          Plotly.relayout(chartEl, { height: heightPx(tier) });
        } else if (details.open) {
          loadViz(details);
        }
      });
    });
  }

  function syncAllFromMainPreset() {
    var mainPreset = document.getElementById("pa-preset");
    if (!mainPreset) return;
    var range = applyDatePreset(mainPreset.value);
    root.querySelectorAll("details.pa-viz20-drop").forEach(function (details) {
      var presetEl = details.querySelector('[data-ctrl="preset"]');
      var startEl = details.querySelector('[data-ctrl="start"]');
      var endEl = details.querySelector('[data-ctrl="end"]');
      if (presetEl) presetEl.value = mainPreset.value;
      if (startEl) startEl.value = range.start;
      if (endEl) endEl.value = range.end;
      if (details.open) loadViz(details);
    });
  }

  function buildUi(meta) {
    root.innerHTML = "";
    (meta.viz || []).forEach(function (viz) {
      var params = defaultParams(viz, meta);
      stateById["pa-viz20-" + viz.id] = params;
      var details = document.createElement("details");
      details.className = "pa-viz20-drop group";
      details.id = "pa-viz20-" + viz.id;
      details.setAttribute("data-viz-id", viz.id);
      details.innerHTML =
        "<summary>" +
        '<div class="flex min-w-0 items-center gap-2">' +
        '<span class="pa-viz20-chevron shrink-0" aria-hidden="true">▸</span>' +
        '<div class="min-w-0">' +
        '<p class="truncate text-sm font-semibold text-slate-800 dark:text-zinc-100">' +
        esc(viz.title) +
        "</p>" +
        (viz.blurb
          ? '<p class="truncate text-[11px] text-slate-500 dark:text-zinc-400">' + esc(viz.blurb) + "</p>"
          : "") +
        "</div></div>" +
        '<span class="pa-viz20-badge pa-viz20-badge-closed">kapalı</span>' +
        "</summary>" +
        '<div class="pa-viz20-body">' +
        (viz.detail
          ? '<p class="pa-viz20-detail mb-3 text-[11px] leading-relaxed text-slate-600 dark:text-zinc-400">' +
            esc(viz.detail) +
            "</p>"
          : "") +
        controlsHtml(viz, meta, params) +
        '<p class="pa-viz20-status mb-1 min-h-[1rem] text-[11px] text-slate-500"></p>' +
        '<div class="pa-viz20-chart rounded-lg border border-slate-200/80 bg-slate-50/50 dark:border-zinc-700 dark:bg-zinc-950/40"></div>' +
        '<div class="pa-viz20-table"></div>' +
        "</div>";
      bindDetails(details, meta);
      root.appendChild(details);
    });
    if (!document.body.getAttribute("data-viz20-metrics-outside")) {
      document.body.setAttribute("data-viz20-metrics-outside", "1");
      document.addEventListener(
        "mousedown",
        function (ev) {
          root.querySelectorAll("details.pa-viz20-drop").forEach(function (details) {
            var list = metricListFor(details);
            if (!list || list.classList.contains("hidden")) return;
            if (viz20MetricEventInside(details, ev.target)) return;
            setViz20MetricDropdownOpen(details, metaCache, false);
          });
        },
        true
      );
      window.addEventListener(
        "scroll",
        function () {
          root.querySelectorAll("details.pa-viz20-drop").forEach(function (details) {
            var list = metricListFor(details);
            if (list && !list.classList.contains("hidden")) positionViz20MetricDropdown(details);
          });
        },
        true
      );
      window.addEventListener("resize", function () {
        root.querySelectorAll("details.pa-viz20-drop").forEach(function (details) {
          var list = metricListFor(details);
          if (list && !list.classList.contains("hidden")) positionViz20MetricDropdown(details);
        });
      });
    }
    var mainPreset = document.getElementById("pa-preset");
    if (mainPreset && !mainPreset.getAttribute("data-viz20-bound")) {
      mainPreset.setAttribute("data-viz20-bound", "1");
      mainPreset.addEventListener("change", syncAllFromMainPreset);
    }
  }

  fetch("/api/play-analytics/viz20/meta", { credentials: "same-origin", cache: "no-store" })
    .then(function (r) {
      return r.json();
    })
    .then(function (meta) {
      metaCache = meta;
      buildUi(meta);
    })
    .catch(function () {
      root.innerHTML = '<p class="text-sm text-rose-600">Viz20 meta yüklenemedi</p>';
    });

  window.addEventListener("seo-theme-change", function () {
    root.querySelectorAll("details[open] .pa-viz20-chart").forEach(function (el) {
      if (el.data && window.Plotly) {
        Plotly.relayout(el, baseLayout());
      }
    });
  });
})();
