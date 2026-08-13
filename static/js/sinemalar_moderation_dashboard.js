/**
 * Sinemalar moderasyon — özet tablo, Plotly analitik (responsive), drill-down.
 */
(function () {
  "use strict";

  var DATA_EL = document.getElementById("mod-panel-data");
  if (!DATA_EL) return;

  var RAW;
  try {
    RAW = JSON.parse(DATA_EL.textContent || "{}");
  } catch (e) {
    return;
  }

  var METRICS = RAW.metric_types || [];
  var MODS = RAW.moderators || [];
  var ANALYTICS = RAW.analytics || {};
  var PALETTE = ["#0ea5e9", "#8b5cf6", "#10b981", "#f59e0b", "#ef4444", "#ec4899"];
  /** Sinemalar_Yonetim (53), ivicincim (748975) — varsayılan kapalı; legend tıklanınca tüm grafiklerde açılır */
  var DEFAULT_HIDDEN_USER_IDS = { "53": true, "748975": true };
  var modVisibility = {};
  MODS.forEach(function (m) {
    modVisibility[String(m.user_id)] = !DEFAULT_HIDDEN_USER_IDS[String(m.user_id)];
  });
  var FOCUS_ZOOM_MIN = 0.05;
  var focusZoomBaseMax = null;
  var focusZoomMax = null;
  var focusZoomWheelBound = false;
  var HTML_LEGEND_IDS = [
    "mod-chart-daily-volume",
    "mod-chart-weekday",
    "mod-chart-cumulative",
    "mod-chart-focus-profile",
    "mod-chart-metric-stack",
    "mod-chart-inactive-summary",
  ];
  var CHART_IDS = [
    "mod-chart-rank-total",
    "mod-chart-daily-volume",
    "mod-chart-activity-heat",
    "mod-chart-metric-stack",
    "mod-chart-rank-matrix",
    "mod-chart-focus-profile",
    "mod-chart-weekday",
    "mod-chart-cumulative",
    "mod-chart-inactive-summary",
  ];

  function th() {
    return window.seoPlotlyTheme
      ? window.seoPlotlyTheme()
      : { paper: "#fff", plot: "#fff", text: "#334155", grid: "#e2e8f0", legend: "#64748b", tick: "#64748b" };
  }

  function plotCfg() {
    return { responsive: true, displayModeBar: false, displaylogo: false };
  }

  function chartW(el) {
    if (!el) return 640;
    var w = el.clientWidth || el.offsetWidth || 0;
    if (w < 40 && el.parentElement) w = el.parentElement.clientWidth || 640;
    return Math.max(280, w || 640);
  }

  function fmtNum(n) {
    return Number(n || 0).toLocaleString("tr-TR");
  }

  function tickStep(count) {
    if (count > 120) return 14;
    if (count > 60) return 7;
    if (count > 30) return 3;
    return 1;
  }

  function sparseTicks(days) {
    var step = tickStep(days.length);
    if (step <= 1) return days;
    return days.filter(function (_, i) {
      return i % step === 0 || i === days.length - 1;
    });
  }

  function modTitleStyle(title) {
    if (!title) return null;
    var t = typeof title === "string" ? { text: title } : title;
    if (!t.text) return t;
    return Object.assign(
      {
        x: 0,
        xanchor: "left",
        automargin: true,
        pad: { t: 0, b: 0 },
        font: { size: 11, color: th().tick },
      },
      t
    );
  }

  function legendEsc(s) {
    return String(s || "")
      .replace(/&/g, "&amp;")
      .replace(/</g, "&lt;")
      .replace(/"/g, "&quot;");
  }

  function modAxisMargins(legendOpts) {
    legendOpts = legendOpts || {};
    var hasTitle = legendOpts.hasTitle !== false;
    var marginTop = hasTitle ? 28 : 10;
    var tickPad = 0;
    if (legendOpts.angledX || legendOpts.tickAngle) {
      var angle = Math.abs(legendOpts.tickAngle != null ? legendOpts.tickAngle : 0);
      tickPad = angle >= 60 ? 76 : angle >= 40 ? 56 : angle >= 20 ? 32 : 0;
    }
    var xTitlePad = legendOpts.xaxisTitle ? 18 : 0;
    var marginBottom = 8 + xTitlePad + tickPad + 4;
    return { marginTop: marginTop, marginBottom: marginBottom };
  }

  function modHtmlLegendHeight(legendCount, chartWidth, compact) {
    if (!legendCount) return 0;
    var w = Math.max(chartWidth, 240);
    var perItem = compact ? 68 : 76;
    var perRow = Math.min(legendCount, Math.max(2, Math.floor(w / perItem)));
    var rows = Math.ceil(legendCount / perRow);
    return rows * 22 + 8;
  }

  function clearHtmlLegends() {
    HTML_LEGEND_IDS.forEach(function (id) {
      var el = document.getElementById(id);
      if (!el) return;
      var card = el.closest(".mod-chart-card");
      if (card) {
        var bar = card.querySelector(".mod-chart-legend-bar");
        if (bar) bar.remove();
      }
    });
  }

  function renderHtmlLegend(chartEl, kind) {
    if (!chartEl) return;
    var card = chartEl.closest(".mod-chart-card");
    if (!card) return;
    var bar = card.querySelector(".mod-chart-legend-bar");
    if (!bar) {
      bar = document.createElement("div");
      bar.className = "mod-chart-legend-bar";
      card.appendChild(bar);
    }
    bar.innerHTML = "";

    if (kind === "mods") {
      MODS.forEach(function (m, i) {
        var btn = document.createElement("button");
        btn.type = "button";
        btn.className = "mod-legend-item" + (isModVisible(m.user_id) ? "" : " is-off");
        btn.innerHTML =
          '<span class="mod-legend-swatch" style="background:' +
          modColor(i) +
          '"></span><span>' +
          legendEsc(m.username) +
          "</span>";
        btn.addEventListener("click", function () {
          modVisibility[String(m.user_id)] = !isModVisible(m.user_id);
          renderCharts();
        });
        bar.appendChild(btn);
      });
      return;
    }

    if (kind === "metrics") {
      METRICS.forEach(function (mt, i) {
        var btn = document.createElement("button");
        btn.type = "button";
        btn.className = "mod-legend-item";
        btn.innerHTML =
          '<span class="mod-legend-swatch" style="background:' +
          PALETTE[i % PALETTE.length] +
          '"></span><span>' +
          legendEsc(mt.label) +
          "</span>";
        btn.addEventListener("click", function () {
          if (!chartEl.data || !chartEl.data[i]) return;
          var vis = chartEl.data[i].visible;
          var newVis = vis === "legendonly" || vis === false ? true : "legendonly";
          Plotly.restyle(chartEl, { visible: newVis }, [i]);
          btn.classList.toggle("is-off", newVis === "legendonly" || newVis === false);
        });
        bar.appendChild(btn);
      });
      return;
    }

    if (kind === "binary") {
      [
        { name: "Aktif gün", color: "#0ea5e9" },
        { name: "İş yapılmayan gün", color: "#cbd5e1" },
      ].forEach(function (item) {
        var span = document.createElement("span");
        span.className = "mod-legend-item mod-legend-static";
        span.innerHTML =
          '<span class="mod-legend-swatch" style="background:' +
          item.color +
          '"></span><span>' +
          legendEsc(item.name) +
          "</span>";
        bar.appendChild(span);
      });
    }
  }

  function modChartHeight(el, lay, heightOpts) {
    heightOpts = heightOpts || {};
    var minPlot = heightOpts.minPlot != null ? heightOpts.minPlot : 160;
    var card = el && el.closest ? el.closest(".mod-chart-card") : null;
    var legendReserve = heightOpts.legendReserve || 0;
    var cardPad = 28;
    var containerH = 0;
    if (card && card.clientHeight > 72) {
      containerH = Math.max(minPlot, card.clientHeight - legendReserve - cardPad);
    } else if (el && el.clientHeight > 72) {
      containerH = el.clientHeight;
    } else if (heightOpts.minHeight) {
      containerH = Math.max(minPlot, heightOpts.minHeight - legendReserve - cardPad);
    } else {
      containerH = heightOpts.fallback || 280;
    }

    var m = (lay && lay.margin) || {};
    var need = minPlot + (m.t || 28) + (m.b || 40);
    var h = Math.max(need, containerH);
    if (heightOpts.maxTotal) h = Math.min(heightOpts.maxTotal, h);
    h = Math.max(h, need);
    if (el) {
      el.style.height = h + "px";
      el.style.minHeight = Math.max((heightOpts.minPlot || 160), h) + "px";
      el.style.flex = "1 1 auto";
    }
    return h;
  }

  function baseLayout(extra) {
    var t = th();
    var lay = {
      paper_bgcolor: t.paper,
      plot_bgcolor: t.plot,
      font: { family: "Inter, system-ui, sans-serif", size: 11, color: t.text || t.tick },
      margin: { l: 44, r: 8, t: 20, b: 28 },
      autosize: true,
      uniformtext: { mode: "hide", minsize: 9 },
    };
    if (extra) {
      Object.keys(extra).forEach(function (k) {
        lay[k] = extra[k];
      });
    }
    return lay;
  }

  function responsiveLayout(el, layout, opts) {
    opts = opts || {};
    var w = chartW(el);
    var lay = baseLayout(layout);

    if (lay.title) {
      lay.title = modTitleStyle(lay.title);
    }

    var legendCount = opts.legendCount || 0;
    var legOpts = Object.assign({ hasTitle: !!lay.title }, opts.legendOpts || {});
    if (opts.htmlLegend) {
      var axis = modAxisMargins(legOpts);
      lay.showlegend = false;
      lay.margin = lay.margin || {};
      lay.margin.t = Math.max(lay.margin.t || 20, axis.marginTop);
      lay.margin.b = Math.max(lay.margin.b || 28, axis.marginBottom);
    } else if (legendCount > 0) {
      var axisOnly = modAxisMargins(legOpts);
      lay.showlegend = false;
      lay.margin = lay.margin || {};
      lay.margin.t = Math.max(lay.margin.t || 20, axisOnly.marginTop);
      lay.margin.b = Math.max(lay.margin.b || 28, axisOnly.marginBottom);
    }

    var heightOpts = Object.assign({}, opts.heightOpts || {});
    if (opts.htmlLegend && legendCount > 0) {
      heightOpts.legendReserve = modHtmlLegendHeight(legendCount, w, legOpts.compactLegend);
    }
    if (opts.fillContainer !== false) {
      lay.height = modChartHeight(el, lay, Object.assign({ minHeight: opts.minHeight }, heightOpts));
    } else if (window.seoPlotlyResolveHeight) {
      lay.height = window.seoPlotlyResolveHeight(el, lay, heightOpts);
    } else {
      lay.height = heightOpts.fallback || 320;
    }
    return lay;
  }

  function plotResponsive(el, traces, layout, opts) {
    if (!el || !window.Plotly) return Promise.resolve();
    var lay = responsiveLayout(el, layout, opts || {});
    return Plotly.newPlot(el, traces, lay, plotCfg())
      .then(function () {
        if (opts.htmlLegend) renderHtmlLegend(el, opts.htmlLegend);
        try {
          Plotly.Plots.resize(el);
        } catch (_) {}
      })
      .catch(function (err) {
        console.error("[mod-chart]", el.id, err);
        el.innerHTML =
          '<p class="flex h-full min-h-[180px] items-center justify-center px-3 text-center text-xs text-rose-600 dark:text-rose-400">Grafik yüklenemedi</p>';
      });
  }

  function purgePlot(id) {
    var el = document.getElementById(id);
    if (el && window.Plotly) {
      try {
        Plotly.purge(el);
      } catch (_) {}
    }
    return el;
  }

  function modColor(i) {
    return PALETTE[i % PALETTE.length];
  }

  function modColorRgba(i, alpha) {
    var hex = modColor(i).replace("#", "");
    if (hex.length !== 6) return modColor(i);
    var r = parseInt(hex.slice(0, 2), 16);
    var g = parseInt(hex.slice(2, 4), 16);
    var b = parseInt(hex.slice(4, 6), 16);
    return "rgba(" + r + "," + g + "," + b + "," + alpha + ")";
  }

  function focusRadialMax(shares) {
    var maxR = 0;
    visibleMods().forEach(function (m) {
      var s = shares[String(m.user_id)] || {};
      METRICS.forEach(function (mt) {
        maxR = Math.max(maxR, Number(s[mt.key] || 0));
      });
    });
    if (maxR <= 0) return 100;
    return Math.min(100, Math.max(28, Math.ceil((maxR * 1.12) / 5) * 5));
  }

  function focusZoomLabel(maxVal) {
    if (maxVal >= 10) return "0–" + Math.round(maxVal) + "%";
    if (maxVal >= 1) return "0–" + maxVal.toFixed(1) + "%";
    return "0–" + maxVal.toFixed(2) + "%";
  }

  function updateFocusZoomLabel() {
    var label = document.getElementById("mod-focus-zoom-label");
    if (label && focusZoomMax != null) label.textContent = focusZoomLabel(focusZoomMax);
  }

  function applyFocusZoom(newMax, redraw) {
    if (focusZoomBaseMax == null) return;
    focusZoomMax = Math.max(FOCUS_ZOOM_MIN, Math.min(focusZoomBaseMax, newMax));
    updateFocusZoomLabel();
    var el = document.getElementById("mod-chart-focus-profile");
    if (!redraw && el && el.querySelector(".js-plotly-plot") && window.Plotly) {
      Plotly.relayout(el, { "polar.radialaxis.range": [0, focusZoomMax] });
      return;
    }
    drawFocusProfile();
  }

  function setupFocusZoomToolbar() {
    var zoomIn = document.getElementById("mod-focus-zoom-in");
    var zoomOut = document.getElementById("mod-focus-zoom-out");
    var zoomReset = document.getElementById("mod-focus-zoom-reset");
    var chartEl = document.getElementById("mod-chart-focus-profile");
    if (zoomIn && !zoomIn.__modBound) {
      zoomIn.__modBound = true;
      zoomIn.addEventListener("click", function () {
        applyFocusZoom(focusZoomMax * 0.65);
      });
    }
    if (zoomOut && !zoomOut.__modBound) {
      zoomOut.__modBound = true;
      zoomOut.addEventListener("click", function () {
        applyFocusZoom(focusZoomMax / 0.65);
      });
    }
    if (zoomReset && !zoomReset.__modBound) {
      zoomReset.__modBound = true;
      zoomReset.addEventListener("click", function () {
        applyFocusZoom(focusZoomBaseMax, true);
      });
    }
    if (chartEl && !focusZoomWheelBound) {
      focusZoomWheelBound = true;
      chartEl.addEventListener(
        "wheel",
        function (e) {
          if (!focusZoomBaseMax || focusZoomMax == null) return;
          e.preventDefault();
          var factor = e.deltaY < 0 ? 0.82 : 1 / 0.82;
          applyFocusZoom(focusZoomMax * factor);
        },
        { passive: false }
      );
    }
  }

  function modIndex(uid) {
    for (var i = 0; i < MODS.length; i++) {
      if (String(MODS[i].user_id) === String(uid)) return i;
    }
    return 0;
  }

  function isModVisible(uid) {
    return modVisibility[String(uid)] !== false;
  }

  function modTraceVisible(m) {
    return isModVisible(m.user_id) ? true : "legendonly";
  }

  function visibleMods() {
    return MODS.filter(function (m) {
      return isModVisible(m.user_id);
    });
  }

  function modByUsername(name) {
    for (var i = 0; i < MODS.length; i++) {
      if (MODS[i].username === name) return MODS[i];
    }
    return null;
  }

  function axisTickFont() {
    var w = window.innerWidth || 1024;
    return { size: w < 480 ? 9 : w < 768 ? 10 : 11, color: th().tick };
  }

  function drawRankTotal() {
    var el = purgePlot("mod-chart-rank-total");
    if (!el || !window.Plotly) return;
    var rank = (ANALYTICS.overall_rank || []).filter(function (r) {
      return isModVisible(r.user_id);
    });
    if (!rank.length) return;
    var names = rank.map(function (r) {
      return "#" + r.rank + " " + r.username;
    });
    var counts = rank.map(function (r) {
      return r.count;
    });
    var colors = rank.map(function (r) {
      return modColor(modIndex(r.user_id));
    });
    var revNames = names.slice().reverse();
    var revCounts = counts.slice().reverse();
    var revColors = colors.slice().reverse();
    plotResponsive(
      el,
      [
        {
          type: "bar",
          orientation: "h",
          y: revNames,
          x: revCounts,
          marker: { color: revColors },
          hovertemplate: "%{y}<br>%{x:,} iş<extra></extra>",
        },
      ],
      {
        title: { text: "Dönem toplamı · moderatör sıralaması", x: 0, font: { size: 12 } },
        hovermode: "y",
        xaxis: {
          title: { text: "İş sayısı", standoff: 8 },
          gridcolor: th().grid,
          rangemode: "tozero",
          tickformat: ",.0f",
          automargin: true,
        },
        yaxis: { automargin: true, tickfont: axisTickFont(), type: "category" },
        margin: { l: 8, r: 16, t: 44, b: 40 },
      },
      { heightOpts: { minPlot: 180, maxTotal: 360, fallback: 260 }, minHeight: 220 }
    );
  }

  function drawDailyVolume() {
    var el = purgePlot("mod-chart-daily-volume");
    if (!el || !window.Plotly) return;
    var days = ANALYTICS.calendar_days || [];
    if (!days.length) return;
    var ticks = sparseTicks(days);
    var traces = MODS.map(function (m, i) {
      var series = (ANALYTICS.daily_by_user || {})[String(m.user_id)] || [];
      return {
        type: "scatter",
        mode: "lines",
        stackgroup: "one",
        name: m.username,
        visible: modTraceVisible(m),
        x: days,
        y: series,
        line: { width: 1.2, color: modColor(i) },
        fillcolor: modColor(i),
        hovertemplate: "%{x}<br>" + m.username + ": %{y:,}<extra></extra>",
      };
    });
    plotResponsive(
      el,
      traces,
      {
        title: { text: "Günlük moderasyon hacmi · moderatör kırılımı", x: 0, font: { size: 12 } },
        xaxis: {
          title: { text: "Tarih", standoff: 10 },
          gridcolor: th().grid,
          tickvals: ticks,
          tickangle: chartW(el) < 520 ? -65 : -40,
          tickfont: axisTickFont(),
          automargin: true,
        },
        yaxis: { title: "Günlük iş", gridcolor: th().grid, tickformat: ",.0f", automargin: true },
      },
      {
        legendCount: MODS.length,
        htmlLegend: "mods",
        legendOpts: {
          angledX: true,
          xaxisTitle: true,
          tickAngle: chartW(el) < 520 ? -65 : -40,
        },
        heightOpts: { minPlot: 160, maxTotal: 800, fallback: 280 },
        minHeight: 280,
      }
    );
  }

  function drawActivityHeatmaps() {
    var el = purgePlot("mod-chart-activity-heat");
    if (!el || !window.Plotly) return;
    var days = ANALYTICS.calendar_days || [];
    var cals = ANALYTICS.calendars || {};
    var shownMods = visibleMods();
    if (!days.length) {
      showChartEmpty("mod-chart-activity-heat", "Takvim verisi yok");
      return;
    }
    if (!shownMods.length) {
      showChartEmpty("mod-chart-activity-heat", "Görünür moderatör yok — legenddan seçin");
      return;
    }

    var minW = Math.max(chartW(el), Math.min(days.length * 10, 1400));
    el.style.minWidth = minW + "px";

    var ticks = sparseTicks(days);
    var yLabels = shownMods.map(function (m) {
      return m.username;
    });
    var zMatrix = shownMods.map(function (m) {
      var cal = cals[String(m.user_id)] || { days: [] };
      return days.map(function (day) {
        var found = cal.days.filter(function (d) {
          return d.date === day;
        })[0];
        return found ? found.count : 0;
      });
    });

    plotResponsive(
      el,
      [
        {
          type: "heatmap",
          x: days,
          y: yLabels,
          z: zMatrix,
          xgap: 1,
          ygap: 2,
          colorscale: [
            [0, "#f1f5f9"],
            [0.001, "#bae6fd"],
            [0.35, "#38bdf8"],
            [0.7, "#0284c7"],
            [1, "#0c4a6e"],
          ],
          colorbar: { title: { text: "iş/gün" }, len: 0.45, thickness: 12 },
          hovertemplate: "%{y}<br>%{x}<br>%{z:,} iş<extra></extra>",
        },
      ],
      {
        title: {
          text: "Aktivite takvimi · boş günler açık gri (0 iş)",
          x: 0,
          font: { size: 12 },
        },
        xaxis: { tickvals: ticks, tickangle: -90, tickfont: { size: 8 }, automargin: true },
        yaxis: { automargin: true, tickfont: axisTickFont() },
        margin: { l: 96, r: 48, t: 44, b: 72 },
      },
      {
        fillContainer: false,
        heightOpts: {
          minPlot: 36 * shownMods.length,
          maxTotal: Math.max(220, 56 + shownMods.length * 44),
          fallback: 40 + shownMods.length * 36,
        },
        minHeight: 40 + shownMods.length * 36,
      }
    );
  }

  function drawMetricStack() {
    var el = purgePlot("mod-chart-metric-stack");
    if (!el || !window.Plotly) return;
    var users = (RAW.users || []).filter(function (u) {
      return isModVisible(u.user_id);
    });
    if (!users.length) return;
    var names = users.map(function (u) {
      return u.username;
    });
    var traces = METRICS.map(function (mt, mi) {
      return {
        type: "bar",
        name: mt.label,
        x: names,
        y: users.map(function (u) {
          return (u.totals || {})[mt.key] || 0;
        }),
        marker: { color: PALETTE[mi % PALETTE.length] },
        hovertemplate: "%{x}<br>" + mt.label + ": %{y:,}<extra></extra>",
      };
    });
    plotResponsive(
      el,
      traces,
      {
        title: { text: "İş türü dağılımı · moderatör bazında", x: 0, font: { size: 12 } },
        barmode: "stack",
        xaxis: {
          tickangle: chartW(el) < 480 ? -35 : -20,
          tickfont: axisTickFont(),
          automargin: true,
        },
        yaxis: { title: "Adet", gridcolor: th().grid, tickformat: ",.0f", automargin: true },
      },
      {
        legendCount: METRICS.length,
        htmlLegend: "metrics",
        legendOpts: {
          angledX: true,
          compactLegend: true,
          tickAngle: chartW(el) < 480 ? -35 : -20,
        },
        heightOpts: { minPlot: 160, maxTotal: 800, fallback: 280 },
        minHeight: 280,
      }
    );
  }

  function drawRankMatrix() {
    var el = purgePlot("mod-chart-rank-matrix");
    if (!el || !window.Plotly) return;
    var rankings = ANALYTICS.rankings_by_metric || {};
    var shownMods = visibleMods();
    if (!shownMods.length) return;
    var yLabels = shownMods.map(function (m) {
      return m.username;
    });
    var xLabels = METRICS.map(function (m) {
      return m.label;
    });
    var z = shownMods.map(function (m) {
      return METRICS.map(function (mt) {
        var list = rankings[mt.key] || [];
        for (var i = 0; i < list.length; i++) {
          if (String(list[i].user_id) === String(m.user_id)) return list[i].rank;
        }
        return MODS.length + 1;
      });
    });
    var text = shownMods.map(function (m, yi) {
      return METRICS.map(function (mt, xi) {
        var v = z[yi][xi];
        return v && v <= MODS.length ? "#" + v : "—";
      });
    });
    el.style.minWidth = Math.max(chartW(el), xLabels.length * 52) + "px";
    plotResponsive(
      el,
      [
        {
          type: "heatmap",
          x: xLabels,
          y: yLabels,
          z: z,
          text: text,
          texttemplate: "%{text}",
          textfont: { size: chartW(el) < 480 ? 8 : 10 },
          colorscale: [
            [0, "#f8fafc"],
            [0.2, "#fde68a"],
            [0.5, "#fb923c"],
            [1, "#dc2626"],
          ],
          reversescale: true,
          hovertemplate: "%{y} · %{x}<br>Sıra: %{text}<extra></extra>",
        },
      ],
      {
        title: { text: "Metrik bazında liderlik sırası (#1 en iyi)", x: 0, font: { size: 12 } },
        xaxis: { tickangle: -35, tickfont: axisTickFont(), automargin: true },
        yaxis: { automargin: true, tickfont: axisTickFont() },
        margin: { l: 88, r: 16, t: 44, b: 88 },
      },
      { heightOpts: { minPlot: 220, maxTotal: 380, fallback: 300 }, minHeight: 260 }
    );
  }

  function drawFocusProfile() {
    var el = purgePlot("mod-chart-focus-profile");
    if (!el || !window.Plotly) return;
    var shares = ANALYTICS.shares_by_metric || {};
    var baseMax = focusRadialMax(shares);
    if (focusZoomBaseMax == null || focusZoomMax == null) {
      focusZoomBaseMax = baseMax;
      focusZoomMax = baseMax;
    } else {
      var zoomRatio = focusZoomMax / focusZoomBaseMax;
      focusZoomBaseMax = baseMax;
      focusZoomMax = Math.max(FOCUS_ZOOM_MIN, Math.min(baseMax, baseMax * zoomRatio));
    }
    var radialMax = focusZoomMax;
    var traces = MODS.map(function (m, i) {
      var s = shares[String(m.user_id)] || {};
      return {
        type: "scatterpolar",
        name: m.username,
        visible: modTraceVisible(m),
        r: METRICS.map(function (mt) {
          return s[mt.key] || 0;
        }),
        theta: METRICS.map(function (mt) {
          return mt.label;
        }),
        fill: "toself",
        fillcolor: modColorRgba(i, 0.48),
        line: { color: modColor(i), width: 2.5 },
        hovertemplate: m.username + "<br>%{theta}: %{r:.1f}%<extra></extra>",
      };
    });
    plotResponsive(
      el,
      traces,
      {
        title: { text: "Odak profili · iş türü payı (%)", x: 0, font: { size: 12 } },
        polar: {
          radialaxis: {
            ticksuffix: "%",
            gridcolor: th().grid,
            tickfont: { size: 9 },
            range: [0, radialMax],
            angle: 90,
          },
          angularaxis: { tickfont: { size: chartW(el) < 480 ? 8 : 9 }, rotation: 90 },
          bgcolor: "rgba(0,0,0,0)",
        },
        margin: { l: 48, r: 48, t: 56, b: 48 },
      },
      { legendCount: MODS.length, htmlLegend: "mods", heightOpts: { minPlot: 180, maxTotal: 800, fallback: 300 }, minHeight: 300 }
    ).then(function () {
      setupFocusZoomToolbar();
      updateFocusZoomLabel();
    });
  }

  function drawWeekday() {
    var el = purgePlot("mod-chart-weekday");
    if (!el || !window.Plotly) return;
    var labels = ANALYTICS.weekday_labels || ["Pzt", "Sal", "Çar", "Per", "Cum", "Cmt", "Paz"];
    var traces = MODS.map(function (m, i) {
      var w = (ANALYTICS.weekday_by_user || {})[String(m.user_id)] || [];
      return {
        type: "bar",
        name: m.username,
        visible: modTraceVisible(m),
        x: labels,
        y: w,
        marker: { color: modColor(i) },
        hovertemplate: m.username + "<br>%{x}: %{y:,} iş<extra></extra>",
      };
    });
    plotResponsive(
      el,
      traces,
      {
        title: { text: "Haftanın günü · iş dağılımı", x: 0, font: { size: 12 } },
        barmode: "group",
        bargap: chartW(el) < 480 ? 0.15 : 0.3,
        xaxis: { automargin: true, tickfont: axisTickFont() },
        yaxis: { title: "Toplam iş", gridcolor: th().grid, tickformat: ",.0f", automargin: true },
      },
      { legendCount: MODS.length, htmlLegend: "mods", heightOpts: { minPlot: 160, maxTotal: 800, fallback: 280 }, minHeight: 280 }
    );
  }

  function drawCumulative() {
    var el = purgePlot("mod-chart-cumulative");
    if (!el || !window.Plotly) return;
    var days = ANALYTICS.calendar_days || [];
    var cum = ANALYTICS.cumulative_by_user || {};
    if (!days.length) return;
    var ticks = sparseTicks(days);
    var traces = MODS.map(function (m, i) {
      var series = cum[String(m.user_id)] || [];
      return {
        type: "scatter",
        mode: "lines",
        name: m.username,
        visible: modTraceVisible(m),
        x: days,
        y: series.map(function (p) {
          return p.cumulative;
        }),
        line: { color: modColor(i), width: 2 },
        hovertemplate: m.username + "<br>%{x}<br>Birikim: %{y:,}<extra></extra>",
      };
    });
    plotResponsive(
      el,
      traces,
      {
        title: { text: "Kümülatif katkı · dönem içi birikim", x: 0, font: { size: 12 } },
        xaxis: {
          title: { text: "Tarih", standoff: 10 },
          tickvals: ticks,
          tickangle: chartW(el) < 520 ? -65 : -40,
          gridcolor: th().grid,
          tickfont: axisTickFont(),
          automargin: true,
        },
        yaxis: { title: "Biriken iş", gridcolor: th().grid, tickformat: ",.0f", automargin: true },
      },
      {
        legendCount: MODS.length,
        htmlLegend: "mods",
        legendOpts: {
          angledX: true,
          xaxisTitle: true,
          tickAngle: chartW(el) < 520 ? -65 : -40,
        },
        heightOpts: { minPlot: 160, maxTotal: 800, fallback: 280 },
        minHeight: 280,
      }
    );
  }

  function drawInactiveSummary() {
    var el = purgePlot("mod-chart-inactive-summary");
    if (!el || !window.Plotly) return;
    var cals = ANALYTICS.calendars || {};
    var names = [];
    var active = [];
    var inactive = [];
    var hasJoin = false;
    visibleMods().forEach(function (m) {
      var cal = cals[String(m.user_id)] || {};
      names.push(m.username);
      active.push(cal.active_days || 0);
      inactive.push(cal.inactive_days || 0);
      if (cal.joined_at) hasJoin = true;
    });
    if (!names.length) {
      showChartEmpty("mod-chart-inactive-summary", "Görünür moderatör yok — legenddan seçin");
      return;
    }
    var titleText =
      "Çalışılan vs boş gün · " +
      (RAW.start || "") +
      " → " +
      (RAW.end || "") +
      (hasJoin ? " · katılım öncesi günler hariç" : "");
    plotResponsive(
      el,
      [
        {
          type: "bar",
          name: "Aktif gün",
          x: names,
          y: active,
          marker: { color: "#0ea5e9" },
          hovertemplate: "%{x}<br>Aktif: %{y} gün<extra></extra>",
        },
        {
          type: "bar",
          name: "İş yapılmayan gün",
          x: names,
          y: inactive,
          marker: { color: "#cbd5e1" },
          hovertemplate: "%{x}<br>Boş: %{y} gün<extra></extra>",
        },
      ],
      {
        title: { text: titleText, x: 0, font: { size: 12 } },
        barmode: "stack",
        xaxis: { tickangle: chartW(el) < 480 ? -25 : 0, tickfont: axisTickFont(), automargin: true },
        yaxis: { title: "Gün sayısı", gridcolor: th().grid, automargin: true },
      },
      {
        legendCount: 2,
        htmlLegend: "binary",
        legendOpts: { tickAngle: chartW(el) < 480 ? -25 : 0 },
        heightOpts: { minPlot: 160, maxTotal: 800, fallback: 280 },
        minHeight: 260,
      }
    );
  }

  function showChartEmpty(id, msg) {
    var el = document.getElementById(id);
    if (!el) return;
    el.innerHTML =
      '<p class="flex h-full min-h-[180px] items-center justify-center px-3 text-center text-xs text-slate-400 dark:text-slate-500">' +
      (msg || "Veri yok") +
      "</p>";
  }

  function renderCharts(secondPass) {
    if (!window.Plotly) return;
    if (!ANALYTICS.calendar_days || !ANALYTICS.calendar_days.length) {
      CHART_IDS.forEach(function (id) {
        showChartEmpty(id, "Veri henüz yok — scrape tamamlanınca grafikler dolacak");
      });
      return;
    }
    clearHtmlLegends();
    var drawers = [
      drawRankTotal,
      drawInactiveSummary,
      drawDailyVolume,
      drawActivityHeatmaps,
      drawMetricStack,
      drawWeekday,
      drawRankMatrix,
      drawFocusProfile,
      drawCumulative,
    ];
    var chain = Promise.resolve();
    drawers.forEach(function (drawFn) {
      chain = chain.then(function () {
        return new Promise(function (resolve) {
          requestAnimationFrame(function () {
            try {
              drawFn();
            } catch (err) {
              console.error("[mod-chart] draw failed", err);
            }
            setTimeout(resolve, 40);
          });
        });
      });
    });
    return chain.then(function () {
      setTimeout(resizeAllCharts, 120);
      if (!secondPass) {
        setTimeout(function () {
          renderCharts(true);
        }, 200);
      }
    });
  }

  function scheduleCharts() {
    function runWhenReady() {
      if (!window.Plotly) return false;
      requestAnimationFrame(function () {
        requestAnimationFrame(function () {
          renderCharts();
        });
      });
      return true;
    }
    if (runWhenReady()) return;
    var tries = 0;
    var t = setInterval(function () {
      tries += 1;
      if (runWhenReady() || tries > 40) clearInterval(t);
    }, 150);
  }

  function resizeAllCharts() {
    CHART_IDS.forEach(function (id) {
      var el = document.getElementById(id);
      if (el && window.Plotly && el.querySelector(".js-plotly-plot")) {
        try {
          Plotly.Plots.resize(el);
        } catch (_) {}
      }
    });
  }

  if (typeof ResizeObserver !== "undefined") {
    var chartsRoot = document.getElementById("mod-charts");
    if (chartsRoot) {
      var roTimer;
      var lastChartShellW = chartsRoot.clientWidth || 0;
      new ResizeObserver(function () {
        clearTimeout(roTimer);
        roTimer = setTimeout(function () {
          var w = chartsRoot.clientWidth || 0;
          if (lastChartShellW && Math.abs(w - lastChartShellW) > 48) {
            lastChartShellW = w;
            renderCharts();
          } else {
            lastChartShellW = w || lastChartShellW;
            resizeAllCharts();
          }
        }, 180);
      }).observe(chartsRoot);
    }
  }

  /* —— drill-down —— */
  var panel = document.getElementById("mod-drill-panel");
  var titleEl = document.getElementById("mod-drill-title");
  var subEl = document.getElementById("mod-drill-sub");
  var bodyEl = document.getElementById("mod-drill-body");
  var footEl = document.getElementById("mod-drill-foot");
  var closeBtn = document.getElementById("mod-drill-close");

  function esc(s) {
    return String(s || "").replace(/&/g, "&amp;").replace(/</g, "&lt;").replace(/"/g, "&quot;");
  }

  function loadDrill(userId, username, metricType, metricLabel, count) {
    if (!panel || !bodyEl) return;
    panel.classList.remove("hidden");
    if (titleEl) titleEl.textContent = username + " · " + metricLabel;
    if (subEl) subEl.textContent = (RAW.start || "") + " → " + (RAW.end || "") + " · " + count + " iş";
    bodyEl.innerHTML = '<tr><td colspan="4" class="px-4 py-8 text-center text-slate-400">Yükleniyor…</td></tr>';
    panel.scrollIntoView({ behavior: "smooth", block: "nearest" });

    var qs = new URLSearchParams({
      start: RAW.start || "",
      end: RAW.end || "",
      user_id: String(userId),
      metric_type: metricType,
      limit: "500",
    });

    fetch("/api/sinemalar-moderation/details?" + qs.toString(), { credentials: "same-origin" })
      .then(function (r) {
        return r.json();
      })
      .then(function (data) {
        var items = data.items || [];
        if (!items.length) {
          bodyEl.innerHTML = '<tr><td colspan="4" class="px-4 py-8 text-center text-slate-400">Kayıt yok</td></tr>';
          if (footEl) footEl.textContent = "0 kayıt";
          return;
        }
        bodyEl.innerHTML = items
          .map(function (it) {
            var link = it.admin_url
              ? '<a href="' + esc(it.admin_url) + '" target="_blank" rel="noopener" class="text-sky-600 hover:underline">Panel</a>'
              : "—";
            return (
              "<tr class=\"border-b border-slate-100 dark:border-slate-800\">" +
              "<td class=\"px-3 py-1.5 font-mono text-[11px] whitespace-nowrap\">" +
              esc(it.event_at) +
              "</td>" +
              "<td class=\"px-3 py-1.5 max-w-[280px] truncate\" title=\"" +
              esc(it.title) +
              "\">" +
              esc(it.title || "—") +
              "</td>" +
              "<td class=\"px-3 py-1.5 max-w-[180px] truncate text-slate-500\">" +
              esc(it.subtitle || "—") +
              "</td>" +
              "<td class=\"px-3 py-1.5 text-center\">" +
              link +
              "</td>" +
              "</tr>"
            );
          })
          .join("");
        if (footEl) footEl.textContent = items.length + " / " + (data.total || items.length) + " kayıt";
      })
      .catch(function () {
        bodyEl.innerHTML = '<tr><td colspan="4" class="px-4 py-8 text-center text-rose-500">Yüklenemedi</td></tr>';
      });
  }

  document.querySelectorAll(".mod-count-btn").forEach(function (btn) {
    btn.addEventListener("click", function () {
      loadDrill(
        btn.getAttribute("data-user-id"),
        btn.getAttribute("data-username"),
        btn.getAttribute("data-metric"),
        btn.getAttribute("data-metric-label"),
        btn.getAttribute("data-count")
      );
    });
  });

  if (closeBtn && panel) {
    closeBtn.addEventListener("click", function () {
      panel.classList.add("hidden");
    });
  }

  scheduleCharts();
  window.addEventListener("resize", function () {
    clearTimeout(window.__modChartResizeT);
    window.__modChartResizeT = setTimeout(resizeAllCharts, 150);
  });
})();
