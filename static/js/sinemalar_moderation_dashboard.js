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
  var LEGEND_SYNC_CHART_IDS = [
    "mod-chart-daily-volume",
    "mod-chart-weekday",
    "mod-chart-cumulative",
    "mod-chart-focus-profile",
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
        pad: { t: 2, b: 6 },
        font: { size: 12, color: th().tick },
      },
      t
    );
  }

  function modLegendLayout(legendCount, chartWidth) {
    if (!legendCount || legendCount <= 0) return null;
    var entryW = Math.max(84, Math.floor(chartWidth * 0.32));
    var perRow = Math.max(2, Math.floor(chartWidth / entryW));
    var rows = Math.ceil(legendCount / perRow);
    var marginBottom = Math.min(112, 32 + rows * 22);
    return {
      legend: {
        orientation: "h",
        x: 0,
        xanchor: "left",
        y: -0.02,
        yanchor: "top",
        font: { size: 10, color: th().legend },
        tracegroupgap: 4,
        entrywidth: entryW,
        itemwidth: 26,
        groupclick: "toggleitem",
      },
      marginBottom: marginBottom,
    };
  }

  function baseLayout(extra) {
    var t = th();
    var lay = {
      paper_bgcolor: t.paper,
      plot_bgcolor: t.plot,
      font: { family: "Inter, system-ui, sans-serif", size: 11, color: t.text || t.tick },
      margin: { l: 52, r: 20, t: 40, b: 48 },
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
      lay.margin = lay.margin || {};
      lay.margin.t = Math.max(lay.margin.t || 40, 44);
    }

    var legendCount = opts.legendCount || 0;
    if (legendCount > 0) {
      var leg = modLegendLayout(legendCount, w);
      if (leg) {
        lay.legend = Object.assign({}, lay.legend || {}, leg.legend);
        lay.margin.b = Math.max(lay.margin.b || 48, leg.marginBottom);
      }
    }
    var heightOpts = opts.heightOpts || {};
    if (opts.minHeight && el) el.style.minHeight = opts.minHeight + "px";
    if (window.seoPlotlyResolveHeight) {
      lay.height = window.seoPlotlyResolveHeight(el, lay, heightOpts);
    } else if (opts.height) {
      lay.height = opts.height;
    }
    return lay;
  }

  function plotResponsive(el, traces, layout, opts) {
    if (!el || !window.Plotly) return Promise.resolve();
    var lay = responsiveLayout(el, layout, opts || {});
    return Plotly.newPlot(el, traces, lay, plotCfg())
      .then(function () {
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
        xaxis: {
          title: { text: "İş sayısı", standoff: 8 },
          gridcolor: th().grid,
          rangemode: "tozero",
          tickformat: ",.0f",
          automargin: true,
        },
        yaxis: { automargin: true, tickfont: axisTickFont() },
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
          title: "Tarih",
          gridcolor: th().grid,
          tickvals: ticks,
          tickangle: chartW(el) < 520 ? -65 : -40,
          tickfont: axisTickFont(),
          automargin: true,
        },
        yaxis: { title: "Günlük iş", gridcolor: th().grid, tickformat: ",.0f", automargin: true },
      },
      { legendCount: MODS.length, heightOpts: { minPlot: 240, maxTotal: 420, fallback: 320 }, minHeight: 280 }
    );
  }

  function drawActivityHeatmaps() {
    var el = purgePlot("mod-chart-activity-heat");
    if (!el || !window.Plotly) return;
    var days = ANALYTICS.calendar_days || [];
    var cals = ANALYTICS.calendars || {};
    var shownMods = visibleMods();
    if (!days.length || !shownMods.length) return;

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
        heightOpts: {
          minPlot: 36 * shownMods.length,
          maxTotal: 48 + shownMods.length * 40,
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
      { legendCount: METRICS.length, heightOpts: { minPlot: 260, maxTotal: 460, fallback: 340 }, minHeight: 300 }
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
    var radialMax = focusRadialMax(shares);
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
        margin: { l: 48, r: 48, t: 56, b: 24 },
      },
      { legendCount: MODS.length, heightOpts: { minPlot: 300, maxTotal: 460, fallback: 400 }, minHeight: 340 }
    );
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
      { legendCount: MODS.length, heightOpts: { minPlot: 220, maxTotal: 380, fallback: 300 }, minHeight: 260 }
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
          tickvals: ticks,
          tickangle: chartW(el) < 520 ? -65 : -40,
          gridcolor: th().grid,
          tickfont: axisTickFont(),
          automargin: true,
        },
        yaxis: { title: "Biriken iş", gridcolor: th().grid, tickformat: ",.0f", automargin: true },
      },
      { legendCount: MODS.length, heightOpts: { minPlot: 220, maxTotal: 400, fallback: 300 }, minHeight: 260 }
    );
  }

  function drawInactiveSummary() {
    var el = purgePlot("mod-chart-inactive-summary");
    if (!el || !window.Plotly) return;
    var cals = ANALYTICS.calendars || {};
    var names = [];
    var active = [];
    var inactive = [];
    var activeHover = [];
    var inactiveHover = [];
    var hasJoin = false;
    visibleMods().forEach(function (m) {
      var cal = cals[String(m.user_id)] || {};
      names.push(m.username);
      active.push(cal.active_days || 0);
      inactive.push(cal.inactive_days || 0);
      if (cal.joined_at) {
        hasJoin = true;
        activeHover.push(
          m.username + "<br>" + cal.joined_at + " katılım (sonrası)<br>Aktif: %{y} gün<extra></extra>"
        );
        inactiveHover.push(
          m.username + "<br>" + cal.joined_at + " katılım (sonrası)<br>Boş: %{y} gün<extra></extra>"
        );
      } else {
        activeHover.push("Aktif<br>%{x}: %{y} gün<extra></extra>");
        inactiveHover.push("Boş<br>%{x}: %{y} gün<extra></extra>");
      }
    });
    if (!names.length) return;
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
          hovertemplate: activeHover,
        },
        {
          type: "bar",
          name: "İş yapılmayan gün",
          x: names,
          y: inactive,
          marker: { color: "#cbd5e1" },
          hovertemplate: inactiveHover,
        },
      ],
      {
        title: { text: titleText, x: 0, font: { size: 12 } },
        barmode: "stack",
        xaxis: { tickangle: chartW(el) < 480 ? -25 : 0, tickfont: axisTickFont(), automargin: true },
        yaxis: { title: "Gün sayısı", gridcolor: th().grid, automargin: true },
      },
      { legendCount: 2, heightOpts: { minPlot: 200, maxTotal: 340, fallback: 280 }, minHeight: 240 }
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

  function renderCharts() {
    if (!window.Plotly) return;
    if (!ANALYTICS.calendar_days || !ANALYTICS.calendar_days.length) {
      CHART_IDS.forEach(function (id) {
        showChartEmpty(id, "Veri henüz yok — scrape tamamlanınca grafikler dolacak");
      });
      return;
    }
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
      attachModLegendSync();
      setTimeout(resizeAllCharts, 120);
    });
  }

  function attachModLegendSync() {
    LEGEND_SYNC_CHART_IDS.forEach(function (id) {
      var el = document.getElementById(id);
      if (!el || !el.on) return;
      if (typeof el.removeAllListeners === "function") {
        el.removeAllListeners("plotly_legendclick");
      }
      el.on("plotly_legendclick", function (ev) {
        var trace = ev.data[ev.curveNumber];
        var mod = modByUsername(trace.name);
        if (!mod) return true;
        modVisibility[String(mod.user_id)] = !isModVisible(mod.user_id);
        renderCharts();
        return false;
      });
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
      new ResizeObserver(function () {
        clearTimeout(roTimer);
        roTimer = setTimeout(resizeAllCharts, 120);
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
