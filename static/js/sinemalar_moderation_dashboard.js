/**
 * Sinemalar moderasyon — özet tablo, Plotly analitik, drill-down.
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

  function th() {
    return window.seoPlotlyTheme
      ? window.seoPlotlyTheme()
      : { paper: "#fff", plot: "#fff", text: "#334155", grid: "#e2e8f0", legend: "#64748b" };
  }

  function plotCfg() {
    return { responsive: true, displayModeBar: false, displaylogo: false };
  }

  function baseLayout(extra) {
    var t = th();
    var lay = {
      paper_bgcolor: t.paper,
      plot_bgcolor: t.plot,
      font: { family: "Inter, system-ui, sans-serif", size: 11, color: t.text },
      margin: { l: 48, r: 16, t: 36, b: 40 },
      legend: { orientation: "h", y: 1.12, x: 0, font: { size: 10, color: t.legend } },
    };
    if (extra) {
      Object.keys(extra).forEach(function (k) {
        lay[k] = extra[k];
      });
    }
    return lay;
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

  function modIndex(uid) {
    for (var i = 0; i < MODS.length; i++) {
      if (String(MODS[i].user_id) === String(uid)) return i;
    }
    return 0;
  }

  function drawRankTotal() {
    var el = purgePlot("mod-chart-rank-total");
    if (!el || !window.Plotly) return;
    var rank = ANALYTICS.overall_rank || [];
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
    Plotly.newPlot(
      el,
      [
        {
          type: "bar",
          orientation: "h",
          y: names.slice().reverse(),
          x: counts.slice().reverse(),
          marker: { color: colors.slice().reverse() },
          text: counts.slice().reverse().map(String),
          textposition: "outside",
          hovertemplate: "%{y}<br>%{x} iş<extra></extra>",
        },
      ],
      baseLayout({
        title: { text: "Dönem toplamı · moderatör sıralaması", x: 0, font: { size: 12 } },
        xaxis: { title: "İş sayısı", gridcolor: th().grid },
        yaxis: { automargin: true },
        height: 260,
      }),
      plotCfg()
    );
  }

  function drawDailyVolume() {
    var el = purgePlot("mod-chart-daily-volume");
    if (!el || !window.Plotly) return;
    var days = ANALYTICS.calendar_days || [];
    if (!days.length) return;
    var traces = MODS.map(function (m, i) {
      var series = (ANALYTICS.daily_by_user || {})[String(m.user_id)] || [];
      return {
        type: "scatter",
        mode: "lines",
        stackgroup: "one",
        name: m.username,
        x: days,
        y: series,
        line: { width: 1.2, color: modColor(i) },
        fillcolor: modColor(i),
        hovertemplate: "%{x}<br>" + m.username + ": %{y}<extra></extra>",
      };
    });
    Plotly.newPlot(
      el,
      traces,
      baseLayout({
        title: { text: "Günlük moderasyon hacmi · moderatör kırılımı", x: 0, font: { size: 12 } },
        xaxis: { title: "Tarih", gridcolor: th().grid, tickangle: -45 },
        yaxis: { title: "Günlük iş", gridcolor: th().grid },
        height: 320,
      }),
      plotCfg()
    );
  }

  function drawActivityHeatmaps() {
    var el = purgePlot("mod-chart-activity-heat");
    if (!el || !window.Plotly) return;
    var days = ANALYTICS.calendar_days || [];
    var cals = ANALYTICS.calendars || {};
    if (!days.length || !MODS.length) return;

    var traces = [];
    MODS.forEach(function (m, mi) {
      var cal = cals[String(m.user_id)] || { days: [] };
      var zRow = days.map(function (day) {
        var found = cal.days.filter(function (d) {
          return d.date === day;
        })[0];
        return found ? found.count : 0;
      });
      traces.push({
        type: "heatmap",
        x: days,
        y: [m.username],
        z: [zRow],
        xgap: 1,
        ygap: 1,
        colorscale: [
          [0, "#f1f5f9"],
          [0.001, "#bae6fd"],
          [0.35, "#38bdf8"],
          [0.7, "#0284c7"],
          [1, "#0c4a6e"],
        ],
        showscale: mi === 0,
        colorbar: mi === 0 ? { title: "iş/gün", len: 0.5 } : undefined,
        hovertemplate: m.username + "<br>%{x}<br>%{z} iş<extra></extra>",
      });
    });

    Plotly.newPlot(
      el,
      traces,
      baseLayout({
        title: {
          text: "Aktivite takvimi · boş günler açık gri (0 iş)",
          x: 0,
          font: { size: 12 },
        },
        xaxis: { tickangle: -90, tickfont: { size: 8 } },
        yaxis: { automargin: true },
        height: 40 + MODS.length * 36,
        margin: { l: 100, r: 16, t: 40, b: 80 },
      }),
      plotCfg()
    );
  }

  function drawMetricStack() {
    var el = purgePlot("mod-chart-metric-stack");
    if (!el || !window.Plotly) return;
    var users = RAW.users || [];
    if (!users.length) return;
    var traces = METRICS.map(function (mt, mi) {
      return {
        type: "bar",
        name: mt.label,
        x: users.map(function (u) {
          return u.username;
        }),
        y: users.map(function (u) {
          return (u.totals || {})[mt.key] || 0;
        }),
        marker: { color: PALETTE[mi % PALETTE.length] },
        hovertemplate: "%{x}<br>" + mt.label + ": %{y}<extra></extra>",
      };
    });
    Plotly.newPlot(
      el,
      traces,
      baseLayout({
        title: { text: "İş türü dağılımı · moderatör bazında", x: 0, font: { size: 12 } },
        barmode: "stack",
        xaxis: { tickangle: -20 },
        yaxis: { title: "Adet", gridcolor: th().grid },
        height: 340,
      }),
      plotCfg()
    );
  }

  function drawRankMatrix() {
    var el = purgePlot("mod-chart-rank-matrix");
    if (!el || !window.Plotly) return;
    var rankings = ANALYTICS.rankings_by_metric || {};
    var yLabels = MODS.map(function (m) {
      return m.username;
    });
    var xLabels = METRICS.map(function (m) {
      return m.label;
    });
    var z = MODS.map(function (m) {
      return METRICS.map(function (mt) {
        var list = rankings[mt.key] || [];
        for (var i = 0; i < list.length; i++) {
          if (String(list[i].user_id) === String(m.user_id)) return list[i].rank;
        }
        return null;
      });
    });
    var text = MODS.map(function (m, yi) {
      return METRICS.map(function (mt, xi) {
        var v = z[yi][xi];
        return v ? "#" + v : "—";
      });
    });
    Plotly.newPlot(
      el,
      [
        {
          type: "heatmap",
          x: xLabels,
          y: yLabels,
          z: z,
          text: text,
          texttemplate: "%{text}",
          textfont: { size: 10 },
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
      baseLayout({
        title: { text: "Metrik bazında liderlik sırası (#1 en iyi)", x: 0, font: { size: 12 } },
        xaxis: { tickangle: -35 },
        height: 300,
      }),
      plotCfg()
    );
  }

  function drawFocusProfile() {
    var el = purgePlot("mod-chart-focus-profile");
    if (!el || !window.Plotly) return;
    var shares = ANALYTICS.shares_by_metric || {};
    var traces = MODS.map(function (m, i) {
      var s = shares[String(m.user_id)] || {};
      return {
        type: "scatterpolar",
        name: m.username,
        r: METRICS.map(function (mt) {
          return s[mt.key] || 0;
        }),
        theta: METRICS.map(function (mt) {
          return mt.label;
        }),
        fill: "toself",
        fillcolor: modColor(i),
        opacity: 0.15,
        line: { color: modColor(i) },
        hovertemplate: m.username + "<br>%{theta}: %{r}%<extra></extra>",
      };
    });
    Plotly.newPlot(
      el,
      traces,
      baseLayout({
        title: { text: "Odak profili · iş türü payı (%)", x: 0, font: { size: 12 } },
        polar: { radialaxis: { ticksuffix: "%", gridcolor: th().grid } },
        height: 380,
      }),
      plotCfg()
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
        x: labels,
        y: w,
        marker: { color: modColor(i) },
        hovertemplate: m.username + "<br>%{x}: %{y} iş<extra></extra>",
      };
    });
    Plotly.newPlot(
      el,
      traces,
      baseLayout({
        title: { text: "Haftanın günü · iş dağılımı", x: 0, font: { size: 12 } },
        barmode: "group",
        yaxis: { title: "Toplam iş", gridcolor: th().grid },
        height: 300,
      }),
      plotCfg()
    );
  }

  function drawCumulative() {
    var el = purgePlot("mod-chart-cumulative");
    if (!el || !window.Plotly) return;
    var days = ANALYTICS.calendar_days || [];
    var cum = ANALYTICS.cumulative_by_user || {};
    if (!days.length) return;
    var traces = MODS.map(function (m, i) {
      var series = cum[String(m.user_id)] || [];
      return {
        type: "scatter",
        mode: "lines",
        name: m.username,
        x: days,
        y: series.map(function (p) {
          return p.cumulative;
        }),
        line: { color: modColor(i), width: 2 },
        hovertemplate: m.username + "<br>%{x}<br>Birikim: %{y}<extra></extra>",
      };
    });
    Plotly.newPlot(
      el,
      traces,
      baseLayout({
        title: { text: "Kümülatif katkı · dönem içi birikim", x: 0, font: { size: 12 } },
        xaxis: { tickangle: -45, gridcolor: th().grid },
        yaxis: { title: "Biriken iş", gridcolor: th().grid },
        height: 300,
      }),
      plotCfg()
    );
  }

  function drawInactiveSummary() {
    var el = purgePlot("mod-chart-inactive-summary");
    if (!el || !window.Plotly) return;
    var cals = ANALYTICS.calendars || {};
    var names = [];
    var active = [];
    var inactive = [];
    MODS.forEach(function (m) {
      var cal = cals[String(m.user_id)] || {};
      names.push(m.username);
      active.push(cal.active_days || 0);
      inactive.push(cal.inactive_days || 0);
    });
    Plotly.newPlot(
      el,
      [
        {
          type: "bar",
          name: "Aktif gün",
          x: names,
          y: active,
          marker: { color: "#0ea5e9" },
        },
        {
          type: "bar",
          name: "İş yapılmayan gün",
          x: names,
          y: inactive,
          marker: { color: "#cbd5e1" },
        },
      ],
      baseLayout({
        title: {
          text: "Çalışılan vs boş gün · seçili dönem (" + (RAW.start || "") + " → " + (RAW.end || "") + ")",
          x: 0,
          font: { size: 12 },
        },
        barmode: "stack",
        yaxis: { title: "Gün sayısı", gridcolor: th().grid },
        height: 280,
      }),
      plotCfg()
    );
  }

  function renderCharts() {
    if (!ANALYTICS.calendar_days || !window.Plotly) return;
    drawRankTotal();
    drawDailyVolume();
    drawActivityHeatmaps();
    drawMetricStack();
    drawRankMatrix();
    drawFocusProfile();
    drawWeekday();
    drawCumulative();
    drawInactiveSummary();
  }

  function scheduleCharts() {
    if (window.Plotly) {
      renderCharts();
      return;
    }
    var tries = 0;
    var t = setInterval(function () {
      tries += 1;
      if (window.Plotly || tries > 40) {
        clearInterval(t);
        if (window.Plotly) renderCharts();
      }
    }, 150);
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
    [
      "mod-chart-rank-total",
      "mod-chart-daily-volume",
      "mod-chart-activity-heat",
      "mod-chart-metric-stack",
      "mod-chart-rank-matrix",
      "mod-chart-focus-profile",
      "mod-chart-weekday",
      "mod-chart-cumulative",
      "mod-chart-inactive-summary",
    ].forEach(function (id) {
      var el = document.getElementById(id);
      if (el && window.Plotly) {
        try {
          Plotly.Plots.resize(el);
        } catch (_) {}
      }
    });
  });
})();
