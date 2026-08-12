/**
 * Sinemalar moderasyon paneli — filtreler + Plotly grafikleri.
 * Veri: #mod-panel-data JSON (get_panel_payload).
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
  if (!RAW || !RAW.daily) return;

  var METRICS = (RAW.metric_types || []).map(function (m) {
    return { key: m.key, label: m.label };
  });
  var USERS = (RAW.users || []).map(function (u) {
    return { username: u.username, user_id: u.user_id };
  });

  var MOD_COLORS = {
    gezginozlem: "#0ea5e9",
    berend: "#8b5cf6",
    "gözde.": "#f59e0b",
    gözde: "#f59e0b",
  };
  var METRIC_PALETTE = [
    "#0ea5e9", "#8b5cf6", "#f59e0b", "#10b981", "#ef4444",
    "#6366f1", "#ec4899", "#14b8a6", "#f97316", "#84cc16", "#64748b",
  ];

  var state = {
    moderators: USERS.map(function (u) { return u.username; }),
    metrics: METRICS.map(function (m) { return m.key; }),
    granularity: "day",
    preset: "all",
    customStart: RAW.start || "",
    customEnd: RAW.end || "",
  };

  function norm(s) {
    return String(s || "").trim().toLowerCase().replace(/\s+/g, "").replace(/\.$/, "");
  }

  function modColor(name) {
    return MOD_COLORS[norm(name)] || "#64748b";
  }

  function metricLabel(key) {
    for (var i = 0; i < METRICS.length; i++) {
      if (METRICS[i].key === key) return METRICS[i].label;
    }
    return key;
  }

  function metricColor(idx) {
    return METRIC_PALETTE[idx % METRIC_PALETTE.length];
  }

  function theme() {
    return window.seoPlotlyTheme ? window.seoPlotlyTheme() : {
      grid: "#e2e8f0", tick: "#64748b", legend: "#475569",
      paper: "rgba(0,0,0,0)", plot: "rgba(0,0,0,0)",
    };
  }

  function axisLayout(title) {
    var th = theme();
    return {
      title: title || "",
      gridcolor: th.grid,
      zerolinecolor: th.grid,
      tickfont: { size: 10, color: th.tick },
      titlefont: { size: 10, color: th.tick },
    };
  }

  function plotCfg() {
    return { responsive: true, displayModeBar: false };
  }

  function parseIso(s) {
    if (!s) return null;
    var p = String(s).slice(0, 10).split("-");
    if (p.length !== 3) return null;
    return new Date(+p[0], +p[1] - 1, +p[2]);
  }

  function isoDate(d) {
    var y = d.getFullYear();
    var m = String(d.getMonth() + 1).padStart(2, "0");
    var day = String(d.getDate()).padStart(2, "0");
    return y + "-" + m + "-" + day;
  }

  function addDays(d, n) {
    var x = new Date(d.getTime());
    x.setDate(x.getDate() + n);
    return x;
  }

  function weekKey(d) {
    var x = new Date(d.getTime());
    var day = (x.getDay() + 6) % 7;
    x.setDate(x.getDate() - day);
    return isoDate(x);
  }

  function monthKey(d) {
    return d.getFullYear() + "-" + String(d.getMonth() + 1).padStart(2, "0");
  }

  function flattenRows() {
    var out = [];
    var daily = RAW.daily || {};
    Object.keys(daily).sort().forEach(function (day) {
      var block = daily[day] || {};
      USERS.forEach(function (u) {
        METRICS.forEach(function (m) {
          var key = u.username + "|" + m.key;
          var cnt = block[key] || 0;
          if (cnt) {
            out.push({
              day: day,
              username: u.username,
              metric_type: m.key,
              metric_label: m.label,
              count: cnt,
            });
          }
        });
      });
    });
    return out;
  }

  function effectiveRange() {
    var end = parseIso(RAW.end) || new Date();
    var start = parseIso(RAW.start) || parseIso("2026-01-01");
    if (state.preset === "7d") {
      start = addDays(end, -6);
    } else if (state.preset === "30d") {
      start = addDays(end, -29);
    } else if (state.preset === "90d") {
      start = addDays(end, -89);
    } else if (state.preset === "custom") {
      start = parseIso(state.customStart) || start;
      end = parseIso(state.customEnd) || end;
    }
    if (start > end) {
      var t = start; start = end; end = t;
    }
    return { start: start, end: end, startIso: isoDate(start), endIso: isoDate(end) };
  }

  function filterRows(rows) {
    var range = effectiveRange();
    var modSet = {};
    state.moderators.forEach(function (m) { modSet[norm(m)] = true; });
    var metSet = {};
    state.metrics.forEach(function (m) { metSet[m] = true; });

    return rows.filter(function (r) {
      if (!modSet[norm(r.username)]) return false;
      if (!metSet[r.metric_type]) return false;
      var d = parseIso(r.day);
      if (!d || d < range.start || d > range.end) return false;
      return true;
    });
  }

  function sumCounts(rows) {
    return rows.reduce(function (a, r) { return a + r.count; }, 0);
  }

  function groupByPeriod(rows) {
    var buckets = {};
    rows.forEach(function (r) {
      var d = parseIso(r.day);
      if (!d) return;
      var pk =
        state.granularity === "week" ? weekKey(d) :
        state.granularity === "month" ? monthKey(d) :
        r.day;
      var bk = pk + "|" + norm(r.username);
      if (!buckets[bk]) buckets[bk] = { period: pk, username: r.username, count: 0 };
      buckets[bk].count += r.count;
    });
    return Object.keys(buckets).map(function (k) { return buckets[k]; });
  }

  function fmt(n) {
    return (Number(n) || 0).toLocaleString("tr-TR");
  }

  function setText(id, txt) {
    var el = document.getElementById(id);
    if (el) el.textContent = txt;
  }

  function updateKpis(rows) {
    var byMod = {};
    USERS.forEach(function (u) {
      byMod[norm(u.username)] = { username: u.username, total: 0 };
    });
    rows.forEach(function (r) {
      var k = norm(r.username);
      if (byMod[k]) byMod[k].total += r.count;
    });
    USERS.forEach(function (u) {
      var card = document.querySelector('[data-mod-kpi="' + u.username + '"]');
      if (!card) return;
      var val = (byMod[norm(u.username)] || {}).total || 0;
      var numEl = card.querySelector("[data-mod-kpi-value]");
      if (numEl) numEl.textContent = fmt(val);
      card.classList.toggle("opacity-40", state.moderators.indexOf(u.username) < 0);
    });
    setText("mod-filter-summary", fmt(sumCounts(rows)) + " iş · " + rows.length + " hücre");
  }

  function updateTable(rows) {
    var tbody = document.getElementById("mod-table-body");
    if (!tbody) return;
    var html = "";
    var byDay = {};
    rows.forEach(function (r) {
      if (!byDay[r.day]) byDay[r.day] = {};
      var k = r.username + "|" + r.metric_type;
      byDay[r.day][k] = (byDay[r.day][k] || 0) + r.count;
    });
    var days = Object.keys(byDay).sort().reverse();
    var mods = state.moderators.slice();
    days.forEach(function (day) {
      mods.forEach(function (uname) {
        var rowTotal = 0;
        var cells = METRICS.map(function (m) {
          if (state.metrics.indexOf(m.key) < 0) {
            return '<td class="px-2 py-1.5 text-right font-mono tabular-nums text-slate-200 dark:text-slate-700">—</td>';
          }
          var cnt = (byDay[day][uname + "|" + m.key] || 0);
          rowTotal += cnt;
          var cls = cnt ? "text-slate-800 dark:text-slate-100 font-semibold" : "text-slate-300 dark:text-slate-600";
          return '<td class="px-2 py-1.5 text-right font-mono tabular-nums ' + cls + '">' + (cnt || "—") + "</td>";
        }).join("");
        if (!rowTotal) return;
        html +=
          '<tr class="border-b border-slate-100 hover:bg-sky-50/40 dark:border-slate-800 dark:hover:bg-slate-800/40">' +
          '<td class="px-2 py-1.5 font-mono text-[11px] tabular-nums text-slate-600 dark:text-slate-300 whitespace-nowrap">' + day + "</td>" +
          '<td class="px-2 py-1.5 font-semibold text-slate-700 dark:text-slate-200">' + uname + "</td>" +
          cells +
          '<td class="px-2 py-1.5 text-right font-mono font-bold tabular-nums text-sky-700 dark:text-sky-300">' + rowTotal + "</td>" +
          "</tr>";
      });
    });
    tbody.innerHTML = html || '<tr><td colspan="' + (METRICS.length + 3) + '" class="px-4 py-8 text-center text-slate-400">Filtreye uygun satır yok</td></tr>';
    setText("mod-table-foot", days.length + " gün · " + mods.length + " moderatör");
  }

  function plotTrend(rows) {
    var el = document.getElementById("mod-chart-trend");
    if (!el || !window.Plotly) return;
    var grouped = groupByPeriod(rows);
    var periods = [];
    grouped.forEach(function (g) {
      if (periods.indexOf(g.period) < 0) periods.push(g.period);
    });
    periods.sort();
    var traces = state.moderators.map(function (uname) {
      var y = periods.map(function (p) {
        var hit = grouped.find(function (g) { return g.period === p && norm(g.username) === norm(uname); });
        return hit ? hit.count : 0;
      });
      return {
        x: periods,
        y: y,
        type: "scatter",
        mode: periods.length > 60 ? "lines" : "lines+markers",
        name: uname,
        line: { color: modColor(uname), width: 2 },
        marker: { size: periods.length > 60 ? 0 : 5, color: modColor(uname) },
        connectgaps: true,
      };
    });
    var th = theme();
    Plotly.newPlot(el, traces, {
      autosize: true,
      height: 320,
      margin: { l: 48, r: 16, t: 12, b: 48 },
      paper_bgcolor: th.paper,
      plot_bgcolor: th.plot,
      font: { size: 11, color: th.tick },
      xaxis: Object.assign(axisLayout(""), { type: state.granularity === "day" ? "date" : "category" }),
      yaxis: axisLayout("Toplam"),
      legend: { orientation: "h", y: 1.14, font: { size: 10, color: th.legend } },
      hovermode: "x unified",
    }, plotCfg());
  }

  function plotMetricBar(rows) {
    var el = document.getElementById("mod-chart-metric");
    if (!el || !window.Plotly) return;
    var totals = {};
    METRICS.forEach(function (m) { totals[m.key] = 0; });
    rows.forEach(function (r) { totals[r.metric_type] = (totals[r.metric_type] || 0) + r.count; });
    var items = METRICS.filter(function (m) { return state.metrics.indexOf(m.key) >= 0; })
      .map(function (m, i) {
        return { label: m.label, key: m.key, val: totals[m.key] || 0, color: metricColor(i) };
      })
      .filter(function (x) { return x.val > 0; })
      .sort(function (a, b) { return b.val - a.val; });
    Plotly.newPlot(el, [{
      type: "bar",
      orientation: "h",
      y: items.map(function (x) { return x.label; }),
      x: items.map(function (x) { return x.val; }),
      marker: { color: items.map(function (x) { return x.color; }) },
      text: items.map(function (x) { return fmt(x.val); }),
      textposition: "outside",
      hovertemplate: "%{y}<br>%{x}<extra></extra>",
    }], {
      autosize: true,
      height: Math.max(260, items.length * 28 + 80),
      margin: { l: 120, r: 24, t: 8, b: 32 },
      paper_bgcolor: theme().paper,
      plot_bgcolor: theme().plot,
      font: { size: 11, color: theme().tick },
      xaxis: axisLayout("Adet"),
      yaxis: Object.assign(axisLayout(""), { automargin: true }),
    }, plotCfg());
  }

  function plotModBar(rows) {
    var el = document.getElementById("mod-chart-moderator");
    if (!el || !window.Plotly) return;
    var totals = {};
    state.moderators.forEach(function (u) { totals[norm(u)] = { name: u, val: 0 }; });
    rows.forEach(function (r) {
      var k = norm(r.username);
      if (totals[k]) totals[k].val += r.count;
    });
    var items = Object.keys(totals).map(function (k) { return totals[k]; }).filter(function (x) { return x.val > 0; });
    Plotly.newPlot(el, [{
      type: "bar",
      x: items.map(function (x) { return x.name; }),
      y: items.map(function (x) { return x.val; }),
      marker: { color: items.map(function (x) { return modColor(x.name); }) },
      text: items.map(function (x) { return fmt(x.val); }),
      textposition: "outside",
    }], {
      autosize: true,
      height: 280,
      margin: { l: 48, r: 16, t: 8, b: 48 },
      paper_bgcolor: theme().paper,
      plot_bgcolor: theme().plot,
      font: { size: 11, color: theme().tick },
      xaxis: axisLayout("Moderatör"),
      yaxis: axisLayout("Toplam"),
    }, plotCfg());
  }

  function plotStacked(rows) {
    var el = document.getElementById("mod-chart-stacked");
    if (!el || !window.Plotly) return;
    var periods = [];
    var byPeriodMetric = {};
    rows.forEach(function (r) {
      var d = parseIso(r.day);
      if (!d) return;
      var pk =
        state.granularity === "week" ? weekKey(d) :
        state.granularity === "month" ? monthKey(d) :
        r.day;
      if (periods.indexOf(pk) < 0) periods.push(pk);
      var k = pk + "|" + r.metric_type;
      byPeriodMetric[k] = (byPeriodMetric[k] || 0) + r.count;
    });
    periods.sort();
    var activeMetrics = METRICS.filter(function (m) { return state.metrics.indexOf(m.key) >= 0; });
    var traces = activeMetrics.map(function (m, i) {
      return {
        x: periods,
        y: periods.map(function (p) { return byPeriodMetric[p + "|" + m.key] || 0; }),
        name: m.label,
        type: "scatter",
        mode: "none",
        stackgroup: "one",
        fillcolor: metricColor(i),
        line: { width: 0.5, color: metricColor(i) },
      };
    });
    Plotly.newPlot(el, traces, {
      autosize: true,
      height: 320,
      margin: { l: 48, r: 16, t: 12, b: 48 },
      paper_bgcolor: theme().paper,
      plot_bgcolor: theme().plot,
      font: { size: 11, color: theme().tick },
      xaxis: axisLayout(""),
      yaxis: axisLayout("Adet"),
      legend: { orientation: "h", y: 1.18, font: { size: 9, color: theme().legend } },
      hovermode: "x unified",
    }, plotCfg());
  }

  function plotHeatmap(rows) {
    var el = document.getElementById("mod-chart-heatmap");
    if (!el || !window.Plotly) return;
    var days = [];
    rows.forEach(function (r) {
      if (days.indexOf(r.day) < 0) days.push(r.day);
    });
    days.sort();
    if (days.length > 45) {
      days = days.slice(-45);
      rows = rows.filter(function (r) { return days.indexOf(r.day) >= 0; });
    }
    var yLabels = state.moderators.slice();
    var xLabels = METRICS.filter(function (m) { return state.metrics.indexOf(m.key) >= 0; }).map(function (m) { return m.label; });
    var z = yLabels.map(function (uname) {
      return METRICS.filter(function (m) { return state.metrics.indexOf(m.key) >= 0; }).map(function (m) {
        return rows.filter(function (r) {
          return norm(r.username) === norm(uname) && r.metric_type === m.key;
        }).reduce(function (a, r) { return a + r.count; }, 0);
      });
    });
    Plotly.newPlot(el, [{
      type: "heatmap",
      x: xLabels,
      y: yLabels,
      z: z,
      colorscale: "Blues",
      hovertemplate: "%{y} · %{x}<br>%{z}<extra></extra>",
    }], {
      autosize: true,
      height: Math.max(220, yLabels.length * 48 + 100),
      margin: { l: 100, r: 16, t: 8, b: 100 },
      paper_bgcolor: theme().paper,
      plot_bgcolor: theme().plot,
      font: { size: 10, color: theme().tick },
      xaxis: { tickangle: -35, side: "bottom" },
      yaxis: { automargin: true },
    }, plotCfg());
  }

  function plotPie(rows) {
    var el = document.getElementById("mod-chart-pie");
    if (!el || !window.Plotly) return;
    var totals = {};
    rows.forEach(function (r) {
      totals[r.metric_type] = (totals[r.metric_type] || 0) + r.count;
    });
    var items = METRICS.filter(function (m) { return state.metrics.indexOf(m.key) >= 0 && totals[m.key]; })
      .map(function (m, i) {
        return { label: m.label, val: totals[m.key], color: metricColor(i) };
      });
    Plotly.newPlot(el, [{
      type: "pie",
      labels: items.map(function (x) { return x.label; }),
      values: items.map(function (x) { return x.val; }),
      marker: { colors: items.map(function (x) { return x.color; }) },
      textinfo: "label+percent",
      textposition: "inside",
      hole: 0.45,
      hovertemplate: "%{label}<br>%{value}<br>%{percent}<extra></extra>",
    }], {
      autosize: true,
      height: 300,
      margin: { l: 16, r: 16, t: 16, b: 16 },
      paper_bgcolor: theme().paper,
      font: { size: 11, color: theme().tick },
      showlegend: false,
    }, plotCfg());
  }

  function renderAll() {
    var rows = filterRows(flattenRows());
    updateKpis(rows);
    updateTable(rows);
    var range = effectiveRange();
    setText("mod-range-label", range.startIso + " → " + range.endIso);
    plotTrend(rows);
    plotMetricBar(rows);
    plotModBar(rows);
    plotStacked(rows);
    plotHeatmap(rows);
    plotPie(rows);
  }

  function toggleInList(list, val) {
    var i = list.indexOf(val);
    if (i >= 0) {
      if (list.length <= 1) return;
      list.splice(i, 1);
    } else {
      list.push(val);
    }
  }

  function bindPills(containerId, values, listKey, labelFn) {
    var box = document.getElementById(containerId);
    if (!box) return;
    box.innerHTML = "";
    values.forEach(function (val) {
      var btn = document.createElement("button");
      btn.type = "button";
      btn.className =
        "mod-pill rounded-full px-2.5 py-1 text-[11px] font-semibold ring-1 transition " +
        (state[listKey].indexOf(val) >= 0
          ? "bg-sky-600 text-white ring-sky-600"
          : "bg-white text-slate-600 ring-slate-200 hover:bg-slate-50 dark:bg-slate-800 dark:text-slate-300 dark:ring-slate-600");
      btn.textContent = labelFn ? labelFn(val) : val;
      btn.addEventListener("click", function () {
        toggleInList(state[listKey], val);
        bindPills(containerId, values, listKey, labelFn);
        if (listKey === "moderators") {
          bindPills("mod-filter-mods", USERS.map(function (u) { return u.username; }), "moderators");
        }
        if (listKey === "metrics") {
          bindPills("mod-filter-metrics", METRICS.map(function (m) { return m.key; }), "metrics", metricLabel);
        }
        renderAll();
      });
      box.appendChild(btn);
    });
  }

  function bindControls() {
    bindPills("mod-filter-mods", USERS.map(function (u) { return u.username; }), "moderators");
    bindPills("mod-filter-metrics", METRICS.map(function (m) { return m.key; }), "metrics", metricLabel);

    var gran = document.getElementById("mod-granularity");
    if (gran) {
      gran.value = state.granularity;
      gran.addEventListener("change", function () {
        state.granularity = gran.value || "day";
        renderAll();
      });
    }

    document.querySelectorAll("[data-mod-preset]").forEach(function (btn) {
      btn.addEventListener("click", function () {
        state.preset = btn.getAttribute("data-mod-preset") || "all";
        document.querySelectorAll("[data-mod-preset]").forEach(function (b) {
          b.classList.toggle("mod-preset-active", b === btn);
        });
        renderAll();
      });
    });

    var applyBtn = document.getElementById("mod-client-apply");
    if (applyBtn) {
      applyBtn.addEventListener("click", function () {
        state.preset = "custom";
        state.customStart = (document.getElementById("mod-client-start") || {}).value || RAW.start;
        state.customEnd = (document.getElementById("mod-client-end") || {}).value || RAW.end;
        document.querySelectorAll("[data-mod-preset]").forEach(function (b) {
          b.classList.remove("mod-preset-active");
        });
        renderAll();
      });
    }

    var resetBtn = document.getElementById("mod-filter-reset");
    if (resetBtn) {
      resetBtn.addEventListener("click", function () {
        state.moderators = USERS.map(function (u) { return u.username; });
        state.metrics = METRICS.map(function (m) { return m.key; });
        state.granularity = "day";
        state.preset = "all";
        bindPills("mod-filter-mods", USERS.map(function (u) { return u.username; }), "moderators");
        bindPills("mod-filter-metrics", METRICS.map(function (m) { return m.key; }), "metrics", metricLabel);
        if (gran) gran.value = "day";
        document.querySelectorAll("[data-mod-preset]").forEach(function (b) {
          b.classList.toggle("mod-preset-active", b.getAttribute("data-mod-preset") === "all");
        });
        renderAll();
      });
    }
  }

  function waitPlotly(cb) {
    if (window.Plotly && typeof window.Plotly.newPlot === "function") {
      cb();
      return;
    }
    var n = 0;
    var t = setInterval(function () {
      n++;
      if ((window.Plotly && window.Plotly.newPlot) || n > 80) {
        clearInterval(t);
        cb();
      }
    }, 100);
  }

  bindControls();
  waitPlotly(renderAll);

  window.addEventListener("resize", function () {
    ["mod-chart-trend", "mod-chart-metric", "mod-chart-moderator", "mod-chart-stacked", "mod-chart-heatmap", "mod-chart-pie"].forEach(function (id) {
      var el = document.getElementById(id);
      if (el && window.Plotly) Plotly.Plots.resize(el);
    });
  });

  document.addEventListener("themechange", renderAll);
})();
