/**
 * Notification Analytics — dönem karşılaştırma, heatmap, platform kıyas, alarmlar, AI context.
 */
(function (global) {
  "use strict";

  var DOW_LABELS = ["Paz", "Pzt", "Sal", "Çar", "Per", "Cum", "Cmt"];
  var lastAlertPayload = null;
  var lastComparePayload = null;
  var selectedCompareRow = null;

  function nt() {
    return global.NT || {};
  }

  function apiFetch(url, opts) {
    if (typeof global.apiFetch === "function") return global.apiFetch(url, opts);
    return fetch(url, opts || {}).then(function (r) {
      if (!r.ok) throw new Error(r.statusText);
      return r.json();
    });
  }

  function fmtDelta(pct) {
    if (pct === null || pct === undefined || isNaN(pct)) return "—";
    var sign = pct > 0 ? "+" : "";
    return sign + Number(pct).toFixed(1) + "%";
  }

  function rowPlatformClicks(row) {
    var p = row.platforms || {};
    return {
      desktop: nt().nCount ? nt().nCount((p.desktop || {}).click) : 0,
      mobileweb: nt().nCount ? nt().nCount((p.mobileweb || {}).click) : 0,
      android: nt().nCount ? nt().nCount((p.android || {}).click) : 0,
      ios: nt().nCount ? nt().nCount((p.ios || {}).click) : 0,
    };
  }

  function rowTotalClick(row) {
    var c = rowPlatformClicks(row);
    return c.desktop + c.mobileweb + c.android + c.ios;
  }

  function aggregatePeriod(rows) {
    var stats = {
      rows: rows.length,
      clicks: 0,
      impressions: 0,
      platform: { desktop: 0, mobileweb: 0, android: 0, ios: 0 },
      app: 0,
      web: 0,
    };
    (rows || []).forEach(function (r) {
      var pc = rowPlatformClicks(r);
      stats.clicks += pc.desktop + pc.mobileweb + pc.android + pc.ios;
      stats.platform.desktop += pc.desktop;
      stats.platform.mobileweb += pc.mobileweb;
      stats.platform.android += pc.android;
      stats.platform.ios += pc.ios;
      stats.app += pc.android + pc.ios;
      stats.web += pc.desktop + pc.mobileweb;
      var p = r.platforms || {};
      ["desktop", "mobileweb", "android"].forEach(function (k) {
        if (nt().platformImpression) stats.impressions += nt().platformImpression(k, p[k] || {});
      });
    });
    stats.ctr = stats.impressions > 0 ? (stats.clicks / stats.impressions) * 100 : 0;
    return stats;
  }

  function primaryDateRange() {
    var startEl = global.document.getElementById("nt-start-date");
    var endEl = global.document.getElementById("nt-end-date");
    return {
      start: startEl && startEl.value ? startEl.value : "",
      end: endEl && endEl.value ? endEl.value : "",
    };
  }

  function previousPeriodRange(start, end) {
    if (!start || !end) return null;
    var s = new Date(start + "T00:00:00");
    var e = new Date(end + "T00:00:00");
    if (isNaN(s.getTime()) || isNaN(e.getTime())) return null;
    var span = Math.round((e - s) / 86400000) + 1;
    var prevEnd = new Date(s);
    prevEnd.setDate(prevEnd.getDate() - 1);
    var prevStart = new Date(prevEnd);
    prevStart.setDate(prevStart.getDate() - span + 1);
    return {
      start: prevStart.toISOString().slice(0, 10),
      end: prevEnd.toISOString().slice(0, 10),
    };
  }

  function fetchRowsForRange(range) {
    var q = "?limit=10000";
    if (range.start) q += "&start=" + encodeURIComponent(range.start);
    if (range.end) q += "&end=" + encodeURIComponent(range.end);
    return apiFetch("/api/notification-analytics/rows" + q).then(function (data) {
      return data.rows || [];
    });
  }

  function renderPeriodCompare(primaryRows) {
    var el = global.document.getElementById("nt-period-compare");
    if (!el) return;
    var range = primaryDateRange();
    var prev = previousPeriodRange(range.start, range.end);
    if (!prev) {
      el.innerHTML = '<p class="text-xs text-slate-500 dark:text-slate-400">Dönem karşılaştırması için başlangıç ve bitiş tarihi seçin.</p>';
      return;
    }
    el.innerHTML = '<p class="text-xs text-slate-500 dark:text-slate-400">Karşılaştırma yükleniyor…</p>';
    var curStats = aggregatePeriod(primaryRows);
    fetchRowsForRange(prev).then(function (prevRows) {
      var prevStats = aggregatePeriod(prevRows);
      lastComparePayload = { current: curStats, previous: prevStats, ranges: { primary: range, compare: prev } };
      function delta(cur, prev) {
        if (!prev) return null;
        if (prev === 0) return cur > 0 ? null : 0;
        return ((cur - prev) / prev) * 100;
      }
      var clickD = delta(curStats.clicks, prevStats.clicks);
      var imprD = delta(curStats.impressions, prevStats.impressions);
      var ctrD = delta(curStats.ctr, prevStats.ctr);
      var cards = [
        { label: "Toplam click", cur: curStats.clicks, prev: prevStats.clicks, d: clickD },
        { label: "Toplam impression", cur: curStats.impressions, prev: prevStats.impressions, d: imprD },
        { label: "CTR (web+android impr)", cur: curStats.ctr, prev: prevStats.ctr, d: ctrD, pct: true },
      ];
      var platHtml = ["desktop", "mobileweb", "android", "ios"].map(function (k) {
        var labels = { desktop: "Web", mobileweb: "MWeb", android: "Android", ios: "iOS" };
        var c = curStats.platform[k];
        var p = prevStats.platform[k];
        var share = curStats.clicks > 0 ? (c / curStats.clicks * 100) : 0;
        return '<div class="rounded-lg border border-slate-200 px-2 py-1.5 dark:border-slate-700">'
          + '<p class="text-[10px] font-bold uppercase text-slate-500">' + labels[k] + '</p>'
          + '<p class="text-sm font-black text-slate-800 dark:text-slate-200">' + (nt().fmtCount ? nt().fmtCount(c) : c) + '</p>'
          + '<p class="text-[10px] text-slate-500">' + fmtDelta(delta(c, p)) + ' · pay %' + share.toFixed(1) + '</p></div>';
      }).join("");
      var appWeb = '<div class="mt-2 grid grid-cols-2 gap-2 text-xs">'
        + '<div class="rounded-lg border border-indigo-200 bg-indigo-50/50 p-2 dark:border-indigo-900 dark:bg-indigo-950/30">'
        + '<p class="font-bold text-indigo-800 dark:text-indigo-300">App (Android+iOS)</p>'
        + '<p class="text-lg font-black">' + (nt().fmtCount ? nt().fmtCount(curStats.app) : curStats.app) + ' <span class="text-xs font-normal">' + fmtDelta(delta(curStats.app, prevStats.app)) + '</span></p></div>'
        + '<div class="rounded-lg border border-slate-200 p-2 dark:border-slate-700">'
        + '<p class="font-bold text-slate-700 dark:text-slate-300">Web (Desktop+MWeb)</p>'
        + '<p class="text-lg font-black">' + (nt().fmtCount ? nt().fmtCount(curStats.web) : curStats.web) + ' <span class="text-xs font-normal">' + fmtDelta(delta(curStats.web, prevStats.web)) + '</span></p></div>'
        + "</div>";
      el.innerHTML = '<p class="mb-2 text-xs text-slate-500 dark:text-slate-400">'
        + range.start + " – " + range.end + " vs " + prev.start + " – " + prev.end + "</p>"
        + '<div class="grid grid-cols-1 gap-2 sm:grid-cols-3">' + cards.map(function (x) {
          return '<div class="rounded-xl border border-slate-200 p-3 dark:border-slate-700">'
            + '<p class="text-xs text-slate-500">' + x.label + '</p>'
            + '<p class="text-xl font-black text-slate-900 dark:text-slate-200">' + (x.pct ? x.cur.toFixed(2) + "%" : (nt().fmtCount ? nt().fmtCount(x.cur) : x.cur)) + '</p>'
            + '<p class="text-xs text-slate-500">Önceki: ' + (x.pct ? x.prev.toFixed(2) + "%" : (nt().fmtCount ? nt().fmtCount(x.prev) : x.prev)) + " · " + fmtDelta(x.d) + "</p></div>";
        }).join("") + "</div>"
        + '<div class="mt-3 grid grid-cols-2 gap-2 sm:grid-cols-4">' + platHtml + "</div>" + appWeb;
    }).catch(function () {
      el.innerHTML = '<p class="text-xs text-rose-600">Karşılaştırma verisi yüklenemedi.</p>';
    });
  }

  function renderHeatmap(rows) {
    var el = global.document.getElementById("nt-heatmap");
    if (!el || !global.Plotly) return;
    var metric = (global.document.getElementById("nt-heatmap-metric") || {}).value || "total";
    var grid = {};
    for (var d = 0; d < 7; d++) {
      grid[d] = {};
      for (var h = 0; h < 24; h++) grid[d][h] = 0;
    }
    (rows || []).forEach(function (r) {
      var iso = String(r.date || "");
      if (!iso) return;
      var dt = new Date(iso);
      if (isNaN(dt.getTime())) return;
      var dow = dt.getDay();
      var hour = dt.getHours();
      var val = 0;
      if (metric === "total") val = rowTotalClick(r);
      else {
        var pc = rowPlatformClicks(r);
        val = pc[metric] || 0;
      }
      grid[dow][hour] += val;
    });
    var z = [];
    var y = DOW_LABELS.slice();
    var x = [];
    for (var hi = 0; hi < 24; hi++) x.push(hi + ":00");
    for (var di = 0; di < 7; di++) {
      var row = [];
      for (var hj = 0; hj < 24; hj++) row.push(grid[di][hj]);
      z.push(row);
    }
    var dark = global.document.documentElement.classList.contains("dark");
    Plotly.newPlot(el, [{
      type: "heatmap",
      x: x,
      y: y,
      z: z,
      colorscale: dark ? "Viridis" : "Blues",
      hoverongaps: false,
    }], {
      margin: { l: 50, r: 10, t: 10, b: 40 },
      paper_bgcolor: "rgba(0,0,0,0)",
      plot_bgcolor: "rgba(0,0,0,0)",
      font: { color: dark ? "#a1a1aa" : "#475569", size: 10 },
      xaxis: { title: "Saat" },
      yaxis: { title: "Gün" },
    }, { responsive: true, displayModeBar: false });
  }

  function renderPlatformCompare(row) {
    var panel = global.document.getElementById("nt-platform-compare");
    var chart = global.document.getElementById("nt-platform-compare-chart");
    var meta = global.document.getElementById("nt-platform-compare-meta");
    if (!panel || !chart) return;
    if (!row) {
      panel.classList.add("hidden");
      selectedCompareRow = null;
      return;
    }
    selectedCompareRow = row;
    panel.classList.remove("hidden");
    var pc = rowPlatformClicks(row);
    var labels = ["Web", "MWeb", "Android", "iOS", "App toplam", "Web toplam"];
    var values = [pc.desktop, pc.mobileweb, pc.android, pc.ios, pc.android + pc.ios, pc.desktop + pc.mobileweb];
    if (meta) {
      meta.textContent = (nt().idString ? nt().idString(row) : row.id) + " · " + (row.text || "").slice(0, 80) + " · " + (nt().dayKey ? nt().dayKey(row.date) : "");
    }
    if (!global.Plotly) return;
    var dark = global.document.documentElement.classList.contains("dark");
    Plotly.newPlot(chart, [{
      type: "bar",
      x: labels.slice(0, 4),
      y: values.slice(0, 4),
      name: "Click",
      marker: { color: ["#6366f1", "#f59e0b", "#22c55e", "#06b6d4"] },
      text: values.slice(0, 4).map(function (v) { return (nt().fmtCount ? nt().fmtCount(v) : v); }),
      textposition: "auto",
    }], {
      margin: { l: 40, r: 20, t: 20, b: 40 },
      paper_bgcolor: "rgba(0,0,0,0)",
      plot_bgcolor: "rgba(0,0,0,0)",
      font: { color: dark ? "#a1a1aa" : "#475569" },
      yaxis: { title: "Click" },
    }, { responsive: true, displayModeBar: false });
    var appWebEl = global.document.getElementById("nt-platform-appweb");
    if (appWebEl) {
      Plotly.newPlot(appWebEl, [{
        type: "bar",
        x: ["App (A+iOS)", "Web (D+M)"],
        y: [values[4], values[5]],
        marker: { color: ["#22c55e", "#6366f1"] },
      }], {
        margin: { l: 40, r: 10, t: 10, b: 40 },
        paper_bgcolor: "rgba(0,0,0,0)",
        plot_bgcolor: "rgba(0,0,0,0)",
        font: { color: dark ? "#a1a1aa" : "#475569" },
      }, { responsive: true, displayModeBar: false });
    }
  }

  function renderAlertsPanel() {
    var el = global.document.getElementById("nt-alerts-panel");
    if (!el) return;
    apiFetch("/api/notification-analytics/alerts/evaluate?send_email=false").then(function (data) {
      lastAlertPayload = data;
      var alerts = data.alerts || [];
      if (!alerts.length) {
        el.innerHTML = '<p class="text-xs text-emerald-600 dark:text-emerald-400">Son 7 günde eşik alarmı yok.</p>'
          + '<p class="mt-1 text-[10px] text-slate-500">Kurallar: click ≥%30 düşüş · CTR medyan altı · e-posta günlük tek sefer</p>';
        return;
      }
      el.innerHTML = alerts.map(function (a) {
        var cls = a.severity === "critical" ? "border-rose-300 bg-rose-50 dark:border-rose-900 dark:bg-rose-950/40" : "border-amber-300 bg-amber-50 dark:border-amber-900 dark:bg-amber-950/40";
        return '<div class="rounded-lg border px-3 py-2 text-xs ' + cls + '"><p class="font-bold">' + (nt().escapeHtml ? nt().escapeHtml(a.title) : a.title) + '</p><p class="mt-1 text-slate-600 dark:text-slate-400">' + (nt().escapeHtml ? nt().escapeHtml(a.summary) : a.summary) + "</p></div>";
      }).join("");
    }).catch(function () {
      el.innerHTML = '<p class="text-xs text-slate-500">Alarm durumu okunamadı.</p>';
    });
  }

  function onRedraw(ev) {
    var rows = (ev && ev.detail && ev.detail.rows) || (nt().getFilteredRows ? nt().getFilteredRows() : []);
    renderPeriodCompare(rows);
    renderHeatmap(rows);
    if (selectedCompareRow) renderPlatformCompare(selectedCompareRow);
  }

  function bindDrill() {
    global.document.addEventListener("click", function (ev) {
      var btn = ev.target && ev.target.closest ? ev.target.closest("[data-nt-drill]") : null;
      if (!btn || !global.NTDrill) return;
      ev.preventDefault();
      var id = btn.getAttribute("data-nt-drill-id") || "";
      var text = btn.getAttribute("data-nt-drill-text") || "";
      var date = btn.getAttribute("data-nt-drill-date") || "";
      global.NTDrill.set({ id: id, text: text, date: date });
      var row = null;
      var all = nt().readRows ? nt().readRows() : [];
      for (var i = 0; i < all.length; i++) {
        var r = all[i];
        if (id && nt().idString && nt().idString(r) === id && (!date || nt().dayKey(r.date) === date)) {
          row = r;
          break;
        }
      }
      if (!row && text) {
        for (var j = 0; j < all.length; j++) {
          if (String(all[j].text || "").trim() === text.trim()) {
            row = all[j];
            break;
          }
        }
      }
      renderPlatformCompare(row);
      var raw = global.document.getElementById("nt-raw-list");
      if (raw && raw.scrollIntoView) raw.scrollIntoView({ behavior: "smooth", block: "start" });
    });
  }

  function bindAlertsButton() {
    var btn = global.document.getElementById("nt-alerts-check-btn");
    if (!btn) return;
    btn.addEventListener("click", function () {
      btn.disabled = true;
      apiFetch("/api/notification-analytics/alerts/check", { method: "POST" }).then(function (data) {
        lastAlertPayload = data;
        renderAlertsPanel();
        btn.disabled = false;
        alert(data.email_sent && data.email_sent.length ? "Alarm e-postası gönderildi." : "Kontrol tamamlandı.");
      }).catch(function () {
        btn.disabled = false;
      });
    });
  }

  function bindHeatmapMetric() {
    var sel = global.document.getElementById("nt-heatmap-metric");
    if (sel) sel.addEventListener("change", function () {
      onRedraw({ detail: { rows: nt().getFilteredRows ? nt().getFilteredRows() : [] } });
    });
  }

  function buildPageContext() {
    var rows = nt().getFilteredRows ? nt().getFilteredRows() : [];
    var stats = aggregatePeriod(rows);
    return {
      page: "notification",
      filters: primaryDateRange(),
      drill: global.NTDrill ? global.NTDrill.get() : null,
      kpis: {
        row_count: rows.length,
        total_clicks: stats.clicks,
        total_impressions: stats.impressions,
        ctr: stats.ctr,
        platform_clicks: stats.platform,
        app_clicks: stats.app,
        web_clicks: stats.web,
      },
      period_compare: lastComparePayload,
      alerts: lastAlertPayload,
      selected_notification: selectedCompareRow ? {
        id: nt().idString ? nt().idString(selectedCompareRow) : selectedCompareRow.id,
        text: selectedCompareRow.text,
        date: selectedCompareRow.date,
        platforms: rowPlatformClicks(selectedCompareRow),
      } : null,
      cross_top_sample: (global.cachedCrossTopList || []).slice(0, 5),
    };
  }

  global.NTExtras = {
    renderPeriodCompare: renderPeriodCompare,
    renderHeatmap: renderHeatmap,
    renderPlatformCompare: renderPlatformCompare,
    renderAlertsPanel: renderAlertsPanel,
    buildPageContext: buildPageContext,
    rowPlatformClicks: rowPlatformClicks,
    rowTotalClick: rowTotalClick,
  };

  global.addEventListener("nt-redraw", onRedraw);
  bindDrill();
  bindAlertsButton();
  bindHeatmapMetric();

  global.__pcPageContext = function () {
    return buildPageContext();
  };

  if (global.document.readyState === "loading") {
    global.document.addEventListener("DOMContentLoaded", function () {
      renderAlertsPanel();
    });
  } else {
    renderAlertsPanel();
  }
})(window);
