/**
 * Notification Analytics — dönem karşılaştırma, heatmap, platform kıyas, alarmlar, AI context.
 */
(function (global) {
  "use strict";

  var DOW_LABELS = ["Pzt", "Sal", "Çar", "Per", "Cum", "Cmt", "Paz"];
  var HEATMAP_HOUR_START = 7;
  var HEATMAP_HOUR_END = 23;
  // Düşük click → kırmızı, yüksek click → koyu yeşil (önemli saatler belirgin)
  var HEATMAP_COLORSCALE = [
    [0, "#7f1d1d"],
    [0.14, "#ef4444"],
    [0.28, "#f97316"],
    [0.42, "#fde047"],
    [0.57, "#eab308"],
    [0.71, "#86efac"],
    [0.85, "#22c55e"],
    [1, "#14532d"],
  ];
  var HEATMAP_NO_DATA = null;

  function dowIndex(iso) {
    var dt = new Date(iso);
    if (isNaN(dt.getTime())) return -1;
    return (dt.getDay() + 6) % 7;
  }

  function effectivePrimaryRange(rows) {
    var range = primaryDateRange();
    if (range.start && range.end) return range;
    if (!rows || !rows.length) return range;
    var days = rows.map(function (r) {
      return nt().dayKey ? nt().dayKey(r.date) : String(r.date || "").slice(0, 10);
    }).filter(Boolean).sort();
    if (!days.length) return range;
    return { start: days[0], end: days[days.length - 1] };
  }

  function findDrillRow(id, text, date) {
    var all = nt().readRows ? nt().readRows() : [];
    var i;
    if (id) {
      for (i = 0; i < all.length; i++) {
        var r = all[i];
        if (nt().idString && nt().idString(r) !== id) continue;
        if (date && nt().dayKey && nt().dayKey(r.date) !== date) continue;
        return r;
      }
    }
    if (text) {
      for (i = 0; i < all.length; i++) {
        if (String(all[i].text || "").trim() === String(text || "").trim()) return all[i];
      }
    }
    return null;
  }
  var lastAlertPayload = null;
  var lastComparePayload = null;
  var lastContentTrafficPayload = null;
  var trafficLoadToken = 0;
  var inlineTrafficLoadToken = 0;
  var NT_TRAFFIC_LOTTIE = "https://assets7.lottiefiles.com/packages/lf20_t9gkkhz4.json";

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

  function fmtDeltaHtml(pct) {
    if (pct === null || pct === undefined || isNaN(pct)) {
      return '<span class="text-slate-500">—</span>';
    }
    var sign = pct > 0 ? "+" : "";
    var cls = pct > 0
      ? "text-emerald-600 dark:text-emerald-400"
      : (pct < 0 ? "text-rose-600 dark:text-rose-400" : "text-slate-500");
    return '<span class="font-semibold ' + cls + '">' + sign + Number(pct).toFixed(1) + "%</span>";
  }

  function ntIsDark() {
    return global.document.documentElement.classList.contains("dark");
  }

  function ntSparkFillRgba(hex, alpha) {
    var h = String(hex || "").replace("#", "");
    if (h.length === 3) h = h[0] + h[0] + h[1] + h[1] + h[2] + h[2];
    var r = parseInt(h.slice(0, 2), 16);
    var g = parseInt(h.slice(2, 4), 16);
    var b = parseInt(h.slice(4, 6), 16);
    return "rgba(" + r + "," + g + "," + b + "," + alpha + ")";
  }

  function ntCompareSparkColors() {
    var dark = ntIsDark();
    // Seçili = yeşil, önceki = turuncu — mavi/mor tonları üst üste karışmasın diye.
    return {
      primary: dark ? "#34d399" : "#059669",
      compare: dark ? "#fb923c" : "#ea580c",
    };
  }

  function ntSparkOverlayFillTrace(xs, ys, colorHex, dark) {
    return {
      x: xs,
      y: ys,
      type: "scatter",
      mode: "lines",
      line: { width: 1, color: colorHex, dash: "dot" },
      fill: "tozeroy",
      fillcolor: ntSparkFillRgba(colorHex, dark ? 0.2 : 0.16),
      connectgaps: false,
      hoverinfo: "skip",
    };
  }

  function ntSparkPrimaryTrace(xs, ys, colorHex, dark) {
    return {
      x: xs,
      y: ys,
      type: "scatter",
      mode: "lines",
      line: { width: 2, color: colorHex },
      fill: "tozeroy",
      fillcolor: ntSparkFillRgba(colorHex, dark ? 0.24 : 0.18),
      connectgaps: false,
      hoverinfo: "skip",
    };
  }

  function aggregateDailyForCompare(rows) {
    var byDay = {};
    (rows || []).forEach(function (r) {
      var k = nt().dayKey ? nt().dayKey(r.date) : String(r.date || "").slice(0, 10);
      if (!k) return;
      if (!byDay[k]) {
        byDay[k] = {
          day: k,
          clicks: 0,
          impressions: 0,
          desktop: 0,
          mobileweb: 0,
          android: 0,
          ios: 0,
          app: 0,
          web: 0,
          ctr: 0,
        };
      }
      var pc = rowPlatformClicks(r);
      byDay[k].clicks += pc.desktop + pc.mobileweb + pc.android + pc.ios;
      byDay[k].desktop += pc.desktop;
      byDay[k].mobileweb += pc.mobileweb;
      byDay[k].android += pc.android;
      byDay[k].ios += pc.ios;
      byDay[k].app += pc.android + pc.ios;
      byDay[k].web += pc.desktop + pc.mobileweb;
      var p = r.platforms || {};
      ["desktop", "mobileweb", "android"].forEach(function (pk) {
        if (nt().platformImpression) byDay[k].impressions += nt().platformImpression(pk, p[pk] || {});
      });
    });
    return Object.keys(byDay).sort().map(function (k) {
      var d = byDay[k];
      d.ctr = d.impressions > 0 ? (d.clicks / d.impressions) * 100 : 0;
      return d;
    });
  }

  function alignPeriodDaily(primaryDaily, compareDaily, key, maxPoints) {
    var p = (primaryDaily || []).slice();
    var c = (compareDaily || []).slice();
    var cap = maxPoints || 21;
    if (p.length < 2 && c.length < 2) return null;
    var n = Math.min(p.length, c.length);
    if (n < 2) {
      var solo = p.length >= 2 ? p : c;
      if (solo.length < 2) return null;
      solo = solo.slice(-cap);
      return {
        xs: solo.map(function (_, i) { return i; }),
        primary: p.length >= 2 ? solo.map(function (d) { return d[key] != null ? d[key] : 0; }) : null,
        compare: c.length >= 2 ? solo.map(function (d) { return d[key] != null ? d[key] : 0; }) : null,
        labels: solo.map(function (d) { return d.day; }),
      };
    }
    if (n > cap) n = cap;
    p = p.slice(-n);
    c = c.slice(-n);
    return {
      xs: p.map(function (_, i) { return i; }),
      primary: p.map(function (d) { return d[key] != null ? d[key] : 0; }),
      compare: c.map(function (d) { return d[key] != null ? d[key] : 0; }),
      labels: p.map(function (d) { return d.day; }),
    };
  }

  function renderNtSparkline(elId, primaryDaily, compareDaily, key) {
    if (!global.Plotly) return;
    var el = global.document.getElementById(elId);
    if (!el) return;
    var aligned = alignPeriodDaily(primaryDaily, compareDaily, key, 21);
    if (!aligned) return;
    var colors = ntCompareSparkColors();
    var dark = ntIsDark();
    var traces = [];
    if (aligned.compare && aligned.compare.some(function (v) { return v !== 0; })) {
      traces.push(ntSparkOverlayFillTrace(aligned.xs, aligned.compare, colors.compare, dark));
    }
    if (aligned.primary && aligned.primary.some(function (v) { return v !== 0; })) {
      traces.push(ntSparkPrimaryTrace(aligned.xs, aligned.primary, colors.primary, dark));
    }
    if (!traces.length) return;
    try { global.Plotly.purge(el); } catch (e) { /* ignore */ }
    global.Plotly.newPlot(el, traces, {
      paper_bgcolor: "rgba(0,0,0,0)",
      plot_bgcolor: "rgba(0,0,0,0)",
      margin: { l: 4, r: 4, t: 2, b: 2 },
      xaxis: { visible: false },
      yaxis: { visible: false },
      showlegend: false,
    }, { responsive: true, displayModeBar: false, staticPlot: true });
  }

  var _ntPeriodChartTimer = null;
  var _lastPeriodDaily = null;

  function ntChartFont() {
    return { color: ntIsDark() ? "#a1a1aa" : "#475569", size: 11 };
  }

  function ntChartGrid() {
    return ntIsDark() ? "#27272a" : "#e2e8f0";
  }

  function renderPeriodCompareCharts(curDaily, prevDaily) {
    if (!global.Plotly) return;
    var trendEl = global.document.getElementById("nt-period-trend-chart");
    if (trendEl) {
      var aligned = alignPeriodDaily(curDaily, prevDaily, "clicks", 90);
      if (aligned && aligned.primary && aligned.primary.length >= 2) {
        var colors = ntCompareSparkColors();
        var dark = ntIsDark();
        var traces = [];
        if (aligned.compare && aligned.compare.some(function (v) { return v !== 0; })) {
          traces.push({
            x: aligned.labels,
            y: aligned.compare,
            type: "scatter",
            mode: "lines",
            name: "Önceki dönem",
            line: { color: colors.compare, width: 2, dash: "dot" },
            hovertemplate: "%{x}<br>%{y:,}<extra>Önceki</extra>",
          });
        }
        traces.push({
          x: aligned.labels,
          y: aligned.primary,
          type: "scatter",
          mode: "lines",
          name: "Seçili dönem",
          line: { color: colors.primary, width: 2.75 },
          fill: "tozeroy",
          fillcolor: ntSparkFillRgba(colors.primary, dark ? 0.18 : 0.12),
          hovertemplate: "%{x}<br>%{y:,}<extra>Seçili</extra>",
        });
        global.Plotly.newPlot(trendEl, traces, {
          autosize: true,
          margin: { l: 52, r: 12, t: 8, b: 40 },
          paper_bgcolor: "rgba(0,0,0,0)",
          plot_bgcolor: "rgba(0,0,0,0)",
          font: ntChartFont(),
          xaxis: { type: "date", tickformat: "%d.%m", gridcolor: ntChartGrid() },
          yaxis: { title: "Click", gridcolor: ntChartGrid(), zerolinecolor: ntChartGrid() },
          legend: { orientation: "h", y: 1.18, font: { size: 10, color: ntChartFont().color } },
          showlegend: true,
        }, { responsive: true, displayModeBar: false });
      } else {
        trendEl.innerHTML = '<p class="flex h-full items-center justify-center text-xs text-slate-500">Trend için yeterli günlük veri yok.</p>';
      }
    }

    var platEl = global.document.getElementById("nt-period-platform-chart");
    if (platEl && lastComparePayload) {
      var cur = lastComparePayload.current;
      var prev = lastComparePayload.previous;
      var labels = ["Web", "MWeb", "Android", "iOS"];
      var keys = ["desktop", "mobileweb", "android", "ios"];
      var curY = keys.map(function (k) { return cur.platform[k]; });
      var prevY = keys.map(function (k) { return prev.platform[k]; });
      var barColors = ntIsDark()
        ? ["#7176c4", "#bf8f4a", "#4ade80", "#f87171"]
        : ["#6366f1", "#f59e0b", "#22c55e", "#ef4444"];
      var prevBar = ntIsDark() ? "rgba(251,146,60,0.35)" : "rgba(234,88,12,0.35)";
      global.Plotly.newPlot(platEl, [
        {
          x: labels,
          y: prevY,
          type: "bar",
          name: "Önceki",
          marker: { color: prevBar },
          hovertemplate: "%{x}<br>%{y:,}<extra>Önceki</extra>",
        },
        {
          x: labels,
          y: curY,
          type: "bar",
          name: "Seçili",
          marker: { color: barColors },
          hovertemplate: "%{x}<br>%{y:,}<extra>Seçili</extra>",
        },
      ], {
        barmode: "group",
        autosize: true,
        margin: { l: 52, r: 12, t: 8, b: 36 },
        paper_bgcolor: "rgba(0,0,0,0)",
        plot_bgcolor: "rgba(0,0,0,0)",
        font: ntChartFont(),
        xaxis: { gridcolor: ntChartGrid() },
        yaxis: { title: "Click", gridcolor: ntChartGrid(), zerolinecolor: ntChartGrid() },
        legend: { orientation: "h", y: 1.18, font: { size: 10, color: ntChartFont().color } },
        showlegend: true,
      }, { responsive: true, displayModeBar: false });
    }
  }

  function scheduleNtPeriodCharts(curDaily, prevDaily) {
    _lastPeriodDaily = { cur: curDaily, prev: prevDaily };
    if (_ntPeriodChartTimer) clearTimeout(_ntPeriodChartTimer);
    _ntPeriodChartTimer = setTimeout(function () {
      _ntPeriodChartTimer = null;
      var sparkDefs = [
        ["nt-spark-clicks", "clicks"],
        ["nt-spark-impressions", "impressions"],
        ["nt-spark-ctr", "ctr"],
        ["nt-spark-desktop", "desktop"],
        ["nt-spark-mobileweb", "mobileweb"],
        ["nt-spark-android", "android"],
        ["nt-spark-ios", "ios"],
        ["nt-spark-app", "app"],
        ["nt-spark-web", "web"],
      ];
      var i = 0;
      function step() {
        if (i >= sparkDefs.length) {
          renderPeriodCompareCharts(curDaily, prevDaily);
          return;
        }
        var pair = sparkDefs[i++];
        renderNtSparkline(pair[0], curDaily, prevDaily, pair[1]);
        setTimeout(step, 10);
      }
      global.requestAnimationFrame(function () { step(); });
    }, 80);
  }

  function periodKpiCard(id, label, value, prevVal, delta, opts) {
    opts = opts || {};
    var valStr = opts.pct
      ? Number(value).toFixed(2) + "%"
      : (nt().fmtCount ? nt().fmtCount(value) : value);
    var prevStr = opts.pct
      ? Number(prevVal).toFixed(2) + "%"
      : (nt().fmtCount ? nt().fmtCount(prevVal) : prevVal);
    return '<div class="rounded-xl border border-slate-200 bg-white p-3 dark:border-slate-700 dark:bg-slate-900/50">'
      + '<p class="text-[10px] font-semibold uppercase tracking-wide text-slate-500 dark:text-slate-400">' + label + "</p>"
      + '<p class="mt-1 text-base font-black text-slate-900 dark:text-slate-200">' + valStr + "</p>"
      + '<p class="mt-0.5 text-[10px] text-slate-500 dark:text-slate-400">Önceki: ' + prevStr + " · " + fmtDeltaHtml(delta) + "</p>"
      + '<div id="' + id + '" class="mt-2 h-9 w-full"></div></div>';
  }

  function periodPlatCard(id, label, value, delta, share) {
    return '<div class="rounded-xl border border-slate-200 bg-white p-2.5 dark:border-slate-700 dark:bg-slate-900/50">'
      + '<p class="text-[10px] font-bold uppercase text-slate-500 dark:text-slate-400">' + label + "</p>"
      + '<p class="mt-0.5 text-sm font-black text-slate-800 dark:text-slate-200">' + (nt().fmtCount ? nt().fmtCount(value) : value) + "</p>"
      + '<p class="text-[10px] text-slate-500 dark:text-slate-400">' + fmtDeltaHtml(delta) + ' · pay %' + share.toFixed(1) + "</p>"
      + '<div id="' + id + '" class="mt-1.5 h-8 w-full"></div></div>';
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
    var range = effectivePrimaryRange(primaryRows);
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
      var curDaily = aggregateDailyForCompare(primaryRows);
      var prevDaily = aggregateDailyForCompare(prevRows);
      var platHtml = ["desktop", "mobileweb", "android", "ios"].map(function (k) {
        var labels = { desktop: "Web", mobileweb: "MWeb", android: "Android", ios: "iOS" };
        var sparkIds = { desktop: "nt-spark-desktop", mobileweb: "nt-spark-mobileweb", android: "nt-spark-android", ios: "nt-spark-ios" };
        var c = curStats.platform[k];
        var p = prevStats.platform[k];
        var share = curStats.clicks > 0 ? (c / curStats.clicks * 100) : 0;
        return periodPlatCard(sparkIds[k], labels[k], c, delta(c, p), share);
      }).join("");
      el.innerHTML = '<p class="mb-2 text-xs text-slate-500 dark:text-slate-400">'
        + range.start + " – " + range.end + " vs " + prev.start + " – " + prev.end + "</p>"
        + '<div class="grid grid-cols-1 gap-2 sm:grid-cols-3">'
        + periodKpiCard("nt-spark-clicks", "Toplam click", curStats.clicks, prevStats.clicks, clickD)
        + periodKpiCard("nt-spark-impressions", "Toplam impression", curStats.impressions, prevStats.impressions, imprD)
        + periodKpiCard("nt-spark-ctr", "CTR (web+android impr)", curStats.ctr, prevStats.ctr, ctrD, { pct: true })
        + "</div>"
        + '<div class="mt-3 grid grid-cols-2 gap-2 sm:grid-cols-4">' + platHtml + "</div>"
        + '<div class="mt-3 grid grid-cols-1 gap-2 sm:grid-cols-2">'
        + periodKpiCard("nt-spark-app", "App (Android+iOS)", curStats.app, prevStats.app, delta(curStats.app, prevStats.app))
        + periodKpiCard("nt-spark-web", "Web (Desktop+MWeb)", curStats.web, prevStats.web, delta(curStats.web, prevStats.web))
        + "</div>"
        + '<div class="mt-4 grid grid-cols-1 gap-3 lg:grid-cols-2">'
        + '<div class="rounded-xl border border-slate-200 bg-white p-3 dark:border-slate-700 dark:bg-slate-900/50">'
        + '<p class="text-[10px] font-semibold uppercase tracking-wide text-slate-500 dark:text-slate-400">Günlük click trendi</p>'
        + '<p class="mt-0.5 text-[10px] text-slate-500 dark:text-slate-400">Yeşil: seçili · turuncu noktalı: önceki dönem (gün hizalı)</p>'
        + '<div id="nt-period-trend-chart" class="mt-2 h-[180px] w-full"></div></div>'
        + '<div class="rounded-xl border border-slate-200 bg-white p-3 dark:border-slate-700 dark:bg-slate-900/50">'
        + '<p class="text-[10px] font-semibold uppercase tracking-wide text-slate-500 dark:text-slate-400">Platform click karşılaştırması</p>'
        + '<p class="mt-0.5 text-[10px] text-slate-500 dark:text-slate-400">Turuncu: önceki · renkli: seçili dönem</p>'
        + '<div id="nt-period-platform-chart" class="mt-2 h-[180px] w-full"></div></div>'
        + "</div>";
      scheduleNtPeriodCharts(curDaily, prevDaily);
    }).catch(function () {
      el.innerHTML = '<p class="text-xs text-rose-600">Karşılaştırma verisi yüklenemedi.</p>';
    });
  }

  function renderHeatmap(rows) {
    var el = global.document.getElementById("nt-heatmap");
    if (!el || !global.Plotly) return;
    if (!rows || !rows.length) {
      el.innerHTML = '<p class="flex h-full items-center justify-center text-xs text-slate-500">Heatmap için veri yok.</p>';
      return;
    }
    var metric = (global.document.getElementById("nt-heatmap-metric") || {}).value || "total";
    var hStart = HEATMAP_HOUR_START;
    var hEnd = HEATMAP_HOUR_END;
    var grid = {};
    for (var d = 0; d < 7; d++) {
      grid[d] = {};
      for (var h = hStart; h <= hEnd; h++) grid[d][h] = HEATMAP_NO_DATA;
    }
    (rows || []).forEach(function (r) {
      var iso = String(r.date || "");
      if (!iso) return;
      var dow = dowIndex(iso);
      if (dow < 0) return;
      var dt = new Date(iso);
      var hour = dt.getHours();
      if (hour < hStart || hour > hEnd) return;
      if (grid[dow][hour] === HEATMAP_NO_DATA) grid[dow][hour] = 0;
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
    var zFlat = [];
    for (var hi = hStart; hi <= hEnd; hi++) {
      x.push((hi < 10 ? "0" : "") + hi + ":00");
    }
    for (var di = 0; di < 7; di++) {
      var row = [];
      for (var hj = hStart; hj <= hEnd; hj++) {
        var cell = grid[di][hj];
        row.push(cell);
        if (cell !== HEATMAP_NO_DATA && cell !== null) zFlat.push(cell);
      }
      z.push(row);
    }
    var zmax = zFlat.length ? Math.max.apply(null, zFlat) : 1;
    if (zmax <= 0) zmax = 1;
    var dark = global.document.documentElement.classList.contains("dark");
    var noDataGray = dark ? "#3f3f46" : "#d4d4d8";
    Plotly.newPlot(el, [{
      type: "heatmap",
      x: x,
      y: y,
      z: z,
      colorscale: HEATMAP_COLORSCALE,
      zmin: 0,
      zmax: zmax,
      hoverongaps: false,
      colorbar: { tickfont: { size: 9, color: dark ? "#a1a1aa" : "#475569" } },
    }], {
      margin: { l: 50, r: 10, t: 10, b: 40 },
      paper_bgcolor: "rgba(0,0,0,0)",
      plot_bgcolor: noDataGray,
      font: { color: dark ? "#a1a1aa" : "#475569", size: 10 },
      xaxis: { title: "Saat (07:00–23:00)", tickangle: -45 },
      yaxis: { title: "Gün" },
    }, { responsive: true, displayModeBar: false });
  }

  function onRedraw(ev) {
    var rows = (ev && ev.detail && ev.detail.rows) || (nt().getFilteredRows ? nt().getFilteredRows() : []);
    renderPeriodCompare(rows);
    renderHeatmap(rows);
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

  function bindDrill() {
    global.document.addEventListener("click", function (ev) {
      var closeBtn = ev.target && ev.target.closest ? ev.target.closest("[data-nt-cross-drill-close]") : null;
      if (closeBtn) {
        ev.preventDefault();
        ev.stopPropagation();
        if (nt().setCrossTopDrill) nt().setCrossTopDrill(null);
        return;
      }
      var btn = ev.target && ev.target.closest ? ev.target.closest("[data-nt-drill]") : null;
      if (!btn) return;
      var inCrossTop = btn.closest("#nt-cross-top-list");
      if (inCrossTop) {
        ev.preventDefault();
        var id = btn.getAttribute("data-nt-drill-id") || "";
        var text = btn.getAttribute("data-nt-drill-text") || "";
        var date = btn.getAttribute("data-nt-drill-date") || "";
        var row = findDrillRow(id, text, date);
        if (nt().setCrossTopDrill) {
          nt().setCrossTopDrill({
            id: id,
            text: row ? String(row.text || "") : text,
            date: date || (row && nt().dayKey ? nt().dayKey(row.date) : "")
          });
        }
        return;
      }
      if (!global.NTDrill) return;
      ev.preventDefault();
      var drillId = btn.getAttribute("data-nt-drill-id") || "";
      var drillText = btn.getAttribute("data-nt-drill-text") || "";
      var drillDate = btn.getAttribute("data-nt-drill-date") || "";
      var drillRow = findDrillRow(drillId, drillText, drillDate);
      global.NTDrill.set({
        id: drillId,
        text: drillRow ? String(drillRow.text || "") : drillText,
        date: drillDate || (drillRow && nt().dayKey ? nt().dayKey(drillRow.date) : "")
      });
    });
    var crossList = global.document.getElementById("nt-cross-top-list");
    if (crossList) {
      crossList.addEventListener("change", function (ev) {
        var sel = ev.target;
        if (!sel || !sel.classList || !sel.classList.contains("nt-inline-traffic-days")) return;
        if (nt().refreshCrossTopDrillContent) nt().refreshCrossTopDrillContent();
      });
    }
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

  function ntTrafficLottieReady() {
    return !!(global.customElements && global.customElements.get("lottie-player"));
  }

  function ntStopTrafficLottie(container) {
    if (!container) return;
    var player = container.querySelector("lottie-player.nt-traffic-lottie");
    if (!player) return;
    try {
      if (typeof player.stop === "function") player.stop();
      else if (typeof player.pause === "function") player.pause();
    } catch (e) { /* lottie-player DOM kaldırılırken hata vermesin */ }
  }

  function ntTrafficLoadingHtml() {
    if (ntTrafficLottieReady()) {
      return '<div class="nt-traffic-loading flex flex-col items-center justify-center gap-1 py-2">'
        + '<lottie-player class="nt-traffic-lottie" src="' + NT_TRAFFIC_LOTTIE + '" background="transparent" speed="1" loop autoplay'
        + ' style="width:96px;height:96px"></lottie-player>'
        + '<p class="text-xs font-medium text-emerald-700 dark:text-emerald-300">GA4 / GSC trafik yükleniyor…</p>'
        + '<p class="text-[10px] text-slate-500 dark:text-slate-400">Search Console ve Analytics eşleşmesi alınıyor</p>'
        + "</div>";
    }
    return '<div class="nt-traffic-loading flex flex-col items-center justify-center gap-2 py-3">'
      + '<div class="h-10 w-10 animate-spin rounded-full border-2 border-emerald-500 border-t-transparent" aria-hidden="true"></div>'
      + '<p class="text-xs text-slate-500 dark:text-slate-400">GA4 / GSC trafik yükleniyor…</p>'
      + "</div>";
  }

  function ntShowTrafficLoading(body) {
    if (!body) return;
    ntStopTrafficLottie(body);
    body.innerHTML = ntTrafficLoadingHtml();
    if (ntTrafficLottieReady()) return;
    var waits = 0;
    var timer = global.setInterval(function () {
      waits += 1;
      if (!body.isConnected) {
        global.clearInterval(timer);
        return;
      }
      if (ntTrafficLottieReady() && body.querySelector(".nt-traffic-loading") && !body.querySelector("lottie-player")) {
        body.innerHTML = ntTrafficLoadingHtml();
        global.clearInterval(timer);
      } else if (waits >= 30) {
        global.clearInterval(timer);
      }
    }, 100);
  }

  function clearContentTraffic() {
    lastContentTrafficPayload = null;
    var panel = global.document.getElementById("nt-content-traffic");
    var body = global.document.getElementById("nt-content-traffic-body");
    var meta = global.document.getElementById("nt-content-traffic-meta");
    if (body) ntStopTrafficLottie(body);
    if (panel) panel.classList.add("hidden");
    if (body) body.innerHTML = "";
    if (meta) meta.textContent = "";
  }

  function clearInlineDrillTraffic() {
    inlineTrafficLoadToken++;
    global.document.querySelectorAll(".nt-inline-traffic-body").forEach(function (el) {
      ntStopTrafficLottie(el);
      el.innerHTML = "";
    });
    global.document.querySelectorAll(".nt-inline-traffic-meta").forEach(function (el) {
      el.textContent = "";
    });
    global.document.querySelectorAll(".nt-inline-drill-cards").forEach(function (el) {
      el.innerHTML = "";
    });
  }

  function renderDrillMiniCards(row, containerEl) {
    if (!containerEl || !row) return;
    var pc = rowPlatformClicks(row);
    containerEl.innerHTML = [
      { k: "Web", v: pc.desktop },
      { k: "MWeb", v: pc.mobileweb },
      { k: "Android", v: pc.android },
      { k: "iOS", v: pc.ios },
    ].map(function (x) {
      return '<div class="rounded-lg border border-slate-200 bg-white px-2 py-2 text-center dark:border-slate-700 dark:bg-slate-900">'
        + '<p class="text-[10px] font-bold uppercase text-slate-500">' + x.k + '</p>'
        + '<p class="text-lg font-black text-indigo-700 dark:text-indigo-300">' + (nt().fmtCount ? nt().fmtCount(x.v) : x.v) + "</p></div>";
    }).join("");
  }

  function renderInlineDrill(row, rootEl) {
    if (!rootEl || !row) return;
    var cards = rootEl.querySelector(".nt-inline-drill-cards");
    renderDrillMiniCards(row, cards);
    var bodyEl = rootEl.querySelector(".nt-inline-traffic-body");
    var metaEl = rootEl.querySelector(".nt-inline-traffic-meta");
    var daysEl = rootEl.querySelector(".nt-inline-traffic-days");
    loadContentTraffic(row, { bodyEl: bodyEl, metaEl: metaEl, daysEl: daysEl, inline: true });
  }

  function ga4NotifBucket(ga4, profileKey) {
    var profiles = ga4 && ga4.source_breakdown_profiles;
    var sb = profiles && profiles[profileKey];
    if (!sb || !sb.buckets) return null;
    for (var i = 0; i < sb.buckets.length; i++) {
      if (sb.buckets[i].key === "notification") return sb.buckets[i];
    }
    return null;
  }

  function renderContentTraffic(data, targets) {
    targets = targets || {};
    var row = targets.row || null;
    var panel = targets.panelEl || global.document.getElementById("nt-content-traffic");
    var body = targets.bodyEl || global.document.getElementById("nt-content-traffic-body");
    var meta = targets.metaEl || global.document.getElementById("nt-content-traffic-meta");
    var inline = !!targets.inline;
    if (!body) return;
    ntStopTrafficLottie(body);
    if (!data || !data.article_id) {
      if (inline) {
        if (body) body.innerHTML = '<p class="text-[10px] text-slate-500">Geçerli içerik ID bulunamadı.</p>';
        if (meta) meta.textContent = "";
      } else {
        clearContentTraffic();
      }
      return;
    }
    if (!inline && panel) panel.classList.remove("hidden");
    var sum = data.summary || {};
    var ga4 = data.ga4 || {};
    var gsc = data.gsc || {};
    var gsc7 = (gsc.scopes && gsc.scopes.live) || (gsc.scopes && gsc.scopes.current_7d_pages) || {};
    var gsc30 = (gsc.scopes && gsc.scopes.current_30d_pages) || {};
    var gscClicks = sum.gsc_clicks != null ? sum.gsc_clicks : (gsc7.clicks || sum.gsc_clicks_7d || 0);
    var gscImpr = sum.gsc_impressions != null ? sum.gsc_impressions : (gsc7.impressions || sum.gsc_impressions_7d || 0);
    var gscPos = sum.gsc_position != null ? sum.gsc_position : (gsc7.position || 0);
    var gscStart = sum.gsc_start || gsc7.start_date || (data.date_range && data.date_range.start) || "";
    var gscEnd = sum.gsc_end || gsc7.end_date || (data.date_range && data.date_range.end) || "";
    var gscRangeLabel = (gscStart && gscEnd) ? (gscStart + " – " + gscEnd) : "seçili pencere";
    if (meta) {
      var dr = data.date_range || {};
      var rangeTxt = (dr.start && dr.end) ? (dr.start + " – " + dr.end) : ("son " + (data.days || 14) + " gün");
      var matchTxt = sum.match_method === "headline" ? " · başlık eşleşmesi" : (sum.match_method === "path_id" ? " · URL ID eşleşmesi" : "");
      meta.textContent = "Bildirim ID " + data.content_id
        + (data.resolved_article_id && data.resolved_article_id !== data.article_id ? (" → makale " + data.resolved_article_id) : "")
        + " · " + (data.site_domain || "") + " · " + rangeTxt + matchTxt;
    }
    var urlHtml = (sum.matched_urls || []).slice(0, 5).map(function (u) {
      return '<a class="block truncate text-emerald-800 underline dark:text-emerald-300" href="' + (nt().escapeHtml ? nt().escapeHtml(u) : u) + '" target="_blank" rel="noopener">' + (nt().escapeHtml ? nt().escapeHtml(u) : u) + "</a>";
    }).join("");
    var ga4Profiles = ga4.profiles || {};
    var ga4ProfileTotals = ga4.profile_totals || {};
    var ga4Detail = ["web", "mweb"].map(function (pf) {
      var pt = ga4ProfileTotals[pf];
      var v = pt && pt.views != null
        ? Number(pt.views)
        : (ga4Profiles[pf] || []).reduce(function (a, r) { return a + Number(r.views || 0); }, 0);
      if (!v && !(ga4Profiles[pf] || []).length) return "";
      var label = pf === "web" ? "WEB" : "MWEB";
      return '<span class="mr-2">' + label + ": " + (nt().fmtCount ? nt().fmtCount(v) : v) + " görüntüleme</span>";
    }).join("");
    var ga4SourceHtml = (function () {
      var sb = ga4.source_breakdown;
      if (!sb || !sb.buckets || !sb.buckets.length) return "";
      var totalS = sb.buckets.reduce(function (a, b) { return a + Number(b.sessions || 0); }, 0) || 1;
      var bucketHtml = sb.buckets.map(function (b) {
        var pct = Math.min(100, Math.round((Number(b.sessions || 0) / totalS) * 100));
        var label = b.label || b.key || "";
        return '<div class="mb-1.5">'
          + '<div class="flex items-center justify-between gap-2 text-[10px]">'
          + '<span class="truncate text-slate-600 dark:text-slate-400">' + (nt().escapeHtml ? nt().escapeHtml(label) : label) + "</span>"
          + '<span class="shrink-0 font-semibold text-slate-700 dark:text-slate-200">'
          + (nt().fmtCount ? nt().fmtCount(b.sessions) : b.sessions) + " oturum"
          + (b.views ? " · " + (nt().fmtCount ? nt().fmtCount(b.views) : b.views) + " gör." : "")
          + "</span></div>"
          + '<div class="h-1 rounded-full bg-slate-200 dark:bg-slate-700"><div class="h-1 rounded-full bg-emerald-600" style="width:' + Math.max(pct, 2) + '%"></div></div>'
          + "</div>";
      }).join("");
      var topSm = sb.source_medium || [];
      var smHtml = topSm.length
        ? '<details class="mt-1" open><summary class="cursor-pointer text-[10px] text-emerald-700 dark:text-emerald-400">Kaynak / medium (' + topSm.length + ")</summary>"
          + '<div class="mt-1 max-h-40 space-y-0.5 overflow-y-auto">' + topSm.map(function (r) {
            var sm = r.source_medium || "";
            return '<div class="flex justify-between gap-2 text-[9px] text-slate-500 dark:text-slate-400">'
              + '<span class="truncate">' + (nt().escapeHtml ? nt().escapeHtml(sm) : sm) + "</span>"
              + '<span class="shrink-0 font-medium">' + (nt().fmtCount ? nt().fmtCount(r.sessions) : r.sessions)
              + (r.views ? " · " + (nt().fmtCount ? nt().fmtCount(r.views) : r.views) : "") + "</span></div>";
          }).join("") + "</div></details>"
        : "";
      var notifCompare = "";
      if (row) {
        var nc = rowPlatformClicks(row);
        var webGa4 = ga4NotifBucket(ga4, "web");
        var mwebGa4 = ga4NotifBucket(ga4, "mweb");
        var csvWebTotal = nc.desktop + nc.mobileweb;
        var ga4NotifTotal = (webGa4 ? Number(webGa4.sessions || 0) : 0) + (mwebGa4 ? Number(mwebGa4.sessions || 0) : 0);
        notifCompare = '<div class="nt-traffic-compare-box mb-2 rounded-lg border border-amber-100 bg-amber-50/70 p-2 text-[9px] leading-relaxed text-amber-950 dark:border-amber-900/50 dark:bg-amber-950/20 dark:text-amber-100">'
          + '<div class="flex items-center justify-between gap-1">'
          + '<p class="font-bold uppercase tracking-wide">Bildirim tıklaması vs GA4</p>'
          + '<button type="button" class="nt-traffic-compare-info-btn inline-flex h-4 w-4 shrink-0 items-center justify-center rounded-full border border-amber-300/80 bg-white/80 text-[10px] font-bold leading-none text-amber-700 hover:bg-amber-100 dark:border-amber-700 dark:bg-amber-950/40 dark:text-amber-200 dark:hover:bg-amber-900/50" aria-expanded="false" aria-label="Metrik açıklaması" title="Metrik açıklaması">i</button>'
          + "</div>"
          + '<p class="mt-1">CSV (bu gönderim): WEB ' + (nt().fmtCount ? nt().fmtCount(nc.desktop) : nc.desktop)
          + " + MWEB " + (nt().fmtCount ? nt().fmtCount(nc.mobileweb) : nc.mobileweb)
          + " = <strong>" + (nt().fmtCount ? nt().fmtCount(csvWebTotal) : csvWebTotal) + " tık</strong>"
          + " · App " + (nt().fmtCount ? nt().fmtCount(nc.android + nc.ios) : (nc.android + nc.ios)) + "</p>"
          + '<p>GA4 <code class="text-[8px]">comet/notification</code> oturum: WEB '
          + (webGa4 ? (nt().fmtCount ? nt().fmtCount(webGa4.sessions) : webGa4.sessions) : "0")
          + " + MWEB " + (mwebGa4 ? (nt().fmtCount ? nt().fmtCount(mwebGa4.sessions) : mwebGa4.sessions) : "0")
          + " = <strong>" + (nt().fmtCount ? nt().fmtCount(ga4NotifTotal) : ga4NotifTotal) + "</strong>"
          + " · " + gscRangeLabel + " penceresi</p>"
          + '<p class="nt-traffic-compare-tip hidden mt-1 rounded border border-amber-200/80 bg-white/70 px-2 py-1 text-amber-900 dark:border-amber-800 dark:bg-amber-950/30 dark:text-amber-100">CSV = push tıklaması (tek bildirim). GA4 = makale sayfasına gelen oturumlar (3 gün, aynı URL’ye başka bildirimler ve tekrar ziyaretler dahil olabilir).</p>'
          + "</div>";
      }
      return notifCompare + '<div class="mt-2 border-t border-emerald-100 pt-2 dark:border-emerald-900">'
        + '<p class="mb-1 text-[10px] font-bold uppercase text-slate-500">Trafik kaynağı</p>'
        + bucketHtml + smHtml + "</div>";
    })();
    var gscPages = sum.gsc_pages || gsc.pages || [];
    var gscPagesHtml = gscPages.length
      ? '<div class="mt-2">'
        + '<p class="text-[10px] font-bold uppercase text-sky-700 dark:text-sky-300">GSC sayfa kırılımı (' + gscPages.length + ")</p>"
        + '<div class="mt-1 max-h-36 space-y-1 overflow-y-auto">' + gscPages.map(function (p) {
          var u = p.url || "";
          return '<div class="rounded border border-sky-100 px-2 py-1 text-[9px] dark:border-sky-900">'
            + '<p class="truncate text-sky-900 dark:text-sky-200">' + (nt().escapeHtml ? nt().escapeHtml(u) : u) + "</p>"
            + '<p class="text-slate-500">' + (nt().fmtCount ? nt().fmtCount(p.clicks || 0) : (p.clicks || 0)) + " click · "
            + (nt().fmtCount ? nt().fmtCount(p.impressions || 0) : (p.impressions || 0)) + " impr · poz "
            + Number(p.position || 0).toFixed(1) + "</p></div>";
        }).join("") + "</div></div>"
      : (gscImpr > 0 && !gscPages.length
        ? '<p class="mt-2 text-[9px] text-slate-500">GSC toplam görünür ama sayfa satırı yok — URL eşleşmesi kontrol ediliyor.</p>'
        : "");
    var gscNote = (gscClicks === 0 && gscImpr > 0)
      ? '<p class="mt-1 text-[9px] text-sky-700/80 dark:text-sky-300/80">0 click = Google arama sonuçlarından tıklama yok. Bildirim trafiği GSC’de görünmez.</p>'
      : "";
    body.innerHTML = '<div class="grid grid-cols-1 gap-2 sm:grid-cols-2">'
      + '<div class="rounded-lg border border-emerald-200 bg-white p-2 dark:border-emerald-900 dark:bg-slate-900">'
      + '<p class="font-bold text-emerald-800 dark:text-emerald-300">GA4</p>'
      + '<p class="mt-1 text-lg font-black">' + (nt().fmtCount ? nt().fmtCount(sum.ga4_views || 0) : (sum.ga4_views || 0)) + ' <span class="text-xs font-normal">görüntüleme</span></p>'
      + '<p class="text-[10px] text-slate-500">' + (nt().fmtCount ? nt().fmtCount(sum.ga4_sessions || 0) : (sum.ga4_sessions || 0)) + " oturum · " + ga4Detail + "</p>"
      + ga4SourceHtml + "</div>"
      + '<div class="rounded-lg border border-sky-200 bg-white p-2 dark:border-sky-900 dark:bg-slate-900">'
      + '<p class="font-bold text-sky-800 dark:text-sky-300">Search Console <span class="text-[9px] font-normal">(organik arama)</span></p>'
      + '<p class="mt-1 text-lg font-black">' + (nt().fmtCount ? nt().fmtCount(gscClicks) : gscClicks) + ' <span class="text-xs font-normal">click</span></p>'
      + '<p class="text-[10px] text-slate-500">' + (nt().fmtCount ? nt().fmtCount(gscImpr) : gscImpr) + " impr · poz " + Number(gscPos || 0).toFixed(1)
      + " · " + gscRangeLabel
      + (gsc30.clicks ? " · 30g depo " + (nt().fmtCount ? nt().fmtCount(gsc30.clicks) : gsc30.clicks) + " click" : "") + "</p>"
      + gscNote + gscPagesHtml + "</div>"
      + "</div>"
      + (urlHtml ? '<div class="mt-2"><p class="text-[10px] font-bold uppercase text-slate-500">Eşleşen URL</p>' + urlHtml + "</div>" : '<p class="mt-2 text-[10px] text-slate-500">Bu bildirim için GA4/GSC URL eşleşmesi bulunamadı. Başlık ve gönderim tarihi ile tekrar denendi.</p>');
  }

  function loadContentTraffic(row, targets) {
    targets = targets || {};
    var inline = !!targets.inline;
    if (!row) {
      if (inline) clearInlineDrillTraffic();
      else clearContentTraffic();
      return;
    }
    var cid = nt().idString ? nt().idString(row) : String(row.id || "");
    if (!cid || !/^\d+$/.test(cid.replace(/\D/g, "").slice(0, 20))) {
      if (inline) {
        if (targets.bodyEl) targets.bodyEl.innerHTML = '<p class="text-[10px] text-slate-500">Geçerli bildirim ID yok.</p>';
      } else {
        clearContentTraffic();
      }
      return;
    }
    var daysEl = targets.daysEl || global.document.getElementById("nt-traffic-days");
    var days = daysEl && daysEl.value ? parseInt(daysEl.value, 10) : 14;
    var sendDay = nt().dayKey ? nt().dayKey(row.date) : String(row.date || "").slice(0, 10);
    var headline = encodeURIComponent(String(row.text || "").trim());
    var token = inline ? ++inlineTrafficLoadToken : ++trafficLoadToken;
    var panel = targets.panelEl || global.document.getElementById("nt-content-traffic");
    var body = targets.bodyEl || global.document.getElementById("nt-content-traffic-body");
    var meta = targets.metaEl || global.document.getElementById("nt-content-traffic-meta");
    if (!inline && panel) panel.classList.remove("hidden");
    if (body) ntShowTrafficLoading(body);
    if (meta && inline) meta.textContent = "";
    var q = "/api/notification-analytics/traffic?content_id=" + encodeURIComponent(cid)
      + "&days=" + days + "&site_id=1";
    if (sendDay) q += "&send_date=" + encodeURIComponent(sendDay);
    if (row.text) q += "&headline=" + headline;
    apiFetch(q)
      .then(function (data) {
        if (inline) {
          if (token !== inlineTrafficLoadToken) return;
        } else if (token !== trafficLoadToken) {
          return;
        }
        if (!inline) lastContentTrafficPayload = data;
        renderContentTraffic(data, {
          bodyEl: body,
          metaEl: meta,
          panelEl: panel,
          inline: inline,
          row: row,
        });
        if (!inline && global.scrollHamDrillIntoView && global.NTDrill && global.NTDrill.get()) {
          global.setTimeout(function () { global.scrollHamDrillIntoView(); }, 150);
        }
      })
      .catch(function () {
        if (inline) {
          if (token !== inlineTrafficLoadToken) return;
        } else if (token !== trafficLoadToken) {
          return;
        }
        if (body) {
          ntStopTrafficLottie(body);
          body.innerHTML = '<p class="text-xs text-rose-600">Trafik verisi yüklenemedi.</p>';
        }
      });
  }

  function bindTrafficDays() {
    var sel = global.document.getElementById("nt-traffic-days");
    if (!sel) return;
    sel.addEventListener("change", function () {
      if (global.NTDrill && global.NTDrill.get()) {
        var f = global.NTDrill.get();
        var row = findDrillRow(f.id, f.text, f.date);
        if (row) loadContentTraffic(row);
      }
    });
  }

  function bindTrafficCompareInfo() {
    if (bindTrafficCompareInfo._bound) return;
    bindTrafficCompareInfo._bound = true;
    global.document.addEventListener("click", function (ev) {
      var btn = ev.target && ev.target.closest ? ev.target.closest(".nt-traffic-compare-info-btn") : null;
      if (!btn) return;
      ev.preventDefault();
      ev.stopPropagation();
      var box = btn.closest(".nt-traffic-compare-box");
      if (!box) return;
      var tip = box.querySelector(".nt-traffic-compare-tip");
      if (!tip) return;
      var willShow = tip.classList.contains("hidden");
      tip.classList.toggle("hidden");
      btn.setAttribute("aria-expanded", willShow ? "true" : "false");
    });
  }

  function buildPageContext() {
    var rows = nt().getFilteredRows ? nt().getFilteredRows() : [];
    var stats = aggregatePeriod(rows);
    var ctx = {
      page: "notification",
      filters: effectivePrimaryRange(rows),
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
      cross_top_sample: (global.cachedCrossTopList || []).slice(0, 5),
      period_compare: lastComparePayload,
      alerts: lastAlertPayload,
      content_traffic: lastContentTrafficPayload,
    };
    ctx.visible_text = "Notification KPI: " + stats.clicks + " click, " + stats.impressions + " impression, "
      + rows.length + " kayıt. App " + stats.app + " / Web " + stats.web + " click.";
    if (lastComparePayload && lastComparePayload.current) {
      ctx.visible_text += " Dönem karşılaştırma yüklendi.";
    }
    if (lastContentTrafficPayload && lastContentTrafficPayload.summary) {
      var ts = lastContentTrafficPayload.summary;
      ctx.visible_text += " Seçili içerik GA4 " + (ts.ga4_views || 0) + " görüntüleme, GSC " + (ts.gsc_clicks_7d || 0) + " click (7g).";
    }
    if (lastAlertPayload && lastAlertPayload.alerts && lastAlertPayload.alerts.length) {
      ctx.visible_text += " Aktif alarm: " + lastAlertPayload.alerts.map(function (a) { return a.title; }).join("; ");
    }
    return ctx;
  }

  function bootInitialRender() {
    if (!nt().getFilteredRows) return;
    onRedraw({ detail: { rows: nt().getFilteredRows() } });
  }

  global.NTExtras = {
    renderPeriodCompare: renderPeriodCompare,
    renderHeatmap: renderHeatmap,
    buildPageContext: buildPageContext,
    rowPlatformClicks: rowPlatformClicks,
    rowTotalClick: rowTotalClick,
    loadContentTraffic: loadContentTraffic,
    clearContentTraffic: clearContentTraffic,
    clearInlineDrillTraffic: clearInlineDrillTraffic,
    renderInlineDrill: renderInlineDrill,
    renderContentTraffic: renderContentTraffic,
  };

  global.addEventListener("nt-redraw", onRedraw);
  global.addEventListener("nt-data-ready", bootInitialRender);
  bindDrill();
  bindHeatmapMetric();
  bindTrafficDays();
  bindTrafficCompareInfo();

  global.__pcPageContext = function () {
    return buildPageContext();
  };

  function initExtras() {
    bootInitialRender();
  }

  if (global.document.readyState === "loading") {
    global.document.addEventListener("DOMContentLoaded", initExtras);
  } else {
    initExtras();
  }
})(window);
