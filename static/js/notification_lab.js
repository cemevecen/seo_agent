(function () {
  var VIEWS_KEY = "notification_analytics_saved_views_v1";
  var leaderboardCache = [];
  var lbSort = { field: "clicks", dir: "desc" };

  function api() { return window.NT || null; }
  function emptyMsg(el, msg) {
    if (el) el.innerHTML = '<p class="text-xs text-slate-500 dark:text-zinc-400">' + (msg || "Veri yok.") + "</p>";
  }
  function plotLayout(extra) {
    var base = { margin: { l: 50, r: 20, t: 10, b: 50 }, paper_bgcolor: "rgba(0,0,0,0)", plot_bgcolor: "rgba(0,0,0,0)" };
    if (!extra) return base;
    Object.keys(extra).forEach(function (k) { base[k] = extra[k]; });
    return base;
  }
  function pearson(xs, ys) {
    var n = Math.min(xs.length, ys.length);
    if (n < 3) return 0;
    var mx = 0, my = 0, i;
    for (i = 0; i < n; i++) { mx += xs[i]; my += ys[i]; }
    mx /= n; my /= n;
    var num = 0, dx = 0, dy = 0;
    for (i = 0; i < n; i++) {
      var a = xs[i] - mx, b = ys[i] - my;
      num += a * b; dx += a * a; dy += b * b;
    }
    if (!dx || !dy) return 0;
    return num / Math.sqrt(dx * dy);
  }
  function stdDev(arr) {
    if (!arr.length) return 0;
    var m = arr.reduce(function (s, v) { return s + v; }, 0) / arr.length;
    var v = arr.reduce(function (s, x) { return s + (x - m) * (x - m); }, 0) / arr.length;
    return Math.sqrt(v);
  }
  function pctChange(a, b) {
    if (!b) return a ? 100 : 0;
    return ((a - b) / b) * 100;
  }
  function downloadCsv(filename, rows) {
    if (!rows || !rows.length) return;
    var keys = Object.keys(rows[0]);
    var lines = [keys.join(",")];
    rows.forEach(function (r) {
      lines.push(keys.map(function (k) {
        var v = r[k];
        var s = v == null ? "" : String(v);
        if (/[",\n]/.test(s)) s = '"' + s.replace(/"/g, '""') + '"';
        return s;
      }).join(","));
    });
    var blob = new Blob(["\ufeff" + lines.join("\n")], { type: "text/csv;charset=utf-8" });
    var a = document.createElement("a");
    a.href = URL.createObjectURL(blob);
    a.download = filename;
    a.click();
    URL.revokeObjectURL(a.href);
  }

  function rowPlatformMetric(nt, row, platformKey, metric) {
    var z = ((row.platforms || {})[platformKey] || {});
    return nt.metricValueFromPlatformData(z, metric);
  }

  function buildHeadlineStats(nt, rows) {
    var pk = nt.mapListPlatformToDataKey(nt.getListPlatform());
    var by = {};
    rows.forEach(function (r) {
      var h = r.text || "";
      if (!by[h]) by[h] = { headline: h, clicks: 0, impressions: 0, rows: 0, lastDay: "", daily: {} };
      var z = ((r.platforms || {})[pk] || {});
      var c = nt.n(z.click), im = nt.n(z.impression);
      by[h].clicks += c;
      by[h].impressions += im;
      by[h].rows += 1;
      var d = nt.dayKey(r.date);
      if (!by[h].lastDay || d > by[h].lastDay) by[h].lastDay = d;
      if (!by[h].daily[d]) by[h].daily[d] = 0;
      by[h].daily[d] += c;
    });
    var days = rows.map(function (r) { return nt.dayKey(r.date); }).filter(Boolean).sort();
    var end = days.length ? days[days.length - 1] : nt.todayKey();
    function sumRange(h, from, to) {
      var s = 0;
      Object.keys(h.daily).forEach(function (d) {
        if (d >= from && d <= to) s += h.daily[d];
      });
      return s;
    }
    var last7From = nt.minusDays(end, 6);
    var prev7From = nt.minusDays(end, 13);
    var prev7To = nt.minusDays(end, 7);
    return Object.keys(by).map(function (k) {
      var h = by[k];
      var ctr = h.impressions > 0 ? (h.clicks / h.impressions) * 100 : 0;
      var l7 = sumRange(h, last7From, end);
      var p7 = sumRange(h, prev7From, prev7To);
      return {
        headline: h.headline,
        clicks: h.clicks,
        impressions: h.impressions,
        ctr: ctr,
        lastDay: h.lastDay,
        rows: h.rows,
        change7: pctChange(l7, p7),
        daily: h.daily
      };
    });
  }

  function renderHeatmap(nt, rows) {
    var el = document.getElementById("nt-lab-heatmap");
    if (!el || !window.Plotly) return;
    if (!rows.length) { Plotly.purge(el); emptyMsg(el); return; }
    var byDay = nt.aggregateByDay(rows);
    var days = byDay.map(function (d) { return d.day; });
    var z = [], yLabels = [];
    nt.PLATFORM_KEYS.forEach(function (p) {
      yLabels.push(p.label);
      z.push(byDay.map(function (d) { return nt.n(d[p.key]); }));
    });
    Plotly.newPlot(el, [{
      type: "heatmap",
      x: days,
      y: yLabels,
      z: z,
      colorscale: "Blues"
    }], plotLayout({ xaxis: { title: "Tarih" }, yaxis: { title: "Platform" } }), { responsive: true, displayModeBar: false });
  }

  function renderLeaderboard(nt, rows) {
    var el = document.getElementById("nt-lab-leaderboard");
    if (!el) return;
    leaderboardCache = buildHeadlineStats(nt, rows);
    if (!leaderboardCache.length) { emptyMsg(el); return; }
    var dir = lbSort.dir === "asc" ? 1 : -1;
    var f = lbSort.field;
    leaderboardCache.sort(function (a, b) {
      var av = a[f], bv = b[f];
      if (typeof av === "string") return av.localeCompare(bv, "tr") * dir;
      return (nt.n(av) - nt.n(bv)) * dir;
    });
    var cols = [
      { key: "headline", label: "Başlık", type: "string" },
      { key: "clicks", label: "Click", type: "num" },
      { key: "impressions", label: "Impression", type: "num" },
      { key: "ctr", label: "CTR %", type: "num" },
      { key: "lastDay", label: "Son gün", type: "string" },
      { key: "rows", label: "Kayıt", type: "num" },
      { key: "change7", label: "7g Δ%", type: "num" }
    ];
    var head = cols.map(function (c) {
      var arrow = lbSort.field === c.key ? (lbSort.dir === "asc" ? "↑" : "↓") : "↕";
      return '<th class="px-2 py-2 text-left"><button type="button" data-lb-sort="' + c.key + '" class="font-bold uppercase text-[11px] text-slate-500">' + c.label + " " + arrow + "</button></th>";
    }).join("");
    var body = leaderboardCache.slice(0, 100).map(function (r) {
      return "<tr class='border-t border-slate-200 dark:border-zinc-700'>"
        + "<td class='max-w-[40ch] truncate px-2 py-1.5'>" + nt.escapeHtml(r.headline) + "</td>"
        + "<td class='px-2 py-1.5'>" + nt.fmt(r.clicks) + "</td>"
        + "<td class='px-2 py-1.5'>" + nt.fmt(r.impressions) + "</td>"
        + "<td class='px-2 py-1.5'>" + r.ctr.toFixed(2) + "</td>"
        + "<td class='px-2 py-1.5'>" + r.lastDay + "</td>"
        + "<td class='px-2 py-1.5'>" + nt.fmt(r.rows) + "</td>"
        + "<td class='px-2 py-1.5 " + (r.change7 >= 0 ? "text-emerald-600" : "text-rose-600") + "'>" + r.change7.toFixed(1) + "%</td>"
        + "</tr>";
    }).join("");
    el.innerHTML = "<table class='min-w-full'><thead class='bg-slate-50 dark:bg-zinc-900'><tr>" + head + "</tr></thead><tbody>" + body + "</tbody></table>";
  }

  function renderVolatility(nt, rows) {
    var el = document.getElementById("nt-lab-volatility");
    if (!el) return;
    var stats = buildHeadlineStats(nt, rows);
    var scored = stats.map(function (s) {
      var vals = Object.keys(s.daily).map(function (k) { return s.daily[k]; });
      if (vals.length < 2) return null;
      var sd = stdDev(vals);
      var mm = Math.max.apply(null, vals) - Math.min.apply(null, vals);
      return { headline: s.headline, sd: sd, range: mm, clicks: s.clicks };
    }).filter(Boolean).sort(function (a, b) { return b.sd - a.sd; }).slice(0, 25);
    if (!scored.length) { emptyMsg(el); return; }
    el.innerHTML = scored.map(function (x, i) {
      return '<div class="rounded-lg border border-slate-200 px-3 py-2 dark:border-zinc-700"><p class="font-semibold text-slate-800 dark:text-zinc-100">' + (i + 1) + ". " + nt.escapeHtml(x.headline) + '</p><p class="text-slate-500">σ=' + x.sd.toFixed(1) + " · max-min=" + nt.fmt(x.range) + " · click=" + nt.fmt(x.clicks) + "</p></div>";
    }).join("");
  }

  function renderMovers(nt, rows) {
    var upEl = document.getElementById("nt-lab-movers-up");
    var downEl = document.getElementById("nt-lab-movers-down");
    if (!upEl || !downEl) return;
    var days = rows.map(function (r) { return nt.dayKey(r.date); }).filter(Boolean).sort();
    if (days.length < 2) { emptyMsg(upEl); emptyMsg(downEl); return; }
    var mid = days[Math.floor(days.length / 2)];
    var pk = nt.mapListPlatformToDataKey(nt.getListPlatform());
    var half = {};
    rows.forEach(function (r) {
      var h = r.text || "";
      if (!half[h]) half[h] = { a: 0, b: 0 };
      var c = nt.n(((r.platforms || {})[pk] || {}).click);
      if (nt.dayKey(r.date) < mid) half[h].a += c; else half[h].b += c;
    });
    var list = Object.keys(half).map(function (h) {
      return { headline: h, change: pctChange(half[h].b, half[h].a), a: half[h].a, b: half[h].b };
    }).filter(function (x) { return x.a + x.b > 0; });
    function renderSide(el, items, cls) {
      if (!items.length) { emptyMsg(el); return; }
      el.innerHTML = items.map(function (x, i) {
        return '<div class="rounded-lg border border-slate-200 px-3 py-2 dark:border-zinc-700"><p class="font-semibold">' + (i + 1) + ". " + nt.escapeHtml(x.headline) + '</p><p class="' + cls + '">' + x.change.toFixed(1) + '% · ' + nt.fmt(x.a) + " → " + nt.fmt(x.b) + "</p></div>";
      }).join("");
    }
    renderSide(upEl, list.slice().sort(function (a, b) { return b.change - a.change; }).slice(0, 15), "text-emerald-600");
    renderSide(downEl, list.slice().sort(function (a, b) { return a.change - b.change; }).slice(0, 15), "text-rose-600");
  }

  function renderDistribution(nt, rows) {
    var hist = document.getElementById("nt-lab-dist-hist");
    var box = document.getElementById("nt-lab-dist-box");
    if (!hist || !box || !window.Plotly) return;
    var pk = nt.mapListPlatformToDataKey(nt.getListPlatform());
    var ctrs = rows.map(function (r) {
      var z = ((r.platforms || {})[pk] || {});
      var im = nt.n(z.impression), cl = nt.n(z.click);
      return im > 0 ? (cl / im) * 100 : 0;
    }).filter(function (v) { return v > 0; });
    if (!ctrs.length) { Plotly.purge(hist); Plotly.purge(box); emptyMsg(hist); return; }
    Plotly.newPlot(hist, [{ type: "histogram", x: ctrs, nbinsx: 24, marker: { color: "#6366f1" } }], plotLayout({ xaxis: { title: "CTR %" } }), { responsive: true, displayModeBar: false });
    Plotly.newPlot(box, [{ type: "box", y: ctrs, name: "CTR", marker: { color: "#06b6d4" } }], plotLayout({ yaxis: { title: "CTR %" } }), { responsive: true, displayModeBar: false });
  }

  function renderStackShare(nt, rows) {
    var el = document.getElementById("nt-lab-stack-share");
    if (!el || !window.Plotly) return;
    var byDay = nt.aggregateByDay(rows);
    var days = byDay.map(function (d) { return d.day; });
    var traces = nt.PLATFORM_KEYS.map(function (p, idx) {
      var vals = byDay.map(function (d) {
        var total = nt.PLATFORM_KEYS.reduce(function (s, q) { return s + nt.n(d[q.key + "_click"]); }, 0);
        return total > 0 ? (nt.n(d[p.key + "_click"]) / total) * 100 : 0;
      });
      var colors = ["#22c55e", "#06b6d4", "#6366f1", "#f59e0b"];
      return { x: days, y: vals, stackgroup: "one", name: p.label, line: { width: 0.5, color: colors[idx] }, fillcolor: colors[idx] };
    });
    Plotly.newPlot(el, traces, plotLayout({ yaxis: { title: "Click payı %", ticksuffix: "%" } }), { responsive: true, displayModeBar: false });
  }

  function renderCorrelation(nt, rows) {
    var el = document.getElementById("nt-lab-corr");
    if (!el || !window.Plotly) return;
    var byDay = nt.aggregateByDay(rows);
    var series = {};
    nt.PLATFORM_KEYS.forEach(function (p) {
      series[p.key] = byDay.map(function (d) { return nt.n(d[p.key + "_click"]); });
    });
    var keys = nt.PLATFORM_KEYS.map(function (p) { return p.key; });
    var labels = nt.PLATFORM_KEYS.map(function (p) { return p.label; });
    var z = keys.map(function (a) {
      return keys.map(function (b) { return pearson(series[a], series[b]); });
    });
    Plotly.newPlot(el, [{
      type: "heatmap",
      x: labels,
      y: labels,
      z: z,
      zmin: -1,
      zmax: 1,
      colorscale: "RdBu",
      text: z.map(function (row) { return row.map(function (v) { return v.toFixed(2); }); }),
      texttemplate: "%{text}"
    }], plotLayout(), { responsive: true, displayModeBar: false });
  }

  function renderPareto(nt, rows) {
    var el = document.getElementById("nt-lab-pareto");
    var sumEl = document.getElementById("nt-lab-pareto-summary");
    if (!el || !window.Plotly) return;
    var nTop = nt.n((document.getElementById("nt-lab-pareto-n") || {}).value) || 30;
    var stats = buildHeadlineStats(nt, rows).sort(function (a, b) { return b.clicks - a.clicks; });
    var total = stats.reduce(function (s, x) { return s + x.clicks; }, 0);
    if (!total) { Plotly.purge(el); if (sumEl) sumEl.textContent = ""; emptyMsg(el); return; }
    var top = stats.slice(0, nTop);
    var cum = 0, xs = [], bar = [], line = [];
    top.forEach(function (x, i) {
      cum += x.clicks;
      xs.push((i + 1) + ". " + (x.headline.length > 28 ? x.headline.slice(0, 28) + "…" : x.headline));
      bar.push(x.clicks);
      line.push((cum / total) * 100);
    });
    Plotly.newPlot(el, [
      { type: "bar", x: xs, y: bar, name: "Click", marker: { color: "#6366f1" } },
      { type: "scatter", x: xs, y: line, name: "Kümülatif %", yaxis: "y2", mode: "lines+markers", line: { color: "#f59e0b" } }
    ], plotLayout({
      yaxis2: { overlaying: "y", side: "right", title: "Kümülatif %", range: [0, 100] },
      margin: { b: 120 },
      xaxis: { tickangle: -35 }
    }), { responsive: true, displayModeBar: false });
    var share = (top.reduce(function (s, x) { return s + x.clicks; }, 0) / total) * 100;
    if (sumEl) sumEl.textContent = "Top " + top.length + " başlık toplam click'in %" + share.toFixed(1) + "'ini taşıyor (80/20 kontrolü).";
  }

  function renderCohort(nt, rows) {
    var el = document.getElementById("nt-lab-cohort");
    if (!el || !window.Plotly) return;
    var first = {}, buckets = { 1: [], 3: [], 7: [], 14: [] };
    rows.forEach(function (r) {
      var h = r.text || "", d = nt.dayKey(r.date);
      if (!first[h] || d < first[h]) first[h] = d;
    });
    rows.forEach(function (r) {
      var h = r.text || "";
      if (!first[h]) return;
      var age = Math.floor((new Date(nt.dayKey(r.date) + "T12:00:00") - new Date(first[h] + "T12:00:00")) / 86400000);
      var pk = nt.mapListPlatformToDataKey(nt.getListPlatform());
      var c = nt.n(((r.platforms || {})[pk] || {}).click);
      [1, 3, 7, 14].forEach(function (target) {
        if (age === target) buckets[target].push(c);
      });
    });
    var xs = [1, 3, 7, 14];
    var ys = xs.map(function (t) {
      var arr = buckets[t];
      return arr.length ? arr.reduce(function (s, v) { return s + v; }, 0) / arr.length : 0;
    });
    Plotly.newPlot(el, [{ type: "scatter", mode: "lines+markers", x: xs, y: ys, line: { color: "#22c55e", width: 2 } }], plotLayout({ xaxis: { title: "Yayın sonrası gün", dtick: 1 }, yaxis: { title: "Ort. click" } }), { responsive: true, displayModeBar: false });
  }

  function renderAnomaly(nt, rows) {
    var el = document.getElementById("nt-lab-anomaly");
    if (!el) return;
    var byDay = {};
    rows.forEach(function (r) {
      var d = nt.dayKey(r.date);
      if (!byDay[d]) byDay[d] = 0;
      nt.PLATFORM_KEYS.forEach(function (p) {
        byDay[d] += nt.n(((r.platforms || {})[p.key] || {}).click);
      });
    });
    var days = Object.keys(byDay).sort();
    var vals = days.map(function (d) { return byDay[d]; });
    var m = vals.reduce(function (s, v) { return s + v; }, 0) / (vals.length || 1);
    var sd = stdDev(vals) || 1;
    var hits = days.map(function (d, i) {
      var z = (vals[i] - m) / sd;
      return { day: d, val: vals[i], z: z };
    }).filter(function (x) { return Math.abs(x.z) >= 2; }).sort(function (a, b) { return Math.abs(b.z) - Math.abs(a.z); }).slice(0, 20);
    if (!hits.length) { el.innerHTML = '<p class="text-xs text-slate-500">Bu aralıkta |z|≥2 anomali yok.</p>'; return; }
    el.innerHTML = hits.map(function (x) {
      var dir = x.z > 0 ? "sıçrama" : "düşüş";
      return '<div class="rounded-lg border border-amber-300 bg-amber-50 px-3 py-2 dark:border-amber-800 dark:bg-amber-950/30"><span class="font-bold">' + x.day + '</span> · ' + nt.fmt(x.val) + ' click · z=' + x.z.toFixed(2) + " (" + dir + ")</div>";
    }).join("");
  }

  function renderQualityOpportunity(nt, rows) {
    var qEl = document.getElementById("nt-lab-quality");
    var oEl = document.getElementById("nt-lab-opportunity");
    var stats = buildHeadlineStats(nt, rows).filter(function (s) { return s.impressions > 0; });
    if (!stats.length) { emptyMsg(qEl); emptyMsg(oEl); return; }
    var imprs = stats.map(function (s) { return s.impressions; }).sort(function (a, b) { return a - b; });
    var ctrs = stats.map(function (s) { return s.ctr; }).sort(function (a, b) { return a - b; });
    function pct(arr, p) { return arr[Math.floor((arr.length - 1) * p)] || 0; }
    var imprHi = pct(imprs, 0.75), ctrLo = pct(ctrs, 0.25), imprLo = pct(imprs, 0.25), ctrHi = pct(ctrs, 0.75);
    var quality = stats.filter(function (s) { return s.impressions >= imprHi && s.ctr <= ctrLo; }).sort(function (a, b) { return b.impressions - a.impressions; }).slice(0, 15);
    var opp = stats.filter(function (s) { return s.impressions <= imprLo && s.ctr >= ctrHi; }).sort(function (a, b) { return b.ctr - a.ctr; }).slice(0, 15);
    function listRender(el, items, sub) {
      if (!items.length) { emptyMsg(el, "Eşik için aday yok."); return; }
      el.innerHTML = items.map(function (x, i) {
        return '<div class="rounded-lg border border-slate-200 px-3 py-2 dark:border-zinc-700"><p class="font-semibold">' + (i + 1) + ". " + nt.escapeHtml(x.headline) + '</p><p class="text-slate-500">' + sub(x) + "</p></div>";
      }).join("");
    }
    listRender(qEl, quality, function (x) { return "impr " + nt.fmt(x.impressions) + " · CTR " + x.ctr.toFixed(2) + "%"; });
    listRender(oEl, opp, function (x) { return "impr " + nt.fmt(x.impressions) + " · CTR " + x.ctr.toFixed(2) + "%"; });
  }

  function runPivot(nt, rows) {
    var out = document.getElementById("nt-lab-pivot-out");
    if (!out) return;
    var rowDim = (document.getElementById("nt-lab-pivot-row") || {}).value || "day";
    var colDim = (document.getElementById("nt-lab-pivot-col") || {}).value || "platform";
    var metric = (document.getElementById("nt-lab-pivot-metric") || {}).value || "click";
    if (rowDim === colDim) { emptyMsg(out, "Satır ve kolon farklı olmalı."); return; }
    function dimVal(r, dim) {
      if (dim === "day") return nt.dayKey(r.date);
      if (dim === "headline") return r.text || "";
      return "platform";
    }
    var rowKeys = {}, colKeys = {}, grid = {};
    if (rowDim === "platform" || colDim === "platform") {
      rows.forEach(function (r) {
        nt.PLATFORM_KEYS.forEach(function (p) {
          var rr = Object.assign({}, r);
          var fake = { date: r.date, text: r.text, platforms: r.platforms, _p: p.key, _pl: p.label };
          var rv = rowDim === "platform" ? p.label : dimVal(r, rowDim);
          var cv = colDim === "platform" ? p.label : dimVal(r, colDim);
          rowKeys[rv] = 1; colKeys[cv] = 1;
          var k = rv + "||" + cv;
          if (!grid[k]) grid[k] = 0;
          grid[k] += rowPlatformMetric(nt, r, p.key, metric);
        });
      });
    } else {
      rows.forEach(function (r) {
        var pk = nt.mapListPlatformToDataKey(nt.getListPlatform());
        var rv = dimVal(r, rowDim), cv = dimVal(r, colDim);
        rowKeys[rv] = 1; colKeys[cv] = 1;
        var k = rv + "||" + cv;
        if (!grid[k]) grid[k] = 0;
        grid[k] += rowPlatformMetric(nt, r, pk, metric);
      });
    }
    var rks = Object.keys(rowKeys).sort();
    var cks = Object.keys(colKeys).sort().slice(0, 40);
    if (!rks.length || !cks.length) { emptyMsg(out); return; }
    var head = "<tr><th class='px-2 py-1'>\\</th>" + cks.map(function (c) { return "<th class='px-2 py-1'>" + nt.escapeHtml(c.length > 24 ? c.slice(0, 24) + "…" : c) + "</th>"; }).join("") + "</tr>";
    var body = rks.slice(0, 80).map(function (rv) {
      return "<tr><td class='px-2 py-1 font-semibold'>" + nt.escapeHtml(rv.length > 24 ? rv.slice(0, 24) + "…" : rv) + "</td>"
        + cks.map(function (cv) {
          var v = grid[rv + "||" + cv] || 0;
          return "<td class='px-2 py-1'>" + (metric === "ctr" ? v.toFixed(2) : nt.fmt(v)) + "</td>";
        }).join("") + "</tr>";
    }).join("");
    out.innerHTML = "<table class='min-w-full border border-slate-200 dark:border-zinc-700'><thead>" + head + "</thead><tbody>" + body + "</tbody></table>";
  }

  function initComparisonDefaults(nt) {
    var end = nt.todayKey();
    var aEnd = document.getElementById("nt-lab-cmp-a-end");
    var aStart = document.getElementById("nt-lab-cmp-a-start");
    var bEnd = document.getElementById("nt-lab-cmp-b-end");
    var bStart = document.getElementById("nt-lab-cmp-b-start");
    if (aEnd && !aEnd.value) {
      aEnd.value = end;
      aStart.value = nt.minusDays(end, 6);
      bEnd.value = nt.minusDays(end, 7);
      bStart.value = nt.minusDays(end, 13);
    }
  }

  function filterRange(nt, allRows, start, end) {
    return allRows.filter(function (r) {
      var d = nt.dayKey(r.date);
      if (start && d < start) return false;
      if (end && d > end) return false;
      return true;
    });
  }

  function rangeSummary(nt, rows) {
    var clicks = 0, impr = 0, heads = {};
    rows.forEach(function (r) {
      heads[r.text] = 1;
      nt.PLATFORM_KEYS.forEach(function (p) {
        var z = (r.platforms || {})[p.key] || {};
        clicks += nt.n(z.click);
        impr += nt.n(z.impression);
      });
    });
    return { rows: rows.length, headlines: Object.keys(heads).length, clicks: clicks, impressions: impr };
  }

  function renderComparison(nt, allRows) {
    var out = document.getElementById("nt-lab-cmp-out");
    if (!out) return;
    var aS = (document.getElementById("nt-lab-cmp-a-start") || {}).value;
    var aE = (document.getElementById("nt-lab-cmp-a-end") || {}).value;
    var bS = (document.getElementById("nt-lab-cmp-b-start") || {}).value;
    var bE = (document.getElementById("nt-lab-cmp-b-end") || {}).value;
    var A = rangeSummary(nt, filterRange(nt, allRows, aS, aE));
    var B = rangeSummary(nt, filterRange(nt, allRows, bS, bE));
    function card(title, s) {
      return '<div class="rounded-lg border border-slate-200 p-3 dark:border-zinc-700"><p class="font-bold">' + title + "</p>"
        + "<p>Kayıt: " + nt.fmt(s.rows) + " · Başlık: " + nt.fmt(s.headlines) + "</p>"
        + "<p>Click: " + nt.fmt(s.clicks) + " · Impression: " + nt.fmt(s.impressions) + "</p></div>";
    }
    out.innerHTML = card("Aralık A (" + (aS || "?") + " → " + (aE || "?") + ")", A) + card("Aralık B (" + (bS || "?") + " → " + (bE || "?") + ")", B)
      + '<div class="sm:col-span-2 rounded-lg border border-indigo-200 bg-indigo-50 p-3 text-xs dark:border-indigo-800 dark:bg-indigo-950/40">'
      + "<p>Click Δ: " + pctChange(A.clicks, B.clicks).toFixed(1) + "% · Impression Δ: " + pctChange(A.impressions, B.impressions).toFixed(1) + "%</p></div>";
  }

  function renderInsights(nt, rows) {
    var el = document.getElementById("nt-lab-insights");
    if (!el) return;
    if (!rows.length) { emptyMsg(el); return; }
    var pk = nt.mapListPlatformToDataKey(nt.getListPlatform());
    var platLabel = nt.getListPlatform() === "web" ? "Web" : nt.getListPlatform();
    var byDay = nt.aggregateByDay(rows);
    var days = byDay.map(function (d) { return d.day; }).sort();
    var end = days[days.length - 1] || nt.todayKey();
    var last7 = days.filter(function (d) { return d >= nt.minusDays(end, 6); });
    var prev7 = days.filter(function (d) { return d >= nt.minusDays(end, 13) && d <= nt.minusDays(end, 7); });
    function sumDays(arr, field) {
      return arr.reduce(function (s, d) {
        var row = byDay.find(function (x) { return x.day === d; });
        return s + (row ? nt.n(row[field]) : 0);
      }, 0);
    }
    var cLast = sumDays(last7, pk + "_click"), cPrev = sumDays(prev7, pk + "_click");
    var ch = pctChange(cLast, cPrev);
    var stats = buildHeadlineStats(nt, rows);
    var top = stats.slice().sort(function (a, b) { return b.clicks - a.clicks; })[0];
    var totalClicks = stats.reduce(function (s, x) { return s + x.clicks; }, 0);
    var cards = [
      platLabel + " click son 7 günde önceki 7 güne göre " + (ch >= 0 ? "%" + ch.toFixed(1) + " arttı" : "%" + Math.abs(ch).toFixed(1) + " azaldı") + ".",
      "Filtrede " + nt.fmt(rows.length) + " kayıt, " + nt.fmt(stats.length) + " benzersiz başlık var.",
      top ? "En yüksek click: «" + (top.headline.length > 48 ? top.headline.slice(0, 48) + "…" : top.headline) + "» (" + nt.fmt(top.clicks) + ")." : "Başlık verisi yok.",
      "Seçili metrik: " + nt.getMetric().toUpperCase() + " · tarih " + (days[0] || "-") + " – " + end + "."
    ];
    if (totalClicks > 0 && top) {
      cards.push("Lider başlık toplam click payının %" + ((top.clicks / totalClicks) * 100).toFixed(1) + "'i.");
    }
    el.innerHTML = cards.map(function (t) {
      return '<div class="rounded-lg border border-slate-200 bg-slate-50 px-3 py-2 text-xs text-slate-700 dark:border-zinc-700 dark:bg-zinc-950/50 dark:text-zinc-200">' + nt.escapeHtml(t) + "</div>";
    }).join("");
  }

  function refreshViewsList() {
    var sel = document.getElementById("nt-lab-view-list");
    if (!sel) return;
    var list = [];
    try { list = JSON.parse(localStorage.getItem(VIEWS_KEY) || "[]"); } catch (e) { list = []; }
    sel.innerHTML = list.map(function (v, i) {
      return '<option value="' + i + '">' + (v.name || "Görünüm") + "</option>";
    }).join("");
    if (!list.length) sel.innerHTML = '<option value="">— kayıtlı görünüm yok —</option>';
  }

  function renderLab(detail) {
    var nt = api();
    if (!nt) return;
    var rows = (detail && detail.rows) ? detail.rows : nt.getFilteredRows();
    initComparisonDefaults(nt);
    renderHeatmap(nt, rows);
    renderLeaderboard(nt, rows);
    renderVolatility(nt, rows);
    renderMovers(nt, rows);
    renderDistribution(nt, rows);
    renderStackShare(nt, rows);
    renderCorrelation(nt, rows);
    renderPareto(nt, rows);
    renderCohort(nt, rows);
    renderAnomaly(nt, rows);
    renderQualityOpportunity(nt, rows);
    runPivot(nt, rows);
    renderComparison(nt, nt.readRows());
    renderInsights(nt, rows);
    refreshViewsList();
  }

  function wireControls() {
    var nt = api();
    if (!nt) return;
    var lbRoot = document.getElementById("nt-lab-leaderboard");
    if (lbRoot) {
      lbRoot.addEventListener("click", function (ev) {
        var btn = ev.target && ev.target.closest ? ev.target.closest("[data-lb-sort]") : null;
        if (!btn) return;
        var key = btn.getAttribute("data-lb-sort");
        if (lbSort.field === key) lbSort.dir = lbSort.dir === "asc" ? "desc" : "asc";
        else { lbSort.field = key; lbSort.dir = "desc"; }
        renderLeaderboard(nt, nt.getFilteredRows());
      });
    }
    var pivotBtn = document.getElementById("nt-lab-pivot-run");
    if (pivotBtn) pivotBtn.addEventListener("click", function () { runPivot(nt, nt.getFilteredRows()); });
    var paretoN = document.getElementById("nt-lab-pareto-n");
    if (paretoN) paretoN.addEventListener("change", function () { renderPareto(nt, nt.getFilteredRows()); });
    var cmpBtn = document.getElementById("nt-lab-cmp-run");
    if (cmpBtn) cmpBtn.addEventListener("click", function () { renderComparison(nt, nt.readRows()); });
    document.getElementById("nt-lab-export-filtered").addEventListener("click", function () {
      var rows = nt.getFilteredRows();
      downloadCsv("notification-filtered.csv", rows.map(function (r) {
        return { id: nt.idString(r), text: r.text, date: nt.dayKey(r.date) };
      }));
    });
    document.getElementById("nt-lab-export-leaderboard").addEventListener("click", function () {
      downloadCsv("notification-leaderboard.csv", leaderboardCache.map(function (r) {
        return { headline: r.headline, clicks: r.clicks, impressions: r.impressions, ctr: r.ctr.toFixed(2), last_day: r.lastDay, rows: r.rows, change_7d_pct: r.change7.toFixed(2) };
      }));
    });
    document.getElementById("nt-lab-view-save").addEventListener("click", function () {
      var name = (document.getElementById("nt-lab-view-name") || {}).value.trim();
      if (!name) return;
      var list = [];
      try { list = JSON.parse(localStorage.getItem(VIEWS_KEY) || "[]"); } catch (e) { list = []; }
      list.push({
        name: name,
        metric: nt.getMetric(),
        listPlatform: nt.getListPlatform(),
        start: (document.getElementById("nt-start-date") || {}).value,
        end: (document.getElementById("nt-end-date") || {}).value,
        preset: (document.getElementById("nt-range-preset") || {}).value
      });
      localStorage.setItem(VIEWS_KEY, JSON.stringify(list));
      refreshViewsList();
      var st = document.getElementById("nt-lab-view-status");
      if (st) st.textContent = "Kaydedildi: " + name;
    });
    document.getElementById("nt-lab-view-apply").addEventListener("click", function () {
      var sel = document.getElementById("nt-lab-view-list");
      var idx = nt.n(sel && sel.value);
      var list = [];
      try { list = JSON.parse(localStorage.getItem(VIEWS_KEY) || "[]"); } catch (e) { list = []; }
      var v = list[idx];
      if (!v) return;
      nt.applySavedView(v);
      var st = document.getElementById("nt-lab-view-status");
      if (st) st.textContent = "Uygulandı: " + v.name;
    });
    document.getElementById("nt-lab-view-delete").addEventListener("click", function () {
      var sel = document.getElementById("nt-lab-view-list");
      var idx = nt.n(sel && sel.value);
      var list = [];
      try { list = JSON.parse(localStorage.getItem(VIEWS_KEY) || "[]"); } catch (e) { list = []; }
      if (!list[idx]) return;
      var removed = list.splice(idx, 1)[0];
      localStorage.setItem(VIEWS_KEY, JSON.stringify(list));
      refreshViewsList();
      var st = document.getElementById("nt-lab-view-status");
      if (st) st.textContent = "Silindi: " + (removed.name || "");
    });
  }

  function boot() {
    if (!api()) {
      setTimeout(boot, 50);
      return;
    }
    wireControls();
    window.addEventListener("nt-redraw", function (ev) { renderLab(ev.detail || {}); });
    renderLab({ rows: api().getFilteredRows() });
  }

  if (document.readyState === "loading") document.addEventListener("DOMContentLoaded", boot);
  else boot();
})();
