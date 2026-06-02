(function () {
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
  function pctChange(a, b) {
    if (!b) return a ? 100 : 0;
    return ((a - b) / b) * 100;
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
        change7: pctChange(l7, p7)
      };
    });
  }

  function renderPareto(nt, rows) {
    var el = document.getElementById("nt-lab-pareto");
    var sumEl = document.getElementById("nt-lab-pareto-summary");
    if (!el || !window.Plotly) return;
    var nTop = nt.n((document.getElementById("nt-lab-pareto-n") || {}).value) || 30;
    var stats = buildHeadlineStats(nt, rows).sort(function (a, b) { return b.clicks - a.clicks; });
    var total = stats.reduce(function (s, x) { return s + x.clicks; }, 0);
    if (!total) {
      Plotly.purge(el);
      if (sumEl) sumEl.textContent = "";
      emptyMsg(el);
      return;
    }
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
    var head = "<tr><th class='px-2 py-1'>\\</th>" + cks.map(function (c) {
      return "<th class='px-2 py-1'>" + nt.escapeHtml(c.length > 24 ? c.slice(0, 24) + "…" : c) + "</th>";
    }).join("") + "</tr>";
    var body = rks.slice(0, 80).map(function (rv) {
      return "<tr><td class='px-2 py-1 font-semibold'>" + nt.escapeHtml(rv.length > 24 ? rv.slice(0, 24) + "…" : rv) + "</td>"
        + cks.map(function (cv) {
          var v = grid[rv + "||" + cv] || 0;
          return "<td class='px-2 py-1'>" + (metric === "ctr" ? v.toFixed(2) : nt.fmt(v)) + "</td>";
        }).join("") + "</tr>";
    }).join("");
    out.innerHTML = "<table class='min-w-full border border-slate-200 dark:border-zinc-700'><thead>" + head + "</thead><tbody>" + body + "</tbody></table>";
  }

  var NT_LOTTIE_UP = "https://assets4.lottiefiles.com/packages/lf20_s2lryxtd.json";
  var NT_LOTTIE_DOWN = "https://assets9.lottiefiles.com/packages/lf20_kxsdyyjr.json";
  var NT_LOTTIE_WARN = "https://assets5.lottiefiles.com/packages/lf20_usmfx66p.json";
  var NT_LOTTIE_INFO = "https://assets2.lottiefiles.com/packages/lf20_p8bfn5ty.json";
  var NT_LOTTIE_ALERT = "https://assets1.lottiefiles.com/packages/lf20_qp1spzqv.json";

  function insightCardHtml(nt, card) {
    var type = card.type || "info";
    var lottie = card.lottie || NT_LOTTIE_INFO;
    return '<article class="nt-insight-card nt-insight-card--' + type + '" role="status">'
      + '<lottie-player class="nt-insight-lottie" src="' + lottie + '" background="transparent" speed="1" loop autoplay></lottie-player>'
      + '<div class="nt-insight-lottie-fallback" aria-hidden="true"></div>'
      + '<span class="nt-insight-badge">' + nt.escapeHtml(card.badge || type) + "</span>"
      + '<p class="nt-insight-title">' + nt.escapeHtml(card.title || "") + "</p>"
      + '<p class="nt-insight-body">' + nt.escapeHtml(card.body || "") + "</p>"
      + "</article>";
  }

  function renderInsights(nt, rows) {
    var el = document.getElementById("nt-lab-insights");
    if (!el) return;
    if (!rows.length) { emptyMsg(el); return; }
    var pk = nt.mapListPlatformToDataKey(nt.getListPlatform());
    var platLabel = nt.getListPlatform() === "web" ? "Web (Desktop)" : nt.getListPlatform();
    var byDay = nt.aggregateByDay(rows);
    var days = byDay.map(function (d) { return d.day; }).sort();
    var end = days[days.length - 1] || nt.todayKey();
    var start = days[0] || end;
    var last7 = days.filter(function (d) { return d >= nt.minusDays(end, 6); });
    var prev7 = days.filter(function (d) { return d >= nt.minusDays(end, 13) && d <= nt.minusDays(end, 7); });
    function sumDays(dayList, field) {
      return dayList.reduce(function (s, d) {
        var row = byDay.find(function (x) { return x.day === d; });
        return s + (row ? nt.n(row[field]) : 0);
      }, 0);
    }
    var cLast = sumDays(last7, pk + "_click");
    var cPrev = sumDays(prev7, pk + "_click");
    var ch = pctChange(cLast, cPrev);
    var stats = buildHeadlineStats(nt, rows);
    var top = stats.slice().sort(function (a, b) { return b.clicks - a.clicks; })[0];
    var totalClicks = stats.reduce(function (s, x) { return s + x.clicks; }, 0);
    var shareTop = totalClicks > 0 && top ? (top.clicks / totalClicks) * 100 : 0;
    var cards = [];
    if (ch >= 10) {
      cards.push({
        type: "up",
        badge: "Trend ↑",
        title: platLabel + " click ivmesi",
        body: "Son 7 gün önceki 7 güne göre %" + ch.toFixed(1) + " arttı (" + nt.fmt(cLast) + " vs " + nt.fmt(cPrev) + ").",
        lottie: NT_LOTTIE_UP
      });
    } else if (ch <= -10) {
      cards.push({
        type: "down",
        badge: "Trend ↓",
        title: platLabel + " click düşüşü",
        body: "Son 7 günde %" + Math.abs(ch).toFixed(1) + " azalma (" + nt.fmt(cLast) + " vs " + nt.fmt(cPrev) + ").",
        lottie: NT_LOTTIE_DOWN
      });
    } else {
      cards.push({
        type: "info",
        badge: "Trend ~",
        title: platLabel + " click stabil",
        body: "7 günlük değişim %" + ch.toFixed(1) + " — belirgin sıçrama/düşüş yok.",
        lottie: NT_LOTTIE_INFO
      });
    }
    if (Math.abs(ch) >= 25) {
      cards.push({
        type: "alert",
        badge: "Kritik",
        title: "Ani hareket uyarısı",
        body: "7 günlük click değişimi eşiği aştı (%" + ch.toFixed(1) + "). Kampanya veya içerik değişimini kontrol edin.",
        lottie: NT_LOTTIE_ALERT
      });
    }
    if (shareTop >= 35) {
      cards.push({
        type: "warn",
        badge: "Konsantrasyon",
        title: "Trafik tek başlıkta toplanmış",
        body: "Lider başlık toplam click'in %" + shareTop.toFixed(1) + "'ini taşıyor — çeşitlilik riski.",
        lottie: NT_LOTTIE_WARN
      });
    }
    if (rows.length < 30) {
      cards.push({
        type: "warn",
        badge: "Veri az",
        title: "Örneklem sınırlı",
        body: "Filtrede yalnızca " + nt.fmt(rows.length) + " kayıt var; trend çıkarımları için daha geniş aralık deneyin.",
        lottie: NT_LOTTIE_WARN
      });
    }
    cards.push({
      type: "info",
      badge: "Özet",
      title: "Hacim",
      body: nt.fmt(rows.length) + " kayıt · " + nt.fmt(stats.length) + " benzersiz başlık · " + start + " – " + end + ".",
      lottie: NT_LOTTIE_INFO
    });
    if (top) {
      cards.push({
        type: "up",
        badge: "Lider",
        title: "En yüksek click",
        body: "«" + (top.headline.length > 56 ? top.headline.slice(0, 56) + "…" : top.headline) + "» — " + nt.fmt(top.clicks) + " click.",
        lottie: NT_LOTTIE_UP
      });
    }
    var qualityCount = stats.filter(function (s) {
      return s.impressions > 0 && s.ctr < 1.5 && s.impressions >= 5000;
    }).length;
    if (qualityCount > 0) {
      cards.push({
        type: "warn",
        badge: "CTR uyarısı",
        title: "İyileştirme adayı",
        body: qualityCount + " başlıkta yüksek gösterim + düşük CTR — Kalite Listesine bakın.",
        lottie: NT_LOTTIE_WARN
      });
    }
    cards.push({
      type: "info",
      badge: "Metrik",
      title: "Aktif görünüm",
      body: "Metrik: " + nt.getMetric().toUpperCase() + " · platform: " + platLabel + ".",
      lottie: NT_LOTTIE_INFO
    });
    el.innerHTML = cards.map(function (c) { return insightCardHtml(nt, c); }).join("");
  }

  function renderLab(detail) {
    var nt = api();
    if (!nt) return;
    var rows = (detail && detail.rows) ? detail.rows : nt.getFilteredRows();
    renderPareto(nt, rows);
    renderQualityOpportunity(nt, rows);
    runPivot(nt, rows);
    renderInsights(nt, rows);
  }

  function wireControls() {
    var nt = api();
    if (!nt) return;
    var pivotBtn = document.getElementById("nt-lab-pivot-run");
    if (pivotBtn) pivotBtn.addEventListener("click", function () { runPivot(nt, nt.getFilteredRows()); });
    var paretoN = document.getElementById("nt-lab-pareto-n");
    if (paretoN) paretoN.addEventListener("change", function () { renderPareto(nt, nt.getFilteredRows()); });
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
