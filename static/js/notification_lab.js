(function () {
  function api() { return window.NT || null; }
  function emptyMsg(el, msg) {
    if (el) el.innerHTML = '<p class="text-xs text-slate-500 dark:text-zinc-400">' + (msg || "Veri yok.") + "</p>";
  }
  function ntIsDark() { return document.documentElement.classList.contains("dark"); }
  function ntFont() { return { color: ntIsDark() ? "#a1a1aa" : "#475569" }; }
  function ntAxis(extra) {
    var dark = ntIsDark();
    var ax = {
      gridcolor: dark ? "#27272a" : "#e2e8f0",
      zerolinecolor: dark ? "#3f3f46" : "#cbd5e1",
      linecolor: dark ? "#3f3f46" : "#cbd5e1",
      tickfont: ntFont(),
      titlefont: ntFont()
    };
    if (extra) Object.keys(extra).forEach(function (k) { ax[k] = extra[k]; });
    return ax;
  }
  function ntLabColors() {
    return ntIsDark()
      ? { bar: "#7176c4", line: "#bf8f4a" }
      : { bar: "#6366f1", line: "#f59e0b" };
  }
  function plotLayout(extra) {
    var base = { margin: { l: 50, r: 20, t: 10, b: 50 }, paper_bgcolor: "rgba(0,0,0,0)", plot_bgcolor: "rgba(0,0,0,0)", font: ntFont() };
    if (!extra) return base;
    Object.keys(extra).forEach(function (k) { base[k] = extra[k]; });
    return base;
  }
  function pctChange(a, b) {
    if (!b) return a ? 100 : 0;
    return ((a - b) / b) * 100;
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
    var lc = ntLabColors();
    Plotly.newPlot(el, [
      { type: "bar", x: xs, y: bar, name: "Click", marker: { color: lc.bar } },
      { type: "scatter", x: xs, y: line, name: "Kümülatif %", yaxis: "y2", mode: "lines+markers", line: { color: lc.line } }
    ], plotLayout({
      yaxis: ntAxis(),
      yaxis2: ntAxis({ overlaying: "y", side: "right", title: "Kümülatif %", range: [0, 100] }),
      margin: { b: 120 },
      xaxis: ntAxis({ tickangle: -35 }),
      legend: { font: ntFont() }
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

  var NT_LOTTIE_TREND = "https://assets3.lottiefiles.com/packages/lf20_khttgaxc.json";
  var NT_LOTTIE_ALERT = "https://assets1.lottiefiles.com/packages/lf20_qp1spzqv.json";

  function insightItemsTake3(items, filler) {
    var out = items.slice(0, 3);
    while (out.length < 3) out.push(filler);
    return out;
  }

  function insightPanelHtml(nt, panel) {
    var list = panel.items.map(function (t) {
      return "<li>" + nt.escapeHtml(t) + "</li>";
    }).join("");
    return '<section class="nt-insight-panel nt-insight-panel--' + panel.kind + '" role="region" aria-label="' + nt.escapeHtml(panel.title) + '">'
      + '<div class="nt-insight-panel-head">'
      + '<div class="nt-insight-panel-media" aria-hidden="true">'
      + '<lottie-player class="nt-insight-lottie" src="' + panel.lottie + '" background="transparent" speed="1" loop autoplay></lottie-player>'
      + "</div>"
      + '<h4 class="nt-insight-panel-title">' + nt.escapeHtml(panel.title) + "</h4>"
      + "</div>"
      + '<ul class="nt-insight-panel-list">' + list + "</ul>"
      + "</section>";
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

    var platformLast7 = nt.PLATFORM_KEYS.map(function (p) {
      return {
        label: p.label,
        val: sumDays(last7, p.key + "_click"),
        prev: sumDays(prev7, p.key + "_click")
      };
    }).sort(function (a, b) { return b.val - a.val; });
    var leadPlat = platformLast7[0] || { label: "-", val: 0, prev: 0 };
    var leadCh = pctChange(leadPlat.val, leadPlat.prev);

    var trendMain;
    if (ch >= 10) trendMain = platLabel + " click son 7 günde %" + ch.toFixed(1) + " arttı (" + nt.fmt(cLast) + " vs " + nt.fmt(cPrev) + ").";
    else if (ch <= -10) trendMain = platLabel + " click son 7 günde %" + Math.abs(ch).toFixed(1) + " azaldı (" + nt.fmt(cLast) + " vs " + nt.fmt(cPrev) + ").";
    else trendMain = platLabel + " click son 7 günde stabil (Δ %" + ch.toFixed(1) + ", " + nt.fmt(cLast) + " vs " + nt.fmt(cPrev) + ").";

    var trendItems = insightItemsTake3([
      trendMain,
      "En aktif platform (son 7 gün): " + leadPlat.label + " · " + nt.fmt(leadPlat.val) + " click (Δ %" + leadCh.toFixed(1) + ").",
      "Son 7 gün günlük ortalama: " + nt.fmt(last7.length ? Math.round(cLast / last7.length) : 0) + " click · aralık " + (days[0] || "-") + " – " + end + "."
    ], "Trend verisi hesaplanıyor.");

    var kritikPool = [];
    if (Math.abs(ch) >= 25) {
      kritikPool.push("Ani click hareketi: 7 günlük değişim %" + ch.toFixed(1) + " — kampanya / içerik değişimini kontrol edin.");
    }
    if (shareTop >= 35 && top) {
      kritikPool.push("Trafik konsantrasyonu: lider başlık toplam click'in %" + shareTop.toFixed(1) + "'ini taşıyor.");
    }
    if (rows.length < 30) {
      kritikPool.push("Örneklem düşük: filtrede yalnızca " + nt.fmt(rows.length) + " kayıt — daha geniş tarih aralığı önerilir.");
    }
    if (Math.abs(ch) >= 15 && Math.abs(ch) < 25) {
      kritikPool.push("Click değişimi yükseliyor: Δ %" + ch.toFixed(1) + " (kritik eşiğe yakın).");
    }
    if (!kritikPool.length) {
      kritikPool.push("Kritik eşik tetiklenmedi: click, konsantrasyon ve hacim normal görünüyor.");
    }
    var kritikItems = insightItemsTake3(kritikPool, "Ek kritik uyarı yok.");

    el.innerHTML = insightPanelHtml(nt, {
      kind: "trend",
      title: "Trend",
      lottie: NT_LOTTIE_TREND,
      items: trendItems
    }) + insightPanelHtml(nt, {
      kind: "alert",
      title: "Kritik",
      lottie: NT_LOTTIE_ALERT,
      items: kritikItems
    });
  }

  function renderLab(detail) {
    var nt = api();
    if (!nt) return;
    var rows = (detail && detail.rows) ? detail.rows : nt.getFilteredRows();
    renderPareto(nt, rows);
    renderQualityOpportunity(nt, rows);
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
