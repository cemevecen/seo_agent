(function () {
  function api() { return window.NT || null; }
  var paretoResizeObs = null;

  function paretoChartSize(el, nTop) {
    if (!el) return { w: 0, h: 400 };
    var w = el.clientWidth || (el.parentElement && el.parentElement.clientWidth) || 0;
    var baseH = Math.min(Math.round(window.innerHeight * 0.48), 560);
    var h = el.clientHeight || 0;
    if (h < 200) h = baseH;
    if (h < 320) h = Math.max(380, baseH);
    return { w: Math.max(280, w), h: Math.max(380, h) };
  }

  function resizeParetoPlot() {
    var el = document.getElementById("nt-lab-pareto");
    if (!el || !window.Plotly || !el.querySelector(".js-plotly-plot")) return;
    var nTop = 30;
    var nInp = document.getElementById("nt-lab-pareto-n");
    if (nInp) nTop = parseInt(nInp.value, 10) || 30;
    var sz = paretoChartSize(el, nTop);
    try {
      Plotly.relayout(el, { width: sz.w, height: sz.h, autosize: true });
      Plotly.Plots.resize(el);
    } catch (e) { /* ignore */ }
  }

  function bindParetoResize() {
    var el = document.getElementById("nt-lab-pareto");
    if (!el || paretoResizeObs) return;
    if (typeof ResizeObserver !== "undefined") {
      paretoResizeObs = new ResizeObserver(function () { resizeParetoPlot(); });
      paretoResizeObs.observe(el);
    }
    window.addEventListener("resize", resizeParetoPlot);
  }

  function emptyMsg(el, msg) {
    if (el) el.innerHTML = '<p class="text-xs text-slate-500 dark:text-zinc-400">' + (msg || "Veri yok.") + "</p>";
  }

  function clearPlotHost(el) {
    if (!el) return;
    if (window.Plotly) {
      try { Plotly.purge(el); } catch (e) { /* ignore */ }
    }
    el.innerHTML = "";
    el.classList.remove("nt-lab-pareto-chart--empty");
  }

  function showParetoEmpty(el, msg) {
    if (!el) return;
    clearPlotHost(el);
    el.classList.add("nt-lab-pareto-chart--empty");
    el.innerHTML = '<p class="text-xs text-slate-500 dark:text-zinc-400">' + (msg || "Veri yok.") + "</p>";
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

  /** Başlık bazında tüm platformların click + impression toplamı (Kalite / Fırsat listeleri). */
  function buildHeadlineStatsAllPlatforms(nt, rows) {
    var by = {};
    rows.forEach(function (r) {
      var h = r.text || "";
      if (!h) return;
      if (!by[h]) by[h] = { headline: h, clicks: 0, impressions: 0, rows: 0, lastDay: "", daily: {} };
      var p = r.platforms || {};
      var rowClicks = 0;
      nt.PLATFORM_KEYS.forEach(function (plat) {
        var z = p[plat.key] || {};
        var c = nt.nCount ? nt.nCount(z.click) : nt.n(z.click);
        var im = nt.platformImpression ? nt.platformImpression(plat.key, z) : (plat.key === "ios" ? 0 : (nt.nCount ? nt.nCount(z.impression) : nt.n(z.impression)));
        by[h].clicks += c;
        by[h].impressions += im;
        rowClicks += c;
      });
      by[h].rows += 1;
      var d = nt.dayKey(r.date);
      if (!by[h].lastDay || d > by[h].lastDay) by[h].lastDay = d;
      if (!by[h].daily[d]) by[h].daily[d] = 0;
      by[h].daily[d] += rowClicks;
    });
    return Object.keys(by).map(function (k) {
      var h = by[k];
      var ctr = h.impressions > 0 ? (h.clicks / h.impressions) * 100 : 0;
      return {
        headline: h.headline,
        clicks: h.clicks,
        impressions: h.impressions,
        ctr: ctr,
        lastDay: h.lastDay,
        rows: h.rows,
        change7: 0
      };
    });
  }

  function buildHeadlineStats(nt, rows) {
    var pk = nt.mapListPlatformToDataKey(nt.getListPlatform());
    var by = {};
    rows.forEach(function (r) {
      var h = r.text || "";
      if (!h) return;
      if (!by[h]) by[h] = { headline: h, clicks: 0, impressions: 0, rows: 0, lastDay: "", daily: {} };
      var z = ((r.platforms || {})[pk] || {});
      var c = nt.nCount ? nt.nCount(z.click) : nt.n(z.click);
      var im = pk === "ios" ? 0 : (nt.nCount ? nt.nCount(z.impression) : nt.n(z.impression));
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
      if (sumEl) sumEl.textContent = "";
      showParetoEmpty(el);
      return;
    }
    clearPlotHost(el);
    var top = stats.slice(0, nTop);
    var cum = 0;
    var rankX = [];
    var clicksY = [];
    var cumPct = [];
    var titles = [];
    top.forEach(function (x, i) {
      cum += x.clicks;
      titles.push(x.headline);
      rankX.push(String(i + 1));
      clicksY.push(x.clicks);
      cumPct.push((cum / total) * 100);
    });
    var lc = ntLabColors();
    var sz = paretoChartSize(el, nTop);
    var dense = top.length > 18;
    var layout = plotLayout({
      width: sz.w,
      height: sz.h,
      autosize: true,
      margin: { l: 56, r: 56, t: 12, b: dense ? 48 : 40 },
      barmode: "overlay",
      hovermode: "x unified",
      xaxis: Object.assign(ntAxis({ title: "Sıra (1 = en yüksek click)", type: "category", tickangle: dense ? -45 : 0 }), {
        categoryorder: "array",
        categoryarray: rankX,
      }),
      yaxis: Object.assign(ntAxis({ title: "Click" }), { rangemode: "tozero" }),
      yaxis2: Object.assign(ntAxis({ title: "Kümülatif %" }), {
        overlaying: "y",
        side: "right",
        range: [0, Math.min(100, Math.ceil(Math.max.apply(null, cumPct) / 5) * 5 + 5)],
        ticksuffix: "%",
      }),
      legend: { font: ntFont(), orientation: "h", y: 1.08, x: 0 },
      shapes: [{
        type: "line",
        xref: "paper",
        x0: 0,
        x1: 1,
        yref: "y2",
        y0: 80,
        y1: 80,
        line: { color: ntIsDark() ? "#f87171" : "#dc2626", width: 1, dash: "dot" },
      }],
      annotations: [{
        xref: "paper",
        x: 1,
        xanchor: "right",
        yref: "y2",
        y: 80,
        yanchor: "bottom",
        text: "80%",
        showarrow: false,
        font: { size: 10, color: ntIsDark() ? "#fca5a5" : "#b91c1c" },
      }],
    });
    var barTrace = {
      type: "bar",
      name: "Click",
      x: rankX,
      y: clicksY,
      marker: { color: lc.bar, opacity: 0.88 },
      hovertext: titles,
      hovertemplate: "<b>%{hovertext}</b><br>Sıra %{x}<br>Click: %{y:,}<extra></extra>",
    };
    var lineTrace = {
      type: "scatter",
      mode: dense ? "lines" : "lines+markers",
      name: "Kümülatif %",
      x: rankX,
      y: cumPct,
      yaxis: "y2",
      line: { color: lc.line, width: 2.5 },
      marker: { size: dense ? 0 : 5, color: lc.line },
      customdata: titles,
      hovertemplate: "<b>%{customdata}</b><br>Kümülatif: %{y:.1f}%<extra></extra>",
    };
    Plotly.newPlot(el, [barTrace, lineTrace], layout, { responsive: true, displayModeBar: false });
    bindParetoResize();
    window.requestAnimationFrame(resizeParetoPlot);
    var share = (top.reduce(function (s, x) { return s + x.clicks; }, 0) / total) * 100;
    var idx80 = -1;
    for (var i = 0; i < cumPct.length; i++) {
      if (cumPct[i] >= 80) { idx80 = i; break; }
    }
    var platNote = "";
    var lp = nt.getListPlatform ? nt.getListPlatform() : "";
    if (lp) platNote = " · Liste platformu: " + lp;
    if (sumEl) {
      sumEl.textContent =
        "Top " + top.length + " başlık toplam click'in %" + share.toFixed(1) + "'ini taşıyor."
        + (idx80 >= 0 ? " %80 eşiği ≈ sıra " + (idx80 + 1) + " (" + cumPct[idx80].toFixed(1) + "% kümülatif)." : "")
        + platNote;
    }
  }

  function renderQualityOpportunity(nt, rows) {
    var qEl = document.getElementById("nt-lab-quality");
    var oEl = document.getElementById("nt-lab-opportunity");
    if (!rows.length) {
      emptyMsg(qEl, "Önce CSV yükleyin veya tarih filtresini genişletin.");
      emptyMsg(oEl, "Önce CSV yükleyin veya tarih filtresini genişletin.");
      return;
    }
    // Tüm platformlar (web+mweb+android+ios) — tek platform seçimi burada çoğu veriyi gizliyordu.
    var stats = buildHeadlineStatsAllPlatforms(nt, rows).filter(function (s) { return s.impressions > 0; });
    if (!stats.length) {
      emptyMsg(qEl, "Seçili aralıkta impression verisi yok (click var, impression sütunları boş olabilir).");
      emptyMsg(oEl, "Seçili aralıkta impression verisi yok (click var, impression sütunları boş olabilir).");
      return;
    }
    var imprs = stats.map(function (s) { return s.impressions; }).sort(function (a, b) { return a - b; });
    var ctrs = stats.map(function (s) { return s.ctr; }).sort(function (a, b) { return a - b; });
    function pct(arr, p) { return arr[Math.floor((arr.length - 1) * p)] || 0; }
    var imprHi = pct(imprs, 0.75), ctrLo = pct(ctrs, 0.25), imprLo = pct(imprs, 0.25), ctrHi = pct(ctrs, 0.75);
    var medianCtr = pct(ctrs, 0.5);
    var quality = stats.filter(function (s) { return s.impressions >= imprHi && s.ctr <= ctrLo; }).sort(function (a, b) { return b.impressions - a.impressions; }).slice(0, 15);
    var opp = stats.filter(function (s) { return s.impressions <= imprLo && s.ctr >= ctrHi; }).sort(function (a, b) { return b.ctr - a.ctr; }).slice(0, 15);
    // Çok katı çeyreklik eşik boş kalırsa: medyan CTR'a göre yedek liste
    if (!quality.length && stats.length >= 3) {
      quality = stats.filter(function (s) { return s.ctr <= medianCtr; }).sort(function (a, b) {
        return b.impressions - a.impressions || a.ctr - b.ctr;
      }).slice(0, 15);
    }
    if (!opp.length && stats.length >= 3) {
      opp = stats.filter(function (s) { return s.ctr >= medianCtr; }).sort(function (a, b) {
        return b.ctr - a.ctr || a.impressions - b.impressions;
      }).slice(0, 15);
    }
    function listRender(el, items, sub) {
      if (!items.length) { emptyMsg(el, "Eşik için aday yok."); return; }
      el.innerHTML = items.map(function (x, i) {
        return '<div class="rounded-lg border border-slate-200 px-3 py-2 dark:border-zinc-700"><p class="font-semibold text-slate-800 dark:text-zinc-300">' + (i + 1) + ". " + nt.escapeHtml(x.headline) + '</p><p class="text-slate-500 dark:text-zinc-500">' + sub(x) + "</p></div>";
      }).join("");
    }
    var subLine = function (x) {
      return "tüm platform · impr " + nt.fmt(x.impressions) + " · CTR " + x.ctr.toFixed(2) + "% · " + nt.fmt(x.clicks) + " click";
    };
    listRender(qEl, quality, subLine);
    listRender(oEl, opp, subLine);
  }

  function renderLab(detail) {
    var nt = api();
    if (!nt) return;
    var rows = (detail && detail.rows) ? detail.rows : nt.getFilteredRows();
    renderPareto(nt, rows);
    renderQualityOpportunity(nt, rows);
  }

  function wireControls() {
    var nt = api();
    if (!nt) return;
    var paretoN = document.getElementById("nt-lab-pareto-n");
    if (paretoN) paretoN.addEventListener("change", function () { renderPareto(nt, nt.getFilteredRows()); });
  }

  function boot() {
    if (!api()) {
      setTimeout(boot, 50);
      return;
    }
    wireControls();
    bindParetoResize();
    window.addEventListener("nt-redraw", function (ev) { renderLab(ev.detail || {}); });
    renderLab({ rows: api().getFilteredRows() });
  }

  if (document.readyState === "loading") document.addEventListener("DOMContentLoaded", boot);
  else boot();
})();
