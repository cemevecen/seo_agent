(function () {
  function api() { return window.NT || null; }

  function emptyMsg(el, msg) {
    if (el) el.innerHTML = '<p class="text-xs text-slate-500 dark:text-zinc-400">' + (msg || "Veri yok.") + "</p>";
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

  function renderQualityOpportunity(nt, rows) {
    var qEl = document.getElementById("nt-lab-quality");
    var oEl = document.getElementById("nt-lab-opportunity");
    if (!rows.length) {
      emptyMsg(qEl, "Önce CSV yükleyin veya tarih filtresini genişletin.");
      emptyMsg(oEl, "Önce CSV yükleyin veya tarih filtresini genişletin.");
      return;
    }
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
    renderQualityOpportunity(nt, rows);
  }

  function wireControls() {
    /* lab controls (quality/opportunity follow nt-redraw) */
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
