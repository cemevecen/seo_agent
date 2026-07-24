(function () {
  function api() { return window.NT || null; }

  function emptyMsg(el, msg) {
    if (el) el.innerHTML = '<p class="text-xs text-slate-500 dark:text-zinc-400">' + (msg || "Veri yok.") + "</p>";
  }

  function dnaFingerprint(pcts) {
    return pcts.map(function (x) { return Math.round(x); }).join(",");
  }

  function formatChronoTime(nt, iso) {
    var isoStr = String(iso || "");
    if (!isoStr) return "";
    var d = new Date(isoStr);
    if (isNaN(d.getTime())) return "";
    var h = String(d.getHours()).padStart(2, "0");
    var m = String(d.getMinutes()).padStart(2, "0");
    return h + ":" + m;
  }

  function formatChronoDay(nt, iso) {
    var k = nt.dayKey(iso);
    if (!k) return "—";
    var p = k.split("-");
    if (p.length === 3) return p[2] + "." + p[1] + "." + p[0];
    return k;
  }

  function dnaBarHtml(c, t) {
    var segs = [
      { w: (c.android / t) * 100, cls: "nt-lab-dna-seg-android", label: "Android" },
      { w: (c.ios / t) * 100, cls: "nt-lab-dna-seg-ios", label: "iOS" },
      { w: (c.desktop / t) * 100, cls: "nt-lab-dna-seg-desktop", label: "Web" },
      { w: (c.mobileweb / t) * 100, cls: "nt-lab-dna-seg-mweb", label: "MWeb" },
    ];
    return segs
      .filter(function (s) { return s.w > 0.35; })
      .map(function (s) {
        return (
          '<div class="' +
          s.cls +
          '" style="width:' +
          s.w.toFixed(2) +
          '%" title="' +
          s.label +
          " " +
          s.w.toFixed(1) +
          '%"></div>'
        );
      })
      .join("");
  }

  var chronoRevealObs = null;
  var chronoRevealScrollBound = false;

  function revealChronoNode(node) {
    if (!node || node.classList.contains("is-revealed")) return;
    node.classList.add("is-revealed");
    if (node.classList.contains("nt-lab-chrono-day-gap")) {
      var next = node.nextElementSibling;
      if (next && next.classList.contains("nt-lab-chrono-item")) revealChronoNode(next);
    }
  }

  function flushChronoRevealInView(container) {
    if (!container) return;
    var margin = 120;
    var vh = window.innerHeight || document.documentElement.clientHeight || 800;
    var viewTop = -margin;
    var viewBottom = vh + margin;
    container.querySelectorAll(".nt-lab-chrono-item:not(.is-revealed)").forEach(function (node) {
      var r = node.getBoundingClientRect();
      if (r.bottom >= viewTop && r.top <= viewBottom) revealChronoNode(node);
    });
    container.querySelectorAll(".nt-lab-chrono-day-gap:not(.is-revealed)").forEach(function (node) {
      var r = node.getBoundingClientRect();
      if (r.bottom >= viewTop && r.top <= viewBottom) revealChronoNode(node);
    });
  }

  function bindChronoReveal(container) {
    if (chronoRevealObs) {
      chronoRevealObs.disconnect();
      chronoRevealObs = null;
    }
    var pending = container.querySelectorAll(".nt-lab-chrono-item:not(.is-revealed), .nt-lab-chrono-day-gap:not(.is-revealed)");
    if (!pending.length) return;
    if (typeof IntersectionObserver === "undefined") {
      pending.forEach(function (node) { revealChronoNode(node); });
      return;
    }
    chronoRevealObs = new IntersectionObserver(
      function (entries) {
        entries.forEach(function (entry) {
          if (entry.isIntersecting) revealChronoNode(entry.target);
        });
      },
      { root: null, rootMargin: "0px 0px 160px 0px", threshold: 0 }
    );
    pending.forEach(function (node) { chronoRevealObs.observe(node); });
    flushChronoRevealInView(container);
    requestAnimationFrame(function () { flushChronoRevealInView(container); });
    if (!chronoRevealScrollBound) {
      chronoRevealScrollBound = true;
      var onScroll = function () {
        var root = document.getElementById("nt-lab-dna-chrono");
        if (root) flushChronoRevealInView(root);
      };
      window.addEventListener("scroll", onScroll, { passive: true, capture: true });
      window.addEventListener("resize", onScroll, { passive: true });
    }
  }

  function renderDnaChronology(nt, rows) {
    var el = document.getElementById("nt-lab-dna-chrono");
    var hint = document.getElementById("nt-lab-dna-hint");
    if (!el) return;
    var MAX = 250;
    var sorted = rows
      .map(function (r) {
        return { row: r, total: rowTotalClicks(nt, r), day: nt.dayKey(r.date) || "" };
      })
      .filter(function (x) { return x.total > 0; })
      .sort(function (a, b) {
        if (a.day !== b.day) return a.day > b.day ? -1 : a.day < b.day ? 1 : 0;
        var ta = String(a.row.date || "");
        var tb = String(b.row.date || "");
        if (ta !== tb) return ta > tb ? -1 : 1;
        return b.total - a.total;
      });
    var truncated = sorted.length > MAX;
    if (truncated) sorted = sorted.slice(0, MAX);
    if (!sorted.length) {
      emptyMsg(el);
      if (hint) hint.textContent = "";
      return;
    }
    var fps = {};
    sorted.forEach(function (x) {
      var c = rowClicks(nt, x.row);
      var t = x.total;
      var fp = dnaFingerprint([c.android / t * 100, c.ios / t * 100, c.desktop / t * 100, c.mobileweb / t * 100]);
      fps[fp] = (fps[fp] || 0) + 1;
    });
    var dup = Object.keys(fps).some(function (k) { return fps[k] > 1; });
    var lastDay = "";
    var parts = [];
    sorted.forEach(function (x, i) {
      var r = x.row;
      var day = x.day;
      if (day && day !== lastDay) {
        parts.push('<div class="nt-lab-chrono-day-gap">' + formatChronoDay(nt, r.date) + "</div>");
        lastDay = day;
      }
      var c = rowClicks(nt, r);
      var t = x.total;
      var isLast = i === sorted.length - 1;
      var time = formatChronoTime(nt, r.date);
      var id = nt.idString ? nt.idString(r) : "";
      parts.push(
        '<div class="nt-lab-chrono-item flex gap-3">' +
        '<div class="w-[3.25rem] shrink-0 pt-0.5 text-right text-[10px] tabular-nums text-slate-500 dark:text-zinc-500">' +
        (time || "—") +
        "</div>" +
        '<div class="nt-lab-chrono-rail">' +
        '<div class="nt-lab-chrono-dot"></div>' +
        (isLast ? "" : '<div class="nt-lab-chrono-line"></div>') +
        "</div>" +
        '<div class="nt-lab-chrono-card">' +
        '<p class="font-semibold leading-snug text-slate-800 dark:text-zinc-100">' +
        nt.escapeHtml(r.text || "") +
        "</p>" +
        '<div class="nt-lab-dna-bar nt-lab-dna-bar-lg mt-2">' +
        dnaBarHtml(c, t) +
        "</div>" +
        '<p class="mt-1.5 text-[10px] text-slate-500 dark:text-zinc-500">' +
        nt.fmtCount(t) +
        " click" +
        (id ? " · ID " + nt.escapeHtml(id) : "") +
        "</p></div></div>"
      );
    });
    el.innerHTML = parts.join("");
    bindChronoReveal(el);
    if (hint) {
      var extra = truncated ? " İlk " + MAX + " gönderim (en yeniler); daha eskiler için aralığı daraltın." : "";
      hint.textContent =
        (dup ? "Benzer platform karışımı tekrar eden gönderimler var." : sorted.length + " gönderim, yeniden eskiye.") + extra;
    }
  }

  function rowClicks(nt, r) {
    var p = r.platforms || {};
    return {
      android: nt.nCount((p.android || {}).click),
      ios: nt.nCount((p.ios || {}).click),
      desktop: nt.nCount((p.desktop || {}).click),
      mobileweb: nt.nCount((p.mobileweb || {}).click),
    };
  }

  function rowTotalClicks(nt, r) {
    var c = rowClicks(nt, r);
    return c.android + c.ios + c.desktop + c.mobileweb;
  }

  function buildHeadlineStatsAllPlatforms(nt, rows) {
    var by = {};
    rows.forEach(function (r) {
      var h = r.text || "";
      if (!h) return;
      if (!by[h]) by[h] = { headline: h, clicks: 0, impressions: 0, rows: 0, lastDay: "", daily: {} };
      var p = r.platforms || {};
      var rowClicksSum = 0;
      nt.PLATFORM_KEYS.forEach(function (plat) {
        var z = p[plat.key] || {};
        var c = nt.nCount ? nt.nCount(z.click) : nt.n(z.click);
        var im = nt.platformImpression ? nt.platformImpression(plat.key, z) : plat.key === "ios" ? 0 : nt.nCount ? nt.nCount(z.impression) : nt.n(z.impression);
        by[h].clicks += c;
        by[h].impressions += im;
        rowClicksSum += c;
      });
      by[h].rows += 1;
      var d = nt.dayKey(r.date);
      if (!by[h].lastDay || d > by[h].lastDay) by[h].lastDay = d;
      if (!by[h].daily[d]) by[h].daily[d] = 0;
      by[h].daily[d] += rowClicksSum;
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
        change7: 0,
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
      el.innerHTML = items
        .map(function (x, i) {
          return (
            '<div class="rounded-lg border border-slate-200 px-3 py-2 dark:border-zinc-700"><p class="font-semibold text-slate-800 dark:text-zinc-300">' +
            (i + 1) +
            ". " +
            nt.escapeHtml(x.headline) +
            '</p><p class="text-slate-500 dark:text-zinc-500">' +
            sub(x) +
            "</p></div>"
          );
        })
        .join("");
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
    var rows = detail && detail.rows ? detail.rows : nt.getFilteredRows();
    renderQualityOpportunity(nt, rows);
    renderDnaChronology(nt, rows);
  }

  function boot() {
    if (!api()) {
      setTimeout(boot, 50);
      return;
    }
    window.addEventListener("nt-redraw", function (ev) {
      renderLab(ev.detail || {});
    });
    renderLab({ rows: api().getFilteredRows() });
  }

  if (document.readyState === "loading") document.addEventListener("DOMContentLoaded", boot);
  else boot();
})();
