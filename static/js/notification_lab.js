(function () {
  function api() { return window.NT || null; }

  function emptyMsg(el, msg) {
    if (el) el.innerHTML = '<p class="text-xs text-slate-500 dark:text-zinc-400">' + (msg || "Veri yok.") + "</p>";
  }

  function ntIsDark() {
    return document.documentElement.classList.contains("dark");
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

  function hourFromRow(nt, r) {
    var iso = String(r.date || "");
    if (!iso) return 12;
    var d = new Date(iso);
    if (isNaN(d.getTime())) return 12;
    return d.getHours();
  }

  function isNightHour(h) {
    return h >= 18 || h < 6;
  }

  function truncate(nt, s, n) {
    var t = String(s || "").trim();
    if (t.length <= n) return t;
    return t.slice(0, n - 1) + "…";
  }

  function dnaFingerprint(pcts) {
    return pcts.map(function (x) { return Math.round(x); }).join(",");
  }

  function renderDna(nt, rows) {
    var el = document.getElementById("nt-lab-dna");
    var hint = document.getElementById("nt-lab-dna-hint");
    if (!el) return;
    var ranked = rows
      .map(function (r) {
        return { row: r, total: rowTotalClicks(nt, r) };
      })
      .filter(function (x) { return x.total > 0; })
      .sort(function (a, b) { return b.total - a.total; })
      .slice(0, 8);
    if (!ranked.length) {
      emptyMsg(el);
      if (hint) hint.textContent = "";
      return;
    }
    var fps = {};
    ranked.forEach(function (x) {
      var c = rowClicks(nt, x.row);
      var t = x.total;
      var fp = dnaFingerprint([c.android / t * 100, c.ios / t * 100, c.desktop / t * 100, c.mobileweb / t * 100]);
      fps[fp] = (fps[fp] || 0) + 1;
    });
    var dup = Object.keys(fps).some(function (k) { return fps[k] > 1; });
    el.innerHTML = ranked
      .map(function (x) {
        var r = x.row;
        var c = rowClicks(nt, r);
        var t = x.total;
        var segs = [
          { k: "android", w: (c.android / t) * 100, cls: "nt-lab-dna-seg-android", label: "Android" },
          { k: "ios", w: (c.ios / t) * 100, cls: "nt-lab-dna-seg-ios", label: "iOS" },
          { k: "desktop", w: (c.desktop / t) * 100, cls: "nt-lab-dna-seg-desktop", label: "Web" },
          { k: "mobileweb", w: (c.mobileweb / t) * 100, cls: "nt-lab-dna-seg-mweb", label: "MWeb" },
        ];
        var bar = segs
          .filter(function (s) { return s.w > 0.4; })
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
        var day = nt.dayKey(r.date);
        var id = nt.idString ? nt.idString(r) : "";
        return (
          '<div class="rounded-lg border border-slate-100 px-2 py-2 dark:border-zinc-800">' +
          '<p class="font-semibold text-slate-800 dark:text-zinc-200">' +
          nt.escapeHtml(truncate(nt, r.text, 72)) +
          "</p>" +
          '<div class="nt-lab-dna-bar mt-1.5">' +
          bar +
          "</div>" +
          '<p class="mt-1 text-[10px] text-slate-500 dark:text-zinc-500">' +
          nt.fmtCount(t) +
          " click · " +
          (day || "—") +
          (id ? " · ID " + nt.escapeHtml(id) : "") +
          "</p></div>"
        );
      })
      .join("");
    if (hint) {
      hint.textContent = dup
        ? "Benzer platform karışımına sahip birden fazla gönderim var — aynı DNA kümesi."
        : "Top 8 gönderim; çubuk = platform click payı.";
    }
  }

  function headlineOptions(nt, rows) {
    var by = {};
    rows.forEach(function (r) {
      var h = String(r.text || "").trim();
      if (!h) return;
      if (!by[h]) by[h] = 0;
      by[h] += rowTotalClicks(nt, r);
    });
    return Object.keys(by)
      .map(function (h) { return { headline: h, clicks: by[h] }; })
      .sort(function (a, b) { return b.clicks - a.clicks; })
      .slice(0, 25);
  }

  function aggregateHeadline(nt, rows, headline) {
    var tot = { android: 0, ios: 0, desktop: 0, mobileweb: 0, night: 0, total: 0 };
    rows.forEach(function (r) {
      if (String(r.text || "").trim() !== headline) return;
      var c = rowClicks(nt, r);
      tot.android += c.android;
      tot.ios += c.ios;
      tot.desktop += c.desktop;
      tot.mobileweb += c.mobileweb;
      var rowT = c.android + c.ios + c.desktop + c.mobileweb;
      tot.total += rowT;
      if (isNightHour(hourFromRow(nt, r))) tot.night += rowT;
    });
    return tot;
  }

  function radarInsight(tot) {
    if (!tot.total) return "";
    var shares = [
      { k: "Android", v: (tot.android / tot.total) * 100 },
      { k: "iOS", v: (tot.ios / tot.total) * 100 },
      { k: "Web", v: (tot.desktop / tot.total) * 100 },
      { k: "MWeb", v: (tot.mobileweb / tot.total) * 100 },
    ].sort(function (a, b) { return b.v - a.v; });
    var top = shares[0];
    var nightPct = (tot.night / tot.total) * 100;
    var parts = [top.k + " baskın (%" + top.v.toFixed(1) + " click payı)."];
    if (nightPct >= 35) parts.push("Gece gönderimleri bu başlıkta güçlü (%" + nightPct.toFixed(0) + ").");
    else if (nightPct <= 10) parts.push("Gündüz ağırlıklı; gece payı düşük.");
    return parts.join(" ");
  }

  function renderRadar(nt, rows, headlineOverride) {
    var plotEl = document.getElementById("nt-lab-radar");
    var sel = document.getElementById("nt-lab-radar-headline");
    var insightEl = document.getElementById("nt-lab-radar-insight");
    if (!plotEl) return;
    var opts = headlineOptions(nt, rows);
    if (!opts.length) {
      if (window.Plotly) Plotly.purge(plotEl);
      plotEl.innerHTML = '<p class="text-xs text-slate-500">Click verisi yok.</p>';
      if (sel) sel.innerHTML = "";
      if (insightEl) insightEl.textContent = "";
      return;
    }
    var prev = sel && sel.value ? sel.value : "";
    if (sel) {
      sel.innerHTML = opts
        .map(function (o) {
          var v = String(o.headline).replace(/&/g, "&amp;").replace(/"/g, "&quot;").replace(/</g, "&lt;");
          return (
            '<option value="' +
            v +
            '">' +
            nt.escapeHtml(truncate(nt, o.headline, 56)) +
            " (" +
            nt.fmtCount(o.clicks) +
            ")</option>"
          );
        })
        .join("");
      if (headlineOverride) sel.value = headlineOverride;
      else if (prev && opts.some(function (o) { return o.headline === prev; })) sel.value = prev;
    }
    var headline = (sel && sel.value) || opts[0].headline;
    var tot = aggregateHeadline(nt, rows, headline);
    if (!tot.total) {
      emptyMsg(plotEl, "Bu başlık için click yok.");
      return;
    }
    var theta = ["Android", "iOS", "Web", "MWeb", "Gece"];
    var rVals = [
      (tot.android / tot.total) * 100,
      (tot.ios / tot.total) * 100,
      (tot.desktop / tot.total) * 100,
      (tot.mobileweb / tot.total) * 100,
      (tot.night / tot.total) * 100,
    ];
    if (!window.Plotly) {
      plotEl.innerHTML = "<p class=\"text-xs text-slate-500\">Plotly yükleniyor…</p>";
      return;
    }
    var dark = ntIsDark();
    var lineColor = dark ? "#818cf8" : "#6366f1";
    var fillColor = dark ? "rgba(129, 140, 248, 0.25)" : "rgba(99, 102, 241, 0.2)";
    Plotly.newPlot(
      plotEl,
      [
        {
          type: "scatterpolar",
          r: rVals.concat(rVals[0]),
          theta: theta.concat(theta[0]),
          fill: "toself",
          fillcolor: fillColor,
          line: { color: lineColor, width: 2 },
          name: "Pay %",
          hovertemplate: "%{theta}: %{r:.1f}%<extra></extra>",
        },
      ],
      {
        margin: { l: 44, r: 44, t: 24, b: 24 },
        paper_bgcolor: "rgba(0,0,0,0)",
        plot_bgcolor: "rgba(0,0,0,0)",
        font: { color: dark ? "#a1a1aa" : "#475569", size: 11 },
        polar: {
          bgcolor: "rgba(0,0,0,0)",
          radialaxis: { visible: true, range: [0, 100], ticksuffix: "%", gridcolor: dark ? "#27272a" : "#e2e8f0" },
          angularaxis: { gridcolor: dark ? "#27272a" : "#e2e8f0" },
        },
        showlegend: false,
      },
      { responsive: true, displayModeBar: false }
    );
    if (insightEl) insightEl.textContent = radarInsight(tot);
  }

  function formatTimelineDate(nt, iso) {
    var k = nt.dayKey(iso);
    if (!k) return "—";
    var p = k.split("-");
    if (p.length === 3) return p[2] + "." + p[1] + "." + p[0];
    return k;
  }

  function renderTimeline(nt, rows) {
    var el = document.getElementById("nt-lab-timeline");
    if (!el) return;
    var top = rows
      .map(function (r) { return { row: r, total: rowTotalClicks(nt, r) }; })
      .filter(function (x) { return x.total > 0; })
      .sort(function (a, b) { return b.total - a.total; })
      .slice(0, 12);
    if (!top.length) {
      emptyMsg(el);
      return;
    }
    top.sort(function (a, b) {
      var da = nt.dayKey(a.row.date) || "";
      var db = nt.dayKey(b.row.date) || "";
      return da < db ? -1 : da > db ? 1 : 0;
    });
    var max = top.reduce(function (m, x) { return Math.max(m, x.total); }, 1);
    el.innerHTML = top
      .map(function (x, i) {
        var r = x.row;
        var w = Math.max(12, Math.round((x.total / max) * 100));
        var isLast = i === top.length - 1;
        return (
          '<div class="flex gap-3">' +
          '<div class="w-[4.5rem] shrink-0 pt-1 text-[10px] text-slate-500 dark:text-zinc-500">' +
          formatTimelineDate(nt, r.date) +
          "</div>" +
          '<div class="nt-lab-timeline-rail">' +
          '<div class="nt-lab-timeline-dot"></div>' +
          (isLast ? "" : '<div class="nt-lab-timeline-line"></div>') +
          "</div>" +
          '<div class="min-w-0 flex-1 pb-4">' +
          '<div class="nt-lab-timeline-bar" style="width:' +
          w +
          '%">' +
          '<span class="truncate font-semibold text-slate-800 dark:text-zinc-200">' +
          nt.escapeHtml(truncate(nt, r.text, 48)) +
          "</span>" +
          "</div>" +
          '<p class="mt-0.5 text-[10px] text-slate-500 dark:text-zinc-500">' +
          nt.fmtCount(x.total) +
          " click · 4 platform" +
          (nt.idString ? " · ID " + nt.escapeHtml(nt.idString(r)) : "") +
          "</p></div></div>"
        );
      })
      .join("");
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
    renderDna(nt, rows);
    renderRadar(nt, rows);
    renderTimeline(nt, rows);
    renderQualityOpportunity(nt, rows);
  }

  function wireControls() {
    var sel = document.getElementById("nt-lab-radar-headline");
    if (sel && !sel.getAttribute("data-bound")) {
      sel.setAttribute("data-bound", "1");
      sel.addEventListener("change", function () {
        var nt = api();
        if (!nt) return;
        renderRadar(nt, nt.getFilteredRows(), sel.value);
      });
    }
  }

  function boot() {
    if (!api()) {
      setTimeout(boot, 50);
      return;
    }
    wireControls();
    window.addEventListener("nt-redraw", function (ev) {
      renderLab(ev.detail || {});
    });
    renderLab({ rows: api().getFilteredRows() });
  }

  if (document.readyState === "loading") document.addEventListener("DOMContentLoaded", boot);
  else boot();
})();
