(function () {
  var bootEl = document.getElementById("pml-boot");
  var embedProfiles = {
    store_charts: {
      rootId: "app-store-charts-root",
      mountId: "app-store-charts-mount",
      whenId: "app-store-charts-when",
      refreshId: "app-store-charts-refresh",
      headerId: "app-store-charts-header",
      bodyId: "app-store-charts-body",
      chevronId: "app-store-charts-chevron",
      fetchUrl: "/api/app/store-charts",
      refreshUrl: "/api/app/store-charts/refresh",
      defaultOpen: true,
    },
    google_news: {
      rootId: "dn-google-news-root",
      mountId: "dn-google-news-mount",
      whenId: "dn-google-news-when",
      refreshId: "dn-google-news-refresh",
      headerId: "dn-google-news-header",
      bodyId: "dn-google-news-body",
      chevronId: "dn-google-news-chevron",
      fetchUrl: "/api/doviz-news/google-news-showcase",
      refreshUrl: "/api/doviz-news/google-news-showcase/refresh",
      defaultOpen: true,
    },
  };
  var embedSectionId = "";
  var embedCfg = null;
  Object.keys(embedProfiles).forEach(function (id) {
    var profile = embedProfiles[id];
    if (document.getElementById(profile.rootId)) {
      embedSectionId = id;
      embedCfg = profile;
    }
  });
  if (!bootEl && !embedCfg) return;
  var isEmbed = !bootEl && !!embedCfg;
  var boot = {};
  if (bootEl) {
    try {
      boot = JSON.parse(bootEl.textContent || "{}");
    } catch (e) {
      return;
    }
  }
  var sections = boot.sections || {};

  function el(html) {
    var d = document.createElement("div");
    d.innerHTML = html;
    return d.firstElementChild;
  }
  function esc(s) {
    return String(s == null ? "" : s)
      .replace(/&/g, "&amp;")
      .replace(/</g, "&lt;")
      .replace(/>/g, "&gt;")
      .replace(/"/g, "&quot;");
  }
  function fmtWhen(iso) {
    if (!iso) return "—";
    var d = new Date(iso);
    if (isNaN(d.getTime())) return String(iso).slice(0, 16).replace("T", " ");
    return d.toLocaleString("tr-TR", { day: "2-digit", month: "2-digit", hour: "2-digit", minute: "2-digit" });
  }
  function chip(text, cls) {
    return '<span class="pml-chip ' + (cls || "") + '">' + esc(text) + "</span>";
  }
  function collectRuns() {
    var runs = [];
    Object.keys(sections).forEach(function (id) {
      (sections[id].runs || []).forEach(function (r) {
        runs.push({ at: r.at, ok: r.ok, id: id });
      });
    });
    runs.sort(function (a, b) {
      return String(a.at).localeCompare(String(b.at));
    });
    return runs.slice(-24);
  }

  function renderStatus() {
    var chart = document.getElementById("pml-run-chart");
    if (!chart) return;
    var runs = collectRuns();
    if (!runs.length) {
      chart.innerHTML = "";
      return;
    }
    var max = 1;
    chart.innerHTML =
      '<div class="pml-spark" title="Recent scans">' +
      runs
        .map(function (r) {
          var h = 8 + Math.round(20 * (r.ok ? 1 : 0.35));
          return '<i class="' + (r.ok ? "on" : "off") + '" style="height:' + h + 'px" title="' + esc(r.id + " · " + fmtWhen(r.at)) + '"></i>';
        })
        .join("") +
      "</div>";
  }

  function sortableTable(headers, rows, opts) {
    opts = opts || {};
    var wrap = document.createElement("div");
    wrap.className = "pml-shell mt-3 ring-1 ring-slate-200 dark:ring-zinc-800";
    var table = document.createElement("table");
    table.className = "pml-table " + (opts.wide === false ? "pml-table-fit" : "pml-table-wide");
    var thead = document.createElement("thead");
    var hr = document.createElement("tr");
    headers.forEach(function (h, i) {
      var th = document.createElement("th");
      th.textContent = h;
      if (i === 0) th.classList.add("pin");
      th.addEventListener("click", function () {
        var dir = table.getAttribute("data-dir") === "asc" && table.getAttribute("data-col") === String(i) ? "desc" : "asc";
        table.setAttribute("data-col", String(i));
        table.setAttribute("data-dir", dir);
        var body = table.tBodies[0];
        var trs = Array.prototype.slice.call(body.rows);
        trs.sort(function (a, b) {
          var av = a.cells[i].getAttribute("data-sort") || a.cells[i].textContent;
          var bv = b.cells[i].getAttribute("data-sort") || b.cells[i].textContent;
          var an = parseFloat(av);
          var bn = parseFloat(bv);
          var cmp = !isNaN(an) && !isNaN(bn) ? an - bn : String(av).localeCompare(String(bv), "tr");
          return dir === "asc" ? cmp : -cmp;
        });
        trs.forEach(function (tr) {
          body.appendChild(tr);
        });
      });
      hr.appendChild(th);
    });
    thead.appendChild(hr);
    table.appendChild(thead);
    var tb = document.createElement("tbody");
    rows.forEach(function (row) {
      tb.appendChild(row);
    });
    table.appendChild(tb);
    wrap.appendChild(table);
    return wrap;
  }

  function tr(cells, cls) {
    var row = document.createElement("tr");
    if (cls) row.className = cls;
    cells.forEach(function (c, i) {
      var td = document.createElement("td");
      if (i === 0) td.classList.add("pin");
      if (c.html) td.innerHTML = c.html;
      else td.textContent = c.text == null ? "" : c.text;
      if (c.sort != null) td.setAttribute("data-sort", c.sort);
      if (c.cls) td.className = (td.className + " " + c.cls).trim();
      row.appendChild(td);
    });
    return row;
  }

  function tabs(labels, onPick, active) {
    var bar = document.createElement("div");
    bar.className = "pml-tabs";
    var on = active == null ? 0 : active;
    labels.forEach(function (lab, i) {
      var b = document.createElement("button");
      b.type = "button";
      b.className = "pml-tab" + (i === on ? " is-on" : "") + (lab === "Total" ? " pml-tab-total" : "");
      b.textContent = lab;
      b.addEventListener("click", function () {
        Array.prototype.forEach.call(bar.children, function (x) {
          x.classList.remove("is-on");
        });
        b.classList.add("is-on");
        onPick(i, lab);
      });
      bar.appendChild(b);
    });
    return bar;
  }

  function renderSerp(root, data) {
    var kws = data.keywords || [];
    if (!kws.length) {
      root.textContent = "No SERP scan yet.";
      return;
    }
    var moves = (data.runs || []).slice(-1)[0];
    var head = document.createElement("div");
    head.className = "flex flex-wrap gap-1.5 mb-3";
    var m = (moves && moves.moves) || {};
    head.innerHTML =
      chip((data.row_count || kws.reduce(function (n, k) { return n + (k.row_count || (k.rows || []).length); }, 0)) + " rows") +
      chip("in " + (m.entered || 0), "pml-chip-new") +
      chip("out " + (m.dropped || 0), "pml-chip-down") +
      chip("up " + (m.up || 0), "pml-chip-up") +
      chip("down " + (m.down || 0), "pml-chip-down");
    var spark = document.createElement("div");
    spark.className = "pml-spark mb-3";
    (data.runs || []).slice(-16).forEach(function (r) {
      var mv = (r.moves || {}).entered || 0;
      var h = 6 + Math.min(22, mv * 2);
      spark.innerHTML += '<i class="' + (r.ok ? "on" : "off") + '" style="height:' + h + 'px" title="' + esc(fmtWhen(r.at)) + '"></i>';
    });
    var stage = document.createElement("div");
    var pages = Number(data.pages) || 4;
    var missRank = pages * 10 + 1;

    function siteKey(host) {
      host = String(host || "")
        .toLowerCase()
        .replace(/^www\./, "");
      var parts = host.split(".").filter(Boolean);
      var multi = { "com.tr": 1, "gov.tr": 1, "org.tr": 1, "net.tr": 1, "gen.tr": 1, "bel.tr": 1, "co.uk": 1 };
      if (parts.length >= 3 && multi[parts.slice(-2).join(".")]) return parts.slice(-3).join(".");
      if (parts.length >= 2) return parts.slice(-2).join(".");
      return host;
    }
    function isDoviz(host) {
      var h = siteKey(host);
      return h === "doviz.com";
    }

    function paintKeyword(kw) {
      var meta = document.createElement("div");
      meta.className = "flex flex-wrap gap-1.5 mb-2";
      var mv = kw.moves || {};
      meta.innerHTML =
        chip("doviz.com: " + (kw.our_rank || "—"), "pml-chip-doviz") +
        chip("+" + (mv.entered || 0) + " in", "pml-chip-new") +
        chip((mv.dropped || 0) + " out", "pml-chip-down");
      if (kw.rows_stale) {
        var staleNote = document.createElement("p");
        staleNote.className = "pml-note text-amber-800 dark:text-amber-200";
        staleNote.textContent =
          "Son tarama boş geldi; tablo önceki kayıtlı SERP listesini gösteriyor. Refresh ile yeniden tarayın.";
        meta.appendChild(staleNote);
      }
      var dropped = kw.dropped || [];
      if (dropped.length) {
        var dropWrap = document.createElement("div");
        dropWrap.className = "pml-serp-dropped mb-2";
        var dropTitle = document.createElement("p");
        dropTitle.className = "pml-note mb-1";
        dropTitle.textContent = "Dropped (" + dropped.length + ")";
        dropWrap.appendChild(dropTitle);
        var dropList = document.createElement("div");
        dropList.className = "flex flex-wrap gap-1";
        dropped.slice(0, 24).forEach(function (d) {
          dropList.appendChild(
            el(
              chip(esc(d.domain) + " (prev #" + d.prev_rank + ")", "pml-chip-down")
            )
          );
        });
        if (dropped.length > 24) {
          dropList.appendChild(el(chip("+" + (dropped.length - 24) + " more", "")));
        }
        dropWrap.appendChild(dropList);
        meta.appendChild(dropWrap);
      }
      var rows = (kw.rows || []).map(function (r) {
        var heat = r.delta === "up" ? "pml-heat-up" : r.delta === "down" ? "pml-heat-down" : r.delta === "new" ? "pml-heat-new" : "";
        var ours = r.ours || isDoviz(r.domain);
        return tr(
          [
            { text: r.rank, sort: r.rank, cls: ours ? "pml-rank" : "" },
            { text: r.page, sort: r.page },
            { html: '<a class="pml-link" href="' + esc(r.url) + '" target="_blank" rel="noopener">' + esc(r.domain) + "</a>" },
            { text: r.title },
            { text: r.snippet },
          ],
          (ours ? "pml-ours pml-doviz " : "") + heat
        );
      });
      stage.appendChild(meta);
      if (!rows.length) {
        var empty = document.createElement("p");
        empty.className = "pml-note text-slate-500 dark:text-slate-400";
        empty.textContent = "Bu kelime için SERP satırı yok. Refresh ile yeniden tarayın.";
        stage.appendChild(empty);
        return;
      }
      stage.appendChild(sortableTable(["Rank", "Page", "Domain", "Title / meta", "Snippet"], rows));
    }

    function paintTotal() {
      var map = {};
      kws.forEach(function (kw, ki) {
        (kw.rows || []).forEach(function (r) {
          var key = siteKey(r.domain);
          if (!key) return;
          var rank = Number(r.rank) || 0;
          if (!rank) return;
          if (!map[key]) map[key] = { domain: key, ranks: {}, ours: isDoviz(key) };
          var prev = map[key].ranks[ki];
          if (prev == null || rank < prev) map[key].ranks[ki] = rank;
          if (r.ours) map[key].ours = true;
        });
      });
      var sites = Object.keys(map).map(function (key) {
        var rec = map[key];
        var sum = 0;
        var hit = 0;
        var per = [];
        for (var i = 0; i < kws.length; i++) {
          var rk = rec.ranks[i];
          if (rk == null) rk = missRank;
          else hit += 1;
          per.push(rk);
          sum += rk;
        }
        rec.avg = sum / kws.length;
        rec.hit = hit;
        rec.per = per;
        return rec;
      });
      sites.sort(function (a, b) {
        return a.avg - b.avg || b.hit - a.hit || a.domain.localeCompare(b.domain, "tr");
      });
      var ours = sites.filter(function (s) {
        return s.ours;
      })[0];
      var meta = document.createElement("div");
      meta.className = "flex flex-wrap gap-1.5 mb-2";
      meta.innerHTML =
        chip(sites.length + " site") +
        chip("if missing #" + missRank, "") +
        chip("doviz avg. " + (ours ? ours.avg.toFixed(1) : "—"), "pml-chip-doviz") +
        (ours ? chip("seen " + ours.hit + "/" + kws.length, "pml-chip-doviz") : "");
      var note = document.createElement("p");
      note.className = "pml-note";
      note.textContent =
        "Average = best rank across 8 queries. Missing from the first " +
        pages +
        " pages counts as " +
        missRank +
        " (4×10+1).";
      var headers = ["Domain", "Avg. rank", "Seen"].concat(
        kws.map(function (k) {
          return k.keyword;
        })
      );
      var rows = sites.map(function (s) {
        var cells = [
          { text: s.domain },
          { text: s.avg.toFixed(1), sort: s.avg, cls: "pml-rank" },
          { text: s.hit + "/" + kws.length, sort: s.hit },
        ];
        s.per.forEach(function (rk) {
          var missing = rk >= missRank;
          cells.push({
            html: missing ? '<span class="pml-miss">—</span>' : String(rk),
            sort: rk,
            cls: missing ? "pml-miss" : s.ours ? "pml-rank" : "",
          });
        });
        return tr(cells, s.ours ? "pml-ours pml-doviz" : "");
      });
      stage.appendChild(meta);
      stage.appendChild(note);
      stage.appendChild(sortableTable(headers, rows));
    }

    function paint(idx) {
      stage.innerHTML = "";
      if (idx >= kws.length) {
        paintTotal();
        return;
      }
      paintKeyword(kws[idx]);
    }
    root.appendChild(head);
    root.appendChild(spark);
    root.appendChild(
      tabs(
        kws
          .map(function (k) {
            return k.keyword;
          })
          .concat(["Total"]),
        paint
      )
    );
    root.appendChild(stage);
    paint(0);
  }

  var SAPMA_THRESHOLDS = {
    usd: [0.08, 0.22],
    eur: [0.08, 0.22],
    gram_altin: [0.12, 0.35],
    ons_altin: [0.12, 0.35],
    bist100: [0.15, 0.40],
    brent: [0.20, 0.55],
    bitcoin: [0.25, 0.70],
    gram_gumus: [0.35, 0.90],
    ceyrek_altin: [0.50, 1.20],
  };
  var SAPMA_RANGES = {
    usd: [25, 90],
    eur: [30, 110],
    bist100: [5000, 30000],
    gram_altin: [4000, 9000],
    gram_gumus: [80, 160],
    ons_altin: [1500, 10000],
    brent: [30, 250],
    ceyrek_altin: [3000, 40000],
    bitcoin: [25000, 150000],
  };

  function parseQuote(aid, raw) {
    var s = String(raw || "").replace(/\$/g, "").replace(/\s/g, "").trim();
    if (!s) return null;
    var comma = s.lastIndexOf(",");
    var dot = s.lastIndexOf(".");
    var norm = s;
    if (comma >= 0 && dot >= 0) {
      norm = comma > dot ? s.replace(/\./g, "").replace(",", ".") : s.replace(/,/g, "");
    } else if (comma >= 0) {
      norm = s.replace(",", ".");
    } else if ((s.match(/\./g) || []).length > 1) {
      norm = s.replace(/\./g, "");
    }
    var val = parseFloat(norm);
    if (!isFinite(val)) return null;
    var cands = [val];
    if (/^\d{1,2}\.\d{3}$/.test(s) && val < 500) {
      var alt = parseFloat(s.replace(".", ""));
      if (isFinite(alt) && alt !== val) cands.push(alt);
    }
    var bounds = SAPMA_RANGES[aid];
    var i;
    for (i = 0; i < cands.length; i++) {
      if (!bounds || (cands[i] >= bounds[0] && cands[i] <= bounds[1])) return cands[i];
    }
    return bounds ? null : val;
  }

  function sapmaBand(aid, doviz, peer, n) {
    var thr = SAPMA_THRESHOLDS[aid] || [0.2, 0.5];
    if (doviz == null || peer == null || !peer) {
      return { pct: null, avg: null, n: n || 0, warn: thr[0], alert: thr[1], band: "" };
    }
    var pct = ((doviz - peer) / peer) * 100;
    var ap = Math.abs(pct);
    var band = ap < thr[0] ? "ok" : ap < thr[1] ? "warn" : "hot";
    return { pct: pct, avg: peer, n: n || 1, warn: thr[0], alert: thr[1], band: band };
  }

  function computeSapma(aid, cells) {
    var doviz = parseQuote(aid, ((cells || {}).doviz || {}).value);
    var peers = [];
    Object.keys(cells || {}).forEach(function (sid) {
      if (sid === "doviz") return;
      var n = parseQuote(aid, (cells[sid] || {}).value);
      if (n != null) peers.push(n);
    });
    if (doviz == null || peers.length < 2) {
      return sapmaBand(aid, null, null, peers.length);
    }
    var sum = 0;
    peers.forEach(function (p) {
      sum += p;
    });
    return sapmaBand(aid, doviz, sum / peers.length, peers.length);
  }

  function computeForeksSapma(aid, cells) {
    var doviz = parseQuote(aid, ((cells || {}).doviz || {}).value);
    var peer = parseQuote(aid, ((cells || {}).foreks || {}).value);
    return sapmaBand(aid, doviz, peer, peer == null ? 0 : 1);
  }

  function fmtSapmaPct(n) {
    var sign = n > 0 ? "+" : n < 0 ? "−" : "";
    return sign + Math.abs(n).toFixed(2).replace(".", ",") + "%";
  }

  var SITE_COL_LABELS = {
    enuygun: "Enuygun",
    bloomberght: "Bloomberg",
    tradingview: "Trading",
    cnnturk: "CNN",
  };

  function ensureSiteColumns(cols) {
    var out = (cols || []).slice();
    var ids = {};
    out.forEach(function (c) {
      ids[c.id] = true;
    });
    if (!ids.paratic) {
      out.push({ id: "paratic", label: "Paratic", url: "https://piyasa.paratic.com/" });
    }
    return out;
  }

  function withSapmaColumn(cols) {
    var out = [];
    var inserted = false;
    (cols || []).forEach(function (c) {
      out.push({
        id: c.id,
        label: SITE_COL_LABELS[c.id] || c.label,
        url: c.url,
        synthetic: c.synthetic,
      });
      if (!inserted && c.id === "doviz") {
        out.push({ id: "sapma", label: "avg. deviation", synthetic: true });
        out.push({ id: "foreks_sapma", label: "Foreks deviation", synthetic: true });
        inserted = true;
      }
    });
    if (!inserted && out.length) {
      out.splice(1, 0, { id: "sapma", label: "avg. deviation", synthetic: true });
      out.splice(2, 0, { id: "foreks_sapma", label: "Foreks deviation", synthetic: true });
    }
    return out;
  }

  var ASSET_ROW_ORDER = [
    "usd",
    "eur",
    "gram_altin",
    "ceyrek_altin",
    "ons_altin",
    "gram_gumus",
    "bitcoin",
    "brent",
    "bist100",
  ];

  function sortAssetRows(matrix) {
    var rank = {};
    ASSET_ROW_ORDER.forEach(function (id, i) {
      rank[id] = i;
    });
    return (matrix || []).slice().sort(function (a, b) {
      var ar = rank[a.id];
      var br = rank[b.id];
      if (ar == null && br == null) return 0;
      if (ar == null) return 1;
      if (br == null) return -1;
      return ar - br;
    });
  }

  function renderCompetitors(root, data) {
    var cols = withSapmaColumn(ensureSiteColumns(data.columns || []));
    var matrix = sortAssetRows(data.matrix || []);
    if (!matrix.length) {
      root.textContent = "No price matrix yet.";
      return;
    }
    var headers = ["Asset"].concat(cols.map(function (c) { return c.label; }));
    var rows = matrix.map(function (r) {
      var sapma = computeSapma(r.id, r.cells || {});
      var foreksSapma = computeForeksSapma(r.id, r.cells || {});
      var cells = [{ text: r.label, cls: "pin" }];
      cols.forEach(function (c) {
        if (c.id === "sapma" || c.id === "foreks_sapma") {
          var rec = c.id === "foreks_sapma" ? foreksSapma : sapma;
          if (rec.pct == null) {
            cells.push({
              html: '<span class="pml-miss">—</span>',
              sort: "",
              cls: "pml-sapma",
            });
            return;
          }
          var title =
            c.id === "foreks_sapma"
              ? "Döviz vs Foreks · threshold ±" + String(rec.warn).replace(".", ",") + "% (volume)"
              : "Döviz vs peer mean (" +
                rec.n +
                ") · threshold ±" +
                String(rec.warn).replace(".", ",") +
                "% (volume)";
          var note =
            c.id === "foreks_sapma"
              ? "±" + String(rec.warn).replace(".", ",") + "%"
              : "n=" + rec.n + " · ±" + String(rec.warn).replace(".", ",") + "%";
          cells.push({
            html:
              '<span title="' +
              esc(title) +
              '">' +
              esc(fmtSapmaPct(rec.pct)) +
              '</span><div class="pml-note">' +
              esc(note) +
              "</div>",
            sort: rec.pct,
            cls: "pml-sapma pml-sapma-" + rec.band,
          });
          return;
        }
        var cell = (r.cells || {})[c.id] || {};
        var v = cell.value || "";
        cells.push({
          html: v
            ? esc(v) + (cell.change ? '<div class="pml-note" style="margin:0">' + esc(cell.change) + "</div>" : "")
            : '<span class="pml-miss">—</span>',
          text: v,
          sort: v.replace(/[^\d,.-]/g, "").replace(",", "."),
        });
      });
      return tr(cells);
    });
    root.appendChild(sortableTable(headers, rows));
  }

  function rankDeltaHtml(a) {
    if (a.delta === "new") return '<span class="pml-chip pml-chip-new">new</span>';
    if (a.delta === "up") return '<span class="pml-delta-up">↑ ' + (a.delta_n || 0) + "</span>";
    if (a.delta === "down") return '<span class="pml-delta-down">↓ ' + Math.abs(a.delta_n || 0) + "</span>";
    if (a.delta === "same") return '<span class="pml-miss">—</span>';
    return "";
  }

  function chartById(charts, id) {
    for (var i = 0; i < charts.length; i++) {
      if (charts[i] && charts[i].id === id) return charts[i];
    }
    return null;
  }

  function appLetter(a) {
    return String(a.name || "?").trim().charAt(0).toUpperCase() || "?";
  }

  function appNameHtml(a) {
    var name = esc(a.name || a.id || "");
    var icon = String(a.icon || "").trim();
    var letter = esc(appLetter(a));
    if (icon) {
      return (
        '<span class="pml-app"><img class="pml-app-icon" src="' +
        esc(icon) +
        '" alt="" width="22" height="22" loading="lazy" decoding="async" referrerpolicy="no-referrer" onerror="this.hidden=true;var f=this.nextElementSibling;if(f)f.hidden=false"><span class="pml-app-icon pml-app-icon-fallback" hidden aria-hidden="true">' +
        letter +
        '</span><span class="pml-app-name">' +
        name +
        "</span></span>"
      );
    }
    return (
      '<span class="pml-app"><span class="pml-app-icon pml-app-icon-fallback" aria-hidden="true">' +
      letter +
      '</span><span class="pml-app-name">' +
      name +
      "</span></span>"
    );
  }

  function platIconHtml(ch) {
    var id = String((ch && ch.id) || "").toLowerCase();
    if (id === "ios" || id.indexOf("ios") >= 0 || id.indexOf("appstore") >= 0) {
      return (
        '<svg class="pml-plat-icon pml-plat-icon-ios" viewBox="0 0 24 24" width="18" height="18" aria-hidden="true">' +
        '<path fill="currentColor" d="M16.365 12.83c-.018-1.977 1.614-2.927 1.687-2.973-.92-1.346-2.35-1.531-2.857-1.55-1.217-.123-2.376.715-2.993.715-.632 0-1.566-.698-2.577-.68-1.326.02-2.547.77-3.23 1.958-1.377 2.387-.352 5.92.99 7.855.656.95 1.44 2.013 2.467 1.974 1-.04 1.377-.64 2.584-.64 1.2 0 1.54.64 2.59.62 1.07-.018 1.75-.97 2.4-1.924.754-1.1 1.064-2.167 1.082-2.222-.024-.01-2.066-.793-2.084-3.143zm-2.215-5.97c.54-.655.91-1.566.81-2.473-.783.032-1.73.522-2.292 1.18-.504.583-.945 1.51-.827 2.4.874.068 1.77-.444 2.31-1.107z"/></svg>'
      );
    }
    return (
      '<svg class="pml-plat-icon pml-plat-icon-android" viewBox="0 0 24 24" width="18" height="18" aria-hidden="true">' +
      '<path fill="#3DDC84" d="M17.523 9.433l1.675-2.9a.478.478 0 10-.828-.478l-1.697 2.94A8.04 8.04 0 0012 8.1c-1.57 0-3.04.4-4.673.895L5.63 6.055a.478.478 0 10-.828.478l1.675 2.9C3.97 10.96 2.25 13.64 2.25 16.8h19.5c0-3.16-1.72-5.84-4.227-7.367zM8.1 14.55a1.2 1.2 0 110-2.4 1.2 1.2 0 010 2.4zm7.8 0a1.2 1.2 0 110-2.4 1.2 1.2 0 010 2.4z"/></svg>'
    );
  }

  function storeColTitle(ch) {
    var id = String((ch && ch.id) || "").toLowerCase();
    if (id === "android" || id === "play") return "Play · Finance free (TR)";
    if (id === "ios" || id.indexOf("ios") >= 0 || id.indexOf("appstore") >= 0) return "App Store · Finance free (TR)";
    return (ch && ch.title) || "";
  }

  function paintStoreCol(host, ch, q) {
    host.innerHTML = "";
    if (!ch) {
      host.textContent = "No list.";
      return;
    }
    var mv = ch.moves || {};
    var head = document.createElement("div");
    head.className = "mb-1.5";
    head.innerHTML =
      '<h4 class="pml-col-title text-sm font-bold text-slate-800 dark:text-zinc-100">' +
      platIconHtml(ch) +
      "<span>" +
      esc(storeColTitle(ch)) +
      "</span></h4>";
    var meta = document.createElement("div");
    meta.className = "flex flex-wrap gap-1.5 mb-2";
    meta.innerHTML = mv.reset
      ? chip(ch.our_label || "") + chip("Δ next scan")
      : chip(ch.our_label || "") +
        chip("↑ " + (mv.up || 0), "pml-chip-up") +
        chip("↓ " + (mv.down || 0), "pml-chip-down") +
        chip("new " + (mv.new || 0), "pml-chip-new") +
        chip("out " + (mv.dropped || 0), "pml-chip-down");
    host.appendChild(head);
    host.appendChild(meta);
    if ((ch.dropped || []).length) {
      var drop = document.createElement("p");
      drop.className = "pml-note";
      drop.textContent =
        "Dropped: " +
        ch.dropped
          .slice(0, 8)
          .map(function (d) {
            return (d.name || d.id) + " (prev #" + d.prev_rank + ")";
          })
          .join(" · ");
      host.appendChild(drop);
    }
    var rows = (ch.apps || [])
      .filter(function (a) {
        if (!q) return true;
        return String(a.name || "").toLowerCase().indexOf(q) >= 0 || String(a.id || "").toLowerCase().indexOf(q) >= 0;
      })
      .map(function (a) {
        var heat = a.delta === "up" ? "pml-heat-up" : a.delta === "down" ? "pml-heat-down" : a.delta === "new" ? "pml-heat-new" : "";
        return tr(
          [
            { text: a.rank, sort: a.rank, cls: "pin" },
            { html: rankDeltaHtml(a), sort: a.delta_n == null ? 0 : a.delta_n },
            { html: appNameHtml(a), sort: a.name || "" },
          ],
          (a.is_ours ? "pml-ours pml-doviz " : "") + heat
        );
      });
    host.appendChild(sortableTable(["Rank", "Δ", "App"], rows, { wide: false }));
  }

  function renderStore(root, data) {
    var charts = data.charts || [];
    if (!charts.length) {
      root.textContent = "No list.";
      return;
    }
    var android = chartById(charts, "android") || chartById(charts, "play") || charts[0];
    var ios = chartById(charts, "ios") || charts[1] || null;
    var tools = document.createElement("div");
    tools.className = "pml-tools";
    var search = document.createElement("input");
    search.className = "pml-search";
    search.setAttribute("inputmode", "search");
    search.setAttribute("autocomplete", "off");
    search.placeholder = "Search apps…";
    tools.appendChild(search);
    var split = document.createElement("div");
    split.className = "pml-store-split";
    var left = document.createElement("section");
    left.className = "pml-store-col";
    var right = document.createElement("section");
    right.className = "pml-store-col";
    split.appendChild(left);
    split.appendChild(right);
    function paint() {
      var q = search.value.trim().toLowerCase();
      paintStoreCol(left, android, q);
      paintStoreCol(right, ios, q);
    }
    search.addEventListener("input", paint);
    root.appendChild(tools);
    root.appendChild(split);
    paint();
  }

  function renderNews(root, data) {
    var kws = data.keywords || [];
    var NEWS_BARS_TOP = 10;
    var avgs = (data.source_averages || data.source_counts || []).filter(function (s) {
      return Number(s && s.count) >= 3;
    });
    avgs.sort(function (a, b) {
      return (b.count || 0) - (a.count || 0) ||
        String(a.source || "").localeCompare(String(b.source || ""), "tr");
    });
    var head = document.createElement("div");
    head.className = "mb-3";
    head.innerHTML = chip((data.article_total || 0) + " stories") + " " + chip((data.runs || []).length + " scans");
    var bars = document.createElement("div");
    bars.className = "pml-news-bars mb-4";
    var max = 1;
    avgs.forEach(function (s) {
      max = Math.max(max, s.count || 0, s.avg || 0);
    });
    function appendBar(parent, s) {
      var row = document.createElement("div");
      row.className = "pml-bar";
      var w = Math.round((100 * (s.count || 0)) / max);
      row.innerHTML =
        '<span class="pml-bar-label">' +
        esc(s.source) +
        "</span><div class=\"pml-bar-track\"><div class=\"pml-bar-fill\" style=\"width:" +
        w +
        '%"></div></div>' +
        chip((s.count || 0) + " · avg " + (s.avg != null ? s.avg : s.count), "");
      parent.appendChild(row);
    }
    var topBars = avgs.slice(0, NEWS_BARS_TOP);
    var restBars = avgs.slice(NEWS_BARS_TOP);
    topBars.forEach(function (s) { appendBar(bars, s); });
    if (restBars.length) {
      var overflow = document.createElement("div");
      overflow.className = "pml-news-bars-overflow";
      overflow.setAttribute("aria-label", "More sources");
      restBars.forEach(function (s) { appendBar(overflow, s); });
      bars.appendChild(overflow);
    }
    var spark = document.createElement("div");
    spark.className = "pml-spark mb-3";
    (data.runs || []).slice(-16).forEach(function (r) {
      var h = 6 + Math.min(22, (r.article_total || 0) / 8);
      spark.innerHTML += '<i class="' + (r.ok ? "on" : "off") + '" style="height:' + h + 'px" title="' + esc(fmtWhen(r.at) + " · " + (r.article_total || 0)) + '"></i>';
    });
    var stage = document.createElement("div");
    function newsIsDoviz(art) {
      var blob = [art && art.source, art && art.publisher, art && art.url, art && art.domain]
        .join(" ")
        .toLowerCase();
      return blob.indexOf("doviz.com") !== -1;
    }
    function paint(i) {
      stage.innerHTML = "";
      var kw = kws[i] || {};
      var arts = kw.articles || [];
      var our = 0;
      arts.forEach(function (a, n) {
        if (!our && newsIsDoviz(a)) our = n + 1;
      });
      var meta = document.createElement("div");
      meta.className = "flex flex-wrap gap-1.5 mb-2";
      meta.innerHTML = chip("doviz.com rank: " + (our || "—"), "pml-chip-doviz");
      stage.appendChild(meta);
      var rows = arts.map(function (a, n) {
        var ours = newsIsDoviz(a);
        return tr(
          [
            { text: n + 1, sort: n + 1, cls: ours ? "pml-rank" : "" },
            { html: '<a class="pml-link" href="' + esc(a.url) + '" target="_blank" rel="noopener">' + esc(a.title) + "</a>" },
            { text: a.source },
            { text: a.time },
          ],
          ours ? "pml-ours pml-doviz" : ""
        );
      });
      stage.appendChild(sortableTable(["#", "Title", "Source", "Time"], rows));
    }
    root.appendChild(head);
    root.appendChild(spark);
    root.appendChild(bars);
    if (kws.length) {
      root.appendChild(tabs(kws.map(function (k) { return k.keyword; }), paint));
      root.appendChild(stage);
      paint(0);
    }
  }

  var renderers = {
    serp: renderSerp,
    competitors: renderCompetitors,
    store_charts: renderStore,
    google_news: renderNews,
  };
  var JOB_WAIT_MS = {
    serp: 20 * 60 * 1000,
    competitors: 12 * 60 * 1000,
    store_charts: 15 * 60 * 1000,
    google_news: 10 * 60 * 1000,
  };
  var refreshing = {};

  function fmtWhenFull(iso) {
    if (!iso) return "";
    var d = new Date(iso);
    if (isNaN(d.getTime())) return String(iso).slice(0, 16).replace("T", " ");
    return d
      .toLocaleString("tr-TR", {
        timeZone: "Europe/Istanbul",
        day: "2-digit",
        month: "2-digit",
        year: "numeric",
        hour: "2-digit",
        minute: "2-digit",
      })
      .replace(",", "")
      .replace(/\s+/g, " ");
  }

  function paintCard(id, data) {
    sections[id] = data || {};
    var node = document.querySelector('.pml-body[data-pml="' + id + '"]');
    if (!node) return;
    node.innerHTML = "";
    var fn = renderers[id];
    if (!fn) {
      node.textContent = (data && data.message) || "This section is empty.";
      return;
    }
    try {
      fn(node, data || {});
    } catch (err) {
      node.textContent = "Render error: " + err;
    }
    var timeEl = document.querySelector('time[data-pml-when="' + id + '"]');
    if (timeEl && data && data.scraped_at) {
      timeEl.hidden = false;
      timeEl.setAttribute("datetime", data.scraped_at);
      timeEl.textContent = fmtWhenFull(data.scraped_at);
    }
  }

  function sleep(ms) {
    return new Promise(function (resolve) {
      setTimeout(resolve, ms);
    });
  }

  function setRefreshBtn(btn, busy, label) {
    if (!btn) return;
    btn.disabled = !!busy;
    btn.classList.toggle("is-busy", !!busy);
    btn.textContent = label;
  }

  function pingBridge(id) {
    var ctrl = typeof AbortController !== "undefined" ? new AbortController() : null;
    var timer = setTimeout(function () {
      if (ctrl) ctrl.abort();
    }, 3500);
    return fetch("http://127.0.0.1:18765/sync-pm-lab?jobs=" + encodeURIComponent(id), {
      method: "POST",
      mode: "cors",
      headers: { Accept: "application/json" },
      signal: ctrl ? ctrl.signal : undefined,
    })
      .then(function (resp) {
        return resp.status === 200 || resp.status === 409;
      })
      .catch(function (err) {
        return !!(err && err.name === "AbortError");
      })
      .then(function (ok) {
        clearTimeout(timer);
        return ok;
      });
  }

  function fetchLabState() {
    if (isEmbed && embedCfg) {
      return fetch(embedCfg.fetchUrl, { credentials: "same-origin", headers: { Accept: "application/json" } }).then(
        function (resp) {
          if (!resp.ok) throw new Error("Could not load status");
          return resp.json().then(function (data) {
            var block = (data && data.section) || {};
            var sectionsOut = {};
            sectionsOut[embedSectionId] = block;
            return {
              sections: sectionsOut,
              queued: data && data.queued ? data.queued : [],
              running: data && data.running ? data.running : "",
            };
          });
        }
      );
    }
    return fetch("/api/pm-lab/state", { credentials: "same-origin", headers: { Accept: "application/json" } }).then(
      function (resp) {
        if (!resp.ok) throw new Error("Could not load status");
        return resp.json();
      }
    );
  }

  function queueLabRefresh(id) {
    var url = isEmbed && embedCfg ? embedCfg.refreshUrl : "/api/pm-lab/refresh";
    var body = isEmbed ? "{}" : JSON.stringify({ job: id });
    return fetch(url, {
      method: "POST",
      credentials: "same-origin",
      headers: { Accept: "application/json", "Content-Type": "application/json" },
      body: body,
    }).then(function (resp) {
      return resp.json().then(function (data) {
        return { resp: resp, data: data || {} };
      }).catch(function () {
        return { resp: resp, data: {} };
      });
    });
  }

  function pollSection(id, prevAt, timeoutMs, btn, kicked) {
    var started = Date.now();
    function tick() {
      var elapsed = Date.now() - started;
      if (elapsed > timeoutMs) {
        return Promise.reject(new Error("Scan timed out — is the Mac bridge running?"));
      }
      return fetchLabState()
        .then(function (state) {
          var block = ((state || {}).sections || {})[id] || {};
          var at = String(block.scraped_at || "");
          if (at && at !== prevAt) return state;
          var queued = (state.queued || []).indexOf(id) >= 0;
          var running = String(state.running || "") === id;
          if (running || kicked) setRefreshBtn(btn, true, "Scanning…");
          else if (queued) setRefreshBtn(btn, true, "Queued…");
          if (!kicked && !running && elapsed > 12000) {
            return Promise.reject(
              new Error("Mac bridge did not pick up the scan. git pull && restart the bridge daemon.")
            );
          }
          return sleep(3000).then(tick);
        });
    }
    return tick();
  }

  function refreshSection(id, btn) {
    if (!id || refreshing[id]) return;
    refreshing[id] = true;
    var prevAt = String((sections[id] || {}).scraped_at || "");
    setRefreshBtn(btn, true, "Scanning…");
    var kicked = false;
    Promise.all([
      queueLabRefresh(id),
      pingBridge(id).then(function (ok) {
        kicked = !!ok;
        return ok;
      }),
    ])
      .then(function (pair) {
        var out = pair[0] || { resp: { ok: false }, data: {} };
        if (!out.resp.ok || out.data.ok === false) {
          if (!kicked) {
            var detail = out.data.detail || out.data.message || "Could not queue scan";
            throw new Error(typeof detail === "string" ? detail : "Could not queue scan");
          }
        }
        setRefreshBtn(btn, true, "Scanning…");
        return pollSection(id, prevAt, JOB_WAIT_MS[id] || 12 * 60 * 1000, btn, kicked);
      })
      .then(function (state) {
        var block = ((state || {}).sections || {})[id] || {};
        if (isEmbed) paintEmbedSection(block);
        else {
          paintCard(id, block);
          renderStatus();
        }
        setRefreshBtn(btn, false, "Refresh");
      })
      .catch(function (err) {
        var msg = String((err && err.message) || err || "Refresh failed");
        setRefreshBtn(btn, false, "Refresh");
        btn.title = msg;
        window.alert(msg);
      })
      .then(function () {
        refreshing[id] = false;
      });
  }

  function bindRefreshButtons() {
    document.querySelectorAll("[data-pml-refresh]").forEach(function (btn) {
      btn.addEventListener("click", function (e) {
        e.preventDefault();
        e.stopPropagation();
        refreshSection(btn.getAttribute("data-pml-refresh"), btn);
      });
    });
    document.querySelectorAll("[data-pml-fold]").forEach(function (btn) {
      btn.addEventListener("click", function () {
        var wrap = btn.closest(".pml-card-wrap");
        if (!wrap) return;
        var open = wrap.classList.toggle("is-open");
        btn.setAttribute("aria-expanded", open ? "true" : "false");
      });
    });
  }

  function paintEmbedSection(block) {
    if (!embedCfg) return;
    sections[embedSectionId] = block || {};
    var mount = document.getElementById(embedCfg.mountId);
    if (!mount) return;
    mount.innerHTML = "";
    var fn = renderers[embedSectionId];
    try {
      if (fn) fn(mount, block || {});
      else mount.textContent = (block && block.message) || "This section is empty.";
    } catch (err) {
      mount.textContent = "Render error: " + err;
    }
    var timeEl = document.getElementById(embedCfg.whenId);
    if (timeEl) {
      if (block && block.scraped_at) {
        timeEl.hidden = false;
        timeEl.setAttribute("datetime", block.scraped_at);
        timeEl.textContent = fmtWhenFull(block.scraped_at);
      } else {
        timeEl.hidden = true;
        timeEl.textContent = "";
      }
    }
  }

  function loadEmbedSection() {
    if (!embedCfg) return Promise.resolve();
    var mount = document.getElementById(embedCfg.mountId);
    if (mount) mount.textContent = "Loading…";
    return fetchLabState()
      .then(function (state) {
        paintEmbedSection((((state || {}).sections || {})[embedSectionId]) || {});
      })
      .catch(function (err) {
        if (mount) mount.textContent = String((err && err.message) || err || "Load failed");
      });
  }

  function wireEmbedShell() {
    if (!embedCfg) return;
    var hdr = document.getElementById(embedCfg.headerId);
    var body = document.getElementById(embedCfg.bodyId);
    var chev = document.getElementById(embedCfg.chevronId);
    var refreshBtn = document.getElementById(embedCfg.refreshId);
    function setOpen(open) {
      if (!body || !hdr) return;
      body.hidden = !open;
      hdr.setAttribute("aria-expanded", open ? "true" : "false");
      if (chev) chev.classList.toggle("rotate-180", open);
    }
    if (hdr && body) {
      hdr.addEventListener("click", function (ev) {
        if (ev.target.closest("#" + embedCfg.refreshId)) return;
        setOpen(body.hidden);
      });
      hdr.addEventListener("keydown", function (ev) {
        if (ev.key === "Enter" || ev.key === " ") {
          ev.preventDefault();
          setOpen(body.hidden);
        }
      });
    }
    if (refreshBtn) {
      refreshBtn.addEventListener("click", function (e) {
        e.preventDefault();
        e.stopPropagation();
        refreshSection(embedSectionId, refreshBtn);
      });
    }
    loadEmbedSection();
    setOpen(!!embedCfg.defaultOpen);
  }

  if (isEmbed) {
    wireEmbedShell();
  } else {
    renderStatus();
    document.querySelectorAll(".pml-body[data-pml]").forEach(function (node) {
      var id = node.getAttribute("data-pml");
      paintCard(id, sections[id] || {});
    });
    bindRefreshButtons();
  }
})();
