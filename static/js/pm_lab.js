(function () {
  var bootEl = document.getElementById("pml-boot");
  if (!bootEl) return;
  var boot = {};
  try {
    boot = JSON.parse(bootEl.textContent || "{}");
  } catch (e) {
    return;
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
      '<div class="pml-spark" title="Son taramalar">' +
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
      b.className = "pml-tab" + (i === on ? " is-on" : "") + (lab === "Toplam" ? " pml-tab-total" : "");
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
      root.textContent = "Henüz SERP taraması yok.";
      return;
    }
    var moves = (data.runs || []).slice(-1)[0];
    var head = document.createElement("div");
    head.className = "flex flex-wrap gap-1.5 mb-3";
    var m = (moves && moves.moves) || {};
    head.innerHTML =
      chip((data.row_count || kws.reduce(function (n, k) { return n + (k.row_count || (k.rows || []).length); }, 0)) + " satır") +
      chip("girdi " + (m.entered || 0), "pml-chip-new") +
      chip("çıktı " + (m.dropped || 0), "pml-chip-down") +
      chip("yükselen " + (m.up || 0), "pml-chip-up") +
      chip("düşen " + (m.down || 0), "pml-chip-down");
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
        chip("doviz.com: " + (kw.our_rank || "yok"), "pml-chip-doviz") +
        chip("+" + (mv.entered || 0) + " girdi", "pml-chip-new") +
        chip((mv.dropped || 0) + " çıktı", "pml-chip-down");
      var drop = (kw.dropped || [])
        .map(function (d) {
          return esc(d.domain) + " (eski #" + d.prev_rank + ")";
        })
        .join(" · ");
      if (drop) {
        var p = document.createElement("p");
        p.className = "pml-note";
        p.textContent = "Çıkanlar: " + drop;
        meta.appendChild(p);
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
      stage.appendChild(sortableTable(["Sıra", "Sayfa", "Domain", "Başlık / meta", "Snippet"], rows));
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
        chip("yoksa #" + missRank, "") +
        chip("doviz ort. " + (ours ? ours.avg.toFixed(1) : "yok"), "pml-chip-doviz") +
        (ours ? chip("göründü " + ours.hit + "/" + kws.length, "pml-chip-doviz") : "");
      var note = document.createElement("p");
      note.className = "pml-note";
      note.textContent =
        "Ortalama = 8 kelimedeki en iyi sıra. İlk " +
        pages +
        " sayfada yoksa " +
        missRank +
        " yazılır (4×10+1).";
      var headers = ["Domain", "Ort. sıra", "Göründü"].concat(
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
          .concat(["Toplam"]),
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
        out.push({ id: "sapma", label: "ort. sapma", synthetic: true });
        out.push({ id: "foreks_sapma", label: "Foreks sapma", synthetic: true });
        inserted = true;
      }
    });
    if (!inserted && out.length) {
      out.splice(1, 0, { id: "sapma", label: "ort. sapma", synthetic: true });
      out.splice(2, 0, { id: "foreks_sapma", label: "Foreks sapma", synthetic: true });
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
    var cols = withSapmaColumn(data.columns || []);
    var matrix = sortAssetRows(data.matrix || []);
    if (!matrix.length) {
      root.textContent = "Fiyat matrisi henüz yok.";
      return;
    }
    var headers = ["Varlık"].concat(cols.map(function (c) { return c.label; }));
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
              ? "Döviz vs Foreks · eşik ±" + String(rec.warn).replace(".", ",") + "% (hacim)"
              : "Döviz vs diğer " +
                rec.n +
                " sitenin ortalaması · eşik ±" +
                String(rec.warn).replace(".", ",") +
                "% (hacim)";
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

  function parseTrDate(s) {
    var m = String(s || "").match(/(\d{1,2})\.(\d{1,2})\.(\d{4})(?:\s+(\d{1,2}):(\d{2}))?/);
    if (!m) return 0;
    return Date.UTC(+m[3], +m[2] - 1, +m[1], +(m[4] || 0), +(m[5] || 0));
  }

  function renderSikayet(root, data) {
    var brands = (data.brands || []).slice().filter(function (b) {
      return b.brand !== "x.com";
    });
    if (!brands.length && data.sikayetvar) {
      brands = [{ brand: "doviz.com", sikayetvar: data.sikayetvar, eksi: data.eksi, x: data.x }];
    }
    if (!brands.length) {
      root.textContent = "Kayıt yok.";
      return;
    }
    var sourceStage = document.createElement("div");
    sourceStage.className = "min-w-0";
    var stage = document.createElement("div");
    stage.className = "min-w-0";
    var currentBrand = 0;
    var currentSrc = 0;
    var srcDefs = [
      { id: "x", label: "X" },
      { id: "eksi", label: "Ekşi" },
      { id: "sikayetvar", label: "Şikayetvar" },
    ];

    function uniqText(rows, textKey) {
      var seen = {};
      return rows.filter(function (row) {
        var key = String(row[textKey] || row.text || row.title || "").replace(/\s+/g, " ").slice(0, 80);
        if (!key || seen[key]) return false;
        seen[key] = 1;
        return true;
      });
    }

    function cardMeta(label, meta, text, url) {
      var card = document.createElement("article");
      card.className = "pml-card";
      card.innerHTML =
        '<p class="pml-card-kicker">' +
        esc(label) +
        (meta ? " · " + esc(meta) : "") +
        "</p>" +
        '<p class="pml-card-text">' +
        esc(text) +
        "</p>" +
        (url ? '<p class="pml-card-url"><a class="pml-link" href="' + esc(url) + '" target="_blank" rel="noopener">' + esc(url) + "</a></p>" : "");
      return card;
    }

    function paintSource() {
      stage.innerHTML = "";
      var b = brands[currentBrand];
      var src = srcDefs[currentSrc];
      if (src.id === "x") {
        var items = uniqText((b.x || {}).items || [], "text").slice(0, 10);
        if (!items.length) {
          stage.textContent = "X’te bu arama için kayıt yok (giriş duvarı olabilir).";
          return;
        }
        items.forEach(function (it) {
          stage.appendChild(cardMeta("X", [it.author, it.date].filter(Boolean).join(" · "), it.text || it.title || "", it.url));
        });
        return;
      }
      if (src.id === "eksi") {
        var ek = b.eksi || {};
        var entries = uniqText(
          (ek.entries || []).map(function (en) {
            return typeof en === "string" ? { text: en, url: ek.url } : en;
          }),
          "text"
        )
          .sort(function (a, b) {
            return parseTrDate(b.date) - parseTrDate(a.date);
          })
          .slice(0, 10);
        if (!entries.length) {
          stage.textContent = "Ekşi kaydı yok.";
          return;
        }
        entries.forEach(function (en) {
          stage.appendChild(cardMeta("Ekşi", [en.author, en.date].filter(Boolean).join(" · "), en.text || "", en.url || ek.url));
        });
        return;
      }
      var sv = b.sikayetvar || {};
      var complaints = uniqText(sv.items || [], "title").slice(0, 10);
      if (!complaints.length) {
        stage.textContent = "Şikayetvar kaydı yok.";
        return;
      }
      complaints.forEach(function (it) {
        var text = [it.title, it.excerpt].filter(Boolean).join("\n");
        stage.appendChild(cardMeta("Şikayetvar", it.meta || "", text, it.url));
      });
    }

    function sourceLabels() {
      var b = brands[currentBrand];
      return srcDefs.map(function (s) {
        var n = 0;
        if (s.id === "x") n = ((b.x || {}).items || []).length;
        else if (s.id === "eksi") n = ((b.eksi || {}).entries || []).length;
        else n = ((b.sikayetvar || {}).items || []).length;
        return s.label + " " + n;
      });
    }

    function paintBrand(i) {
      currentBrand = i;
      var labels = sourceLabels();
      currentSrc = 0;
      for (var si = 0; si < srcDefs.length; si++) {
        if (/\s[1-9]\d*$/.test(labels[si] || "")) {
          currentSrc = si;
          break;
        }
      }
      sourceStage.innerHTML = "";
      sourceStage.appendChild(
        tabs(labels, function (si) {
          currentSrc = si;
          paintSource();
        }, currentSrc)
      );
      paintSource();
    }

    root.appendChild(tabs(brands.map(function (b) { return b.brand; }), paintBrand));
    root.appendChild(sourceStage);
    root.appendChild(stage);
    paintBrand(0);
  }

  function rankDeltaHtml(a) {
    if (a.delta === "new") return '<span class="pml-chip pml-chip-new">yeni</span>';
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

  function appNameHtml(a) {
    var name = esc(a.name || a.id || "");
    var icon = String(a.icon || "").trim();
    if (icon) {
      return (
        '<span class="pml-app"><img class="pml-app-icon" src="' +
        esc(icon) +
        '" alt="" width="22" height="22" loading="lazy" decoding="async" referrerpolicy="no-referrer"><span class="pml-app-name">' +
        name +
        "</span></span>"
      );
    }
    var letter = String(a.name || "?").trim().charAt(0).toUpperCase() || "?";
    return (
      '<span class="pml-app"><span class="pml-app-icon pml-app-icon-fallback" aria-hidden="true">' +
      esc(letter) +
      '</span><span class="pml-app-name">' +
      name +
      "</span></span>"
    );
  }

  function paintStoreCol(host, ch, q) {
    host.innerHTML = "";
    if (!ch) {
      host.textContent = "Liste yok.";
      return;
    }
    var mv = ch.moves || {};
    var head = document.createElement("div");
    head.className = "mb-1.5";
    head.innerHTML = "<h4 class=\"text-sm font-bold text-slate-800 dark:text-zinc-100\">" + esc(ch.title || "") + "</h4>";
    var meta = document.createElement("div");
    meta.className = "flex flex-wrap gap-1.5 mb-2";
    meta.innerHTML = mv.reset
      ? chip(ch.our_label || "") + chip("Δ sonraki taramada")
      : chip(ch.our_label || "") +
        chip("↑ " + (mv.up || 0), "pml-chip-up") +
        chip("↓ " + (mv.down || 0), "pml-chip-down") +
        chip("yeni " + (mv.new || 0), "pml-chip-new") +
        chip("çıktı " + (mv.dropped || 0), "pml-chip-down");
    host.appendChild(head);
    host.appendChild(meta);
    if ((ch.dropped || []).length) {
      var drop = document.createElement("p");
      drop.className = "pml-note";
      drop.textContent =
        "Çıkanlar: " +
        ch.dropped
          .slice(0, 8)
          .map(function (d) {
            return (d.name || d.id) + " (eski #" + d.prev_rank + ")";
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
    host.appendChild(sortableTable(["Sıra", "Δ", "Uygulama"], rows, { wide: false }));
  }

  function renderStore(root, data) {
    var charts = data.charts || [];
    if (!charts.length) {
      root.textContent = "Liste yok.";
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
    search.placeholder = "Uygulama ara…";
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
    var avgs = data.source_averages || data.source_counts || [];
    var head = document.createElement("div");
    head.className = "mb-3";
    head.innerHTML = chip((data.article_total || 0) + " haber") + " " + chip((data.runs || []).length + " tarama");
    var bars = document.createElement("div");
    bars.className = "mb-4";
    var max = 1;
    avgs.slice(0, 15).forEach(function (s) {
      max = Math.max(max, s.count || 0, s.avg || 0);
    });
    avgs.slice(0, 15).forEach(function (s) {
      var row = document.createElement("div");
      row.className = "pml-bar";
      var w = Math.round((100 * (s.count || 0)) / max);
      row.innerHTML =
        '<span class="pml-bar-label">' +
        esc(s.source) +
        "</span><div class=\"pml-bar-track\"><div class=\"pml-bar-fill\" style=\"width:" +
        w +
        '%"></div></div>' +
        chip((s.count || 0) + " · ort " + (s.avg != null ? s.avg : s.count), "");
      bars.appendChild(row);
    });
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
      meta.innerHTML = chip("doviz.com sıra: " + (our || "yok"), "pml-chip-doviz");
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
      stage.appendChild(sortableTable(["#", "Başlık", "Kaynak", "Zaman"], rows));
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
    sikayet: renderSikayet,
    store_charts: renderStore,
    google_news: renderNews,
  };

  renderStatus();
  document.querySelectorAll(".pml-body[data-pml]").forEach(function (node) {
    var id = node.getAttribute("data-pml");
    var fn = renderers[id];
    var data = sections[id] || {};
    if (!fn) {
      node.textContent = data.message || "Bu blok henüz yok.";
      return;
    }
    try {
      fn(node, data);
    } catch (err) {
      node.textContent = "Çizim hatası: " + err;
    }
  });
})();
