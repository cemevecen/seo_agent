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
    var box = document.getElementById("pml-status");
    if (!box) return;
    box.innerHTML =
      chip("son " + fmtWhen(boot.scraped_at), boot.sync_ok ? "pml-chip-ok" : "") +
      chip("sonraki ~" + fmtWhen(boot.next_at), "") +
      chip((boot.interval_hours || 3) + " saatte bir", "");
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
    table.className = "pml-table";
    var thead = document.createElement("thead");
    var hr = document.createElement("tr");
    headers.forEach(function (h, i) {
      var th = document.createElement("th");
      th.textContent = h;
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

  function tabs(labels, onPick) {
    var bar = document.createElement("div");
    bar.className = "pml-tabs";
    labels.forEach(function (lab, i) {
      var b = document.createElement("button");
      b.type = "button";
      b.className = "pml-tab" + (i === 0 ? " is-on" : "") + (lab === "Toplam" ? " pml-tab-total" : "");
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
        chip("bizim sıra: " + (kw.our_rank || "yok"), "pml-chip-doviz") +
        chip("+" + (mv.entered || 0) + " girdi", "pml-chip-new") +
        chip((mv.dropped || 0) + " çıktı", "pml-chip-down");
      var drop = (kw.dropped || [])
        .map(function (d) {
          return esc(d.domain) + " (eski #" + d.prev_rank + ")";
        })
        .join(" · ");
      if (drop) {
        var p = document.createElement("p");
        p.className = "text-[11px] text-slate-500 mb-2";
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
            { html: '<a class="hover:underline" href="' + esc(r.url) + '" target="_blank" rel="noopener">' + esc(r.domain) + "</a>" },
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
      note.className = "text-[11px] text-slate-500 mb-2";
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

  function renderCompetitors(root, data) {
    var cols = data.columns || [];
    var matrix = data.matrix || [];
    if (!matrix.length) {
      root.textContent = "Fiyat matrisi henüz yok.";
      return;
    }
    var headers = ["Varlık"].concat(cols.map(function (c) { return c.label; }));
    var rows = matrix.map(function (r) {
      var cells = [{ text: r.label, cls: "pin" }];
      cols.forEach(function (c) {
        var cell = (r.cells || {})[c.id] || {};
        var v = cell.value || "";
        cells.push({
          html: v
            ? esc(v) + (cell.change ? '<div class="text-[10px] text-slate-400">' + esc(cell.change) + "</div>" : "")
            : '<span class="pml-miss">—</span>',
          text: v,
          sort: v.replace(/[^\d,.-]/g, "").replace(",", "."),
        });
      });
      return tr(cells);
    });
    var legend = document.createElement("p");
    legend.className = "text-[11px] text-slate-500 mb-2";
    legend.textContent = "Satır = varlık, sütun = site. Boş hücre o sitede yok / okunamadı.";
    root.appendChild(legend);
    root.appendChild(sortableTable(headers, rows));
  }

  function parseTrDate(s) {
    var m = String(s || "").match(/(\d{1,2})\.(\d{1,2})\.(\d{4})(?:\s+(\d{1,2}):(\d{2}))?/);
    if (!m) return 0;
    return Date.UTC(+m[3], +m[2] - 1, +m[1], +(m[4] || 0), +(m[5] || 0));
  }

  function renderSikayet(root, data) {
    var brands = (data.brands || []).slice();
    if (!brands.length && data.sikayetvar) {
      brands = [{ brand: "doviz.com", sikayetvar: data.sikayetvar, eksi: data.eksi }];
    }
    if (!brands.length) {
      root.textContent = "Kayıt yok.";
      return;
    }
    brands.sort(function (a, b) {
      var da = a.brand === "x.com" ? 0 : 1;
      var db = b.brand === "x.com" ? 0 : 1;
      return da - db;
    });
    var stage = document.createElement("div");
    function paint(i) {
      stage.innerHTML = "";
      var b = brands[i];
      var sv = b.sikayetvar || {};
      var ek = b.eksi || {};
      var items = (sv.items || []).slice(0, 10);
      var entries = (ek.entries || []).slice();
      var seen = {};
      entries = entries
        .map(function (en) {
          if (typeof en === "string") return { text: en, url: ek.url, date: "", author: "" };
          return en;
        })
        .filter(function (en) {
          var key = String(en.text || "").replace(/\s+/g, " ").slice(0, 80);
          if (seen[key]) return false;
          seen[key] = 1;
          return String(en.text || "").length > 12;
        })
        .sort(function (a, b) {
          return parseTrDate(b.date) - parseTrDate(a.date);
        })
        .slice(0, 10);
      var h = document.createElement("div");
      h.className = "flex flex-wrap gap-1.5 mb-2";
      h.innerHTML = chip("şikayetvar " + items.length) + chip("ekşi son " + entries.length);
      stage.appendChild(h);
      items.forEach(function (it) {
        var card = document.createElement("article");
        card.className = "rounded-xl bg-slate-50 p-3 mb-2 dark:bg-zinc-950/40";
        card.innerHTML =
          "<p class=\"text-sm font-semibold\">" +
          (it.url ? '<a class="hover:underline" href="' + esc(it.url) + '" target="_blank" rel="noopener">' + esc(it.title || "") + "</a>" : esc(it.title || "")) +
          "</p>" +
          '<p class="text-[11px] text-slate-500">' + esc(it.meta || sv.url || "") + "</p>" +
          '<p class="mt-1 text-xs text-slate-600 dark:text-zinc-400">' + esc(it.excerpt || "") + "</p>";
        stage.appendChild(card);
      });
      entries.forEach(function (en) {
        var text = en.text || "";
        var url = en.url || ek.url;
        var meta = [en.author, en.date].filter(Boolean).join(" · ");
        var card = document.createElement("article");
        card.className = "rounded-xl ring-1 ring-slate-200 p-3 mb-2 dark:ring-zinc-800";
        card.innerHTML =
          '<p class="text-[10px] uppercase tracking-wide text-slate-400">Ekşi' +
          (meta ? " · " + esc(meta) : "") +
          "</p>" +
          '<p class="text-xs whitespace-pre-wrap">' +
          esc(text) +
          "</p>" +
          (url ? '<p class="mt-1 text-[11px]"><a class="hover:underline" href="' + esc(url) + '" target="_blank" rel="noopener">' + esc(url) + "</a></p>" : "");
        stage.appendChild(card);
      });
    }
    root.appendChild(tabs(brands.map(function (b) { return b.brand; }), paint));
    root.appendChild(stage);
    paint(0);
  }

  function rankDeltaHtml(a) {
    if (a.delta === "new") return '<span class="pml-chip pml-chip-new">yeni</span>';
    if (a.delta === "up") return '<span class="pml-delta-up">↑ ' + (a.delta_n || 0) + "</span>";
    if (a.delta === "down") return '<span class="pml-delta-down">↓ ' + Math.abs(a.delta_n || 0) + "</span>";
    if (a.delta === "same") return '<span class="pml-miss">—</span>';
    return "";
  }

  function renderStore(root, data) {
    var charts = data.charts || [];
    if (!charts.length) {
      root.textContent = "Liste yok.";
      return;
    }
    var tools = document.createElement("div");
    tools.className = "flex flex-wrap items-center gap-2 mb-2";
    var search = document.createElement("input");
    search.className = "pml-search";
    search.placeholder = "Uygulama ara…";
    tools.appendChild(search);
    var stage = document.createElement("div");
    var current = 0;
    function paint(i) {
      current = i;
      stage.innerHTML = "";
      var ch = charts[i];
      var mv = ch.moves || {};
      var meta = document.createElement("div");
      meta.className = "flex flex-wrap gap-1.5 mb-2";
      meta.innerHTML =
        chip(ch.our_label || ch.title || "") +
        chip("↑ " + (mv.up || 0), "pml-chip-up") +
        chip("↓ " + (mv.down || 0), "pml-chip-down") +
        chip("yeni " + (mv.new || 0), "pml-chip-new") +
        chip("çıktı " + (mv.dropped || 0), "pml-chip-down");
      var q = search.value.trim().toLowerCase();
      var rows = (ch.apps || [])
        .filter(function (a) {
          if (!q) return true;
          return String(a.name || "").toLowerCase().indexOf(q) >= 0 || String(a.id || "").toLowerCase().indexOf(q) >= 0;
        })
        .map(function (a) {
          var heat = a.delta === "up" ? "pml-heat-up" : a.delta === "down" ? "pml-heat-down" : a.delta === "new" ? "pml-heat-new" : "";
          return tr(
            [
              { text: a.rank, sort: a.rank },
              { html: rankDeltaHtml(a), sort: a.delta_n == null ? 0 : a.delta_n },
              { text: a.name },
            ],
            (a.is_ours ? "pml-ours pml-doviz " : "") + heat
          );
        });
      stage.appendChild(meta);
      if ((ch.dropped || []).length) {
        var drop = document.createElement("p");
        drop.className = "text-[11px] text-slate-500 mb-2";
        drop.textContent =
          "Çıkanlar: " +
          ch.dropped
            .slice(0, 12)
            .map(function (d) {
              return (d.name || d.id) + " (eski #" + d.prev_rank + ")";
            })
            .join(" · ");
        stage.appendChild(drop);
      }
      stage.appendChild(sortableTable(["Sıra", "Δ", "Uygulama"], rows));
    }
    search.addEventListener("input", function () {
      paint(current);
    });
    root.appendChild(tabs(charts.map(function (c) { return c.title; }), paint));
    root.appendChild(tools);
    root.appendChild(stage);
    paint(0);
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
        '<span class="w-28 truncate text-[11px]">' +
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
    function paint(i) {
      stage.innerHTML = "";
      var kw = kws[i];
      var rows = (kw.articles || []).map(function (a, n) {
        return tr([
          { text: n + 1, sort: n + 1 },
          { html: '<a class="hover:underline" href="' + esc(a.url) + '" target="_blank" rel="noopener">' + esc(a.title) + "</a>" },
          { text: a.source },
          { text: a.time },
        ]);
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
