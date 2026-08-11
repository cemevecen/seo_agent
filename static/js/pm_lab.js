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
      b.className = "pml-tab" + (i === 0 ? " is-on" : "");
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
    function paint(idx) {
      stage.innerHTML = "";
      var kw = kws[idx];
      var meta = document.createElement("div");
      meta.className = "flex flex-wrap gap-1.5 mb-2";
      var mv = kw.moves || {};
      meta.innerHTML =
        chip("bizim sıra: " + (kw.our_rank || "yok")) +
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
        return tr(
          [
            { text: r.rank, sort: r.rank },
            { text: r.page, sort: r.page },
            { html: '<a class="hover:underline" href="' + esc(r.url) + '" target="_blank" rel="noopener">' + esc(r.domain) + "</a>" },
            { text: r.title },
            { text: r.snippet },
          ],
          (r.ours ? "pml-ours " : "") + heat
        );
      });
      stage.appendChild(meta);
      stage.appendChild(
        sortableTable(["Sıra", "Sayfa", "Domain", "Başlık / meta", "Snippet"], rows)
      );
    }
    root.appendChild(head);
    root.appendChild(spark);
    root.appendChild(tabs(kws.map(function (k) { return k.keyword; }), paint));
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

  function renderSikayet(root, data) {
    var brands = data.brands || [];
    if (!brands.length && data.sikayetvar) {
      brands = [{ brand: "doviz.com", sikayetvar: data.sikayetvar, eksi: data.eksi }];
    }
    if (!brands.length) {
      root.textContent = "Kayıt yok.";
      return;
    }
    var stage = document.createElement("div");
    function paint(i) {
      stage.innerHTML = "";
      var b = brands[i];
      var sv = b.sikayetvar || {};
      var ek = b.eksi || {};
      var items = sv.items || [];
      var entries = ek.entries || [];
      var h = document.createElement("div");
      h.className = "flex flex-wrap gap-1.5 mb-2";
      h.innerHTML = chip("şikayetvar " + items.length) + chip("ekşi " + entries.length);
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
        var text = typeof en === "string" ? en : en.text || "";
        var url = typeof en === "string" ? ek.url : en.url || ek.url;
        var card = document.createElement("article");
        card.className = "rounded-xl ring-1 ring-slate-200 p-3 mb-2 dark:ring-zinc-800";
        card.innerHTML =
          '<p class="text-[10px] uppercase tracking-wide text-slate-400">Ekşi</p>' +
          '<p class="text-xs whitespace-pre-wrap">' + esc(text) + "</p>" +
          (url ? '<p class="mt-1 text-[11px]"><a class="hover:underline" href="' + esc(url) + '" target="_blank" rel="noopener">' + esc(url) + "</a></p>" : "");
        stage.appendChild(card);
      });
    }
    root.appendChild(tabs(brands.map(function (b) { return b.brand; }), paint));
    root.appendChild(stage);
    paint(0);
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
      var p = document.createElement("p");
      p.className = "text-[11px] text-slate-500 mb-2";
      p.textContent = ch.our_label || ch.title || "";
      var q = search.value.trim().toLowerCase();
      var rows = (ch.apps || [])
        .filter(function (a) {
          if (!q) return true;
          return String(a.name || "").toLowerCase().indexOf(q) >= 0 || String(a.id || "").toLowerCase().indexOf(q) >= 0;
        })
        .map(function (a) {
          return tr(
            [
              { text: a.rank, sort: a.rank },
              { text: a.name },
            ],
            a.is_ours ? "pml-ours pml-heat-new" : ""
          );
        });
      stage.appendChild(p);
      stage.appendChild(sortableTable(["Sıra", "Uygulama"], rows));
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
    avgs.slice(0, 12).forEach(function (s) {
      max = Math.max(max, s.count || 0, s.avg || 0);
    });
    avgs.slice(0, 12).forEach(function (s) {
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
