/**
 * d-lab kartları — /d-lab sayfası ve android/ios sekmelerindeki
 * «android-lab» / «ios-lab» bölümleri aynı kodu paylaşır.
 *
 * Kullanım:
 *   var lab = DLab.mount({
 *     host: document.getElementById("xg-body"),
 *     meta: document.getElementById("xg-meta"),   // opsiyonel durum satırı
 *     profile: "android",                          // yoksa tüm yüzeyler
 *     days: 7,
 *     toolbar: document.querySelector("[data-lab-toolbar]"),
 *     autoload: true                               // false → lab.load() beklenir
 *   });
 *
 * profile verildiğinde istek sunucuya `profile=` ile gider; kart başına
 * yüzey filtresi de gizlenir (tek yüzey zaten sabit).
 */
(function (global) {
  "use strict";

  function esc(s) {
    return String(s == null ? "" : s).replace(/[&<>"]/g, function (c) {
      return { "&": "&amp;", "<": "&lt;", ">": "&gt;", '"': "&quot;" }[c];
    });
  }
  function num(n) { return (Number(n) || 0).toLocaleString("tr-TR"); }
  function dur(sec) {
    var s = Number(sec) || 0;
    if (s < 60) return s.toFixed(0) + " sn";
    var m = Math.floor(s / 60);
    if (m < 60) return m + " dk " + Math.round(s % 60) + " sn";
    return Math.floor(m / 60) + " sa " + (m % 60) + " dk";
  }
  function empty(msg) { return '<p class="xg-muted">' + esc(msg || "Veri yok.") + "</p>"; }

  function table(headers, rows) {
    if (!rows.length) return empty();
    return '<div class="xg-scroll"><table class="xg-table"><thead><tr>' +
      headers.map(function (h) {
        return '<th class="' + (h.num ? "num" : "") + '">' + esc(h.label) + "</th>";
      }).join("") +
      "</tr></thead><tbody>" +
      rows.map(function (r) {
        return "<tr>" + r.map(function (c, i) {
          var h = headers[i] || {};
          return '<td class="' + (h.num ? "num" : "") + '" data-label="' + esc(h.label || "") + '">' +
            c + "</td>";
        }).join("") + "</tr>";
      }).join("") + "</tbody></table></div>";
  }

  function barRows(items, labelKey, valueKey) {
    var max = items.reduce(function (m, r) { return Math.max(m, Number(r[valueKey]) || 0); }, 0) || 1;
    return items.map(function (r) {
      var v = Number(r[valueKey]) || 0;
      return [
        '<span class="xg-key" title="' + esc(r[labelKey]) + '">' + esc(r[labelKey]) + "</span>",
        num(v),
        '<div class="xg-bartrack"><div class="xg-bar" style="width:' +
          (v / max * 100).toFixed(1) + '%"></div></div>'
      ];
    });
  }

  function mount(opts) {
    opts = opts || {};
    var host = opts.host;
    if (!host) return null;
    var metaEl = opts.meta || null;
    var fixedProfile = (opts.profile || "").trim() || null;
    var state = { days: Number(opts.days) || 7, loading: false, loaded: false };
    var CARDS = {};
    var seq = 0;

    function note(msg) {
      if (!metaEl) return;
      metaEl.textContent = msg || "";
      metaEl.hidden = !msg;
    }

    // ── İlerleme çubuğu ─────────────────────────────────────────────────────
    // Genişlik sunucudan gelen «tamamlanan / toplam GA4 isteği» sayısına bağlı.
    // Sunucu henüz sayı vermediyse belirsiz (kayan) kip kullanılır; sahte bir
    // yüzde uydurmak, uzun bekleyişte yanlış bilgi vermek olurdu.
    var bar = null;
    (function buildBar() {
      if (!host.parentNode) return;
      bar = document.createElement("div");
      bar.className = "xg-progress";
      bar.hidden = true;
      bar.setAttribute("role", "progressbar");
      bar.setAttribute("aria-valuemin", "0");
      bar.setAttribute("aria-valuemax", "100");
      bar.innerHTML =
        '<div class="xg-progress__track"><div class="xg-progress__bar"></div></div>' +
        '<div class="xg-progress__label">' +
        '<span class="xg-progress__what"></span>' +
        '<span class="xg-progress__count"></span></div>';
      host.parentNode.insertBefore(bar, host);
    })();

    function barShow(what) {
      if (!bar) return;
      bar.hidden = false;
      bar.classList.add("xg-progress--indeterminate");
      bar.removeAttribute("aria-valuenow");
      bar.querySelector(".xg-progress__bar").style.width = "";
      bar.querySelector(".xg-progress__what").textContent = what || "GA4'e bağlanılıyor…";
      bar.querySelector(".xg-progress__count").textContent = "";
    }

    function barPaint(p) {
      if (!bar || bar.hidden) return;
      if (!p || !p.known || !p.total) return;      // sayı yoksa kayan kipte kal
      bar.classList.remove("xg-progress--indeterminate");
      var pct = Math.max(0, Math.min(100, Number(p.percent) || 0));
      bar.setAttribute("aria-valuenow", String(pct));
      bar.querySelector(".xg-progress__bar").style.width = pct + "%";
      bar.querySelector(".xg-progress__what").textContent =
        p.label ? "Alınıyor: " + p.label : "GA4 istekleri…";
      bar.querySelector(".xg-progress__count").textContent =
        p.done + " / " + p.total + " · %" + pct;
    }

    function barDone(okText) {
      if (!bar) return;
      bar.classList.remove("xg-progress--indeterminate");
      bar.querySelector(".xg-progress__bar").style.width = "100%";
      bar.setAttribute("aria-valuenow", "100");
      if (okText) bar.querySelector(".xg-progress__what").textContent = okText;
      // Dolu çubuk bir an görünsün, sonra kaybolsun
      window.setTimeout(function () { if (bar) bar.hidden = true; }, 420);
    }

    function newToken() {
      return "d" + Date.now().toString(36) + Math.random().toString(36).slice(2, 8);
    }

    /**
     * Container iskeleti. Filtre yalnızca GERÇEKTEN verisi olan platformları
     * listeler — boş ya da tanımsız yüzey seçenek olarak sunulmaz. Sabit
     * profilde (android-lab/ios-lab) filtre hiç çizilmez.
     */
    function card(o) {
      var id = "c" + (++seq);
      CARDS[id] = { profiles: o.profiles || [], selected: "hepsi", body: o.body };
      var pf = o.profiles || [];
      var right = "";
      if (!fixedProfile && pf.length > 1) {
        right = '<div class="xg-cardfilter" data-card="' + id + '">' +
          ["hepsi"].concat(pf).map(function (p, i) {
            return '<button type="button" class="xg-chip' + (i === 0 ? " is-active" : "") +
              '" data-p="' + esc(p) + '">' + esc(p === "hepsi" ? "Hepsi" : p) + "</button>";
          }).join("") + "</div>";
      } else if (!fixedProfile && pf.length === 1) {
        right = '<span class="xg-onlypf">' + esc(pf[0]) + "</span>";
      }
      return '<section class="xg-card" data-cardid="' + id + '">' +
        '<div class="xg-cardhead"><div><h3>' + esc(o.title) + "</h3>" +
          (o.sub ? '<p class="xg-sub">' + esc(o.sub) + "</p>" : "") + "</div>" + right + "</div>" +
        '<div class="xg-cardbody" data-body="' + id + '"></div></section>';
    }

    function paintCard(id) {
      var c = CARDS[id];
      var body = host.querySelector('[data-body="' + id + '"]');
      if (!c || !body) return;
      body.innerHTML = c.body(c.selected);
    }
    function paintAll() { Object.keys(CARDS).forEach(paintCard); }

    /** Profil profil bölüm — «hepsi» seçiliyse hepsi, değilse yalnızca seçilen. */
    function perProfile(profiles, selected, fn) {
      var list = selected === "hepsi"
        ? profiles
        : profiles.filter(function (p) { return p === selected; });
      if (!list.length) return empty();
      return list.map(function (p) {
        return (list.length > 1 ? '<span class="xg-pf">' + esc(p) + "</span>" : "") + fn(p);
      }).join("");
    }

    // ── Bildirimsel kırılımlar ──────────────────────────────────────────────
    function breakdownCard(bd) {
      var pp = bd.per_profile || {};
      var withData = Object.keys(pp).filter(function (p) {
        return (pp[p].rows || []).length > 0;
      });
      var gaps = Object.keys(pp).filter(function (p) {
        return pp[p].undefined || pp[p].error;
      }).map(function (p) {
        return p + (pp[p].undefined ? ": tanımlı değil" : ": " + String(pp[p].error).slice(0, 60));
      });
      if (!withData.length && !gaps.length) return "";
      return card({
        title: bd.label,
        sub: bd.hint || bd.dimension,
        profiles: withData,
        body: function (sel) {
          var main = withData.length
            ? perProfile(withData, sel, function (p) {
                return table(
                  [{ label: "Değer" },
                   { label: bd.metric === "sessions" ? "Oturum" : "Sayı", num: true },
                   { label: "" }],
                  barRows(pp[p].rows, "value", "metric"));
              })
            : empty("Bu boyut hiçbir yüzeyde veri döndürmedi.");
          // Eksik yüzeyler filtreye girmez ama sessizce de yutulmaz
          return main + (gaps.length
            ? '<p class="xg-muted" style="margin-top:.5rem">Kapsam dışı — ' +
              esc(gaps.join(" · ")) + "</p>"
            : "");
        }
      });
    }

    // ── Kullanıcı ───────────────────────────────────────────────────────────
    function usersCard(b) {
      if (b.ok === false) {
        return card({ title: "Kullanıcı", profiles: [], body: function () {
          return '<p class="xg-err">Alınamadı: ' + esc(b.error) + "</p>"; } });
      }
      var rows = (b.rows || []).filter(function (r) { return r.active1DayUsers != null; });
      if (!rows.length) return "";
      return card({
        title: "Kullanıcı", sub: "DAU / WAU / MAU (dün)",
        profiles: rows.map(function (r) { return r.profile; }),
        body: function (sel) {
          var use = sel === "hepsi" ? rows : rows.filter(function (r) { return r.profile === sel; });
          return table(
            [{ label: "Profil" }, { label: "DAU", num: true },
             { label: "WAU", num: true }, { label: "MAU", num: true }],
            use.map(function (r) {
              return [esc(r.profile), num(r.active1DayUsers),
                      num(r.active7DayUsers), num(r.active28DayUsers)];
            }));
        }
      });
    }

    // ── İçerik derinliği ────────────────────────────────────────────────────
    var PAGE_MAX = 50;
    function depthCard(b) {
      if (b.ok === false) {
        return card({ title: "İçerik derinliği", profiles: [], body: function () {
          return '<p class="xg-err">Alınamadı: ' + esc(b.error) + "</p>"; } });
      }
      var rows = b.rows || [];
      if (!rows.length) return "";
      var profiles = rows.map(function (r) { return r.profile; }).filter(function (p, i, a) {
        return a.indexOf(p) === i;
      });
      return card({
        title: "İçerik derinliği",
        sub: "Görüntüleme başına gerçek okuma süresi ve yeni kullanıcı",
        profiles: profiles,
        body: function (sel) {
          var use = sel === "hepsi" ? rows : rows.filter(function (r) { return r.profile === sel; });
          if (!use.length) return empty();
          // Yol 50 karakteri aşarsa artan kısım hemen altındaki satıra taşınır
          var tb = use.map(function (r) {
            var full = String(r.page || "");
            var head = full.length > PAGE_MAX ? full.slice(0, PAGE_MAX) : full;
            var tail = full.length > PAGE_MAX ? full.slice(PAGE_MAX) : "";
            var main = '<tr><td class="xg-page-cell" data-label="Sayfa" title="' + esc(full) + '">' +
              esc(head) + "</td>" +
              '<td data-label="Profil">' + esc(r.profile) + "</td>" +
              '<td class="num" data-label="Görüntüleme">' + num(r.views) + "</td>" +
              '<td class="num" data-label="Süre">' + dur(r.seconds_per_view) + "</td>" +
              '<td class="num" data-label="Yeni">' + num(r.new_users) + "</td></tr>";
            return tail ? main +
              '<tr class="xg-overflow"><td colspan="5" class="xg-page-tail" data-label="" title="' +
              esc(full) + '">' + esc(tail) + "</td></tr>" : main;
          }).join("");
          return '<div class="xg-scroll"><table class="xg-table xg-table--depth"><thead><tr>' +
            '<th>Sayfa</th><th>Profil</th><th class="num">Görüntüleme</th>' +
            '<th class="num">Süre</th><th class="num">Yeni</th></tr></thead><tbody>' +
            tb + "</tbody></table></div>";
        }
      });
    }

    // ── Saatlik ritim ───────────────────────────────────────────────────────
    function hourlyCard(b) {
      if (b.ok === false) {
        return card({ title: "Saatlik ritim", profiles: [], body: function () {
          return '<p class="xg-err">Alınamadı: ' + esc(b.error) + "</p>"; } });
      }
      var series = b.series || {};
      var profiles = Object.keys(series).filter(function (p) {
        return ((series[p] || {}).hours || []).length > 0;
      });
      if (!profiles.length) return "";
      return card({
        title: "Saatlik ritim", sub: "hour × activeUsers — yayın saati kararı için",
        profiles: profiles,
        body: function (sel) {
          return perProfile(profiles, sel, function (p) {
            var s = series[p], hours = s.hours || [];
            var max = hours.reduce(function (m, h) { return Math.max(m, h.users); }, 0) || 1;
            var bars = hours.map(function (h) {
              return '<div style="flex:1;display:flex;flex-direction:column;justify-content:flex-end;' +
                'align-items:center;gap:2px" title="' + h.hour + ":00 · " + num(h.users) +
                ' kullanıcı">' +
                '<div style="width:100%;background:#4f46e5;border-radius:2px;height:' +
                Math.max(2, h.users / max * 46).toFixed(0) + 'px"></div>' +
                '<span style="font-size:8px;opacity:.55">' + h.hour + "</span></div>";
            }).join("");
            return '<div style="display:flex;align-items:flex-end;gap:2px;height:62px">' + bars +
              "</div>" +
              (s.other_users ? '<p class="xg-muted" style="margin-top:.3rem">(other) kovası: ' +
                num(s.other_users) + " kullanıcı</p>" : "");
          });
        }
      });
    }

    // ── Kitle: her liste kendi container'ında ───────────────────────────────
    function audienceCards(b) {
      if (b.ok === false) {
        return card({ title: "Kitle", profiles: [], body: function () {
          return '<p class="xg-err">Alınamadı: ' + esc(b.error) + "</p>"; } });
      }
      var defs = [
        ["İlgi alanları", "brandingInterest", b.interests],
        ["Yaş / cinsiyet", "userAgeBracket × userGender", b.demographics],
        ["Tanımlı kitleler", "audienceName", b.audiences]
      ];
      return defs.map(function (d) {
        var items = d[2] || [];
        if (!items.length) return "";
        return card({
          title: d[0], sub: d[1], profiles: [],
          body: function () {
            return table([{ label: "" }, { label: "Kullanıcı", num: true }, { label: "" }],
                         barRows(items, "label", "users"));
          }
        });
      }).join("");
    }

    function render(data) {
      CARDS = {}; seq = 0;
      if (!data || data.ok === false) {
        note("Alınamadı: " + ((data && data.error) || "bilinmeyen hata"));
        host.innerHTML = "";
        return;
      }
      note("");
      var b = data.blocks || {};
      var html = [
        usersCard(b.user_stability || {}),
        depthCard(b.content_depth || {}),
        hourlyCard(b.hourly || {})
      ].concat((data.breakdowns || []).map(breakdownCard))
       .concat([audienceCards(b.audience || {})])
       .join("");
      host.innerHTML = html || empty("Bu yüzey için veri dönmedi.");
      paintAll();
    }

    var pollTimer = null;
    function stopPoll() {
      if (pollTimer) { window.clearInterval(pollTimer); pollTimer = null; }
    }

    function load(force) {
      if (state.loading) return;
      state.loading = true;
      var token = newToken();
      barShow();
      note("");
      var url = "/api/x-ga4/report?days=" + state.days +
        (fixedProfile ? "&profile=" + encodeURIComponent(fixedProfile) : "") +
        (force ? "&force=true" : "") +
        "&progress=" + encodeURIComponent(token);

      stopPoll();
      pollTimer = window.setInterval(function () {
        fetch("/api/x-ga4/progress?token=" + encodeURIComponent(token),
              { headers: { Accept: "application/json" } })
          .then(function (r) { return r.ok ? r.json() : null; })
          .then(function (p) { barPaint(p); })
          .catch(function () { /* yoklama hatası yüklemeyi bozmasın */ });
      }, 700);

      fetch(url, { headers: { Accept: "application/json" } })
        .then(function (r) { return r.ok ? r.json() : null; })
        .then(function (d) {
          state.loaded = true;
          stopPoll();
          barDone(d && d.cached ? "Önbellekten" : "Tamam");
          render(d);
        })
        .catch(function () {
          stopPoll();
          barDone("");
          note("İstek başarısız.");
        })
        .then(function () { state.loading = false; });
    }

    // Container filtreleri: tek delege dinleyici, kart yeniden çizilir
    host.addEventListener("click", function (ev) {
      var btn = ev.target && ev.target.closest && ev.target.closest("[data-p]");
      if (!btn) return;
      var wrap = btn.closest("[data-card]");
      if (!wrap) return;
      var id = wrap.getAttribute("data-card");
      var c = CARDS[id];
      var p = btn.getAttribute("data-p");
      if (!c || c.selected === p) return;
      c.selected = p;
      wrap.querySelectorAll("[data-p]").forEach(function (x) {
        x.classList.toggle("is-active", x === btn);
      });
      paintCard(id);
    });

    // Gün seçici + yenile
    var scope = opts.toolbar || document;
    scope.querySelectorAll("[data-xg-days]").forEach(function (btn) {
      btn.addEventListener("click", function () {
        var d = Number(btn.getAttribute("data-xg-days")) || 7;
        if (d === state.days) return;
        state.days = d;
        scope.querySelectorAll("[data-xg-days]").forEach(function (x) {
          x.classList.toggle("is-active", x === btn);
        });
        load(false);
      });
    });
    var rb = opts.refresh || null;
    if (rb) rb.addEventListener("click", function () { load(true); });

    if (opts.autoload !== false) load(false);

    return {
      load: load,
      /** Bölüm ilk açıldığında çek — kapalı dropdown boşuna kota harcamasın. */
      loadOnce: function () { if (!state.loaded && !state.loading) load(false); },
      isLoaded: function () { return state.loaded; }
    };
  }

  global.DLab = { mount: mount };
})(window);
