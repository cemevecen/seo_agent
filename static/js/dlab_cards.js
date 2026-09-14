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
    var siteId = Math.max(1, Number(opts.siteId) || 1);
    var bindControls = opts.bindControls !== false;
    var state = { days: Number(opts.days) || 7, loading: false, loaded: false };
    var CARDS = {};
    var seq = 0;

    function note(msg) {
      if (!metaEl) return;
      metaEl.textContent = msg || "";
      metaEl.hidden = !msg;
    }

    function formatFetchedAt(iso) {
      if (!iso) return "";
      var d = new Date(iso);
      if (isNaN(d.getTime())) return "";
      try {
        return d.toLocaleString("tr-TR", {
          timeZone: "Europe/Istanbul",
          day: "2-digit",
          month: "short",
          year: "numeric",
          hour: "2-digit",
          minute: "2-digit"
        });
      } catch (_) {
        return d.toLocaleString("tr-TR");
      }
    }

    function statusLine(data) {
      if (!data || !data.ok) return data && data.error ? String(data.error) : "";
      var parts = [];
      var when = formatFetchedAt(data.fetched_at);
      if (when) parts.push("Son çekim: " + when);
      if (data.stale) parts.push("arka planda yenileniyor");
      else if (data.cached) parts.push("önbellek");
      if (data.requests) parts.push(data.requests + " GA4 isteği");
      return parts.join(" · ");
    }

    // ── İlerleme kabuğu ─────────────────────────────────────────────────────
    // Gerçek tamamlanan/toplam sayısına bağlı. Önbellek yanıtı hızlıysa çubuk
    // hiç açılmaz (180ms gecikme). Skeleton + halka + şerit birlikte çalışır.
    var bar = null;
    var barShowTimer = null;
    (function buildBar() {
      if (!host.parentNode) return;
      bar = document.createElement("div");
      bar.className = "xg-progress";
      bar.hidden = true;
      bar.setAttribute("role", "progressbar");
      bar.setAttribute("aria-valuemin", "0");
      bar.setAttribute("aria-valuemax", "100");
      bar.innerHTML =
        '<div class="xg-progress__shell">' +
          '<div class="xg-progress__ring" aria-hidden="true">' +
            '<svg viewBox="0 0 48 48">' +
              '<circle class="xg-progress__ring-bg" cx="24" cy="24" r="18"></circle>' +
              '<circle class="xg-progress__ring-fg" cx="24" cy="24" r="18"></circle>' +
            "</svg>" +
            '<span class="xg-progress__pct">0</span>' +
          "</div>" +
          '<div class="xg-progress__copy">' +
            '<div class="xg-progress__title">GA4 verileri hazırlanıyor</div>' +
            '<div class="xg-progress__what">Bağlanılıyor…</div>' +
            '<div class="xg-progress__track"><div class="xg-progress__bar"></div></div>' +
            '<div class="xg-progress__count"></div>' +
          "</div>" +
        "</div>" +
        '<div class="xg-skeleton" aria-hidden="true">' +
          '<div class="xg-skeleton__card"></div>' +
          '<div class="xg-skeleton__card"></div>' +
          '<div class="xg-skeleton__card"></div>' +
        "</div>";
      host.parentNode.insertBefore(bar, host);
    })();

    var RING_LEN = 2 * Math.PI * 18;

    function barSetPct(pct) {
      if (!bar) return;
      var p = Math.max(0, Math.min(100, Number(pct) || 0));
      var fg = bar.querySelector(".xg-progress__ring-fg");
      var pctEl = bar.querySelector(".xg-progress__pct");
      var fill = bar.querySelector(".xg-progress__bar");
      if (fg) {
        fg.style.strokeDasharray = String(RING_LEN);
        fg.style.strokeDashoffset = String(RING_LEN * (1 - p / 100));
      }
      if (pctEl) pctEl.textContent = String(Math.round(p));
      if (fill) fill.style.width = p + "%";
      bar.setAttribute("aria-valuenow", String(Math.round(p)));
    }

    function barShow(what) {
      if (!bar) return;
      if (barShowTimer) window.clearTimeout(barShowTimer);
      barShowTimer = window.setTimeout(function () {
        barShowTimer = null;
        if (!state.loading) return;
        bar.hidden = false;
        host.classList.add("is-loading");
        bar.classList.add("xg-progress--indeterminate", "is-visible");
        bar.removeAttribute("aria-valuenow");
        barSetPct(8);
        var title = bar.querySelector(".xg-progress__title");
        var whatEl = bar.querySelector(".xg-progress__what");
        var countEl = bar.querySelector(".xg-progress__count");
        if (title) title.textContent = "GA4 verileri hazırlanıyor";
        if (whatEl) whatEl.textContent = what || "Bağlanılıyor…";
        if (countEl) countEl.textContent = "";
      }, 180);
    }

    function barPaint(p) {
      if (!bar) return;
      if (!p || !p.known || !p.total) return;
      if (bar.hidden && state.loading) {
        if (barShowTimer) {
          window.clearTimeout(barShowTimer);
          barShowTimer = null;
        }
        bar.hidden = false;
        host.classList.add("is-loading");
        bar.classList.add("is-visible");
      }
      if (bar.hidden) return;
      bar.classList.remove("xg-progress--indeterminate");
      var pct = Math.max(0, Math.min(100, Number(p.percent) || 0));
      barSetPct(pct);
      var whatEl = bar.querySelector(".xg-progress__what");
      var countEl = bar.querySelector(".xg-progress__count");
      if (whatEl) whatEl.textContent = p.label ? ("Alınıyor: " + p.label) : "GA4 istekleri…";
      if (countEl) countEl.textContent = p.done + " / " + p.total;
    }

    function barDone(okText, instant) {
      if (barShowTimer) {
        window.clearTimeout(barShowTimer);
        barShowTimer = null;
      }
      if (!bar) return;
      host.classList.remove("is-loading");
      if (bar.hidden) return;
      bar.classList.remove("xg-progress--indeterminate");
      barSetPct(100);
      var whatEl = bar.querySelector(".xg-progress__what");
      if (okText && whatEl) whatEl.textContent = okText;
      var delay = instant ? 0 : 380;
      window.setTimeout(function () {
        if (!bar) return;
        bar.classList.remove("is-visible");
        bar.hidden = true;
      }, delay);
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
      CARDS[id] = { body: o.body };
      return '<section class="xg-card" data-cardid="' + id + '">' +
        '<div class="xg-cardhead"><div><h3>' + esc(o.title) + "</h3>" +
          (o.sub ? '<p class="xg-sub">' + esc(o.sub) + "</p>" : "") + "</div></div>" +
        '<div class="xg-cardbody" data-body="' + id + '"></div></section>';

    }

    function paintCard(id) {
      var c = CARDS[id];
      var body = host.querySelector('[data-body="' + id + '"]');
      if (!c || !body) return;
      body.innerHTML = c.body();
    }
    function paintAll() { Object.keys(CARDS).forEach(paintCard); }

    /** Profil profil bölüm — yüzeyler YAN YANA, hepsi açık.
     *
     * Eskiden «hepsi / web / mweb / …» chip'leriyle tek yüzey gösteriliyordu;
     * karşılaştırma için tıklamak gerekiyordu. Artık dört yüzey aynı anda
     * görünüyor, container tam genişlikte.
     */
    function perProfile(profiles, fn) {
      var list = profiles || [];
      if (!list.length) return empty();
      return '<div class="xg-cols" style="--xg-cols:' + list.length + '">' +
        list.map(function (p) {
          return '<div class="xg-col"><span class="xg-pf">' + esc(p) + "</span>" +
            fn(p) + "</div>";
        }).join("") + "</div>";

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
        body: function () {
          var main = withData.length
            ? perProfile(withData, function (p) {
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
        body: function () {
          var use = rows;
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
        body: function () {
          var use = rows;
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
        body: function () {
          return perProfile(profiles, function (p) {
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

    // ── Etkileşim kalitesi ──────────────────────────────────────────────────
    function pct(v) {
      var n = Number(v);
      return isFinite(n) ? (n * 100).toFixed(1) + "%" : "—";
    }
    function ratio(v) {
      var n = Number(v);
      return isFinite(n) ? n.toFixed(1) : "—";
    }
    function engagementCard(b) {
      if (b.ok === false) {
        return card({ title: "Etkileşim kalitesi", profiles: [], body: function () {
          return '<p class="xg-err">Alınamadı: ' + esc(b.error) + "</p>"; } });
      }
      var rows = b.rows || [];
      if (!rows.length) return "";
      return card({
        title: "Etkileşim kalitesi",
        sub: "Oturum başına derinlik ve kalma — yüzeyler yan yana",
        profiles: rows.map(function (r) { return r.profile; }),
        body: function () {
          var use = rows;
          if (!use.length) return empty();
          return table(
            [{ label: "Profil" }, { label: "Oturum", num: true },
             { label: "Etkileşim", num: true }, { label: "Hemen çıkma", num: true },
             { label: "Ort. süre", num: true }, { label: "Görünt./oturum", num: true },
             { label: "Olay/oturum", num: true }],
            use.map(function (r) {
              return [
                esc(r.profile), num(r.sessions),
                pct(r.engagement_rate), pct(r.bounce_rate),
                dur(r.avg_session_sec),
                ratio(r.views_per_session), ratio(r.events_per_session)
              ];
            }));
        }
      });
    }

    // ── Uygulama yapışkanlığı ───────────────────────────────────────────────
    function stickinessCard(b) {
      if (b.ok === false) {
        return card({ title: "Uygulama yapışkanlığı", profiles: [], body: function () {
          return '<p class="xg-err">Alınamadı: ' + esc(b.error) + "</p>"; } });
      }
      var rows = b.rows || [];
      if (!rows.length) return "";
      return card({
        title: "Uygulama yapışkanlığı",
        sub: "Ne sıklıkla geri dönülüyor (dün) — yalnız Android / iOS",
        body: function () {
          return table(
            [{ label: "Profil" }, { label: "DAU/MAU", num: true }, { label: "DAU/WAU", num: true },
             { label: "WAU/MAU", num: true }, { label: "Oturum/kullanıcı", num: true },
             { label: "Olay/kullanıcı", num: true }, { label: "Etkileşimli oturum", num: true }],
            rows.map(function (r) {
              return [
                esc(r.profile), pct(r.dau_per_mau), pct(r.dau_per_wau), pct(r.wau_per_mau),
                ratio(r.sessions_per_user), ratio(r.events_per_user), num(r.engaged_sessions)
              ];
            }));
        }
      });
    }

    // ── Kitle: her liste kendi container'ında ───────────────────────────────
    function audienceCards(b, emit) {
      if (b.ok === false) {
        emit(card({ title: "Kitle", profiles: [], body: function () {
          return '<p class="xg-err">Alınamadı: ' + esc(b.error) + "</p>"; } }));
        return;
      }
      var defs = [
        ["İlgi alanları", "brandingInterest", b.interests],
        ["Yaş / cinsiyet", "userAgeBracket × userGender", b.demographics],
        ["Tanımlı kitleler", "audienceName", b.audiences]
      ];
      defs.forEach(function (d) {
        var items = d[2] || [];
        if (!items.length) return;
        emit(card({
          title: d[0], sub: d[1], profiles: [],
          body: function () {
            return table([{ label: "" }, { label: "Kullanıcı", num: true }, { label: "" }],
                         barRows(items, "label", "users"));
          }
        }));
      });
    }

    // Bespoke blokların hangi başlık altında duracağı — kırılımlar bunu
    // sunucudan `group` ile taşıyor, bloklar burada eşleşiyor.
    var BLOCK_GROUP = {
      user_stability: "engagement",
      engagement: "engagement",
      app_stickiness: "app",
      content_depth: "behavior",
      hourly: "behavior",
      audience: "audience"
    };
    var DEFAULT_GROUPS = [
      { key: "engagement", label: "Kullanıcı & etkileşim" },
      { key: "acquisition", label: "Edinim" },
      { key: "behavior", label: "Davranış" },
      { key: "audience", label: "Kitle & cihaz" },
      { key: "app", label: "Uygulama" }
    ];
    // İlk iki grup açık gelir; gerisi kapalı. Veri zaten tek istekte geldiği
    // için kapalı olmak istek tasarrufu değil, yalnızca göz yorgunluğunu azaltır.
    var OPEN_BY_DEFAULT = 2;

    function groupSection(g, cardsHtml, count, index) {
      if (!count) return "";
      return '<details class="xg-group"' + (index < OPEN_BY_DEFAULT ? " open" : "") + '>' +
        "<summary>" +
        '<span class="xg-group__title"><span class="xg-group__caret">▶</span>' +
        esc(g.label) + "</span>" +
        '<span class="xg-group__count">' + count + " kart</span>" +
        "</summary>" +
        '<div class="xg-grid">' + cardsHtml + "</div></details>";
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
      var groups = (data.groups && data.groups.length) ? data.groups : DEFAULT_GROUPS;

      // Kart html'i grubuna göre biriktirilir; kart sayısı üretim sırasında
      // sayılır çünkü veri dönmeyen container hiç çizilmiyor.
      var bucket = {}, counts = {};
      groups.forEach(function (g) { bucket[g.key] = []; counts[g.key] = 0; });

      function put(groupKey, html) {
        if (!html) return;
        var key = bucket[groupKey] ? groupKey : groups[0].key;
        bucket[key].push(html);
        counts[key] += 1;
      }

      put(BLOCK_GROUP.user_stability, usersCard(b.user_stability || {}));
      put(BLOCK_GROUP.engagement, engagementCard(b.engagement || {}));
      put(BLOCK_GROUP.app_stickiness, stickinessCard(b.app_stickiness || {}));
      put(BLOCK_GROUP.content_depth, depthCard(b.content_depth || {}));
      put(BLOCK_GROUP.hourly, hourlyCard(b.hourly || {}));

      (data.breakdowns || []).forEach(function (bd) {
        put(bd.group || "behavior", breakdownCard(bd));
      });

      // Kitle blokları üç ayrı kart üretiyor; her biri kendi container'ı
      audienceCards(b.audience || {}, function (html) {
        put(BLOCK_GROUP.audience, html);
      });

      var html = groups.map(function (g, i) {
        return groupSection(g, bucket[g.key].join(""), counts[g.key], i);
      }).join("");
      host.innerHTML = html || empty("Bu yüzey için veri dönmedi.");
      host.classList.add("xg-grid--reveal");
      paintAll();
      window.setTimeout(function () {
        host.classList.remove("xg-grid--reveal");
      }, 600);
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
        "&site_id=" + encodeURIComponent(String(siteId)) +
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
      }, 450);

      fetch(url, { headers: { Accept: "application/json" } })
        .then(function (r) { return r.ok ? r.json() : null; })
        .then(function (d) {
          state.loaded = true;
          stopPoll();
          var fromCache = !!(d && (d.cached || d.stale));
          barDone(fromCache ? "Önbellekten açıldı" : "Hazır", fromCache);
          note(statusLine(d));
          render(d);
        })
        .catch(function () {
          stopPoll();
          barDone("");
          note("İstek başarısız.");
        })
        .then(function () { state.loading = false; });
    }

    // Gün seçici + yenile — android/ios gömülü bölüm kendi toolbar'ını bağlar.
    // /d-lab site sekmelerinde ortak toolbar üst sayfada yönetilir (bindControls:false).
    if (bindControls) {
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
    }

    if (opts.autoload !== false) load(false);

    return {
      load: load,
      /** Bölüm ilk açıldığında çek — kapalı dropdown boşuna kota harcamasın. */
      loadOnce: function () { if (!state.loaded && !state.loading) load(false); },
      isLoaded: function () { return state.loaded; },
      setDays: function (d, reload) {
        var next = Number(d) || 7;
        if (next === state.days && !reload) return;
        state.days = next;
        if (reload) load(false);
      },
      siteId: siteId
    };
  }

  global.DLab = { mount: mount };
})(window);
