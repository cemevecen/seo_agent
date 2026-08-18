/**
 * Mac köprüsündeki (127.0.0.1:18765) sıradaki tarama slotlarını okur.
 *
 * Köprü kullanıcının Mac'inde çalışıyor; Railway'deki backend ona ulaşamaz
 * (oradaki 127.0.0.1 kendi container'ı). Bu yüzden okuma tarayıcı tarafında
 * yapılır — sayfalar arası tek kaynak olsun diye burada toplandı.
 */
(function (global) {
  var HEALTH_URL = "http://127.0.0.1:18765/health";
  var CACHE_MS = 30000;
  var TIMEOUT_MS = 2500;

  var LABELS = {
    notification: "Notification analytics",
    news: "Döviz news",
    market: "Market quotes",
    gsc_links: "Search Console links",
    policy: "Ad Manager policy",
    pagespeed: "PageSpeed",
    noads: "Sinemalar no-ads",
    seo_audit: "SEO audit",
    sinemalar_moderation: "Sinemalar moderation",
    gsc_cwv: "Web Vitals",
    virgul: "Virgül revenue",
    revenue_targets: "Revenue targets",
    play: "Play Console",
    asc: "App Store Connect",
    firebase: "Firebase",
    empower_intel: "Empower intel",
    empower_intel_sinemalar: "Empower intel · Sinemalar",
    login_warmup: "Login warmup",
  };

  function labelFor(kind) {
    var k = String(kind || "").trim();
    if (LABELS[k]) return LABELS[k];
    if (!k) return "Job";
    return k.replace(/_/g, " ").replace(/^./, function (c) { return c.toUpperCase(); });
  }

  /** 245 → "4h 5m" ; 0 → "now" */
  function formatIn(minutes) {
    var m = Number(minutes);
    if (!isFinite(m) || m < 0) return "";
    m = Math.round(m);
    if (m <= 0) return "now";
    if (m < 60) return m + "m";
    var h = Math.floor(m / 60);
    var rest = m % 60;
    return rest ? h + "h " + rest + "m" : h + "h";
  }

  var _cache = null; /* { at: ms, data: {...} } */
  var _inflight = null;

  function fetchHealth() {
    /* AbortController yoksa (çok eski tarayıcı) yine de dene — sadece timeout yok */
    var ctrl = typeof AbortController === "function" ? new AbortController() : null;
    var timer = ctrl ? setTimeout(function () { ctrl.abort(); }, TIMEOUT_MS) : null;
    var opts = { cache: "no-store" };
    if (ctrl) opts.signal = ctrl.signal;
    return fetch(HEALTH_URL, opts)
      .then(function (r) {
        if (!r.ok) throw new Error("HTTP " + r.status);
        return r.json();
      })
      .then(function (j) {
        var live = (j && j.live) || {};
        var raw = live.upcoming || [];
        var out = [];
        for (var i = 0; i < raw.length; i++) {
          var u = raw[i] || {};
          out.push({
            kind: u.kind,
            label: labelFor(u.kind),
            slot: String(u.slot_tr || ""),
            inMin: Number(u.in_min),
            today: !!u.today,
          });
        }
        out.sort(function (a, b) { return (a.inMin || 0) - (b.inMin || 0); });
        return { ok: true, upcoming: out, error: null };
      })
      .catch(function (e) {
        return {
          ok: false,
          upcoming: [],
          error: (e && e.name === "AbortError") ? "timeout" : String((e && e.message) || e),
        };
      })
      .then(function (res) {
        if (timer) clearTimeout(timer);
        return res;
      });
  }

  /**
   * @param {{force?: boolean, maxAgeMs?: number}} [opts]
   * @returns {Promise<{ok: boolean, upcoming: Array, error: string|null}>}
   */
  function seoBridgeUpcoming(opts) {
    opts = opts || {};
    var maxAge = typeof opts.maxAgeMs === "number" ? opts.maxAgeMs : CACHE_MS;
    var now = Date.now();
    if (!opts.force && _cache && now - _cache.at < maxAge) {
      return Promise.resolve(_cache.data);
    }
    if (_inflight) return _inflight;
    _inflight = fetchHealth().then(function (data) {
      _cache = { at: Date.now(), data: data };
      _inflight = null;
      return data;
    });
    return _inflight;
  }

  /** Tek bir işin sıradaki slotu (ör. "seo_audit") — yoksa null */
  function seoBridgeNextFor(kind, opts) {
    return seoBridgeUpcoming(opts).then(function (res) {
      if (!res.ok) return { ok: false, item: null, error: res.error };
      for (var i = 0; i < res.upcoming.length; i++) {
        if (res.upcoming[i].kind === kind) return { ok: true, item: res.upcoming[i], error: null };
      }
      return { ok: true, item: null, error: null };
    });
  }

  global.seoBridgeUpcoming = seoBridgeUpcoming;
  global.seoBridgeNextFor = seoBridgeNextFor;
  global.seoBridgeFormatIn = formatIn;
  global.seoBridgeJobLabel = labelFor;
})(typeof window !== "undefined" ? window : globalThis);
