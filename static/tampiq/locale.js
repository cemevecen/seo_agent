(function () {
  var STORAGE_KEY = "tampiq.public.locale";
  var COOKIE_NAME = "tampiq_public_locale";
  var SUPPORTED = {
    tr: 1,
    "en-US": 1,
    ar: 1,
    "zh-Hant": 1,
    fr: 1,
    de: 1,
    hi: 1,
    ja: 1,
    "pt-BR": 1,
    "es-ES": 1,
    ur: 1
  };

  function normalize(raw) {
    if (!raw) return null;
    var value = String(raw).trim().replace(/_/g, "-");
    if (!value) return null;
    var lower = value.toLowerCase();
    var keys = Object.keys(SUPPORTED);
    for (var i = 0; i < keys.length; i++) {
      if (keys[i].toLowerCase() === lower) return keys[i];
    }
    var primary = lower.split("-")[0];
    if (primary === "tr") return "tr";
    if (primary === "en") return "en-US";
    if (primary === "ar") return "ar";
    if (primary === "fr") return "fr";
    if (primary === "de") return "de";
    if (primary === "hi") return "hi";
    if (primary === "ja") return "ja";
    if (primary === "ur") return "ur";
    if (primary === "es") return "es-ES";
    if (primary === "pt") return "pt-BR";
    if (primary === "zh") return "zh-Hant";
    return null;
  }

  function readQueryLang() {
    try {
      return normalize(new URLSearchParams(window.location.search).get("lang"));
    } catch (_) {
      return null;
    }
  }

  function readStored() {
    try {
      return normalize(window.localStorage.getItem(STORAGE_KEY));
    } catch (_) {
      return null;
    }
  }

  function persist(locale) {
    try {
      window.localStorage.setItem(STORAGE_KEY, locale);
    } catch (_) {}
    try {
      var maxAge = 60 * 60 * 24 * 365;
      document.cookie =
        COOKIE_NAME +
        "=" +
        encodeURIComponent(locale) +
        "; Path=/; Max-Age=" +
        maxAge +
        "; SameSite=Lax";
    } catch (_) {}
  }

  function buildUrl(locale) {
    var url = new URL(window.location.href);
    url.searchParams.set("lang", locale);
    return url.pathname + url.search + url.hash;
  }

  function currentServerLocale() {
    return document.body && document.body.getAttribute("data-locale");
  }

  function applyLocale(locale, opts) {
    var options = opts || {};
    if (!SUPPORTED[locale]) return;
    persist(locale);
    var server = currentServerLocale();
    if (server === locale && !options.forceFetch) {
      if (window.history && window.history.replaceState) {
        window.history.replaceState({}, "", buildUrl(locale));
      }
      return;
    }
    var next = buildUrl(locale);
    // Soft navigation: fetch HTML and replace document without full browser chrome reload feel.
    if (window.fetch) {
      fetch(next, {
        headers: { Accept: "text/html", "X-Tampiq-Locale-Nav": "1" },
        credentials: "same-origin"
      })
        .then(function (res) {
          if (!res.ok) throw new Error("locale fetch failed");
          return res.text();
        })
        .then(function (html) {
          var parser = new DOMParser();
          var doc = parser.parseFromString(html, "text/html");
          document.documentElement.lang = doc.documentElement.lang;
          document.documentElement.dir = doc.documentElement.dir;
          document.title = doc.title;
          if (doc.body) {
            document.body.innerHTML = doc.body.innerHTML;
            document.body.setAttribute("data-locale", locale);
          }
          if (window.history && window.history.replaceState) {
            window.history.replaceState({}, "", next);
          }
          bind();
          // Restore hash target after swap when present.
          if (window.location.hash) {
            var el = document.querySelector(window.location.hash);
            if (el && el.scrollIntoView) el.scrollIntoView();
          }
        })
        .catch(function () {
          window.location.href = next;
        });
      return;
    }
    window.location.href = next;
  }

  function bind() {
    var select = document.querySelector("[data-lang-select]");
    if (!select || select.getAttribute("data-bound") === "1") return;
    select.setAttribute("data-bound", "1");
    select.addEventListener("change", function () {
      var locale = normalize(select.value);
      if (locale) applyLocale(locale, { forceFetch: true });
    });
  }

  function bootstrap() {
    bind();
    var query = readQueryLang();
    if (query) {
      persist(query);
      return;
    }
    var stored = readStored();
    var server = currentServerLocale();
    if (stored && server && stored !== server) {
      applyLocale(stored, { forceFetch: true });
    }
  }

  if (document.readyState === "loading") {
    document.addEventListener("DOMContentLoaded", bootstrap);
  } else {
    bootstrap();
  }
})();
