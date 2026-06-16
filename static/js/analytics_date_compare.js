/**
 * GA4 + Search Console — /ad ile aynı tarih karşılaştırması (sayfa geneli).
 */
(function (global) {
  "use strict";

  var STORAGE = {
    ga4: "seo-ga4-date-compare",
    sc: "seo-sc-date-compare",
  };

  var ANALYTICS_COMPARE_LOTTIE =
    "https://assets1.lottiefiles.com/packages/lf20_poqmycwy.json";

  var debounceTimers = { ga4: null, sc: null };
  var lastState = { ga4: null, sc: null };
  var reloadPending = { ga4: 0, sc: 0 };
  var reloadActivePage = null;

  function read(storageKey) {
    try {
      var raw = global.localStorage.getItem(storageKey);
      if (!raw) return defaultState();
      var o = JSON.parse(raw);
      return {
        enabled: !!o.enabled,
        mode: o.mode || "previous_period",
        start: o.start || "",
        end: o.end || "",
      };
    } catch (e) {
      return defaultState();
    }
  }

  function defaultState() {
    return { enabled: false, mode: "previous_period", start: "", end: "" };
  }

  function write(storageKey, state) {
    try {
      global.localStorage.setItem(storageKey, JSON.stringify(state));
    } catch (e) {
      /* ignore */
    }
  }

  function statesEqual(a, b) {
    if (!a || !b) return false;
    return (
      !!a.enabled === !!b.enabled &&
      (a.mode || "") === (b.mode || "") &&
      (a.start || "") === (b.start || "") &&
      (a.end || "") === (b.end || "")
    );
  }

  /** Karşılaştırma ayarı değişince kartları yeniden yükle (önceki döneme dönüş dahil). */
  function needsServerReload(st, prevSt) {
    prevSt = prevSt || defaultState();
    if (statesEqual(st, prevSt)) return false;
    if (st.enabled || prevSt.enabled) return true;
    return false;
  }

  function queryParams(pageKey) {
    var st = read(STORAGE[pageKey] || STORAGE.ga4);
    var p = new URLSearchParams();
    if (!st.enabled) return p;
    p.set("compare", "1");
    p.set("compare_mode", st.mode || "previous_period");
    if (st.mode === "custom") {
      if (st.start) p.set("compare_start", st.start);
      if (st.end) p.set("compare_end", st.end);
    }
    return p;
  }

  function appendToUrl(url, pageKey) {
    var q = queryParams(pageKey);
    if (!q.toString()) return url;
    var sep = url.indexOf("?") >= 0 ? "&" : "?";
    return url + sep + q.toString();
  }

  function compareLoadingEl() {
    var wrap = document.querySelector("[data-analytics-date-compare]");
    return wrap ? wrap.querySelector("[data-compare-loading]") : null;
  }

  function lottiePlayerReady() {
    return !!(global.customElements && global.customElements.get("lottie-player"));
  }

  function showCompareLoading(pageKey) {
    var el = compareLoadingEl();
    if (!el) return;
    el.classList.remove("hidden");
    el.setAttribute("aria-hidden", "false");
    var player = el.querySelector("lottie-player");
    if (player && lottiePlayerReady()) {
      try {
        player.play();
      } catch (e) {
        /* ignore */
      }
    }
  }

  function hideCompareLoading(pageKey) {
    var el = compareLoadingEl();
    if (!el) return;
    el.classList.add("hidden");
    el.setAttribute("aria-hidden", "true");
    var player = el.querySelector("lottie-player");
    if (player) {
      try {
        player.pause();
      } catch (e) {
        /* ignore */
      }
    }
    reloadPending[pageKey] = 0;
    if (reloadActivePage === pageKey) reloadActivePage = null;
    if (global.__seoCompareReloadWatch) {
      global.clearTimeout(global.__seoCompareReloadWatch);
      global.__seoCompareReloadWatch = null;
    }
  }

  function hideAllCompareLoading() {
    hideCompareLoading("ga4");
    hideCompareLoading("sc");
    reloadActivePage = null;
  }

  function requestPath(ev) {
    var pi = ev && ev.detail && ev.detail.pathInfo;
    return (pi && pi.requestPath) || "";
  }

  function onCardSwapDone(pageKey) {
    if (reloadActivePage !== pageKey) return;
    reloadPending[pageKey] -= 1;
    if (reloadPending[pageKey] <= 0) {
      hideCompareLoading(pageKey);
    }
  }

  function armCompareReloadWatch(pageKey, count) {
    if (global.__seoCompareReloadWatch) {
      global.clearTimeout(global.__seoCompareReloadWatch);
    }
    var ms = Math.min(120000, Math.max(45000, count * 12000));
    global.__seoCompareReloadWatch = global.setTimeout(function () {
      if (reloadActivePage === pageKey) {
        hideCompareLoading(pageKey);
      }
    }, ms);
  }

  function bindHtmxCompareSwapOnce() {
    if (global.__seoCompareHtmxSwapBound) return;
    global.__seoCompareHtmxSwapBound = true;
    document.body.addEventListener("htmx:afterSwap", function (ev) {
      var t = ev.detail && ev.detail.target;
      if (!t || !t.getAttribute) return;
      if (t.id === "ga4-site-list") {
        hideCompareLoading("ga4");
        return;
      }
      if (t.id === "search-console-site-list") {
        hideCompareLoading("sc");
        return;
      }
    });
    document.body.addEventListener("htmx:afterSettle", function (ev) {
      if (!reloadActivePage) return;
      var path = requestPath(ev);
      if (reloadActivePage === "ga4" && path.indexOf("/ga4/site/") === 0) {
        onCardSwapDone("ga4");
      }
      if (reloadActivePage === "sc" && path.indexOf("/search-console/site/") === 0) {
        onCardSwapDone("sc");
      }
    });
    document.body.addEventListener("htmx:responseError", function () {
      if (reloadActivePage) hideCompareLoading(reloadActivePage);
    });
    window.addEventListener("pageshow", function () {
      hideAllCompareLoading();
    });
  }

  function parallelHtmxReload(cards, urlBuilder, pageKey) {
    if (!global.htmx || !cards.length) return;
    bindHtmxCompareSwapOnce();
    reloadActivePage = pageKey;
    reloadPending[pageKey] = cards.length;
    showCompareLoading(pageKey);
    armCompareReloadWatch(pageKey, cards.length);
    cards.forEach(function (el) {
      if (!el || !el.parentElement) {
        onCardSwapDone(pageKey);
        return;
      }
      delete el.dataset.chartReady;
      global.htmx.ajax("GET", urlBuilder(el), { target: el, swap: "outerHTML" });
    });
  }

  function applyControlUi(wrap, st) {
    if (!wrap) return;
    var mode = wrap.querySelector("[data-compare-mode]");
    var custom = wrap.querySelector("[data-compare-custom]");
    var cs = wrap.querySelector("[data-compare-start]");
    var ce = wrap.querySelector("[data-compare-end]");
    if (mode) {
      mode.disabled = !st.enabled;
      if (st.enabled) {
        mode.removeAttribute("disabled");
      } else {
        mode.setAttribute("disabled", "disabled");
      }
    }
    var isCustom = st.enabled && st.mode === "custom";
    if (cs) {
      cs.disabled = !isCustom;
      if (isCustom) cs.removeAttribute("disabled");
      else cs.setAttribute("disabled", "disabled");
    }
    if (ce) {
      ce.disabled = !isCustom;
      if (isCustom) ce.removeAttribute("disabled");
      else ce.setAttribute("disabled", "disabled");
    }
    if (custom) custom.classList.toggle("hidden", !isCustom);
  }

  function syncControls(root, pageKey) {
    var wrap = (root || document).querySelector("[data-analytics-date-compare]");
    if (!wrap) return;
    var st = read(STORAGE[pageKey] || STORAGE.ga4);
    var en = wrap.querySelector("[data-compare-enabled]");
    var mode = wrap.querySelector("[data-compare-mode]");
    var cs = wrap.querySelector("[data-compare-start]");
    var ce = wrap.querySelector("[data-compare-end]");
    if (en) en.checked = st.enabled;
    if (mode) mode.value = st.mode;
    if (cs) cs.value = st.start;
    if (ce) ce.value = st.end;
    applyControlUi(wrap, st);
    updateBanner(pageKey, st);
  }

  function updateBanner() {
    /* Banner kaldırıldı — karşılaştırma yalnızca üst kontrolde. */
  }

  function reloadGa4CardsImpl(prevSt) {
    var st = read(STORAGE.ga4);
    if (!needsServerReload(st, prevSt)) {
      updateBanner("ga4", st);
      return;
    }
    var cards = document.querySelectorAll("#ga4-site-list [data-ga4-site-card]");
    if (!cards.length) return;
    parallelHtmxReload(
      cards,
      function (el) {
        var sid = el.getAttribute("data-site-id") || "";
        return appendToUrl("/ga4/site/" + encodeURIComponent(sid), "ga4");
      },
      "ga4"
    );
  }

  function reloadScCardsImpl(prevSt) {
    var st = read(STORAGE.sc);
    if (!needsServerReload(st, prevSt)) {
      updateBanner("sc", st);
      return;
    }
    var cards = document.querySelectorAll("#search-console-site-list [data-sc-site-card]");
    if (!cards.length) return;
    parallelHtmxReload(
      cards,
      function (el) {
        var sid = el.getAttribute("data-site-id") || "";
        return appendToUrl("/search-console/site/" + encodeURIComponent(sid), "sc");
      },
      "sc"
    );
  }

  function scheduleReload(pageKey, prevSt) {
    var key = pageKey === "sc" ? "sc" : "ga4";
    if (debounceTimers[key]) {
      global.clearTimeout(debounceTimers[key]);
    }
    debounceTimers[key] = global.setTimeout(function () {
      debounceTimers[key] = null;
      if (key === "sc") reloadScCardsImpl(prevSt);
      else reloadGa4CardsImpl(prevSt);
    }, 280);
  }

  function bind(pageKey, onChange) {
    var storageKey = STORAGE[pageKey] || STORAGE.ga4;
    var wrap = document.querySelector("[data-analytics-date-compare]");
    if (!wrap) return false;
    if (wrap.dataset.compareBound === "1") return true;
    wrap.dataset.compareBound = "1";
    lastState[pageKey === "sc" ? "sc" : "ga4"] = read(storageKey);
    bindHtmxCompareSwapOnce();
    hideCompareLoading(pageKey === "sc" ? "sc" : "ga4");

    function persistAndNotify() {
      var prevSt = read(storageKey);
      var en = wrap.querySelector("[data-compare-enabled]");
      var mode = wrap.querySelector("[data-compare-mode]");
      var cs = wrap.querySelector("[data-compare-start]");
      var ce = wrap.querySelector("[data-compare-end]");
      var st = {
        enabled: !!(en && en.checked),
        mode: (mode && mode.value) || "previous_period",
        start: (cs && cs.value) || "",
        end: (ce && ce.value) || "",
      };
      applyControlUi(wrap, st);

      if (statesEqual(st, prevSt)) {
        return;
      }
      write(storageKey, st);
      updateBanner(pageKey, st);
      scheduleReload(pageKey, prevSt);
      if (typeof onChange === "function") onChange(st);
    }

    wrap.querySelector("[data-compare-enabled]").addEventListener("change", persistAndNotify);
    wrap.querySelector("[data-compare-mode]").addEventListener("change", persistAndNotify);
    wrap.querySelector("[data-compare-start]").addEventListener("change", persistAndNotify);
    wrap.querySelector("[data-compare-end]").addEventListener("change", persistAndNotify);
    syncControls(document, pageKey);
    return true;
  }

  function bindWhenReady(pageKey, onChange, attempt) {
    var n = attempt || 0;
    if (bind(pageKey, onChange)) return;
    if (n < 80) {
      global.setTimeout(function () {
        bindWhenReady(pageKey, onChange, n + 1);
      }, 40);
    }
  }

  function reloadGa4Cards() {
    reloadGa4CardsImpl(read(STORAGE.ga4));
  }

  function reloadScCards() {
    reloadScCardsImpl(read(STORAGE.sc));
  }

  global.SeoAnalyticsCompare = {
    read: read,
    queryParams: queryParams,
    appendToUrl: appendToUrl,
    bind: bind,
    bindWhenReady: bindWhenReady,
    syncControls: syncControls,
    reloadGa4Cards: reloadGa4Cards,
    reloadScCards: reloadScCards,
    LOTTIE_SRC: ANALYTICS_COMPARE_LOTTIE,
    hideAllCompareLoading: hideAllCompareLoading,
  };
})(typeof window !== "undefined" ? window : this);
