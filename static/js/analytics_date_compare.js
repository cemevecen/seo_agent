/**
 * GA4 + Search Console — /ad ile aynı tarih karşılaştırması (sayfa geneli).
 */
(function (global) {
  "use strict";

  var STORAGE = {
    ga4: "seo-ga4-date-compare",
    sc: "seo-sc-date-compare",
  };

  var debounceTimers = { ga4: null, sc: null };
  var lastState = { ga4: null, sc: null };

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

  /** Sunucuda KPI değişimi gerektiren mod (önceki dönem = varsayılan veri). */
  function needsServerReload(st, prevSt) {
    prevSt = prevSt || defaultState();
    if (st.enabled) {
      return st.mode !== "previous_period";
    }
    if (prevSt.enabled && prevSt.mode !== "previous_period") {
      return true;
    }
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

  function modeLabel(mode) {
    if (mode === "previous_year") return "Geçen yıl (aynı tarihler)";
    if (mode === "custom") return "Özel karşılaştırma aralığı";
    return "Önceki dönem (aynı uzunluk)";
  }

  function updateBanner(pageKey, st) {
    var id = pageKey === "sc" ? "sc-compare-period-banner" : "ga4-compare-period-banner";
    var banner = document.getElementById(id);
    var modeEl = document.getElementById(id + "-mode");
    if (!banner) return;
    if (!st.enabled) {
      banner.classList.add("hidden");
      return;
    }
    banner.classList.remove("hidden");
    if (modeEl) {
      modeEl.textContent = modeLabel(st.mode);
      if (st.mode === "custom" && st.start && st.end) {
        modeEl.textContent += " · " + st.start + " – " + st.end;
      }
    }
  }

  function staggerHtmxReload(cards, urlBuilder) {
    if (!global.htmx || !cards.length) return;
    var idx = 0;
    function step() {
      if (idx >= cards.length) return;
      var el = cards[idx];
      idx += 1;
      if (!el || !el.parentElement) {
        step();
        return;
      }
      delete el.dataset.chartReady;
      global.htmx.ajax("GET", urlBuilder(el), { target: el, swap: "outerHTML" });
      global.setTimeout(step, 120);
    }
    step();
  }

  function reloadGa4CardsImpl(prevSt) {
    var st = read(STORAGE.ga4);
    if (!needsServerReload(st, prevSt)) {
      updateBanner("ga4", st);
      return;
    }
    var cards = document.querySelectorAll("#ga4-site-list [data-ga4-site-card]");
    if (!cards.length) return;
    staggerHtmxReload(cards, function (el) {
      var sid = el.getAttribute("data-site-id") || "";
      return appendToUrl("/ga4/site/" + encodeURIComponent(sid), "ga4");
    });
  }

  function reloadScCardsImpl(prevSt) {
    var st = read(STORAGE.sc);
    if (!needsServerReload(st, prevSt)) {
      updateBanner("sc", st);
      return;
    }
    var cards = document.querySelectorAll("#search-console-site-list [data-sc-site-card]");
    if (!cards.length) return;
    staggerHtmxReload(cards, function (el) {
      var sid = el.getAttribute("data-site-id") || "";
      return appendToUrl("/search-console/site/" + encodeURIComponent(sid), "sc");
    });
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
    }, 450);
  }

  function bind(pageKey, onChange) {
    var storageKey = STORAGE[pageKey] || STORAGE.ga4;
    var wrap = document.querySelector("[data-analytics-date-compare]");
    if (!wrap) return false;
    if (wrap.dataset.compareBound === "1") return true;
    wrap.dataset.compareBound = "1";
    lastState[pageKey === "sc" ? "sc" : "ga4"] = read(storageKey);

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
  };
})(typeof window !== "undefined" ? window : this);
