/**
 * GA4 + Search Console — /ad ile aynı tarih karşılaştırması (sayfa geneli).
 */
(function (global) {
  "use strict";

  var STORAGE = {
    ga4: "seo-ga4-date-compare",
    sc: "seo-sc-date-compare",
  };

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

  function syncControls(root, pageKey) {
    var wrap = (root || document).querySelector("[data-analytics-date-compare]");
    if (!wrap) return;
    var st = read(STORAGE[pageKey] || STORAGE.ga4);
    var en = wrap.querySelector("[data-compare-enabled]");
    var mode = wrap.querySelector("[data-compare-mode]");
    var custom = wrap.querySelector("[data-compare-custom]");
    var cs = wrap.querySelector("[data-compare-start]");
    var ce = wrap.querySelector("[data-compare-end]");
    if (en) en.checked = st.enabled;
    if (mode) {
      mode.value = st.mode;
      mode.disabled = !st.enabled;
    }
    if (cs) {
      cs.value = st.start;
      cs.disabled = !st.enabled;
    }
    if (ce) {
      ce.value = st.end;
      ce.disabled = !st.enabled;
    }
    var isCustom = st.enabled && st.mode === "custom";
    if (custom) custom.classList.toggle("hidden", !isCustom);
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

  function bind(pageKey, onChange) {
    var storageKey = STORAGE[pageKey] || STORAGE.ga4;
    var wrap = document.querySelector("[data-analytics-date-compare]");
    if (!wrap || wrap.dataset.compareBound === "1") return;
    wrap.dataset.compareBound = "1";

    function persistAndNotify() {
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
      write(storageKey, st);
      syncControls(document, pageKey);
      if (typeof onChange === "function") onChange(st);
    }

    wrap.querySelector("[data-compare-enabled]").addEventListener("change", persistAndNotify);
    wrap.querySelector("[data-compare-mode]").addEventListener("change", persistAndNotify);
    wrap.querySelector("[data-compare-start]").addEventListener("change", persistAndNotify);
    wrap.querySelector("[data-compare-end]").addEventListener("change", persistAndNotify);
    syncControls(document, pageKey);
  }

  function reloadGa4Cards() {
    document.querySelectorAll("[data-ga4-site-card], [id^='ga4-card-'][data-site-id]").forEach(function (el) {
      var sid = el.getAttribute("data-site-id") || (el.id || "").replace("ga4-card-", "");
      if (!sid) return;
      delete el.dataset.chartReady;
      var url = appendToUrl("/ga4/site/" + encodeURIComponent(sid), "ga4");
      if (global.htmx) {
        global.htmx.ajax("GET", url, { target: el, swap: "outerHTML" });
      } else if (global.fetch) {
        global.fetch(url, { credentials: "same-origin" }).then(function (r) {
          return r.text();
        }).then(function (html) {
          el.outerHTML = html;
          if (typeof global.initGa4Panels === "function") {
            global.initGa4Panels(document);
          }
        });
      }
    });
  }

  function reloadScCards() {
    document.querySelectorAll("[data-sc-site-card], [id^='sc-card-']").forEach(function (el) {
      var sid = el.getAttribute("data-site-id") || (el.id || "").replace("sc-card-", "");
      if (!sid) return;
      var url = appendToUrl("/search-console/site/" + encodeURIComponent(sid), "sc");
      if (global.htmx) {
        global.htmx.ajax("GET", url, { target: el, swap: "outerHTML" });
      } else if (global.fetch) {
        global.fetch(url, { credentials: "same-origin" }).then(function (r) {
          return r.text();
        }).then(function (html) {
          el.outerHTML = html;
          if (typeof global.initSearchConsolePanels === "function") {
            global.initSearchConsolePanels(document);
          }
        });
      }
    });
  }

  global.SeoAnalyticsCompare = {
    read: read,
    queryParams: queryParams,
    appendToUrl: appendToUrl,
    bind: bind,
    syncControls: syncControls,
    reloadGa4Cards: reloadGa4Cards,
    reloadScCards: reloadScCards,
  };
})(typeof window !== "undefined" ? window : this);
