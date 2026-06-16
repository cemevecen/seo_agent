/**
 * Google Sheets piyasa kapanış — Plotly trend grafiklerine ikinci eksen overlay (çoklu seçim).
 */
(function (global) {
  "use strict";

  var LINE_COLOR = "#9D174D";
  var SERIES_COLORS = [
    "#9D174D",
    "#2563EB",
    "#059669",
    "#D97706",
    "#7C3AED",
    "#DC2626",
    "#0891B2",
  ];
  var INDEXED_KEYS = ["gram_altin", "usd_try", "eur_try", "bist100", "gram_gumus", "brent"];
  var OPTION_LABELS = {
    usd_try: "USD/TRY",
    eur_try: "EUR/TRY",
    gram_altin: "Gram altın",
    gram_gumus: "Gram gümüş",
    bist100: "BIST 100",
    brent: "Brent",
    all_indexed: "Tümü (%)",
  };

  var cache = {
    payload: null,
    rangeKey: "",
    pending: null,
  };

  function rootEl(controlId) {
    if (controlId) {
      var byId = document.getElementById(controlId);
      if (byId && byId.getAttribute("data-market-overlay-root") != null) return byId;
    }
    return (
      document.querySelector("[data-market-overlay-root]") ||
      document.getElementById("seo-market-overlay-root")
    );
  }

  function storageKeyForRoot(root) {
    return "seo-market-overlay-keys-" + ((root && root.getAttribute("data-overlay-storage-key")) || "seo");
  }

  function readStored(root) {
    try {
      var raw = global.localStorage.getItem(storageKeyForRoot(root));
      if (!raw) return [];
      var arr = JSON.parse(raw);
      return Array.isArray(arr) ? arr.filter(Boolean) : [];
    } catch (e) {
      return [];
    }
  }

  function writeStored(root, keys) {
    try {
      global.localStorage.setItem(storageKeyForRoot(root), JSON.stringify(keys || []));
    } catch (e) {
      /* ignore */
    }
  }

  function selectedFromDom(root) {
    if (!root) return [];
    var keys = [];
    root.querySelectorAll("[data-market-overlay-panel] input[type=checkbox]:checked").forEach(function (cb) {
      if (cb.value) keys.push(cb.value);
    });
    return keys;
  }

  function syncDomFromStored(root) {
    if (!root) return;
    var keys = readStored(root);
    root.querySelectorAll("[data-market-overlay-panel] input[type=checkbox]").forEach(function (cb) {
      cb.checked = keys.indexOf(cb.value) >= 0;
    });
    updateTriggerLabel(root);
  }

  function updateTriggerLabel(root) {
    if (!root) return;
    var labelEl = root.querySelector("[data-market-overlay-label]");
    if (!labelEl) return;
    var keys = selectedFromDom(root);
    if (!keys.length) {
      labelEl.textContent = "Piyasa: kapalı";
      return;
    }
    if (keys.length === 1) {
      labelEl.textContent = OPTION_LABELS[keys[0]] || keys[0];
      return;
    }
    if (keys.length === 1 && keys[0] === "all_indexed") {
      labelEl.textContent = OPTION_LABELS.all_indexed;
      return;
    }
    labelEl.textContent = "Piyasa: " + keys.length + " seri";
  }

  /** @deprecated tek seçim — ilk mod */
  function mode(controlId) {
    var m = modes(controlId);
    return m.length ? m[0] : "";
  }

  function modes(controlId) {
    var root = rootEl(controlId);
    if (!root) {
      var legacy = document.getElementById(controlId || "seo-market-overlay-mode");
      if (legacy && legacy.tagName === "SELECT") {
        var v = legacy.value;
        return v ? [v] : [];
      }
      return [];
    }
    var keys = selectedFromDom(root);
    if (!keys.length) keys = readStored(root);
    return normalizeKeys(keys);
  }

  function normalizeKeys(keys) {
    if (!keys || !keys.length) return [];
    if (keys.indexOf("all_indexed") >= 0) return ["all_indexed"];
    return keys.filter(function (k) {
      return k && k !== "all_indexed";
    });
  }

  function resolveSeriesKeys(overlayMode) {
    if (!overlayMode) return [];
    if (Array.isArray(overlayMode)) {
      var n = normalizeKeys(overlayMode);
      if (n.indexOf("all_indexed") >= 0) return INDEXED_KEYS.slice();
      return n;
    }
    if (overlayMode === "all_indexed") return INDEXED_KEYS.slice();
    return [overlayMode];
  }

  function useIndexedScale(seriesKeys, overlayInput) {
    if (Array.isArray(overlayInput) && overlayInput.indexOf("all_indexed") >= 0) return true;
    if (overlayInput === "all_indexed") return true;
    return seriesKeys.length > 1;
  }

  function indexSeries(ys) {
    var base = null;
    for (var i = 0; i < ys.length; i++) {
      if (ys[i] != null && ys[i] > 0) {
        base = ys[i];
        break;
      }
    }
    if (!base) return null;
    return ys.map(function (v) {
      return v != null ? (v / base) * 100 : null;
    });
  }

  function clearCache() {
    cache.payload = null;
    cache.rangeKey = "";
    cache.pending = null;
  }

  function closeMap(seriesBlock) {
    var m = {};
    ((seriesBlock && seriesBlock.by_date) || []).forEach(function (pt) {
      if (pt && pt.date) m[String(pt.date).slice(0, 10)] = Number(pt.close);
    });
    return m;
  }

  function ensureOverlay(startIso, endIso) {
    var key = (startIso || "") + "|" + (endIso || "");
    if (cache.payload && cache.rangeKey === key) {
      return Promise.resolve(cache.payload);
    }
    if (cache.pending) return cache.pending;
    var p = new URLSearchParams();
    if (startIso) p.set("start", startIso);
    if (endIso) p.set("end", endIso);
    cache.pending = fetch("/api/market-quotes/overlay?" + p.toString(), { credentials: "same-origin" })
      .then(function (r) {
        if (!r.ok) throw new Error("Piyasa verisi alınamadı");
        return r.json();
      })
      .then(function (data) {
        cache.rangeKey = key;
        cache.payload = data;
        cache.pending = null;
        return data;
      })
      .catch(function (err) {
        cache.pending = null;
        throw err;
      });
    return cache.pending;
  }

  function layoutKeyForYaxis(yaxisId) {
    var id = yaxisId || "y5";
    if (id === "y") return "yaxis";
    var n = id.replace(/^y/, "");
    return n ? "yaxis" + n : "yaxis";
  }

  function normalizeDateKey(d) {
    if (d == null || d === "") return "";
    if (typeof d === "number" && !isNaN(d)) {
      return new Date(d).toISOString().slice(0, 10);
    }
    var s = String(d).trim();
    if (/^\d{8}$/.test(s)) {
      return s.slice(0, 4) + "-" + s.slice(4, 6) + "-" + s.slice(6, 8);
    }
    if (s.length >= 10 && s.charAt(4) === "-") return s.slice(0, 10);
    var parsed = Date.parse(s);
    if (!isNaN(parsed)) {
      return new Date(parsed).toISOString().slice(0, 10);
    }
    return s.slice(0, 10);
  }

  /** Plotly layout + trace’lerde kullanılmayan ilk yN (GA4: y2 eng. rate → piyasa y3). */
  function pickFreeYaxisId(layout, traces) {
    var used = { y: true };
    if (layout) {
      Object.keys(layout).forEach(function (k) {
        if (k === "yaxis") used.y = true;
        var m = /^yaxis(\d+)$/.exec(k);
        if (m) used["y" + m[1]] = true;
      });
    }
    (traces || []).forEach(function (t) {
      var ax = t.yaxis || "y";
      used[ax] = true;
    });
    for (var n = 3; n <= 8; n++) {
      if (!used["y" + n]) return "y" + n;
    }
    return "y8";
  }

  function defaultMarketAxisLayout(title, tickColor) {
    var c = tickColor || LINE_COLOR;
    return {
      title: title ? { text: title, font: { size: 11, color: c } } : undefined,
      tickfont: { size: 10, color: c },
      overlaying: "y",
      side: "right",
      showgrid: false,
      zeroline: false,
      automargin: true,
    };
  }

  /**
   * @param {string|string[]} overlayMode
   * @param {object} opts - yaxisId (default y5), axisTitle, tickColor, marginRight
   */
  function apply(traces, layout, dateKeys, overlayMode, opts) {
    var keys = resolveSeriesKeys(overlayMode);
    if (!keys.length || !dateKeys || !dateKeys.length || !traces || !layout) {
      return Promise.resolve(false);
    }
    opts = opts || {};
    var yaxisId = opts.yaxisId || pickFreeYaxisId(layout, traces);
    var layoutKey = layoutKeyForYaxis(yaxisId);
    var indexed = useIndexedScale(keys, overlayMode);
    var startIso = normalizeDateKey(dateKeys[0]);
    var endIso = normalizeDateKey(dateKeys[dateKeys.length - 1]);
    return ensureOverlay(startIso, endIso)
      .then(function (payload) {
        var series = (payload && payload.series) || {};
        var added = false;
        var colorIdx = 0;
        keys.forEach(function (sk) {
          var block = series[sk];
          if (!block) return;
          var clos = closeMap(block);
          var ys = dateKeys.map(function (d) {
            var k = normalizeDateKey(d);
            return clos[k] != null ? clos[k] : null;
          });
          if (indexed) {
            var indexedYs = indexSeries(ys);
            if (!indexedYs) return;
            ys = indexedYs;
          }
          if (!ys.some(function (v) {
            return v != null;
          })) return;
          var lineColor =
            keys.length === 1 && !indexed
              ? LINE_COLOR
              : SERIES_COLORS[colorIdx % SERIES_COLORS.length];
          colorIdx += 1;
          traces.push({
            x: dateKeys,
            y: ys,
            type: "scatter",
            mode: "lines",
            name: block.label + (indexed ? " %" : ""),
            yaxis: yaxisId,
            line: { color: lineColor, width: 2 },
            connectgaps: false,
          });
          added = true;
        });
        if (!added) return false;
        var axisTitle =
          opts.axisTitle ||
          (indexed ? "Endeks (100)" : keys.length > 1 ? "Piyasa (çoklu)" : "Piyasa");
        layout[layoutKey] = Object.assign(
          {},
          defaultMarketAxisLayout(axisTitle, opts.tickColor || LINE_COLOR),
          layout[layoutKey] || {}
        );
        var m = layout.margin || { l: 52, r: 52, t: 20, b: 56 };
        var minR = opts.marginRight != null ? opts.marginRight : 64;
        layout.margin = Object.assign({}, m, { r: Math.max(m.r || 12, minR) });
        layout.showlegend = true;
        return true;
      })
      .catch(function () {
        return false;
      });
  }

  function bindPanel(root, onChange) {
    if (!root || root.dataset.marketOverlayBound === "1") return;
    root.dataset.marketOverlayBound = "1";
    syncDomFromStored(root);

    var trigger = root.querySelector("[data-market-overlay-trigger]");
    var panel = root.querySelector("[data-market-overlay-panel]");

    function closePanel() {
      if (!panel) return;
      panel.classList.add("hidden");
      if (trigger) trigger.setAttribute("aria-expanded", "false");
    }

    function openPanel() {
      if (!panel) return;
      panel.classList.remove("hidden");
      if (trigger) trigger.setAttribute("aria-expanded", "true");
    }

    if (trigger) {
      trigger.addEventListener("click", function (e) {
        e.stopPropagation();
        if (panel && panel.classList.contains("hidden")) openPanel();
        else closePanel();
      });
    }

    root.querySelectorAll("[data-market-overlay-panel] input[type=checkbox]").forEach(function (cb) {
      cb.addEventListener("change", function () {
        if (cb.value === "all_indexed" && cb.checked) {
          root.querySelectorAll("[data-market-overlay-panel] input[type=checkbox]").forEach(function (other) {
            if (other !== cb) other.checked = false;
          });
        } else if (cb.checked && cb.value !== "all_indexed") {
          var allCb = root.querySelector('[data-market-overlay-panel] input[value="all_indexed"]');
          if (allCb) allCb.checked = false;
        }
        var keys = normalizeKeys(selectedFromDom(root));
        root.querySelectorAll("[data-market-overlay-panel] input[type=checkbox]").forEach(function (box) {
          box.checked = keys.indexOf(box.value) >= 0;
        });
        writeStored(root, keys);
        updateTriggerLabel(root);
        clearCache();
        if (typeof onChange === "function") onChange(keys);
      });
    });

    if (!global.__seoMarketOverlayDocClose) {
      global.__seoMarketOverlayDocClose = true;
      document.addEventListener("click", function () {
        document.querySelectorAll("[data-market-overlay-root]").forEach(function (r) {
          var p = r.querySelector("[data-market-overlay-panel]");
          var t = r.querySelector("[data-market-overlay-trigger]");
          if (p) p.classList.add("hidden");
          if (t) t.setAttribute("aria-expanded", "false");
        });
      });
    }

    root.querySelectorAll("[data-market-overlay-panel]").forEach(function (p) {
      p.addEventListener("click", function (e) {
        e.stopPropagation();
      });
    });
  }

  function bindSelect(controlId, onChange) {
    var root = rootEl(controlId);
    if (root) {
      bindPanel(root, onChange);
      return;
    }
    var id = controlId || "seo-market-overlay-mode";
    var el = document.getElementById(id);
    if (!el || el.dataset.marketOverlayBound === "1") return;
    el.dataset.marketOverlayBound = "1";
    el.addEventListener("change", function () {
      clearCache();
      if (typeof onChange === "function") onChange();
    });
  }

  global.SeoMarketOverlay = {
    LINE_COLOR: LINE_COLOR,
    SERIES_COLORS: SERIES_COLORS,
    mode: mode,
    modes: modes,
    clearCache: clearCache,
    ensureOverlay: ensureOverlay,
    apply: apply,
    bindSelect: bindSelect,
    bindPanel: bindPanel,
    pickFreeYaxisId: pickFreeYaxisId,
    normalizeDateKey: normalizeDateKey,
  };
})(typeof window !== "undefined" ? window : this);
