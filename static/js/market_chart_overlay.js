/**
 * Google Sheets piyasa kapanış — Plotly trend grafiklerine ikinci eksen overlay.
 */
(function (global) {
  "use strict";

  var LINE_COLOR = "#9D174D";
  var INDEXED_KEYS = ["gram_altin", "usd_try", "eur_try", "bist100", "gram_gumus", "brent"];

  var cache = {
    payload: null,
    rangeKey: "",
    pending: null,
  };

  function mode(selectId) {
    var id = selectId || "seo-market-overlay-mode";
    var el = document.getElementById(id);
    return (el && el.value) || "";
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
   * @param {object} opts - yaxisId (default y5), axisTitle, tickColor, marginRight
   */
  function apply(traces, layout, dateKeys, overlayMode, opts) {
    if (!overlayMode || !dateKeys || !dateKeys.length || !traces || !layout) {
      return Promise.resolve(false);
    }
    opts = opts || {};
    var yaxisId = opts.yaxisId || "y5";
    var layoutKey = layoutKeyForYaxis(yaxisId);
    var startIso = String(dateKeys[0]).slice(0, 10);
    var endIso = String(dateKeys[dateKeys.length - 1]).slice(0, 10);
    return ensureOverlay(startIso, endIso)
      .then(function (payload) {
        var series = (payload && payload.series) || {};
        var keys = overlayMode === "all_indexed" ? INDEXED_KEYS.slice() : [overlayMode];
        var added = false;
        keys.forEach(function (sk) {
          var block = series[sk];
          if (!block) return;
          var clos = closeMap(block);
          var ys = dateKeys.map(function (d) {
            var k = String(d).slice(0, 10);
            return clos[k] != null ? clos[k] : null;
          });
          if (overlayMode === "all_indexed") {
            var base = null;
            for (var i = 0; i < ys.length; i++) {
              if (ys[i] != null && ys[i] > 0) {
                base = ys[i];
                break;
              }
            }
            if (!base) return;
            ys = ys.map(function (v) {
              return v != null ? (v / base) * 100 : null;
            });
          }
          if (!ys.some(function (v) {
            return v != null;
          })) return;
          traces.push({
            x: dateKeys,
            y: ys,
            type: "scatter",
            mode: "lines",
            name: block.label + (overlayMode === "all_indexed" ? " %" : ""),
            yaxis: yaxisId,
            line: { color: LINE_COLOR, width: 2 },
            connectgaps: false,
          });
          added = true;
        });
        if (!added) return false;
        var axisTitle =
          opts.axisTitle ||
          (overlayMode === "all_indexed" ? "Endeks (100)" : "Piyasa");
        layout[layoutKey] = Object.assign(
          {},
          defaultMarketAxisLayout(axisTitle, opts.tickColor),
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

  function bindSelect(selectId, onChange) {
    var id = selectId || "seo-market-overlay-mode";
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
    mode: mode,
    clearCache: clearCache,
    ensureOverlay: ensureOverlay,
    apply: apply,
    bindSelect: bindSelect,
  };
})(typeof window !== "undefined" ? window : this);
