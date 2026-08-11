/**
 * Piyasa kapanış (doviz.com tarama) — Plotly overlay + bağımsız çizgi grafik.
 */
(function (global) {
  "use strict";

  var SERIES_COLORS = global.seoMatteMarketOverlayPalette
    ? global.seoMatteMarketOverlayPalette()
    : [
    "#1d4ed8",
    "#0f766e",
    "#2563eb",
    "#059669",
    "#0284c7",
    "#14b8a6",
    "#1e40af",
    "#047857",
    "#0e7490",
    "#4338ca",
  ];
  var LINE_COLOR = SERIES_COLORS[0];
  var INDEXED_KEYS = [
    "gram_altin",
    "ceyrek_altin",
    "usd_try",
    "eur_try",
    "bist100",
    "gram_gumus",
    "brent",
    "bitcoin",
  ];
  var OPTION_LABELS = {
    usd_try: "USD/TRY",
    eur_try: "EUR/TRY",
    gram_altin: "Gram altın",
    ceyrek_altin: "Çeyrek altın",
    gram_gumus: "Gram gümüş",
    bist100: "BIST 100",
    brent: "Brent",
    bitcoin: "Bitcoin",
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

  var DEFAULT_OVERLAY_KEYS = [];

  function storageKeyForRoot(root) {
    return "seo-market-overlay-keys-v2-" + ((root && root.getAttribute("data-overlay-storage-key")) || "seo");
  }

  function readStored(root) {
    try {
      var raw = global.localStorage.getItem(storageKeyForRoot(root));
      if (raw === null || raw === "") {
        return DEFAULT_OVERLAY_KEYS.slice();
      }
      var arr = JSON.parse(raw);
      return Array.isArray(arr) ? arr.filter(Boolean) : DEFAULT_OVERLAY_KEYS.slice();
    } catch (e) {
      return DEFAULT_OVERLAY_KEYS.slice();
    }
  }

  function writeStored(root, keys) {
    try {
      global.localStorage.setItem(storageKeyForRoot(root), JSON.stringify(keys || []));
    } catch (e) {
      /* ignore */
    }
  }

  function countLegendTraces(traces) {
    var n = 0;
    (traces || []).forEach(function (t) {
      if (t && t.showlegend === false) return;
      n += 1;
    });
    return n;
  }

  function panelForRoot(root) {
    if (!root) return null;
    var rid = root.id;
    if (rid) {
      var docked = document.querySelector(
        '[data-market-overlay-panel][data-market-overlay-for="' + rid + '"]'
      );
      if (docked) return docked;
    }
    return root.querySelector("[data-market-overlay-panel]");
  }

  function panelCheckboxes(root, selector) {
    var panel = panelForRoot(root);
    if (!panel) return [];
    return Array.prototype.slice.call(panel.querySelectorAll(selector || "input[type=checkbox]"));
  }

  function selectedFromDom(root) {
    if (!root) return [];
    var keys = [];
    panelCheckboxes(root, "input[type=checkbox]:checked").forEach(function (cb) {
      if (cb.value) keys.push(cb.value);
    });
    return keys;
  }

  function syncDomFromStored(root) {
    if (!root) return;
    var keys = readStored(root);
    try {
      if (global.localStorage.getItem(storageKeyForRoot(root)) === null) {
        writeStored(root, keys);
      }
    } catch (e) {
      /* ignore */
    }
    panelCheckboxes(root).forEach(function (cb) {
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
    if (root.dataset.marketOverlayBound === "1") {
      return normalizeKeys(selectedFromDom(root));
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

  function pointsForSeries(payload, seriesKey) {
    var block = payload && payload.series && payload.series[seriesKey];
    var out = [];
    ((block && block.by_date) || []).forEach(function (pt) {
      if (!pt || !pt.date) return;
      var v = Number(pt.close);
      if (!Number.isFinite(v)) return;
      out.push({ key: String(pt.date).slice(0, 10), value: v });
    });
    return out;
  }

  function indexPointSeries(series) {
    var base = null;
    (series || []).forEach(function (r) {
      if (base == null && r && Number(r.value) > 0) base = Number(r.value);
    });
    if (!base) return series || [];
    return (series || []).map(function (r) {
      var v = r && r.value != null ? Number(r.value) : null;
      return {
        key: r && r.key,
        value: v != null && Number.isFinite(v) ? (v / base) * 100 : null,
      };
    });
  }

  function seriesLabel(payload, seriesKey, indexed) {
    var block = payload && payload.series && payload.series[seriesKey];
    var label = (block && block.label) || OPTION_LABELS[seriesKey] || seriesKey;
    return indexed ? label + " %" : label;
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

  /** #RRGGBB rengini beyaza doğru aç (hafta sonu köprü çizgileri). */
  function lightenHex(hex, mix) {
    if (!hex || hex.charAt(0) !== "#") return hex;
    var h = hex.replace("#", "");
    if (h.length === 3) {
      h = h[0] + h[0] + h[1] + h[1] + h[2] + h[2];
    }
    if (h.length !== 6) return hex;
    var t = mix == null ? 0.55 : mix;
    function ch(i) {
      var v = parseInt(h.slice(i, i + 2), 16);
      return Math.round(v + (255 - v) * t)
        .toString(16)
        .padStart(2, "0");
    }
    return "#" + ch(0) + ch(2) + ch(4);
  }

  var MARKET_LEGEND_PREFIX = "seo_mkt_";

  function isMarketLegendGroup(group) {
    return group != null && String(group).indexOf(MARKET_LEGEND_PREFIX) === 0;
  }

  function marketGroupLeaderTrace(data, group) {
    for (var i = 0; i < (data || []).length; i++) {
      var tr = data[i];
      if (tr && tr.legendgroup === group && tr.showlegend !== false) return tr;
    }
    return null;
  }

  function traceShownOnGraph(visible) {
    return visible !== false && visible !== "legendonly";
  }

  /** Legend tıklamasında ana seri + köprü segmentlerini birlikte gizle/göster. */
  function toggleMarketLegendGroup(plotEl, curveNumber) {
    if (!plotEl || !global.Plotly || curveNumber == null) return;
    var data = plotEl.data || [];
    var clicked = data[curveNumber];
    if (!clicked || !isMarketLegendGroup(clicked.legendgroup)) return;
    var group = clicked.legendgroup;
    var anyOnGraph = data.some(function (tr) {
      return tr && tr.legendgroup === group && traceShownOnGraph(tr.visible);
    });
    var newVis = anyOnGraph ? false : true;
    var idx = [];
    var vis = [];
    data.forEach(function (tr, i) {
      if (tr && tr.legendgroup === group) {
        idx.push(i);
        vis.push(newVis);
      }
    });
    if (idx.length) {
      Plotly.restyle(plotEl, { visible: vis }, idx);
    }
    return false;
  }

  function syncAllMarketBridgeGroups(plotEl) {
    if (!plotEl || !global.Plotly) return;
    var data = plotEl.data || [];
    var idx = [];
    var vis = [];
    data.forEach(function (tr, i) {
      if (!tr || tr.showlegend !== false || !isMarketLegendGroup(tr.legendgroup)) return;
      var leader = marketGroupLeaderTrace(data, tr.legendgroup);
      if (!leader) return;
      var targetVis = leader.visible;
      if (targetVis == null) targetVis = true;
      if (tr.visible !== targetVis) {
        idx.push(i);
        vis.push(targetVis);
      }
    });
    if (idx.length) {
      Plotly.restyle(plotEl, { visible: vis }, idx);
    }
  }

  function bindMarketLegendGroupSync(gd) {
    var el = typeof gd === "string" ? document.getElementById(gd) : gd;
    if (!el || !el.on || !el.data) return;
    var hasMarket = el.data.some(function (t) {
      return t && isMarketLegendGroup(t.legendgroup);
    });
    if (!hasMarket) return;
    if (el._seoMarketLegendHandler) {
      el.removeListener("plotly_legendclick", el._seoMarketLegendHandler);
      el.removeListener("plotly_legenddoubleclick", el._seoMarketLegendDblHandler);
    }
    el._seoMarketLegendHandler = function (evt) {
      var handled = toggleMarketLegendGroup(el, evt.curveNumber);
      if (handled === false) return false;
    };
    el._seoMarketLegendDblHandler = function (evt) {
      window.setTimeout(function () {
        syncAllMarketBridgeGroups(el);
      }, 0);
    };
    el.on("plotly_legendclick", el._seoMarketLegendHandler);
    el.on("plotly_legenddoubleclick", el._seoMarketLegendDblHandler);
  }

  function patchPlotlyMarketLegendSync() {
    if (!global.Plotly || global.Plotly.__seoMarketLegendPatched) return;
    global.Plotly.__seoMarketLegendPatched = true;
    var orig = global.Plotly.newPlot;
    global.Plotly.newPlot = function (gd, data, layout, config) {
      var p = orig.call(global.Plotly, gd, data, layout, config);
      if (!p || typeof p.then !== "function") return p;
      return p.then(function (gdOut) {
        var root = gdOut || (typeof gd === "string" ? document.getElementById(gd) : gd);
        bindMarketLegendGroupSync(root);
        return gdOut;
      });
    };
  }

  if (global.Plotly) {
    patchPlotlyMarketLegendSync();
  } else if (global.document) {
    var _legendPatchAttempts = 0;
    var _legendPatchTimer = global.setInterval(function () {
      _legendPatchAttempts += 1;
      if (global.Plotly) {
        global.clearInterval(_legendPatchTimer);
        patchPlotlyMarketLegendSync();
      } else if (_legendPatchAttempts > 200) {
        global.clearInterval(_legendPatchTimer);
      }
    }, 50);
  }

  /** Piyasa serisindeki takvim boşlukları (hafta sonu/tatil) için uç-uç köprü trace'leri. */
  function marketGapBridgeTraces(dateKeys, ys, lineColor, yaxisId, legendGroup, legendName) {
    var bridges = [];
    if (!dateKeys || !ys || dateKeys.length !== ys.length) return bridges;
    var bridgeColor = lightenHex(lineColor);
    var n = ys.length;
    var i = 0;
    while (i < n) {
      while (i < n && (ys[i] == null || isNaN(ys[i]))) i++;
      if (i >= n) break;
      var segEnd = i;
      while (segEnd < n && ys[segEnd] != null && !isNaN(ys[segEnd])) segEnd++;
      segEnd -= 1;
      var j = segEnd + 1;
      while (j < n && (ys[j] == null || isNaN(ys[j]))) j++;
      if (j < n && j > segEnd + 1) {
        bridges.push({
          x: [dateKeys[segEnd], dateKeys[j]],
          y: [ys[segEnd], ys[j]],
          type: "scatter",
          mode: "lines",
          name: legendName,
          showlegend: false,
          legendgroup: legendGroup,
          visible: true,
          yaxis: yaxisId,
          line: { color: bridgeColor, width: 2 },
          connectgaps: true,
          hoverinfo: "skip",
        });
      }
      i = j;
    }
    return bridges;
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
          var legendName = block.label + (indexed ? " %" : "");
          var legendGroup = "seo_mkt_" + sk + (indexed ? "_i" : "");
          traces.push({
            x: dateKeys,
            y: ys,
            type: "scatter",
            mode: "lines",
            name: legendName,
            legendgroup: legendGroup,
            visible: true,
            yaxis: yaxisId,
            line: { color: lineColor, width: 2 },
            connectgaps: false,
          });
          marketGapBridgeTraces(dateKeys, ys, lineColor, yaxisId, legendGroup, legendName).forEach(function (bt) {
            traces.push(bt);
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
        var compactLeg =
          global.seoPlotlyCompactLegend &&
          global.seoPlotlyCompactLegend({
            legendCount: countLegendTraces(traces),
            chartWidth: opts.chartWidth || 720,
          });
        if (compactLeg) {
          layout.legend = Object.assign(
            { groupclick: "toggleitem" },
            layout.legend || {},
            compactLeg.legend
          );
          layout.margin.t = Math.max(layout.margin.t || 0, compactLeg.marginTop);
        } else {
          layout.legend = Object.assign({ groupclick: "toggleitem" }, layout.legend || {});
        }
        return true;
      })
      .catch(function () {
        return false;
      });
  }

  function syncOverlayOpenBodyClass() {
    var open = false;
    document.querySelectorAll("[data-market-overlay-panel]").forEach(function (p) {
      if (!p.classList.contains("hidden")) open = true;
    });
    document.body.classList.toggle("seo-market-overlay-open", open);
  }

  function dockMarketOverlayPanel(panel) {
    if (!panel || panel.dataset.marketOverlayDocked === "1") return;
    panel._marketOverlayHome = {
      parent: panel.parentNode,
      next: panel.nextSibling,
    };
    document.body.appendChild(panel);
    panel.dataset.marketOverlayDocked = "1";
  }

  function undockMarketOverlayPanel(panel) {
    if (!panel || panel.dataset.marketOverlayDocked !== "1") return;
    var home = panel._marketOverlayHome;
    if (home && home.parent) {
      if (home.next) home.parent.insertBefore(panel, home.next);
      else home.parent.appendChild(panel);
    }
    delete panel._marketOverlayHome;
    delete panel.dataset.marketOverlayDocked;
  }

  function resetMarketOverlayPanelPosition(panel) {
    if (!panel) return;
    panel.classList.remove("seo-market-overlay-panel--open");
    panel.style.position = "";
    panel.style.top = "";
    panel.style.left = "";
    panel.style.right = "";
    panel.style.zIndex = "";
    panel.style.width = "";
    panel.style.maxWidth = "";
    panel.style.maxHeight = "";
  }

  function positionMarketOverlayPanel(trigger, panel) {
    if (!trigger || !panel) return;
    dockMarketOverlayPanel(panel);
    panel.classList.add("seo-market-overlay-panel--open");
    var margin = 8;
    var maxW = Math.max(200, Math.min(300, window.innerWidth - margin * 2));
    panel.style.width = maxW + "px";
    panel.style.maxWidth = maxW + "px";
    panel.style.maxHeight = Math.min(Math.round(window.innerHeight * 0.7), 352) + "px";
    var w = panel.offsetWidth || maxW;
    var r = trigger.getBoundingClientRect();
    var left = Math.round(Math.min(r.right - w, window.innerWidth - w - margin));
    if (left < margin) left = margin;
    var top = Math.round(r.bottom + 4);
    var panelH = panel.offsetHeight || 280;
    if (top + panelH > window.innerHeight - margin) {
      top = Math.max(margin, Math.round(r.top - panelH - 4));
    }
    panel.style.position = "fixed";
    panel.style.top = top + "px";
    panel.style.left = left + "px";
    panel.style.right = "auto";
    panel.style.zIndex = "10000";
  }

  function bindPanel(root, onChange) {
    if (!root || root.dataset.marketOverlayBound === "1") return;
    root.dataset.marketOverlayBound = "1";
    if (typeof onChange === "function") root._marketOverlayOnChange = onChange;
    syncDomFromStored(root);

    var trigger = root.querySelector("[data-market-overlay-trigger]");
    var panel = root.querySelector("[data-market-overlay-panel]");
    if (panel && root.id) {
      panel.setAttribute("data-market-overlay-for", root.id);
    }

    function closePanel() {
      if (!panel) return;
      panel.classList.add("hidden");
      resetMarketOverlayPanelPosition(panel);
      undockMarketOverlayPanel(panel);
      if (trigger) trigger.setAttribute("aria-expanded", "false");
      syncOverlayOpenBodyClass();
    }

    function openPanel() {
      if (!panel) return;
      panel.classList.remove("hidden");
      if (trigger) {
        positionMarketOverlayPanel(trigger, panel);
        trigger.setAttribute("aria-expanded", "true");
      }
      syncOverlayOpenBodyClass();
    }

    if (trigger) {
      trigger.addEventListener("click", function (e) {
        e.stopPropagation();
        global.__seoMarketOverlayIgnoreCloseUntil = Date.now() + 120;
        if (panel && panel.classList.contains("hidden")) openPanel();
        else closePanel();
      });
    }

    panelCheckboxes(root).forEach(function (cb) {
      cb.addEventListener("change", function () {
        if (cb.value === "all_indexed" && cb.checked) {
          panelCheckboxes(root).forEach(function (other) {
            if (other !== cb) other.checked = false;
          });
        } else if (cb.checked && cb.value !== "all_indexed") {
          var allCb = panelForRoot(root);
          allCb = allCb && allCb.querySelector('input[value="all_indexed"]');
          if (allCb) allCb.checked = false;
        }
        var keys = normalizeKeys(selectedFromDom(root));
        panelCheckboxes(root).forEach(function (box) {
          box.checked = keys.indexOf(box.value) >= 0;
        });
        writeStored(root, keys);
        updateTriggerLabel(root);
        clearCache();
        var cbFn =
          (root && root._marketOverlayOnChange) ||
          onChange ||
          resolveMarketOverlayOnChange(root);
        if (typeof cbFn === "function") cbFn(keys);
      });
    });

    if (!global.__seoMarketOverlayDocClose) {
      global.__seoMarketOverlayDocClose = true;
      document.addEventListener("click", function (e) {
        if (global.__seoMarketOverlayIgnoreCloseUntil && Date.now() < global.__seoMarketOverlayIgnoreCloseUntil) {
          return;
        }
        var target = e.target;
        document.querySelectorAll("[data-market-overlay-root]").forEach(function (r) {
          var p = panelForRoot(r);
          if (target && (r.contains(target) || (p && p.contains(target)))) return;
          var t = r.querySelector("[data-market-overlay-trigger]");
          if (p) {
            p.classList.add("hidden");
            resetMarketOverlayPanelPosition(p);
            undockMarketOverlayPanel(p);
          }
          if (t) t.setAttribute("aria-expanded", "false");
        });
        syncOverlayOpenBodyClass();
      });
    }

    var panelForClicks = panelForRoot(root);
    if (panelForClicks) {
      panelForClicks.addEventListener("click", function (e) {
        e.stopPropagation();
      });
    }
  }

  function bindSelect(controlId, onChange) {
    var root = rootEl(controlId);
    if (root) {
      if (typeof onChange === "function") root._marketOverlayOnChange = onChange;
      if (root.dataset.marketOverlayBound === "1") return true;
      bindPanel(root, onChange);
      return true;
    }
    var id = controlId || "seo-market-overlay-mode";
    var el = document.getElementById(id);
    if (!el) return false;
    if (el.dataset.marketOverlayBound === "1") return true;
    el.dataset.marketOverlayBound = "1";
    el.addEventListener("change", function () {
      clearCache();
      if (typeof onChange === "function") onChange();
    });
    return true;
  }

  /** defer script sonrası — sayfa inline script’lerinden çağrılabilir */
  function bindWhenReady(controlId, onChange, attempt) {
    var n = attempt || 0;
    if (bindSelect(controlId, onChange)) return;
    if (n < 80) {
      global.setTimeout(function () {
        bindWhenReady(controlId, onChange, n + 1);
      }, 40);
    }
  }

  function ensureBound(controlId, onChange) {
    var root = rootEl(controlId);
    if (!root) return false;
    if (typeof onChange === "function") root._marketOverlayOnChange = onChange;
    if (root.dataset.marketOverlayBound !== "1") {
      bindPanel(root, onChange || resolveMarketOverlayOnChange(root));
    }
    return true;
  }

  function ensureClosed() {
    document.querySelectorAll("[data-market-overlay-root]").forEach(function (r) {
      var p = panelForRoot(r);
      var t = r.querySelector("[data-market-overlay-trigger]");
      if (p) {
        p.classList.add("hidden");
        resetMarketOverlayPanelPosition(p);
        undockMarketOverlayPanel(p);
      }
      if (t) t.setAttribute("aria-expanded", "false");
    });
    syncOverlayOpenBodyClass();
  }

  function resolveMarketOverlayOnChange(root) {
    if (!root) return null;
    var hook = root.getAttribute("data-overlay-on-change");
    if (hook && typeof global[hook] === "function") {
      return global[hook];
    }
    var rootId = root.id || "";
    if (rootId === "mz-market-overlay-root") {
      if (typeof global.mzOnMarketOverlayChange === "function") {
        return global.mzOnMarketOverlayChange;
      }
      if (typeof global.refreshChartLine === "function") {
        return function () {
          global.refreshChartLine();
        };
      }
      return null;
    }
    if (rootId === "seo-market-overlay-root") {
      if (typeof global.ga4RerenderTrendChartsForMarket === "function") {
        return global.ga4RerenderTrendChartsForMarket;
      }
      if (typeof global.scRerenderTrendChartsForMarket === "function") {
        return global.scRerenderTrendChartsForMarket;
      }
    }
    return null;
  }

  function isDarkUi() {
    var html = document.documentElement;
    return html.classList.contains("dark") || html.classList.contains("midnight");
  }

  function standaloneDateKeys(series, keys, startIso, endIso) {
    var set = {};
    (keys || []).forEach(function (sk) {
      var block = series && series[sk];
      ((block && block.by_date) || []).forEach(function (pt) {
        if (!pt || !pt.date) return;
        var d = String(pt.date).slice(0, 10);
        if (startIso && d < startIso) return;
        if (endIso && d > endIso) return;
        set[d] = true;
      });
    });
    return Object.keys(set).sort();
  }

  function renderStandalone(plotEl, overlayMode, startIso, endIso, opts) {
    var el = typeof plotEl === "string" ? document.getElementById(plotEl) : plotEl;
    if (!el || !global.Plotly) return Promise.resolve(false);
    opts = opts || {};
    var keys = resolveSeriesKeys(overlayMode);
    var emptyEl = opts.emptyEl || null;
    if (!keys.length) {
      try {
        Plotly.purge(el);
      } catch (e) {
        el.innerHTML = "";
      }
      if (emptyEl) emptyEl.classList.remove("hidden");
      el.classList.add("hidden");
      return Promise.resolve(false);
    }
    el.classList.remove("hidden");
    if (emptyEl) emptyEl.classList.add("hidden");
    var indexed = useIndexedScale(keys, overlayMode);
    return ensureOverlay(startIso, endIso)
      .then(function (payload) {
        var series = (payload && payload.series) || {};
        var dateKeys = standaloneDateKeys(series, keys, startIso, endIso);
        if (!dateKeys.length) {
          if (emptyEl) {
            emptyEl.classList.remove("hidden");
            emptyEl.textContent = "Seçilen aralıkta piyasa verisi yok.";
          }
          el.classList.add("hidden");
          return false;
        }
        var traces = [];
        var colorIdx = 0;
        keys.forEach(function (sk) {
          var block = series[sk];
          if (!block) return;
          var clos = closeMap(block);
          var ys = dateKeys.map(function (d) {
            return clos[d] != null ? clos[d] : null;
          });
          if (indexed) {
            var indexedYs = indexSeries(ys);
            if (!indexedYs) return;
            ys = indexedYs;
          }
          if (
            !ys.some(function (v) {
              return v != null;
            })
          )
            return;
          var lineColor =
            keys.length === 1 && !indexed
              ? LINE_COLOR
              : SERIES_COLORS[colorIdx % SERIES_COLORS.length];
          colorIdx += 1;
          var legendName = (block.label || sk) + (indexed ? " %" : "");
          var legendGroup = "seo_mkt_" + sk + (indexed ? "_i" : "");
          traces.push({
            x: dateKeys,
            y: ys,
            type: "scatter",
            mode: "lines",
            name: legendName,
            legendgroup: legendGroup,
            visible: true,
            line: { color: lineColor, width: 2 },
            connectgaps: false,
            hovertemplate: "%{x}<br>%{y}<extra>" + legendName + "</extra>",
          });
          marketGapBridgeTraces(dateKeys, ys, lineColor, "y", legendGroup, legendName).forEach(
            function (bt) {
              traces.push(bt);
            }
          );
        });
        if (!traces.length) {
          if (emptyEl) emptyEl.classList.remove("hidden");
          el.classList.add("hidden");
          return false;
        }
        var dark = isDarkUi();
        var layout = {
          paper_bgcolor: "rgba(0,0,0,0)",
          plot_bgcolor: "rgba(0,0,0,0)",
          font: { color: dark ? "#d4d4d8" : "#334155", size: 11 },
          margin: { l: 48, r: 16, t: 28, b: 40 },
          showlegend: true,
          legend: { orientation: "h", y: 1.12, x: 0, font: { size: 10 } },
          xaxis: {
            type: "date",
            gridcolor: dark ? "rgba(255,255,255,0.06)" : "rgba(15,23,42,0.06)",
            zeroline: false,
          },
          yaxis: {
            title: indexed ? "Endeks (100)" : keys.length > 1 ? "Piyasa" : "",
            gridcolor: dark ? "rgba(255,255,255,0.06)" : "rgba(15,23,42,0.06)",
            zeroline: false,
            automargin: true,
          },
          hovermode: "x unified",
        };
        Plotly.react(el, traces, layout, {
          displayModeBar: false,
          responsive: true,
        });
        bindMarketLegendGroupSync(el);
        return true;
      })
      .catch(function () {
        if (emptyEl) emptyEl.classList.remove("hidden");
        return false;
      });
  }

  function bindStandaloneCard(card) {
    if (!card || card.dataset.marketStandaloneBound === "1") return;
    card.dataset.marketStandaloneBound = "1";
    var controlId = card.getAttribute("data-control-id");
    var plotId = card.getAttribute("data-plot-id");
    var startId = card.getAttribute("data-start-input");
    var endId = card.getAttribute("data-end-input");
    var emptyEl = card.querySelector("[data-market-standalone-empty]");
    function redraw() {
      var startEl = startId ? document.getElementById(startId) : null;
      var endEl = endId ? document.getElementById(endId) : null;
      var startIso = startEl && startEl.value ? startEl.value : "";
      var endIso = endEl && endEl.value ? endEl.value : "";
      renderStandalone(plotId, modes(controlId), startIso, endIso, { emptyEl: emptyEl });
    }
    ensureBound(controlId, redraw);
    [startId, endId].forEach(function (id) {
      var el = id ? document.getElementById(id) : null;
      if (!el) return;
      el.addEventListener("change", redraw);
      el.addEventListener("input", redraw);
    });
    var runBtn = document.getElementById(startId && startId.indexOf("ia-") === 0 ? "ia-run" : "pa-run");
    if (runBtn) runBtn.addEventListener("click", function () {
      global.setTimeout(redraw, 80);
    });
    var preset = document.getElementById(startId && startId.indexOf("ia-") === 0 ? "ia-preset" : "pa-preset");
    if (preset) preset.addEventListener("change", function () {
      global.setTimeout(redraw, 80);
    });
    redraw();
    global.setTimeout(redraw, 400);
    global.setTimeout(redraw, 1400);
  }

  function autoBindStandalones() {
    document.querySelectorAll("[data-market-standalone]").forEach(bindStandaloneCard);
  }

  function autoBindMarketOverlays() {
    document.querySelectorAll("[data-market-overlay-root]").forEach(function (root) {
      if (root.closest("[data-market-standalone]")) return;
      if (root.dataset.marketOverlayBound === "1") return;
      bindPanel(root, resolveMarketOverlayOnChange(root));
    });
    autoBindStandalones();
  }

  function installMarketOverlayAutoBind() {
    if (global.__seoMarketOverlayAutoBindInstalled) return;
    global.__seoMarketOverlayAutoBindInstalled = true;
    function run() {
      autoBindMarketOverlays();
    }
    if (document.readyState === "loading") {
      document.addEventListener("DOMContentLoaded", run);
    } else {
      run();
    }
    if (document.body) {
      document.body.addEventListener("htmx:afterSwap", run);
      document.body.addEventListener("htmx:load", run);
    } else {
      document.addEventListener("DOMContentLoaded", function () {
        document.body.addEventListener("htmx:afterSwap", run);
        document.body.addEventListener("htmx:load", run);
      });
    }
  }

  installMarketOverlayAutoBind();

  global.SeoMarketOverlay = {
    LINE_COLOR: LINE_COLOR,
    SERIES_COLORS: SERIES_COLORS,
    INDEXED_KEYS: INDEXED_KEYS,
    OPTION_LABELS: OPTION_LABELS,
    mode: mode,
    modes: modes,
    clearCache: clearCache,
    ensureOverlay: ensureOverlay,
    pointsForSeries: pointsForSeries,
    indexPointSeries: indexPointSeries,
    seriesLabel: seriesLabel,
    apply: apply,
    bindSelect: bindSelect,
    bindWhenReady: bindWhenReady,
    ensureBound: ensureBound,
    ensureClosed: ensureClosed,
    autoBindMarketOverlays: autoBindMarketOverlays,
    autoBindStandalones: autoBindStandalones,
    renderStandalone: renderStandalone,
    bindPanel: bindPanel,
    bindLegendGroupSync: bindMarketLegendGroupSync,
    pickFreeYaxisId: pickFreeYaxisId,
    normalizeDateKey: normalizeDateKey,
  };
})(typeof window !== "undefined" ? window : this);
