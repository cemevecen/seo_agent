/**
 * Empower uygulama metrikleri — Plotly overlay (/api/mz-analytics/app-empower).
 */
(function (global) {
  "use strict";

  var LINE_COLOR = "#6D28D9";
  var SERIES_COLORS = ["#6D28D9", "#2563EB", "#059669", "#D97706", "#DC2626", "#0891B2"];

  var cache = { payload: null, rangeKey: "", pending: null };

  function rootEl(controlId) {
    if (controlId) {
      var byId = document.getElementById(controlId);
      if (byId && byId.getAttribute("data-app-empower-overlay-root") != null) return byId;
    }
    return document.querySelector("[data-app-empower-overlay-root]");
  }

  function storageKeyForRoot(root) {
    return "app-empower-overlay-keys-v1-" + ((root && root.getAttribute("data-overlay-storage-key")) || "empower");
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

  function panelForRoot(root) {
    if (!root) return null;
    return root.querySelector("[data-app-empower-overlay-panel]");
  }

  function selectedFromDom(root) {
    var panel = panelForRoot(root);
    if (!panel) return [];
    var keys = [];
    panel.querySelectorAll("input[type=checkbox]:checked").forEach(function (cb) {
      if (cb.value) keys.push(cb.value);
    });
    return keys;
  }

  function updateTriggerLabel(root) {
    if (!root) return;
    var labelEl = root.querySelector("[data-app-empower-overlay-label]");
    if (!labelEl) return;
    var keys = selectedFromDom(root);
    if (!keys.length) {
      labelEl.textContent = "Empower: kapalı";
      return;
    }
    if (keys.length === 1) {
      labelEl.textContent = "Empower: " + keys[0];
      return;
    }
    labelEl.textContent = "Empower: " + keys.length + " seri";
  }

  function modes(controlId) {
    var root = rootEl(controlId);
    if (!root) return [];
    if (root.dataset.appEmpowerOverlayBound === "1") {
      return selectedFromDom(root);
    }
    var keys = selectedFromDom(root);
    if (!keys.length) keys = readStored(root);
    return keys;
  }

  function clearCache() {
    cache.payload = null;
    cache.rangeKey = "";
    cache.pending = null;
  }

  function normalizeDateKey(d) {
    if (d == null || d === "") return "";
    var s = String(d).trim();
    if (s.length >= 10 && s.charAt(4) === "-") return s.slice(0, 10);
    var parsed = Date.parse(s);
    if (!isNaN(parsed)) return new Date(parsed).toISOString().slice(0, 10);
    return s.slice(0, 10);
  }

  function valueMap(seriesBlock) {
    var m = {};
    ((seriesBlock && seriesBlock.by_date) || []).forEach(function (pt) {
      if (pt && pt.date) m[normalizeDateKey(pt.date)] = Number(pt.value);
    });
    return m;
  }

  function ensureOverlay(platform, startIso, endIso) {
    var key = (platform || "") + "|" + (startIso || "") + "|" + (endIso || "");
    if (cache.payload && cache.rangeKey === key) {
      return Promise.resolve(cache.payload);
    }
    if (cache.pending) return cache.pending;
    var p = new URLSearchParams();
    p.set("platform", platform);
    if (startIso) p.set("start", startIso);
    if (endIso) p.set("end", endIso);
    cache.pending = fetch("/api/mz-analytics/app-empower/overlay?" + p.toString(), { credentials: "same-origin" })
      .then(function (r) {
        if (!r.ok) throw new Error("Empower verisi alınamadı");
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
      used[t.yaxis || "y"] = true;
    });
    for (var n = 3; n <= 8; n++) {
      if (!used["y" + n]) return "y" + n;
    }
    return "y8";
  }

  function layoutKeyForYaxis(yaxisId) {
    var id = yaxisId || "y5";
    if (id === "y") return "yaxis";
    var n = id.replace(/^y/, "");
    return n ? "yaxis" + n : "yaxis";
  }

  function apply(traces, layout, dateKeys, platform, overlayKeys, opts) {
    var keys = overlayKeys || [];
    if (!keys.length || !platform || !dateKeys || !dateKeys.length || !traces || !layout) {
      return Promise.resolve(false);
    }
    opts = opts || {};
    var yaxisId = opts.yaxisId || pickFreeYaxisId(layout, traces);
    var layoutKey = layoutKeyForYaxis(yaxisId);
    var startIso = normalizeDateKey(dateKeys[0]);
    var endIso = normalizeDateKey(dateKeys[dateKeys.length - 1]);
    return ensureOverlay(platform, startIso, endIso)
      .then(function (payload) {
        var series = (payload && payload.series) || {};
        var added = false;
        var colorIdx = 0;
        keys.forEach(function (sk) {
          var block = series[sk];
          if (!block) return;
          var vals = valueMap(block);
          var ys = dateKeys.map(function (d) {
            var k = normalizeDateKey(d);
            return vals[k] != null ? vals[k] : null;
          });
          if (!ys.some(function (v) { return v != null; })) return;
          var lineColor = SERIES_COLORS[colorIdx % SERIES_COLORS.length];
          colorIdx += 1;
          traces.push({
            x: dateKeys,
            y: ys,
            type: "scatter",
            mode: "lines",
            name: "Empower · " + (block.label || sk),
            visible: true,
            yaxis: yaxisId,
            line: { color: lineColor, width: 2 },
            connectgaps: false,
          });
          added = true;
        });
        if (!added) return false;
        layout[layoutKey] = Object.assign(
          {
            title: { text: "Empower", font: { size: 11, color: LINE_COLOR } },
            tickfont: { size: 10, color: LINE_COLOR },
            overlaying: "y",
            side: "right",
            showgrid: false,
            zeroline: false,
            automargin: true,
          },
          layout[layoutKey] || {}
        );
        var m = layout.margin || { l: 52, r: 52, t: 20, b: 56 };
        var minR = opts.marginRight != null ? opts.marginRight : 72;
        layout.margin = Object.assign({}, m, { r: Math.max(m.r || 12, minR) });
        layout.showlegend = true;
        return true;
      })
      .catch(function () {
        return false;
      });
  }

  function bindRoot(root, onChange) {
    if (!root || root.dataset.appEmpowerOverlayBound === "1") return;
    root.dataset.appEmpowerOverlayBound = "1";
    var panel = panelForRoot(root);
    var trigger = root.querySelector("[data-app-empower-overlay-trigger]");
    var stored = readStored(root);
    if (panel) {
      panel.querySelectorAll("input[type=checkbox]").forEach(function (cb) {
        cb.checked = stored.indexOf(cb.value) >= 0;
      });
    }
    updateTriggerLabel(root);
    function fire() {
      writeStored(root, selectedFromDom(root));
      updateTriggerLabel(root);
      clearCache();
      if (typeof onChange === "function") onChange();
      var attr = root.getAttribute("data-overlay-on-change");
      if (attr && typeof global[attr] === "function") global[attr]();
    }
    if (panel) {
      panel.querySelectorAll("input[type=checkbox]").forEach(function (cb) {
        cb.addEventListener("change", fire);
      });
    }
    if (trigger && panel) {
      trigger.addEventListener("click", function (ev) {
        ev.stopPropagation();
        var open = !panel.classList.contains("hidden");
        panel.classList.toggle("hidden", open);
        trigger.setAttribute("aria-expanded", open ? "false" : "true");
      });
      document.addEventListener("click", function (ev) {
        if (!root.contains(ev.target)) {
          panel.classList.add("hidden");
          trigger.setAttribute("aria-expanded", "false");
        }
      });
    }
  }

  function bindWhenReady(controlId, onChange) {
    function tryBind() {
      var root = rootEl(controlId);
      if (!root) return false;
      bindRoot(root, onChange);
      return true;
    }
    if (tryBind()) return;
    var n = 0;
    var t = global.setInterval(function () {
      n += 1;
      if (tryBind() || n > 80) global.clearInterval(t);
    }, 100);
  }

  global.AppEmpowerOverlay = {
    modes: modes,
    apply: apply,
    clearCache: clearCache,
    bindWhenReady: bindWhenReady,
    ensureBound: bindWhenReady,
  };
})(typeof window !== "undefined" ? window : this);
