/**
 * Empower uygulama metrikleri — Plotly overlay (/api/mz-analytics/app-empower).
 */
(function (global) {
  "use strict";

  /** Tezatlı sıra: mavi → sarı → yeşil → turuncu → magenta … (birbirine yakın mor/lacivert yok) */
  var SERIES_COLORS = [
    "#2563EB",
    "#CA8A04",
    "#16A34A",
    "#EA580C",
    "#C026D3",
    "#0891B2",
    "#BE123C",
    "#4F46E5",
  ];
  var LINE_COLOR = SERIES_COLORS[0];

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
    var rid = root.id;
    if (rid) {
      var docked = document.querySelector(
        '[data-app-empower-overlay-panel][data-app-empower-overlay-for="' + rid + '"]'
      );
      if (docked) return docked;
    }
    return root.querySelector("[data-app-empower-overlay-panel]");
  }

  function selectedFromDom(root) {
    if (!root) return [];
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
    if (!keys.length) keys = readStored(root);
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

  function applyResponsiveMarginRight(layout, opts) {
    var m = layout.margin || { l: 52, r: 52, t: 20, b: 56 };
    var w = opts.chartWidth;
    if (!w && typeof global.innerWidth === "number") {
      w = global.innerWidth;
    }
    w = w || 640;
    var base = opts.marginRight != null ? opts.marginRight : 56;
    var capped = Math.min(base, Math.max(40, Math.round(w * 0.1)));
    layout.margin = Object.assign({}, m, { r: Math.max(m.r || 12, capped) });
  }

  function hiddenOverlayYaxisLayout() {
    return {
      overlaying: "y",
      side: "right",
      showgrid: false,
      zeroline: false,
      automargin: false,
      showticklabels: false,
      showline: false,
      ticks: "",
      title: { text: "" },
    };
  }

  function visibleEmpowerYaxisLayout(lineColor) {
    return {
      title: { text: "Empower", font: { size: 10, color: lineColor } },
      tickfont: { size: 10, color: lineColor },
      overlaying: "y",
      side: "right",
      showgrid: false,
      zeroline: false,
      automargin: true,
      nticks: 5,
    };
  }

  function apply(traces, layout, dateKeys, platform, overlayKeys, opts) {
    var keys = overlayKeys || [];
    if (!keys.length || !platform || !dateKeys || !dateKeys.length || !traces || !layout) {
      return Promise.resolve(false);
    }
    opts = opts || {};
    var startIso = normalizeDateKey(dateKeys[0]);
    var endIso = normalizeDateKey(dateKeys[dateKeys.length - 1]);
    return ensureOverlay(platform, startIso, endIso)
      .then(function (payload) {
        var series = (payload && payload.series) || {};
        var added = false;
        var colorIdx = 0;
        var empowerAxisCount = 0;
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
          var traceYaxis = pickFreeYaxisId(layout, traces);
          var layoutKey = layoutKeyForYaxis(traceYaxis);
          var axisTitle = block.label || sk;
          var showAxisChrome = empowerAxisCount === 0;
          traces.push({
            x: dateKeys,
            y: ys,
            type: "scatter",
            mode: "lines",
            name: axisTitle,
            visible: true,
            yaxis: traceYaxis,
            line: { color: lineColor, width: 2 },
            connectgaps: false,
            hovertemplate: axisTitle + ": %{y:,.4~g}<extra></extra>",
          });
          empowerAxisCount += 1;
          layout[layoutKey] = Object.assign(
            showAxisChrome ? visibleEmpowerYaxisLayout(lineColor) : hiddenOverlayYaxisLayout(),
            layout[layoutKey] || {}
          );
          added = true;
        });
        if (!added) return false;
        applyResponsiveMarginRight(layout, opts);
        layout.showlegend = true;
        return true;
      })
      .catch(function () {
        return false;
      });
  }

  function dockPanel(panel) {
    if (!panel || panel.dataset.appEmpowerOverlayDocked === "1") return;
    panel._appEmpowerHome = { parent: panel.parentNode, next: panel.nextSibling };
    document.body.appendChild(panel);
    panel.dataset.appEmpowerOverlayDocked = "1";
  }

  function undockPanel(panel) {
    if (!panel || panel.dataset.appEmpowerOverlayDocked !== "1") return;
    var home = panel._appEmpowerHome;
    if (home && home.parent) {
      if (home.next) home.parent.insertBefore(panel, home.next);
      else home.parent.appendChild(panel);
    }
    delete panel._appEmpowerHome;
    delete panel.dataset.appEmpowerOverlayDocked;
  }

  function resetPanelPosition(panel) {
    if (!panel) return;
    panel.style.position = "";
    panel.style.top = "";
    panel.style.left = "";
    panel.style.right = "";
    panel.style.zIndex = "";
    panel.style.width = "";
    panel.style.maxWidth = "";
    panel.style.maxHeight = "";
    panel.style.overflowY = "";
  }

  function positionPanel(trigger, panel) {
    if (!trigger || !panel) return;
    dockPanel(panel);
    var margin = 8;
    var maxW = Math.max(220, Math.min(320, window.innerWidth - margin * 2));
    panel.style.width = maxW + "px";
    panel.style.maxWidth = maxW + "px";
    panel.style.maxHeight = Math.min(Math.round(window.innerHeight * 0.7), 360) + "px";
    panel.style.overflowY = "auto";
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

  function closePanel(root, trigger, panel) {
    if (!panel) return;
    panel.classList.add("hidden");
    resetPanelPosition(panel);
    undockPanel(panel);
    if (trigger) trigger.setAttribute("aria-expanded", "false");
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
      if (root.id) panel.setAttribute("data-app-empower-overlay-for", root.id);
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
        global.__appEmpowerOverlayIgnoreCloseUntil = Date.now() + 120;
        if (panel.classList.contains("hidden")) {
          panel.classList.remove("hidden");
          positionPanel(trigger, panel);
          trigger.setAttribute("aria-expanded", "true");
        } else {
          closePanel(root, trigger, panel);
        }
      });
    }
    if (!global.__appEmpowerOverlayDocClose) {
      global.__appEmpowerOverlayDocClose = true;
      document.addEventListener("click", function (ev) {
        if (global.__appEmpowerOverlayIgnoreCloseUntil && Date.now() < global.__appEmpowerOverlayIgnoreCloseUntil) {
          return;
        }
        var target = ev.target;
        document.querySelectorAll("[data-app-empower-overlay-root]").forEach(function (r) {
          var p = panelForRoot(r);
          var t = r.querySelector("[data-app-empower-overlay-trigger]");
          if (target && (r.contains(target) || (p && p.contains(target)))) return;
          closePanel(r, t, p);
        });
      });
      global.addEventListener("resize", function () {
        document.querySelectorAll("[data-app-empower-overlay-panel]").forEach(function (p) {
          if (p.classList.contains("hidden")) return;
          var forId = p.getAttribute("data-app-empower-overlay-for");
          var r = forId ? document.getElementById(forId) : null;
          var t = r && r.querySelector("[data-app-empower-overlay-trigger]");
          if (t) positionPanel(t, p);
        });
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
