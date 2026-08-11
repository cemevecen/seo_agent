/**
 * Metrik veri tablosu UX: heat hücre boyası, sütun genişletme, sürükleyerek sıralama.
 * Android / iOS ortak.
 */
(function (global) {
  "use strict";

  var STYLE_ID = "seo-mtux-style";

  function injectStyles() {
    var style = document.getElementById(STYLE_ID);
    if (!style) {
      style = document.createElement("style");
      style.id = STYLE_ID;
      document.head.appendChild(style);
    }
    style.textContent =
      ".mtux-heat-cell{border-radius:0.35rem;}" +
      ".mtux-legend{display:flex;flex-wrap:wrap;align-items:center;justify-content:flex-end;gap:0.5rem 0.75rem;" +
      "padding:0.4rem 0.75rem 0.45rem;border-bottom:1px solid rgba(148,163,184,0.28);" +
      "font-size:0.62rem;font-weight:600;letter-spacing:0.04em;text-transform:uppercase;color:#94a3b8;}" +
      "html.dark .mtux-legend{color:#71717a;border-bottom-color:rgba(113,113,122,0.45);}" +
      ".mtux-legend-bar{display:flex;height:0.4rem;width:7.5rem;border-radius:9999px;overflow:hidden;" +
      "border:1px solid rgba(148,163,184,0.35);}" +
      ".mtux-legend-bar>span{flex:1;}" +
      ".mtux-legend.is-off .mtux-legend-bar{opacity:0.25;filter:grayscale(1);}" +
      ".mtux-legend-scale{margin-right:auto;}" +
      ".mtux-heat-toggle{display:inline-flex;align-items:center;gap:0.35rem;" +
      "padding:0.2rem 0.55rem;border-radius:9999px;border:1px solid rgba(148,163,184,0.45);" +
      "background:transparent;color:#64748b;font-size:0.62rem;font-weight:700;letter-spacing:0.03em;" +
      "text-transform:uppercase;cursor:pointer;}" +
      ".mtux-heat-toggle:hover{border-color:#0ea5e9;color:#0ea5e9;}" +
      ".mtux-heat-toggle[aria-pressed='true']{border-color:#94a3b8;background:rgba(148,163,184,0.12);color:#475569;}" +
      "html.dark .mtux-heat-toggle{color:#a1a1aa;border-color:rgba(113,113,122,0.55);}" +
      "html.dark .mtux-heat-toggle[aria-pressed='true']{background:rgba(63,63,70,0.55);color:#e4e4e7;}" +
      "th.mtux-th{position:relative;user-select:none;}" +
      "th.mtux-th.is-dragging{opacity:0.55;}" +
      "th.mtux-th.is-drag-over{box-shadow:inset 2px 0 0 #0ea5e9;}" +
      ".mtux-col-resizer{position:absolute;top:0;right:0;width:6px;height:100%;cursor:col-resize;" +
      "z-index:3;}" +
      ".mtux-col-resizer:hover,.mtux-col-resizer.is-active{background:rgba(14,165,233,0.35);}" +
      ".mtux-drag-hint{opacity:0.45;font-size:0.65rem;margin-right:0.2rem;cursor:grab;flex:0 0 auto;}" +
      "th.mtux-th:active .mtux-drag-hint{cursor:grabbing;}" +
      "table.mtux-interactive{table-layout:fixed;width:max-content;min-width:100%;}" +
      "table.mtux-interactive th.mtux-th{overflow:visible;text-overflow:clip;white-space:nowrap;" +
      "vertical-align:middle;}" +
      "table.mtux-interactive th.mtux-th:not([data-mtux-fixed='1']){min-width:8.5rem;}" +
      "table.mtux-interactive th[data-mtux-fixed='1']{min-width:5.5rem;}" +
      "table.mtux-interactive td{overflow:hidden;text-overflow:ellipsis;white-space:nowrap;}" +
      ".mtux-th-label{display:inline-flex;align-items:center;justify-content:flex-end;gap:0.25rem;" +
      "max-width:100%;min-width:0;vertical-align:middle;}" +
      ".mtux-th-text{overflow:hidden;text-overflow:ellipsis;white-space:nowrap;max-width:11rem;}";
  }

  var MIN_COL_WIDTH = 110;
  var MAX_COL_WIDTH = 420;

  function parseColor(color) {
    var c = String(color || "#2563eb").trim();
    if (c.charAt(0) === "#" && (c.length === 7 || c.length === 4)) {
      if (c.length === 4) {
        c = "#" + c[1] + c[1] + c[2] + c[2] + c[3] + c[3];
      }
      return {
        r: parseInt(c.slice(1, 3), 16),
        g: parseInt(c.slice(3, 5), 16),
        b: parseInt(c.slice(5, 7), 16),
      };
    }
    var m = c.match(/rgba?\(\s*(\d+)\s*,\s*(\d+)\s*,\s*(\d+)/i);
    if (m) return { r: +m[1], g: +m[2], b: +m[3] };
    return { r: 37, g: 99, b: 235 };
  }

  function heatBackground(color, t) {
    var rgb = parseColor(color);
    var a = 0.1 + Math.max(0, Math.min(1, t)) * 0.42;
    return "rgba(" + rgb.r + "," + rgb.g + "," + rgb.b + "," + a.toFixed(3) + ")";
  }

  function colMinMax(values) {
    var min = Infinity;
    var max = -Infinity;
    (values || []).forEach(function (v) {
      if (v == null || !Number.isFinite(v)) return;
      if (v < min) min = v;
      if (v > max) max = v;
    });
    if (!Number.isFinite(min) || !Number.isFinite(max)) return { min: 0, max: 0 };
    return { min: min, max: max };
  }

  function heatT(v, min, max) {
    if (v == null || !Number.isFinite(v)) return null;
    if (max <= min) return 0.45;
    return (v - min) / (max - min);
  }

  function readJson(key, fallback) {
    try {
      var raw = localStorage.getItem(key);
      if (!raw) return fallback;
      var v = JSON.parse(raw);
      return v == null ? fallback : v;
    } catch (e) {
      return fallback;
    }
  }

  function writeJson(key, value) {
    try {
      localStorage.setItem(key, JSON.stringify(value));
    } catch (e) {}
  }

  function orderKeys(preferred, available) {
    var avail = (available || []).slice();
    var out = [];
    var seen = {};
    (preferred || []).forEach(function (k) {
      if (avail.indexOf(k) >= 0 && !seen[k]) {
        out.push(k);
        seen[k] = true;
      }
    });
    avail.forEach(function (k) {
      if (!seen[k]) out.push(k);
    });
    return out;
  }

  var HEAT_PREF_KEY = "mtux-heat-enabled";

  function isHeatEnabled() {
    var v = readJson(HEAT_PREF_KEY, true);
    return v !== false;
  }

  function setHeatEnabled(on) {
    writeJson(HEAT_PREF_KEY, !!on);
  }

  function syncLegendState(legend) {
    if (!legend) return;
    var on = isHeatEnabled();
    legend.classList.toggle("is-off", !on);
    var btn = legend.querySelector(".mtux-heat-toggle");
    if (btn) {
      btn.setAttribute("aria-pressed", on ? "false" : "true");
      btn.textContent = on ? "Remove colors" : "Show colors";
    }
    var scale = legend.querySelector(".mtux-legend-scale");
    if (scale) scale.style.visibility = on ? "visible" : "hidden";
  }

  function placeLegendAtTop(shell, legend) {
    if (!shell || !legend) return;
    var scroll = shell.querySelector(".rdl-scroll") || shell.querySelector("[id$='-table-wrap']");
    if (scroll && scroll.parentNode === shell) {
      if (legend.nextSibling !== scroll) shell.insertBefore(legend, scroll);
    } else if (shell.firstChild !== legend) {
      shell.insertBefore(legend, shell.firstChild);
    }
  }

  function ensureLegend(shell, opts) {
    opts = opts || {};
    if (!shell) return;
    var onToggle = typeof opts.onHeatToggle === "function" ? opts.onHeatToggle : null;
    var existing = shell.querySelector(".mtux-legend");
    if (existing) {
      placeLegendAtTop(shell, existing);
      // Her render’da güncel callback — aksi halde ilk açılıştaki stale bundles ile tablo eski metrik setine döner
      existing._mtuxOnHeatToggle = onToggle;
      if (!existing._mtuxHeatBound) {
        existing._mtuxHeatBound = true;
        existing.addEventListener("click", function (ev) {
          var btn = ev.target && ev.target.closest ? ev.target.closest(".mtux-heat-toggle") : null;
          if (!btn) return;
          ev.preventDefault();
          setHeatEnabled(!isHeatEnabled());
          syncLegendState(existing);
          if (typeof existing._mtuxOnHeatToggle === "function") {
            existing._mtuxOnHeatToggle(isHeatEnabled());
          }
        });
      }
      syncLegendState(existing);
      return existing;
    }
    var legend = document.createElement("div");
    legend.className = "mtux-legend";
    legend.innerHTML =
      '<span class="mtux-legend-scale" style="display:inline-flex;align-items:center;gap:0.5rem;">' +
        '<span class="mtux-legend-bar">' +
          '<span style="background:rgba(37,99,235,0.12)"></span>' +
          '<span style="background:rgba(37,99,235,0.28)"></span>' +
          '<span style="background:rgba(37,99,235,0.42)"></span>' +
          '<span style="background:rgba(37,99,235,0.55)"></span>' +
        "</span>" +
        "<span>low → high</span>" +
      "</span>" +
      '<button type="button" class="mtux-heat-toggle">Remove colors</button>';
    placeLegendAtTop(shell, legend);
    legend._mtuxOnHeatToggle = onToggle;
    legend._mtuxHeatBound = true;
    legend.addEventListener("click", function (ev) {
      var btn = ev.target && ev.target.closest ? ev.target.closest(".mtux-heat-toggle") : null;
      if (!btn) return;
      ev.preventDefault();
      setHeatEnabled(!isHeatEnabled());
      syncLegendState(legend);
      if (typeof legend._mtuxOnHeatToggle === "function") {
        legend._mtuxOnHeatToggle(isHeatEnabled());
      }
    });
    syncLegendState(legend);
    return legend;
  }

  function applyWidths(table, widths) {
    if (!table || !widths) return;
    var ths = table.querySelectorAll("thead th[data-mtux-key]");
    ths.forEach(function (th) {
      var key = th.getAttribute("data-mtux-key");
      var w = Number(widths[key]);
      // Eski dar records başlık "..." oluyordu — yok say
      if (!Number.isFinite(w) || w < MIN_COL_WIDTH) return;
      w = Math.min(MAX_COL_WIDTH, Math.round(w));
      th.style.width = w + "px";
      th.style.minWidth = w + "px";
      th.style.maxWidth = w + "px";
    });
  }

  function ensureHeaderMinWidths(table) {
    if (!table) return;
    var ths = table.querySelectorAll("thead th[data-mtux-key]:not([data-mtux-fixed='1'])");
    ths.forEach(function (th) {
      var cur = parseFloat(th.style.minWidth) || th.getBoundingClientRect().width || 0;
      if (cur < MIN_COL_WIDTH) {
        th.style.minWidth = MIN_COL_WIDTH + "px";
        if (!th.style.width) th.style.width = MIN_COL_WIDTH + "px";
      }
    });
  }

  function bindInteractive(table, opts) {
    opts = opts || {};
    if (!table) return;
    injectStyles();
    table.classList.add("mtux-interactive");
    var widthsKey = opts.widthsKey || "";
    var orderKey = opts.orderKey || "";
    var onOrder = typeof opts.onOrderChange === "function" ? opts.onOrderChange : null;
    var widths = widthsKey ? readJson(widthsKey, {}) : {};
    // Dar geçmiş records clear
    if (widthsKey && widths && typeof widths === "object") {
      var cleaned = {};
      var dirty = false;
      Object.keys(widths).forEach(function (k) {
        var w = Number(widths[k]);
        if (Number.isFinite(w) && w >= MIN_COL_WIDTH) cleaned[k] = w;
        else dirty = true;
      });
      if (dirty) {
        widths = cleaned;
        writeJson(widthsKey, cleaned);
      }
    }
    applyWidths(table, widths);
    ensureHeaderMinWidths(table);

    var ths = Array.prototype.slice.call(table.querySelectorAll("thead th[data-mtux-key]"));
    ths.forEach(function (th) {
      th.classList.add("mtux-th");
      if (th.getAttribute("data-mtux-fixed") === "1") return;

      if (!th.querySelector(".mtux-col-resizer")) {
        var resizer = document.createElement("span");
        resizer.className = "mtux-col-resizer";
        resizer.title = "Drag to resize column";
        th.appendChild(resizer);
        resizer.addEventListener("pointerdown", function (ev) {
          ev.preventDefault();
          ev.stopPropagation();
          var startX = ev.clientX;
          var startW = th.getBoundingClientRect().width;
          resizer.classList.add("is-active");
          function onMove(e) {
            var next = Math.max(MIN_COL_WIDTH, Math.min(MAX_COL_WIDTH, Math.round(startW + (e.clientX - startX))));
            th.style.width = next + "px";
            th.style.minWidth = next + "px";
            th.style.maxWidth = next + "px";
            var key = th.getAttribute("data-mtux-key");
            if (key && widthsKey) {
              widths[key] = next;
              writeJson(widthsKey, widths);
            }
          }
          function onUp() {
            resizer.classList.remove("is-active");
            document.removeEventListener("pointermove", onMove);
            document.removeEventListener("pointerup", onUp);
          }
          document.addEventListener("pointermove", onMove);
          document.addEventListener("pointerup", onUp);
        });
      }

      if (!th.querySelector(".mtux-drag-hint")) {
        var hint = document.createElement("span");
        hint.className = "mtux-drag-hint";
        hint.setAttribute("aria-hidden", "true");
        hint.textContent = "⋮⋮";
        var label = th.querySelector(".mtux-th-label") || th.firstElementChild;
        if (label) th.insertBefore(hint, label);
        else th.insertBefore(hint, th.firstChild);
      }

      th.setAttribute("draggable", "true");
      th.addEventListener("dragstart", function (ev) {
        if (ev.target && ev.target.classList && ev.target.classList.contains("mtux-col-resizer")) {
          ev.preventDefault();
          return;
        }
        if (ev.target && ev.target.closest && ev.target.closest("button")) {
          ev.preventDefault();
          return;
        }
        th.classList.add("is-dragging");
        try {
          ev.dataTransfer.setData("text/mtux-key", th.getAttribute("data-mtux-key") || "");
          ev.dataTransfer.effectAllowed = "move";
        } catch (e) {}
      });
      th.addEventListener("dragend", function () {
        th.classList.remove("is-dragging");
        table.querySelectorAll("th.is-drag-over").forEach(function (el) {
          el.classList.remove("is-drag-over");
        });
      });
      th.addEventListener("dragover", function (ev) {
        ev.preventDefault();
        th.classList.add("is-drag-over");
      });
      th.addEventListener("dragleave", function () {
        th.classList.remove("is-drag-over");
      });
      th.addEventListener("drop", function (ev) {
        ev.preventDefault();
        th.classList.remove("is-drag-over");
        var fromKey = "";
        try {
          fromKey = ev.dataTransfer.getData("text/mtux-key") || "";
        } catch (e) {}
        var toKey = th.getAttribute("data-mtux-key") || "";
        if (!fromKey || !toKey || fromKey === toKey) return;
        var keys = Array.prototype.map.call(
          table.querySelectorAll("thead th[data-mtux-key]:not([data-mtux-fixed='1'])"),
          function (el) {
            return el.getAttribute("data-mtux-key");
          }
        ).filter(Boolean);
        var fromIdx = keys.indexOf(fromKey);
        var toIdx = keys.indexOf(toKey);
        if (fromIdx < 0 || toIdx < 0) return;
        keys.splice(fromIdx, 1);
        keys.splice(toIdx, 0, fromKey);
        if (orderKey) writeJson(orderKey, keys);
        if (onOrder) onOrder(keys);
      });
    });
  }

  function fitTextToWidth(root, selector, opts) {
    opts = opts || {};
    if (!root) return;
    var minPx = opts.minPx != null ? opts.minPx : 8;
    var sel = selector || ".sf-card__value, .home-sf-metric__value";
    root.querySelectorAll(sel).forEach(function (el) {
      el.style.whiteSpace = "nowrap";
      el.style.overflow = "hidden";
      el.style.fontSize = "";
      var boxW = el.clientWidth;
      if (!(boxW > 1)) return;
      var maxPx = parseFloat(window.getComputedStyle(el).fontSize) || 22;
      if (!(maxPx > minPx)) {
        el.style.fontSize = minPx + "px";
        return;
      }
      if (el.scrollWidth <= boxW + 1) return;
      var lo = minPx;
      var hi = maxPx;
      var best = minPx;
      for (var i = 0; i < 16; i++) {
        var mid = (lo + hi) / 2;
        el.style.fontSize = mid + "px";
        if (el.scrollWidth <= el.clientWidth + 1) {
          best = mid;
          lo = mid;
        } else {
          hi = mid;
        }
      }
      el.style.fontSize = Math.max(minPx, best) + "px";
    });
  }

  var SF_FIT_SEL =
    ".sf-card__title, .sf-card__sub, .sf-card__value, " +
    ".home-sf-metric__title, .home-sf-metric__sub, .home-sf-metric__value, " +
    ".home-store-kpi__l, .home-store-kpi__v";

  function fitSideChips(root) {
    if (!root) return;
    root.querySelectorAll(".sf-card__side, .home-sf-metric__side").forEach(function (side) {
      side.style.fontSize = "";
      var body = side.closest(".sf-card__body, .home-sf-metric__body");
      if (body) body.classList.remove("is-chip-stack");
      var rows = side.querySelectorAll(".sf-card__side-row, .home-sf-metric__side-row");
      if (!rows.length) return;
      var maxPx = parseFloat(window.getComputedStyle(side).fontSize) || 9;
      var minPx = 5;
      function overflowing() {
        for (var i = 0; i < rows.length; i++) {
          var row = rows[i];
          if (row.scrollWidth > row.clientWidth + 0.5) return true;
          var val = row.querySelector(".sf-card__side-val, .home-sf-metric__side-val");
          if (val && val.scrollWidth > val.clientWidth + 0.5) return true;
        }
        return false;
      }
      if (side.clientWidth < 2) return;
      if (!overflowing()) return;
      var lo = minPx;
      var hi = maxPx;
      var best = minPx;
      for (var n = 0; n < 16; n++) {
        var mid = (lo + hi) / 2;
        side.style.fontSize = mid + "px";
        if (!overflowing()) {
          best = mid;
          lo = mid;
        } else {
          hi = mid;
        }
      }
      side.style.fontSize = Math.max(minPx, best) + "px";
      if (overflowing() && body) {
        body.classList.add("is-chip-stack");
        side.style.fontSize = "";
      }
    });
  }

  function fitCardTexts(root, extraSelector) {
    var sel = extraSelector ? SF_FIT_SEL + ", " + extraSelector : SF_FIT_SEL;
    fitTextToWidth(root, sel, { minPx: 8 });
    fitSideChips(root);
  }

  global.SeoMetricTableUx = {
    injectStyles: injectStyles,
    heatBackground: heatBackground,
    colMinMax: colMinMax,
    heatT: heatT,
    isHeatEnabled: isHeatEnabled,
    setHeatEnabled: setHeatEnabled,
    orderKeys: orderKeys,
    readJson: readJson,
    writeJson: writeJson,
    ensureLegend: ensureLegend,
    bindInteractive: bindInteractive,
    applyWidths: applyWidths,
    fitTextToWidth: fitTextToWidth,
    fitSideChips: fitSideChips,
    fitCardTexts: fitCardTexts,
  };
})(typeof window !== "undefined" ? window : this);
