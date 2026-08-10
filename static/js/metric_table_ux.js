/**
 * Metrik veri tablosu UX: heat hücre boyası, sütun genişletme, sürükleyerek sıralama.
 * Android / iOS ortak.
 */
(function (global) {
  "use strict";

  var STYLE_ID = "seo-mtux-style";

  function injectStyles() {
    if (document.getElementById(STYLE_ID)) return;
    var style = document.createElement("style");
    style.id = STYLE_ID;
    style.textContent =
      ".mtux-heat-cell{border-radius:0.35rem;}" +
      ".mtux-legend{display:flex;align-items:center;gap:0.5rem;padding:0.45rem 0.75rem 0.55rem;" +
      "font-size:0.62rem;font-weight:600;letter-spacing:0.04em;text-transform:uppercase;color:#94a3b8;}" +
      "html.dark .mtux-legend{color:#71717a;}" +
      ".mtux-legend-bar{display:flex;height:0.4rem;width:7.5rem;border-radius:9999px;overflow:hidden;" +
      "border:1px solid rgba(148,163,184,0.35);}" +
      ".mtux-legend-bar>span{flex:1;}" +
      "th.mtux-th{position:relative;user-select:none;}" +
      "th.mtux-th.is-dragging{opacity:0.55;}" +
      "th.mtux-th.is-drag-over{box-shadow:inset 2px 0 0 #0ea5e9;}" +
      ".mtux-col-resizer{position:absolute;top:0;right:0;width:6px;height:100%;cursor:col-resize;" +
      "z-index:3;}" +
      ".mtux-col-resizer:hover,.mtux-col-resizer.is-active{background:rgba(14,165,233,0.35);}" +
      ".mtux-drag-hint{opacity:0.45;font-size:0.65rem;margin-right:0.15rem;cursor:grab;}" +
      "th.mtux-th:active .mtux-drag-hint{cursor:grabbing;}" +
      "table.mtux-interactive{table-layout:fixed;}" +
      "table.mtux-interactive th,table.mtux-interactive td{overflow:hidden;text-overflow:ellipsis;}";
    document.head.appendChild(style);
  }

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

  function ensureLegend(shell) {
    if (!shell) return;
    var existing = shell.querySelector(".mtux-legend");
    if (existing) return existing;
    var scroll = shell.querySelector(".rdl-scroll") || shell.querySelector("[id$='-table-wrap']");
    var legend = document.createElement("div");
    legend.className = "mtux-legend";
    legend.setAttribute("aria-hidden", "true");
    legend.innerHTML =
      '<span class="mtux-legend-bar">' +
        '<span style="background:rgba(37,99,235,0.12)"></span>' +
        '<span style="background:rgba(37,99,235,0.28)"></span>' +
        '<span style="background:rgba(37,99,235,0.42)"></span>' +
        '<span style="background:rgba(37,99,235,0.55)"></span>' +
      "</span>" +
      "<span>düşük → yüksek</span>";
    if (scroll && scroll.parentNode === shell) {
      shell.insertBefore(legend, scroll.nextSibling);
    } else {
      shell.appendChild(legend);
    }
    return legend;
  }

  function applyWidths(table, widths) {
    if (!table || !widths) return;
    var ths = table.querySelectorAll("thead th[data-mtux-key]");
    ths.forEach(function (th) {
      var key = th.getAttribute("data-mtux-key");
      var w = widths[key];
      if (w && w > 40) {
        th.style.width = w + "px";
        th.style.minWidth = w + "px";
        th.style.maxWidth = w + "px";
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
    applyWidths(table, widths);

    var ths = Array.prototype.slice.call(table.querySelectorAll("thead th[data-mtux-key]"));
    ths.forEach(function (th) {
      th.classList.add("mtux-th");
      if (th.getAttribute("data-mtux-fixed") === "1") return;

      if (!th.querySelector(".mtux-col-resizer")) {
        var resizer = document.createElement("span");
        resizer.className = "mtux-col-resizer";
        resizer.title = "Sürükleyerek sütun genişliği";
        th.appendChild(resizer);
        resizer.addEventListener("pointerdown", function (ev) {
          ev.preventDefault();
          ev.stopPropagation();
          var startX = ev.clientX;
          var startW = th.getBoundingClientRect().width;
          resizer.classList.add("is-active");
          function onMove(e) {
            var next = Math.max(56, Math.min(420, Math.round(startW + (e.clientX - startX))));
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

  global.SeoMetricTableUx = {
    injectStyles: injectStyles,
    heatBackground: heatBackground,
    colMinMax: colMinMax,
    heatT: heatT,
    orderKeys: orderKeys,
    readJson: readJson,
    writeJson: writeJson,
    ensureLegend: ensureLegend,
    bindInteractive: bindInteractive,
    applyWidths: applyWidths,
  };
})(typeof window !== "undefined" ? window : this);
