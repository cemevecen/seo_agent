/**
 * Metrik veri tablosu UX: heat, pin, eksen değiştir, sütun genişletme / sıralama.
 * Android / iOS ortak.
 */
(function (global) {
  "use strict";

  var STYLE_ID = "seo-mtux-style";
  var HEAT_PREF_KEY = "mtux-heat-enabled";
  var PIN_PREF_KEY = "mtux-pin-enabled";
  var LAYOUT_PREF_KEY = "mtux-layout"; // standard | transposed

  function injectStyles() {
    var style = document.getElementById(STYLE_ID);
    if (!style) {
      style = document.createElement("style");
      style.id = STYLE_ID;
      document.head.appendChild(style);
    }
    style.textContent =
      ".mtux-shell{border-radius:0.75rem;overflow:hidden;}" +
      ".mtux-shell .rdl-scroll,.mtux-shell [id$='-table-wrap']{" +
        "border-radius:0 0 0.75rem 0.75rem;}" +
      ".mtux-legend{display:flex;flex-wrap:wrap;align-items:center;justify-content:flex-end;gap:0.45rem 0.65rem;" +
      "padding:0.45rem 0.75rem;border-bottom:1px solid rgba(148,163,184,0.22);" +
      "font-size:0.62rem;font-weight:600;letter-spacing:0.04em;text-transform:uppercase;color:#94a3b8;" +
      "background:linear-gradient(180deg,rgba(248,250,252,0.95),rgba(241,245,249,0.88));}" +
      "html.dark .mtux-legend{color:#71717a;border-bottom-color:rgba(63,63,70,0.55);" +
      "background:linear-gradient(180deg,rgba(24,24,27,0.98),rgba(9,9,11,0.92));}" +
      ".mtux-legend-leading{display:inline-flex;flex-wrap:wrap;align-items:center;gap:0.35rem;margin-right:auto;}" +
      ".mtux-legend-actions{display:inline-flex;flex-wrap:wrap;align-items:center;gap:0.35rem;}" +
      ".mtux-opt-toggle,.mtux-heat-toggle{display:inline-flex;align-items:center;gap:0.3rem;" +
      "padding:0.22rem 0.55rem;border-radius:9999px;border:1px solid rgba(148,163,184,0.42);" +
      "background:rgba(255,255,255,0.65);color:#64748b;font-size:0.62rem;font-weight:700;" +
      "letter-spacing:0.03em;text-transform:uppercase;cursor:pointer;white-space:nowrap;}" +
      ".mtux-opt-toggle:hover,.mtux-heat-toggle:hover{border-color:#0ea5e9;color:#0ea5e9;}" +
      ".mtux-opt-toggle[aria-pressed='true']{border-color:#0ea5e9;background:rgba(14,165,233,0.12);color:#0369a1;}" +
      ".mtux-heat-toggle[aria-pressed='true']{border-color:#94a3b8;background:rgba(148,163,184,0.14);color:#475569;}" +
      "html.dark .mtux-opt-toggle,html.dark .mtux-heat-toggle{color:#a1a1aa;border-color:rgba(113,113,122,0.55);background:rgba(39,39,42,0.55);}" +
      "html.dark .mtux-opt-toggle[aria-pressed='true']{background:rgba(14,165,233,0.18);color:#7dd3fc;border-color:#0284c7;}" +
      "html.dark .mtux-heat-toggle[aria-pressed='true']{background:rgba(63,63,70,0.55);color:#e4e4e7;}" +
      "table.mtux-grid-table{border-collapse:separate;border-spacing:0;width:max-content;min-width:100%;" +
      "font-variant-numeric:tabular-nums;}" +
      "table.mtux-grid-table th,table.mtux-grid-table td{" +
      "border-right:1px solid rgba(148,163,184,0.14);border-bottom:1px solid rgba(148,163,184,0.14);}" +
      "html.dark table.mtux-grid-table th,html.dark table.mtux-grid-table td{border-color:rgba(63,63,70,0.75);}" +
      "table.mtux-grid-table thead th{background:#eef2f7;color:#64748b;font-size:0.62rem;" +
      "font-weight:700;letter-spacing:0.05em;text-transform:uppercase;}" +
      "html.dark table.mtux-grid-table thead th{background:#18181b;color:#a1a1aa;}" +
      "table.mtux-grid-table tbody tr:nth-child(even) td.mtux-dim-cell{background:rgba(248,250,252,0.72);}" +
      "html.dark table.mtux-grid-table tbody tr:nth-child(even) td.mtux-dim-cell{background:rgba(24,24,27,0.55);}" +
      "table.mtux-grid-table tbody tr:hover td{filter:brightness(0.98);}" +
      "html.dark table.mtux-grid-table tbody tr:hover td{filter:brightness(1.06);}" +
      ".mtux-heat-cell{border-radius:0!important;padding:0.38rem 0.62rem;font-weight:600;}" +
      ".mtux-dim-cell{padding:0.38rem 0.62rem;font-weight:500;color:#334155;}" +
      "html.dark .mtux-dim-cell{color:#e4e4e7;}" +
      ".mtux-avg-row td{font-weight:700;background:rgba(241,245,249,0.95)!important;}" +
      "html.dark .mtux-avg-row td{background:rgba(39,39,42,0.92)!important;}" +
      ".mtux-metric-dot{width:7px;height:7px;border-radius:9999px;flex:0 0 auto;box-shadow:0 0 0 1px rgba(0,0,0,0.06);}" +
      ".mtux-metric-row-label{display:inline-flex;align-items:center;gap:0.4rem;max-width:100%;min-width:0;}" +
      ".mtux-metric-row-text{overflow:hidden;text-overflow:ellipsis;white-space:nowrap;}" +
      ".mtux-pin-mode table.mtux-grid-table .mtux-sticky-top{position:sticky;top:0;z-index:14;}" +
      ".mtux-pin-mode table.mtux-grid-table .mtux-sticky-left{position:sticky;left:0;z-index:12;}" +
      ".mtux-pin-mode table.mtux-grid-table .mtux-sticky-corner{position:sticky;left:0;top:0;z-index:22;" +
      "box-shadow:2px 2px 6px rgba(15,23,42,0.08);}" +
      ".mtux-pin-mode table.mtux-grid-table .mtux-sticky-left{background:inherit;}" +
      ".mtux-pin-mode table.mtux-grid-table thead .mtux-sticky-left," +
      ".mtux-pin-mode table.mtux-grid-table thead .mtux-sticky-corner{background:#eef2f7;}" +
      "html.dark .mtux-pin-mode table.mtux-grid-table thead .mtux-sticky-left," +
      "html.dark .mtux-pin-mode table.mtux-grid-table thead .mtux-sticky-corner{background:#18181b;}" +
      ".mtux-pin-mode table.mtux-grid-table tbody .mtux-sticky-left{background:#fff;}" +
      "html.dark .mtux-pin-mode table.mtux-grid-table tbody .mtux-sticky-left{background:#09090b;}" +
      ".mtux-pin-mode table.mtux-grid-table tbody tr:nth-child(even) .mtux-sticky-left{background:#f8fafc;}" +
      "html.dark .mtux-pin-mode table.mtux-grid-table tbody tr:nth-child(even) .mtux-sticky-left{background:#18181b;}" +
      "th.mtux-th{position:relative;user-select:none;}" +
      "th.mtux-th.is-dragging{opacity:0.55;}" +
      "th.mtux-th.is-drag-over{box-shadow:inset 2px 0 0 #0ea5e9;}" +
      ".mtux-col-resizer{position:absolute;top:0;right:0;width:6px;height:100%;cursor:col-resize;z-index:3;}" +
      ".mtux-col-resizer:hover,.mtux-col-resizer.is-active{background:rgba(14,165,233,0.35);}" +
      ".mtux-drag-hint{opacity:0.45;font-size:0.65rem;margin-right:0.2rem;cursor:grab;flex:0 0 auto;}" +
      "th.mtux-th:active .mtux-drag-hint{cursor:grabbing;}" +
      "table.mtux-interactive{table-layout:fixed;width:max-content;min-width:100%;}" +
      "table.mtux-interactive th.mtux-th{overflow:visible;text-overflow:clip;white-space:nowrap;vertical-align:middle;}" +
      "table.mtux-interactive th.mtux-th:not([data-mtux-fixed='1']){min-width:5.25rem;}" +
      "table.mtux-interactive th[data-mtux-fixed='1']{min-width:5.5rem;}" +
      "table.mtux-interactive td{overflow:hidden;text-overflow:ellipsis;white-space:nowrap;}" +
      ".mtux-th-label{display:inline-flex;align-items:center;justify-content:flex-end;gap:0.25rem;" +
      "max-width:100%;min-width:0;vertical-align:middle;}" +
      ".mtux-th-text{overflow:hidden;text-overflow:ellipsis;white-space:nowrap;max-width:11rem;}";
  }

  var MIN_COL_WIDTH = 72;
  var MAX_COL_WIDTH = 420;
  var MONTH_ABBR = ["Jan", "Feb", "Mar", "Apr", "May", "Jun", "Jul", "Aug", "Sep", "Oct", "Nov", "Dec"];

  /** Tablo kırılım sütunları: ISO gün → "11 Aug" */
  function formatTableDateKey(key) {
    var s = String(key == null ? "" : key).trim();
    var m = s.match(/^(20\d{2})-(\d{2})-(\d{2})$/);
    if (m) {
      var mi = parseInt(m[2], 10) - 1;
      var mon = MONTH_ABBR[mi] || m[2];
      return String(parseInt(m[3], 10)) + " " + mon;
    }
    var yw = s.match(/^(20\d{2})-W(\d{2})$/i);
    if (yw) return "W" + yw[2];
    var ym = s.match(/^(20\d{2})-(\d{2})$/);
    if (ym) {
      var mi2 = parseInt(ym[2], 10) - 1;
      return (MONTH_ABBR[mi2] || ym[2]) + " '" + ym[1].slice(2);
    }
    return s;
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
    var a = 0.08 + Math.max(0, Math.min(1, t)) * 0.48;
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

  function isHeatEnabled() {
    return readJson(HEAT_PREF_KEY, true) !== false;
  }

  function setHeatEnabled(on) {
    writeJson(HEAT_PREF_KEY, !!on);
  }

  function isPinEnabled() {
    return readJson(PIN_PREF_KEY, true) !== false;
  }

  function setPinEnabled(on) {
    writeJson(PIN_PREF_KEY, !!on);
  }

  function isTransposed() {
    return readJson(LAYOUT_PREF_KEY, "standard") === "transposed";
  }

  function setTransposed(on) {
    writeJson(LAYOUT_PREF_KEY, on ? "transposed" : "standard");
  }

  function stickyClasses(pin, corner, top, left) {
    var cls = [];
    if (pin) {
      if (corner) cls.push("mtux-sticky-corner");
      else {
        if (top) cls.push("mtux-sticky-top");
        if (left) cls.push("mtux-sticky-left");
      }
    }
    return cls.join(" ");
  }

  function syncShell(shell) {
    if (!shell) return;
    shell.classList.add("mtux-shell");
    shell.classList.toggle("mtux-pin-mode", isPinEnabled());
    shell.classList.toggle("mtux-layout-transposed", isTransposed());
  }

  function syncLegendState(legend) {
    if (!legend) return;
    var heatOn = isHeatEnabled();
    legend.classList.toggle("is-off", !heatOn);
    var heatBtn = legend.querySelector(".mtux-heat-toggle");
    if (heatBtn) {
      heatBtn.setAttribute("aria-pressed", heatOn ? "false" : "true");
      heatBtn.textContent = heatOn ? "Remove colors" : "Show colors";
    }
    var pinBtn = legend.querySelector('[data-mtux-opt="pin"]');
    if (pinBtn) {
      pinBtn.setAttribute("aria-pressed", isPinEnabled() ? "true" : "false");
      pinBtn.textContent = isPinEnabled() ? "Pin on" : "Pin off";
    }
    var layoutBtn = legend.querySelector('[data-mtux-opt="transpose"]');
    if (layoutBtn) {
      layoutBtn.setAttribute("aria-pressed", isTransposed() ? "true" : "false");
      layoutBtn.textContent = "Reverse list table";
    }
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
    if (!shell) return null;
    var onRefresh =
      typeof opts.onRefresh === "function"
        ? opts.onRefresh
        : typeof opts.onHeatToggle === "function"
          ? opts.onHeatToggle
          : null;

    function bindLegend(legend) {
      legend._mtuxOnRefresh = onRefresh;
      if (legend._mtuxLegendBound) {
        syncLegendState(legend);
        return;
      }
      legend._mtuxLegendBound = true;
      legend.addEventListener("click", function (ev) {
        var heatBtn = ev.target && ev.target.closest ? ev.target.closest(".mtux-heat-toggle") : null;
        if (heatBtn) {
          ev.preventDefault();
          setHeatEnabled(!isHeatEnabled());
          syncLegendState(legend);
          syncShell(shell);
          if (typeof legend._mtuxOnRefresh === "function") legend._mtuxOnRefresh();
          return;
        }
        var optBtn = ev.target && ev.target.closest ? ev.target.closest(".mtux-opt-toggle") : null;
        if (!optBtn) return;
        ev.preventDefault();
        var opt = optBtn.getAttribute("data-mtux-opt");
        if (opt === "pin") setPinEnabled(!isPinEnabled());
        else if (opt === "transpose") setTransposed(!isTransposed());
        syncLegendState(legend);
        syncShell(shell);
        if (typeof legend._mtuxOnRefresh === "function") legend._mtuxOnRefresh();
      });
      syncLegendState(legend);
    }

    var existing = shell.querySelector(".mtux-legend");
    if (existing) {
      if (existing.querySelector(".mtux-legend-scale") || !existing.querySelector(".mtux-legend-leading")) {
        existing.innerHTML =
          '<div class="mtux-legend-leading">' +
            '<button type="button" class="mtux-opt-toggle" data-mtux-opt="transpose" aria-pressed="false">Reverse list table</button>' +
          "</div>" +
          '<div class="mtux-legend-actions">' +
            '<button type="button" class="mtux-opt-toggle" data-mtux-opt="pin" aria-pressed="true">Pin on</button>' +
            '<button type="button" class="mtux-heat-toggle">Remove colors</button>' +
          "</div>";
      }
      placeLegendAtTop(shell, existing);
      bindLegend(existing);
      return existing;
    }

    var legend = document.createElement("div");
    legend.className = "mtux-legend";
    legend.innerHTML =
      '<div class="mtux-legend-leading">' +
        '<button type="button" class="mtux-opt-toggle" data-mtux-opt="transpose" aria-pressed="false">Reverse list table</button>' +
      "</div>" +
      '<div class="mtux-legend-actions">' +
        '<button type="button" class="mtux-opt-toggle" data-mtux-opt="pin" aria-pressed="true">Pin on</button>' +
        '<button type="button" class="mtux-heat-toggle">Remove colors</button>' +
      "</div>";
    placeLegendAtTop(shell, legend);
    bindLegend(legend);
    return legend;
  }

  function heatCellHtml(v, color, st, esc, fmtVal, title, col) {
    var heatOn = isHeatEnabled();
    var t = heatOn ? heatT(v, st.min, st.max) : null;
    var bg = t == null ? "" : "background:" + heatBackground(color, t) + ";";
    var txt = fmtVal(v, col);
    var tip = title != null ? title : txt;
    return (
      '<td class="mtux-heat-cell tabular-nums text-slate-900 dark:text-zinc-100" style="' + bg + '" title="' +
        esc(tip) + '">' + esc(txt) + "</td>"
    );
  }

  function rowAverage(map, keys) {
    var sum = 0;
    var n = 0;
    keys.forEach(function (k) {
      var v = map[k];
      if (v == null || !Number.isFinite(v)) return;
      sum += v;
      n += 1;
    });
    return n ? sum / n : null;
  }

  /**
   * Heat grid tablosu — standard (gün×metrik) veya transposed (metrik×gün).
   * opts: shell, tableEl, theadRow, tbody, keys, colItems, esc, fmtKey, fmtVal,
   *       breakdownLabel, averageLabel, onRefresh, bindInteractive,
   *       renderStandardHeaderCell(col) -> th inner HTML (android remove btn)
   */
  function renderHeatGrid(opts) {
    opts = opts || {};
    injectStyles();
    var shell = opts.shell;
    var tableEl = opts.tableEl;
    var theadRow = opts.theadRow;
    var tbody = opts.tbody;
    var keys = opts.keys || [];
    var colItems = opts.colItems || [];
    var esc = typeof opts.esc === "function" ? opts.esc : function (s) { return String(s == null ? "" : s); };
    var fmtKey = typeof opts.fmtKey === "function" ? opts.fmtKey : function (k) { return k; };
    var fmtVal = typeof opts.fmtVal === "function" ? opts.fmtVal : function (v) { return v == null ? "—" : String(v); };
    var breakdownLabel = opts.breakdownLabel || "Breakdown";
    var averageLabel = opts.averageLabel || "Average";
    var renderHeader = typeof opts.renderStandardHeaderCell === "function" ? opts.renderStandardHeaderCell : null;

    if (!theadRow || !tbody) return { rowCount: 0 };

    syncShell(shell);
    if (tableEl) {
      tableEl.classList.add("mtux-grid-table", "mtux-interactive", "pa-data-table", "text-left", "text-xs");
    }

    var pin = isPinEnabled();
    var transposed = isTransposed();
    var colCount = 1 + (transposed ? keys.length + 1 : colItems.length);

    if (!colItems.length) {
      tbody.innerHTML =
        '<tr><td colspan="' + Math.max(2, colCount) +
        '" class="px-3 py-4 text-center text-slate-400">All columns removed — refresh via Metrics → Select</td></tr>';
      if (shell) ensureLegend(shell, { onRefresh: opts.onRefresh });
      return { rowCount: 0 };
    }
    if (!keys.length) {
      tbody.innerHTML =
        '<tr><td colspan="' + colCount + '" class="px-3 py-4 text-center text-slate-400">No rows</td></tr>';
      if (shell) ensureLegend(shell, { onRefresh: opts.onRefresh });
      return { rowCount: 0 };
    }

    if (!transposed) {
      var stats = colItems.map(function (col) {
        return colMinMax(keys.map(function (k) { return col.map[k]; }));
      });
      var cornerCls = stickyClasses(pin, true, true, true);
      theadRow.innerHTML =
        '<th class="mtux-th mtux-dim-cell px-2 py-2 sm:px-3 ' + cornerCls + '" data-mtux-key="kirilim" data-mtux-fixed="1">' +
          esc(breakdownLabel) +
        "</th>" +
        colItems.map(function (col) {
          if (renderHeader) return renderHeader(col);
          var short = col.shortLabel || col.label || col.key;
          return (
            '<th class="mtux-th px-1 py-2 sm:px-1.5 ' + stickyClasses(pin, false, true, false) + '" data-mtux-key="' +
              esc(col.key) + '" style="color:' + esc(col.color || "#2563eb") + '" title="' + esc(col.label || "") + '">' +
              '<span class="mtux-th-label"><span class="mtux-th-text">' + esc(short) + "</span></span>" +
            "</th>"
          );
        }).join("");

      tbody.innerHTML = keys.map(function (key) {
        var cells =
          '<td class="mtux-dim-cell tabular-nums ' + stickyClasses(pin, false, false, true) + '">' +
            esc(fmtKey(key)) +
          "</td>";
        colItems.forEach(function (col, i) {
          cells += heatCellHtml(col.map[key], col.color, stats[i], esc, fmtVal, null, col);
        });
        return "<tr>" + cells + "</tr>";
      }).join("");

      var avgCells =
        '<td class="mtux-dim-cell tabular-nums ' + stickyClasses(pin, false, false, true) + '" title="Average of days that have data">' +
          esc(averageLabel) +
        "</td>";
      colItems.forEach(function (col, i) {
        var avg = rowAverage(col.map, keys);
        avgCells += heatCellHtml(avg, col.color, stats[i], esc, fmtVal, null, col);
      });
      tbody.innerHTML += '<tr class="mtux-avg-row">' + avgCells + "</tr>";
    } else {
      var rowStats = colItems.map(function (col) {
        return colMinMax(keys.map(function (k) { return col.map[k]; }));
      });

      var head =
        '<th class="mtux-th mtux-dim-cell px-2 py-2 sm:px-3 ' + stickyClasses(pin, true, true, true) +
          '" data-mtux-key="metric" data-mtux-fixed="1">Metric</th>';
      keys.forEach(function (key, ki) {
        head +=
          '<th class="mtux-th px-1 py-2 text-right sm:px-1.5 ' + stickyClasses(pin, false, true, false) +
          '" data-mtux-key="d:' + esc(key) + '" title="' + esc(fmtKey(key)) + '">' +
            esc(fmtKey(key)) +
          "</th>";
      });
      head +=
        '<th class="mtux-th px-1 py-2 text-right sm:px-1.5 ' + stickyClasses(pin, false, true, false) +
        '" data-mtux-key="avg" data-mtux-fixed="1">' + esc(averageLabel) + "</th>";
      theadRow.innerHTML = head;

      tbody.innerHTML = colItems.map(function (col, ri) {
        var st = rowStats[ri];
        var cells =
          '<td class="mtux-dim-cell ' + stickyClasses(pin, false, false, true) + '" title="' + esc(col.label || "") + '">' +
            '<span class="mtux-metric-row-label">' +
              '<span class="mtux-metric-dot" style="background:' + esc(col.color || "#2563eb") + '"></span>' +
              '<span class="mtux-metric-row-text">' + esc(col.shortLabel || col.label || col.key) + "</span>" +
            "</span>" +
          "</td>";
        keys.forEach(function (key, ki) {
          cells += heatCellHtml(col.map[key], col.color, st, esc, fmtVal, null, col);
        });
        var avg = rowAverage(col.map, keys);
        cells += heatCellHtml(avg, col.color, st, esc, fmtVal, null, col);
        return "<tr>" + cells + "</tr>";
      }).join("");
    }

    if (shell) {
      ensureLegend(shell, { onRefresh: opts.onRefresh });
    }
    if (tableEl && opts.bindInteractive && !transposed) {
      bindInteractive(tableEl, opts.bindInteractive);
    } else if (tableEl && transposed) {
      tableEl.classList.add("mtux-interactive");
      applyWidths(tableEl, opts.bindInteractive && opts.bindInteractive.widthsKey
        ? readJson(opts.bindInteractive.widthsKey, {})
        : {});
    }

    return { rowCount: keys.length + (transposed ? 0 : 1) };
  }

  function applyWidths(table, widths) {
    if (!table || !widths) return;
    var ths = table.querySelectorAll("thead th[data-mtux-key]");
    ths.forEach(function (th) {
      var key = th.getAttribute("data-mtux-key");
      var w = Number(widths[key]);
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
    if (!table || isTransposed()) return;
    injectStyles();
    table.classList.add("mtux-interactive");
    var widthsKey = opts.widthsKey || "";
    var orderKey = opts.orderKey || "";
    var onOrder = typeof opts.onOrderChange === "function" ? opts.onOrderChange : null;
    var widths = widthsKey ? readJson(widthsKey, {}) : {};
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
    isPinEnabled: isPinEnabled,
    setPinEnabled: setPinEnabled,
    isTransposed: isTransposed,
    setTransposed: setTransposed,
    syncShell: syncShell,
    orderKeys: orderKeys,
    readJson: readJson,
    writeJson: writeJson,
    ensureLegend: ensureLegend,
    renderHeatGrid: renderHeatGrid,
    bindInteractive: bindInteractive,
    applyWidths: applyWidths,
    fitTextToWidth: fitTextToWidth,
    fitSideChips: fitSideChips,
    fitCardTexts: fitCardTexts,
    formatTableDateKey: formatTableDateKey,
  };
})(typeof window !== "undefined" ? window : this);
