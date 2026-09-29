/**
 * /assets — doviz.com piyasa serileri Metrics-benzeri görünüm.
 */
(function () {
  "use strict";

  var cfg = window.SEO_ASSETS_PAGE || {};
  var ALL_SERIES = Array.isArray(cfg.series) ? cfg.series : [];
  var DEFAULTS = Array.isArray(cfg.defaults) ? cfg.defaults.slice() : [];
  var CATEGORY_LABELS = cfg.categories || {
    gold: "Altın",
    silver: "Gümüş",
    commodity: "Emtia",
    fx: "Döviz",
    index: "Endeks",
    crypto: "Kripto",
    equity: "Hisse",
    other: "Diğer",
  };
  var CATEGORY_ORDER = ["gold", "silver", "commodity", "fx", "index", "crypto", "equity", "other"];

  var PALETTE = [
    "#1d4ed8", "#059669", "#dc2626", "#7c3aed", "#ea580c",
    "#0891b2", "#db2777", "#65a30d", "#4f46e5", "#0f766e",
    "#b45309", "#0369a1", "#be123c", "#15803d", "#6d28d9",
  ];

  var el = {
    start: document.getElementById("as-start"),
    end: document.getElementById("as-end"),
    preset: document.getElementById("as-preset"),
    breakdown: document.getElementById("as-breakdown"),
    compare: document.getElementById("as-compare-mode"),
    run: document.getElementById("as-run"),
    runLabel: document.getElementById("as-run-label"),
    loading: document.getElementById("as-loading"),
    kpiGrid: document.getElementById("as-kpi-grid"),
    chart: document.getElementById("as-chart"),
    chartWrap: document.getElementById("as-chart-wrap"),
    tableWrap: document.getElementById("as-table-wrap"),
    thead: document.getElementById("as-thead-row"),
    tbody: document.getElementById("as-tbody"),
    legend: document.getElementById("as-legend"),
    compareRange: document.getElementById("as-compare-range"),
    sync: document.getElementById("as-sync"),
    metricTrigger: document.getElementById("as-metric-trigger"),
    metricLabel: document.getElementById("as-metric-label"),
    metricList: document.getElementById("as-metric-list"),
    metricScroll: document.getElementById("as-metric-list-scroll"),
    tip: document.getElementById("as-tooltip"),
    tipTitle: document.getElementById("as-tip-title"),
    tipBody: document.getElementById("as-tip-body"),
  };

  var selected = DEFAULTS.length
    ? DEFAULTS.slice()
    : ALL_SERIES.slice(0, 10).map(function (s) { return s.key; });
  var viewMode = "line";
  var lastPayload = null;
  var lastComparePayload = null;
  var seriesByKey = {};
  ALL_SERIES.forEach(function (s) { seriesByKey[s.key] = s; });

  function pad(n) { return n < 10 ? "0" + n : String(n); }
  function iso(d) {
    return d.getFullYear() + "-" + pad(d.getMonth() + 1) + "-" + pad(d.getDate());
  }
  function parseIso(s) {
    var p = String(s || "").slice(0, 10).split("-");
    if (p.length !== 3) return null;
    return new Date(+p[0], +p[1] - 1, +p[2]);
  }
  function addDays(d, n) {
    var x = new Date(d.getTime());
    x.setDate(x.getDate() + n);
    return x;
  }
  function fmtNum(v, unit) {
    if (v == null || !Number.isFinite(v)) return "—";
    var abs = Math.abs(v);
    var opts;
    if (abs >= 1000) opts = { maximumFractionDigits: 2 };
    else if (abs >= 1) opts = { maximumFractionDigits: 4 };
    else opts = { maximumFractionDigits: 8 };
    var s = v.toLocaleString("tr-TR", opts);
    return unit ? s : s;
  }
  function fmtPct(v) {
    if (v == null || !Number.isFinite(v)) return "—";
    var sign = v > 0 ? "+" : "";
    return sign + v.toLocaleString("tr-TR", { maximumFractionDigits: 2 }) + "%";
  }
  function colorFor(key, idx) {
    var i = typeof idx === "number" ? idx : selected.indexOf(key);
    return PALETTE[(i >= 0 ? i : 0) % PALETTE.length];
  }

  function applyPreset() {
    var today = new Date();
    today.setHours(0, 0, 0, 0);
    var end = today;
    var start;
    var p = el.preset ? el.preset.value : "90";
    if (p === "since2025") {
      start = new Date(2025, 0, 1);
    } else {
      var days = parseInt(p, 10) || 90;
      start = addDays(end, -(days - 1));
    }
    if (el.start) el.start.value = iso(start);
    if (el.end) el.end.value = iso(end);
  }

  function setLoading(on) {
    if (!el.loading) return;
    el.loading.classList.toggle("hidden", !on);
    el.loading.classList.toggle("flex", !!on);
    el.loading.setAttribute("aria-busy", on ? "true" : "false");
    if (el.run) el.run.disabled = !!on;
    if (el.runLabel) el.runLabel.textContent = on ? "Loading…" : "Apply";
  }

  function updateMetricTrigger() {
    if (!el.metricLabel) return;
    var n = selected.length;
    if (!n) el.metricLabel.textContent = "Select";
    else if (n === 1) {
      var s = seriesByKey[selected[0]];
      el.metricLabel.textContent = (s && s.label) || selected[0];
    } else el.metricLabel.textContent = n + " assets selected";
  }

  function buildMetricList() {
    if (!el.metricScroll) return;
    var byCat = {};
    ALL_SERIES.forEach(function (s) {
      var c = s.category || "other";
      if (!byCat[c]) byCat[c] = [];
      byCat[c].push(s);
    });
    var html = "";
    html += '<button type="button" class="as-metric-opt" data-as-clear="1">' +
      '<span class="w-4 text-center">' + (!selected.length ? "✓" : "") + "</span>Clear selection</button>";
    CATEGORY_ORDER.forEach(function (cat) {
      var items = byCat[cat];
      if (!items || !items.length) return;
      html += '<div class="as-metric-group">' + (CATEGORY_LABELS[cat] || cat) + "</div>";
      items.forEach(function (s) {
        var on = selected.indexOf(s.key) >= 0;
        html += '<button type="button" role="option" aria-selected="' + on + '" data-as-key="' + s.key + '" class="as-metric-opt' + (on ? " is-on" : "") + '">' +
          '<span class="w-4 text-center">' + (on ? "✓" : "") + "</span>" +
          '<span class="min-w-0 flex-1 truncate">' + (s.label || s.key) + "</span>" +
          '<span class="text-[10px] font-medium text-slate-400">' + (s.unit || "") + "</span></button>";
      });
    });
    el.metricScroll.innerHTML = html;
  }

  function positionMetricDropdown() {
    if (!el.metricList || !el.metricTrigger) return;
    var r = el.metricTrigger.getBoundingClientRect();
    var maxW = Math.min(420, window.innerWidth - 16);
    var w = Math.max(r.width, Math.min(maxW, 320));
    var left = Math.min(Math.max(8, r.left), window.innerWidth - w - 8);
    var spaceBelow = window.innerHeight - r.bottom - 12;
    var spaceAbove = r.top - 12;
    var maxH = Math.min(360, Math.max(spaceBelow, spaceAbove, 140));
    var top = r.bottom + 4;
    if (spaceBelow < 160 && spaceAbove > spaceBelow) top = Math.max(8, r.top - maxH - 4);
    el.metricList.style.left = left + "px";
    el.metricList.style.top = top + "px";
    el.metricList.style.width = w + "px";
    el.metricList.style.maxHeight = maxH + "px";
  }

  function openMetricList(on) {
    if (!el.metricList || !el.metricTrigger) return;
    if (on) {
      buildMetricList();
      positionMetricDropdown();
      el.metricList.classList.remove("hidden");
      el.metricTrigger.setAttribute("aria-expanded", "true");
    } else {
      el.metricList.classList.add("hidden");
      el.metricTrigger.setAttribute("aria-expanded", "false");
    }
  }

  function shiftRange(startIso, endIso, mode) {
    var a = parseIso(startIso);
    var b = parseIso(endIso);
    if (!a || !b) return null;
    if (mode === "previous_year") {
      a.setFullYear(a.getFullYear() - 1);
      b.setFullYear(b.getFullYear() - 1);
      return { start: iso(a), end: iso(b) };
    }
    if (mode === "previous_period") {
      var days = Math.round((b - a) / 86400000) + 1;
      var end2 = addDays(a, -1);
      var start2 = addDays(end2, -(days - 1));
      return { start: iso(start2), end: iso(end2) };
    }
    return null;
  }

  function fetchOverlay(start, end, keys) {
    var p = new URLSearchParams();
    p.set("start", start);
    p.set("end", end);
    if (keys && keys.length) p.set("series", keys.join(","));
    return fetch("/api/market-quotes/overlay?" + p.toString(), {
      credentials: "same-origin",
      cache: "no-store",
    }).then(function (r) {
      if (!r.ok) throw new Error("HTTP " + r.status);
      return r.json();
    });
  }

  function pointsOf(payload, key) {
    var block = payload && payload.series && payload.series[key];
    var out = [];
    ((block && block.by_date) || []).forEach(function (pt) {
      if (!pt || pt.close == null || pt.close === "") return;
      var v = Number(pt.close);
      if (!Number.isFinite(v)) return;
      out.push({ date: String(pt.date).slice(0, 10), value: v });
    });
    return out;
  }

  function aggregate(points, breakdown) {
    if (breakdown === "day" || !breakdown) return points.slice();
    var buckets = {};
    var order = [];
    points.forEach(function (pt) {
      var d = parseIso(pt.date);
      if (!d) return;
      var key;
      if (breakdown === "week") {
        var day = (d.getDay() + 6) % 7;
        var mon = addDays(d, -day);
        key = iso(mon);
      } else {
        key = d.getFullYear() + "-" + pad(d.getMonth() + 1) + "-01";
      }
      if (!buckets[key]) {
        buckets[key] = { date: key, sum: 0, n: 0 };
        order.push(key);
      }
      buckets[key].sum += pt.value;
      buckets[key].n += 1;
    });
    return order.map(function (k) {
      var b = buckets[k];
      return { date: b.date, value: b.sum / b.n };
    });
  }

  function lastClose(points) {
    if (!points || !points.length) return null;
    return points[points.length - 1].value;
  }
  function firstClose(points) {
    if (!points || !points.length) return null;
    return points[0].value;
  }
  function deltaPct(cur, prev) {
    if (cur == null || prev == null || !Number.isFinite(cur) || !Number.isFinite(prev) || prev === 0) return null;
    return ((cur - prev) / Math.abs(prev)) * 100;
  }

  function normalizeSeries(points) {
    if (!points.length) return [];
    var vals = points.map(function (p) { return p.value; });
    var lo = Math.min.apply(null, vals);
    var hi = Math.max.apply(null, vals);
    var span = hi - lo;
    if (span <= 0) {
      return points.map(function (p) { return { date: p.date, value: p.value, norm: 50 }; });
    }
    return points.map(function (p) {
      return { date: p.date, value: p.value, norm: ((p.value - lo) / span) * 100 };
    });
  }

  function sparklineSvg(points, color) {
    if (!points || points.length < 2) {
      return '<svg class="as-kpi-spark" viewBox="0 0 120 48" preserveAspectRatio="none"><path d="M4 24 H116" stroke="#cbd5e1" stroke-width="1.5" fill="none"/></svg>';
    }
    var vals = points.map(function (p) { return p.value; });
    var lo = Math.min.apply(null, vals);
    var hi = Math.max.apply(null, vals);
    var span = hi - lo || 1;
    var w = 120, h = 48, padY = 4;
    var coords = points.map(function (p, i) {
      var x = (i / (points.length - 1)) * w;
      var y = h - padY - ((p.value - lo) / span) * (h - padY * 2);
      return x.toFixed(1) + "," + y.toFixed(1);
    });
    var line = coords.join(" ");
    var area = "0," + h + " " + line + " " + w + "," + h;
    return '<svg class="as-kpi-spark" viewBox="0 0 120 48" preserveAspectRatio="none">' +
      '<polygon points="' + area + '" fill="' + color + '" fill-opacity="0.12"/>' +
      '<polyline points="' + line + '" fill="none" stroke="' + color + '" stroke-width="1.8" stroke-linecap="round" stroke-linejoin="round"/>' +
      "</svg>";
  }

  function renderKpis(payload, comparePayload, breakdown) {
    if (!el.kpiGrid) return;
    var html = "";
    selected.forEach(function (key, idx) {
      var spec = seriesByKey[key] || { label: key, unit: "" };
      var pts = aggregate(pointsOf(payload, key), breakdown);
      var cur = lastClose(pts);
      var prevPts = comparePayload ? aggregate(pointsOf(comparePayload, key), breakdown) : [];
      var prev = lastClose(prevPts);
      if (prev == null && pts.length > 1) prev = firstClose(pts);
      var d = deltaPct(cur, prev);
      var cls = d == null ? "is-flat" : d > 0.05 ? "is-up" : d < -0.05 ? "is-down" : "is-flat";
      var arrow = d == null ? "" : d > 0.05 ? "▲" : d < -0.05 ? "▼" : "●";
      var color = colorFor(key, idx);
      html += '<article class="as-kpi-card" data-as-kpi="' + key + '">' +
        '<div class="as-kpi-card__head">' +
        '<p class="as-kpi-card__title">' + (spec.label || key) + "</p>" +
        '<a class="text-slate-400 hover:text-emerald-600" href="' + (spec.source_url || "#") + '" target="_blank" rel="noopener" title="Kaynak">' +
        '<svg viewBox="0 0 20 20" width="14" height="14" fill="currentColor" aria-hidden="true"><path fill-rule="evenodd" d="M18 10a8 8 0 11-16 0 8 8 0 0116 0zm-7-4a1 1 0 11-2 0 1 1 0 012 0zM9 9a1 1 0 000 2v3a1 1 0 001 1h1a1 1 0 100-2v-3a1 1 0 00-1-1H9z" clip-rule="evenodd"/></svg></a>' +
        "</div>" +
        '<div class="as-kpi-card__body">' +
        '<div><p class="as-kpi-card__value tabular-nums">' + fmtNum(cur) + "</p>" +
        '<p class="as-kpi-card__delta ' + cls + '"><span>' + arrow + "</span><span>" + fmtPct(d) + "</span></p></div>" +
        sparklineSvg(pts, color) +
        "</div>" +
        '<div class="as-kpi-card__chev" aria-hidden="true"><svg viewBox="0 0 20 20" width="12" height="12" fill="currentColor"><path fill-rule="evenodd" d="M5.23 7.21a.75.75 0 011.06.02L10 10.94l3.71-3.71a.75.75 0 111.06 1.06l-4.24 4.25a.75.75 0 01-1.06 0L5.21 8.29a.75.75 0 01.02-1.08z" clip-rule="evenodd"/></svg></div>' +
        "</article>";
    });
    el.kpiGrid.innerHTML = html || '<p class="text-xs text-slate-400">Varlık seçin.</p>';
  }

  function unionDates(seriesMap) {
    var set = {};
    Object.keys(seriesMap).forEach(function (k) {
      seriesMap[k].forEach(function (p) { set[p.date] = 1; });
    });
    return Object.keys(set).sort();
  }

  function renderLegend() {
    if (!el.legend) return;
    el.legend.innerHTML = selected.map(function (key, idx) {
      var spec = seriesByKey[key] || { label: key };
      var c = colorFor(key, idx);
      return '<span class="inline-flex items-center gap-1.5">' +
        '<span class="h-2 w-2 rounded-full" style="background:' + c + '"></span>' +
        (spec.label || key) + "</span>";
    }).join("");
  }

  function renderChart(payload, breakdown) {
    if (!el.chart) return;
    if (viewMode === "table") {
      el.chartWrap && el.chartWrap.classList.add("hidden");
      el.tableWrap && el.tableWrap.classList.remove("hidden");
      renderTable(payload, breakdown);
      return;
    }
    el.chartWrap && el.chartWrap.classList.remove("hidden");
    el.tableWrap && el.tableWrap.classList.add("hidden");

    var seriesMap = {};
    selected.forEach(function (key) {
      seriesMap[key] = normalizeSeries(aggregate(pointsOf(payload, key), breakdown));
    });
    var dates = unionDates(seriesMap);
    var W = 900, H = 300;
    var padL = 48, padR = 16, padT = 16, padB = 36;
    var plotW = W - padL - padR;
    var plotH = H - padT - padB;

    function xOf(i) {
      if (dates.length <= 1) return padL + plotW / 2;
      return padL + (i / (dates.length - 1)) * plotW;
    }
    function yOf(norm) {
      return padT + (1 - norm / 100) * plotH;
    }

    var grid = "";
    for (var g = 0; g <= 4; g++) {
      var gy = yOf(g * 25);
      grid += '<line x1="' + padL + '" y1="' + gy + '" x2="' + (W - padR) + '" y2="' + gy + '" stroke="#e2e8f0" stroke-width="1"/>';
      grid += '<text x="' + (padL - 8) + '" y="' + (gy + 3) + '" text-anchor="end" font-size="10" fill="#94a3b8">' + (g * 25) + "%</text>";
    }
    var xLabels = "";
    var labelCount = Math.min(6, dates.length);
    for (var li = 0; li < labelCount; li++) {
      var di = labelCount === 1 ? 0 : Math.round((li / (labelCount - 1)) * (dates.length - 1));
      xLabels += '<text x="' + xOf(di) + '" y="' + (H - 10) + '" text-anchor="middle" font-size="10" fill="#94a3b8">' + dates[di] + "</text>";
    }

    var paths = "";
    selected.forEach(function (key, idx) {
      var pts = seriesMap[key];
      if (!pts.length) return;
      var byDate = {};
      pts.forEach(function (p) { byDate[p.date] = p; });
      var color = colorFor(key, idx);
      if (viewMode === "bar") {
        var barW = Math.max(2, (plotW / Math.max(dates.length, 1)) * 0.55 / Math.max(selected.length, 1));
        dates.forEach(function (d, i) {
          var p = byDate[d];
          if (!p) return;
          var x = xOf(i) - (selected.length * barW) / 2 + idx * barW;
          var y = yOf(p.norm);
          var h = padT + plotH - y;
          paths += '<rect x="' + x.toFixed(1) + '" y="' + y.toFixed(1) + '" width="' + barW.toFixed(1) + '" height="' + Math.max(0, h).toFixed(1) + '" fill="' + color + '" fill-opacity="0.75" rx="1"/>';
        });
      } else {
        var coords = [];
        dates.forEach(function (d, i) {
          var p = byDate[d];
          if (!p) return;
          coords.push(xOf(i).toFixed(1) + "," + yOf(p.norm).toFixed(1));
        });
        if (coords.length) {
          paths += '<polyline points="' + coords.join(" ") + '" fill="none" stroke="' + color + '" stroke-width="2" stroke-linecap="round" stroke-linejoin="round"/>';
        }
      }
    });

    el.chart.setAttribute("viewBox", "0 0 " + W + " " + H);
    el.chart.innerHTML = grid + paths + xLabels +
      '<rect id="as-hit" x="' + padL + '" y="' + padT + '" width="' + plotW + '" height="' + plotH + '" fill="transparent"/>';

    var hit = document.getElementById("as-hit");
    if (hit) {
      hit.addEventListener("mousemove", function (ev) {
        if (!dates.length) return;
        var rect = el.chart.getBoundingClientRect();
        var mx = ((ev.clientX - rect.left) / rect.width) * W;
        var ratio = (mx - padL) / plotW;
        var idx = Math.round(Math.max(0, Math.min(1, ratio)) * (dates.length - 1));
        var d = dates[idx];
        showTip(ev, d, seriesMap);
      });
      hit.addEventListener("mouseleave", hideTip);
    }
  }

  function showTip(ev, dateKey, seriesMap) {
    if (!el.tip) return;
    el.tip.classList.remove("hidden");
    if (el.tipTitle) el.tipTitle.textContent = dateKey;
    var lines = selected.map(function (key, idx) {
      var pts = seriesMap[key] || [];
      var found = null;
      for (var i = 0; i < pts.length; i++) if (pts[i].date === dateKey) { found = pts[i]; break; }
      var spec = seriesByKey[key] || { label: key };
      var c = colorFor(key, idx);
      return '<div class="flex items-center gap-2"><span class="h-2 w-2 rounded-full" style="background:' + c + '"></span>' +
        '<span class="flex-1 truncate">' + (spec.label || key) + '</span>' +
        '<span class="tabular-nums font-bold">' + (found ? fmtNum(found.value) : "—") + "</span></div>";
    });
    if (el.tipBody) el.tipBody.innerHTML = lines.join("");
    var wrap = el.chartWrap.getBoundingClientRect();
    var left = ev.clientX - wrap.left + 12;
    var top = ev.clientY - wrap.top + 12;
    if (left + 200 > wrap.width) left = Math.max(8, wrap.width - 220);
    if (top + 120 > wrap.height) top = Math.max(8, wrap.height - 140);
    el.tip.style.left = left + "px";
    el.tip.style.top = top + "px";
  }
  function hideTip() {
    if (el.tip) el.tip.classList.add("hidden");
  }

  function renderTable(payload, breakdown) {
    if (!el.thead || !el.tbody) return;
    var seriesMap = {};
    selected.forEach(function (key) {
      seriesMap[key] = aggregate(pointsOf(payload, key), breakdown);
    });
    var dates = unionDates(seriesMap).slice().reverse();
    el.thead.innerHTML = '<th class="px-2 py-2 font-bold">Date</th>' + selected.map(function (key) {
      var spec = seriesByKey[key] || { label: key };
      return '<th class="px-2 py-2 font-bold tabular-nums">' + (spec.label || key) + "</th>";
    }).join("");
    el.tbody.innerHTML = dates.map(function (d) {
      return "<tr><td class=\"px-2 py-1.5 tabular-nums text-slate-500\">" + d + "</td>" +
        selected.map(function (key) {
          var pts = seriesMap[key] || [];
          var found = null;
          for (var i = 0; i < pts.length; i++) if (pts[i].date === d) { found = pts[i]; break; }
          return '<td class="px-2 py-1.5 tabular-nums font-semibold">' + (found ? fmtNum(found.value) : "—") + "</td>";
        }).join("") + "</tr>";
    }).join("");
  }

  function refreshViews() {
    if (!lastPayload) return;
    var breakdown = el.breakdown ? el.breakdown.value : "day";
    renderKpis(lastPayload, lastComparePayload, breakdown);
    renderLegend();
    renderChart(lastPayload, breakdown);
  }

  function run() {
    if (!selected.length) {
      updateMetricTrigger();
      return;
    }
    var start = el.start && el.start.value;
    var end = el.end && el.end.value;
    if (!start || !end) applyPreset();
    start = el.start.value;
    end = el.end.value;
    setLoading(true);
    var mode = el.compare ? el.compare.value : "";
    var cmp = mode ? shiftRange(start, end, mode) : null;
    if (el.compareRange) {
      el.compareRange.textContent = cmp
        ? ((mode === "previous_year" ? "Previous year: " : "Previous period: ") + cmp.start + " → " + cmp.end)
        : "";
    }
    var main = fetchOverlay(start, end, selected);
    var side = cmp ? fetchOverlay(cmp.start, cmp.end, selected) : Promise.resolve(null);
    Promise.all([main, side]).then(function (pair) {
      lastPayload = pair[0];
      lastComparePayload = pair[1];
      if (el.sync && lastPayload && lastPayload.synced_at) {
        el.sync.textContent = "Last sync · " + String(lastPayload.synced_at).replace("T", " ").slice(0, 19);
      }
      refreshViews();
    }).catch(function (err) {
      if (el.kpiGrid) {
        el.kpiGrid.innerHTML = '<p class="text-xs text-red-600">Veri yüklenemedi: ' +
          String((err && err.message) || err) + "</p>";
      }
    }).then(function () { setLoading(false); });
  }

  // Events
  if (el.preset) el.preset.addEventListener("change", applyPreset);
  if (el.run) el.run.addEventListener("click", run);
  if (el.breakdown) el.breakdown.addEventListener("change", refreshViews);
  if (el.metricTrigger) {
    el.metricTrigger.addEventListener("click", function (e) {
      e.stopPropagation();
      var open = el.metricList && !el.metricList.classList.contains("hidden");
      openMetricList(!open);
    });
  }
  if (el.metricList) {
    el.metricList.addEventListener("click", function (e) {
      var btn = e.target.closest("[data-as-key], [data-as-clear]");
      if (!btn) return;
      e.preventDefault();
      if (btn.getAttribute("data-as-clear")) {
        selected = [];
      } else {
        var key = btn.getAttribute("data-as-key");
        var i = selected.indexOf(key);
        if (i >= 0) selected.splice(i, 1);
        else selected.push(key);
      }
      updateMetricTrigger();
      buildMetricList();
    });
  }
  document.addEventListener("click", function (e) {
    if (!el.metricList || el.metricList.classList.contains("hidden")) return;
    if (el.metricTrigger && el.metricTrigger.contains(e.target)) return;
    if (el.metricList.contains(e.target)) return;
    openMetricList(false);
  });
  window.addEventListener("resize", function () {
    if (el.metricList && !el.metricList.classList.contains("hidden")) positionMetricDropdown();
  });

  document.querySelectorAll("#as-view-toggle [data-as-view]").forEach(function (btn) {
    btn.addEventListener("click", function () {
      viewMode = btn.getAttribute("data-as-view") || "line";
      document.querySelectorAll("#as-view-toggle [data-as-view]").forEach(function (b) {
        b.classList.toggle("is-active", b === btn);
      });
      refreshViews();
    });
  });

  applyPreset();
  updateMetricTrigger();
  run();
})();
