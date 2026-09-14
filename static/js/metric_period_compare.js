/**
 * Android / iOS metrik kıyası.
 * previous_period: seçili aralıkla aynı uzunlukta hemen önceki pencere.
 * previous_year: aynı günler, bir yıl önce.
 * Pencere depoda yoksa delta üretilmez.
 */
(function (global) {
  "use strict";

  var MODES = { previous_period: 1, previous_year: 1 };
  var EDGE_SLACK_DAYS = 3;
  var MISSING = "Karşılaştırma aralığı depoda yok";

  function iso(d) {
    var y = d.getFullYear();
    var m = String(d.getMonth() + 1);
    var day = String(d.getDate());
    if (m.length < 2) m = "0" + m;
    if (day.length < 2) day = "0" + day;
    return y + "-" + m + "-" + day;
  }

  function parse(s) {
    var m = String(s || "").slice(0, 10).match(/^(20\d{2})-(\d{2})-(\d{2})$/);
    if (!m) return null;
    return new Date(Number(m[1]), Number(m[2]) - 1, Number(m[3]), 12, 0, 0);
  }

  function shiftYears(d, years) {
    var next = new Date(d.getTime());
    next.setFullYear(next.getFullYear() + years);
    if (next.getMonth() !== d.getMonth()) next.setDate(0);
    return next;
  }

  function bounds(startS, endS, mode) {
    if (!MODES[mode]) return null;
    var a = parse(startS);
    var b = parse(endS);
    if (!a || !b || a > b) return null;
    if (mode === "previous_year") {
      return { start: iso(shiftYears(a, -1)), end: iso(shiftYears(b, -1)), mode: mode };
    }
    var span = Math.round((b.getTime() - a.getTime()) / 86400000) + 1;
    var pe = new Date(a.getTime());
    pe.setDate(pe.getDate() - 1);
    var ps = new Date(pe.getTime());
    ps.setDate(ps.getDate() - (span - 1));
    return { start: iso(ps), end: iso(pe), mode: mode };
  }

  function seriesDates(series) {
    var out = [];
    (series || []).forEach(function (r) {
      var d = parse(r && r.key);
      if (d) out.push(d.getTime());
    });
    return out;
  }

  function windowCovered(series, startS, endS) {
    var start = parse(startS);
    var end = parse(endS);
    var times = seriesDates(series);
    if (!start || !end || start > end || !times.length) return false;
    var lo = Math.min.apply(null, times);
    var hi = Math.max.apply(null, times);
    var slack = EDGE_SLACK_DAYS * 86400000;
    if (lo > start.getTime() + slack || hi < end.getTime() - slack) return false;
    return times.some(function (t) {
      return t >= start.getTime() - slack && t <= end.getTime() + slack;
    });
  }

  function label(mode) {
    if (mode === "previous_year") return "Previous year";
    if (mode === "previous_period") return "Previous period";
    return "Compare";
  }

  function seriesTotal(series, totalMode) {
    var vals = [];
    (series || []).forEach(function (r) {
      var n = Number(r && r.value);
      if (Number.isFinite(n)) vals.push(n);
    });
    if (!vals.length) return null;
    if (totalMode === "last") return vals[vals.length - 1];
    var sum = 0;
    vals.forEach(function (n) { sum += n; });
    if (totalMode === "avg") return sum / vals.length;
    return sum;
  }

  function deltaPct(current, previous) {
    var c = Number(current);
    var p = Number(previous);
    if (!Number.isFinite(c) || !Number.isFinite(p) || !p) return null;
    return Math.round(((c - p) / Math.abs(p)) * 10000) / 100;
  }

  function unavailable(mode, win, reason) {
    return {
      mode: mode || "",
      start: (win && win.start) || "",
      end: (win && win.end) || "",
      total: null,
      delta_pct: null,
      series: [],
      available: false,
      missing_reason: reason || MISSING,
    };
  }

  function fromSeries(currentPack, prevPack, mode, win) {
    var prevSeries = (prevPack && prevPack.series) || [];
    if (!win || !windowCovered(prevSeries, win.start, win.end)) {
      return unavailable(mode, win);
    }
    var modeTotal = (currentPack && currentPack.total_mode) || "sum";
    var curTotal = currentPack && currentPack.total != null && Number.isFinite(Number(currentPack.total))
      ? Number(currentPack.total)
      : seriesTotal(currentPack && currentPack.series, modeTotal);
    var prevTotal = prevPack && prevPack.total != null && Number.isFinite(Number(prevPack.total))
      ? Number(prevPack.total)
      : seriesTotal(prevSeries, modeTotal);
    return {
      mode: mode,
      start: win.start,
      end: win.end,
      total: prevTotal,
      delta_pct: deltaPct(curTotal, prevTotal),
      series: prevSeries,
      total_mode: modeTotal,
      available: true,
      missing_reason: null,
    };
  }

  function shiftYearKey(key, years) {
    var m = String(key || "").match(/^(20\d{2})-(\d{2})-(\d{2})$/);
    if (!m) return null;
    var month = Number(m[2]) - 1;
    var d = new Date(Number(m[1]) + years, month, Number(m[3]), 12, 0, 0);
    if (d.getMonth() !== month) d.setDate(0);
    var mm = String(d.getMonth() + 1);
    var dd = String(d.getDate());
    if (mm.length < 2) mm = "0" + mm;
    if (dd.length < 2) dd = "0" + dd;
    return d.getFullYear() + "-" + mm + "-" + dd;
  }

  function alignedDeltaMap(currentSeries, prevSeries, mode) {
    var cur = (currentSeries || []).filter(function (r) { return r && r.key != null; });
    var prev = (prevSeries || []).filter(function (r) { return r && r.key != null; });
    var map = {};
    if (mode === "previous_year") {
      var prevMap = {};
      var prevKeys = [];
      prev.forEach(function (r) {
        var n = Number(r.value);
        if (!Number.isFinite(n)) return;
        var k = String(r.key).slice(0, 10);
        prevMap[k] = n;
        prevKeys.push(k);
      });
      prevKeys.sort();
      cur.forEach(function (r) {
        var cv = Number(r.value);
        if (!Number.isFinite(cv)) return;
        var target = shiftYearKey(r.key, -1);
        if (!target) return;
        var pv = prevMap[target];
        if (pv == null) {
          var best = null;
          for (var i = prevKeys.length - 1; i >= 0; i--) {
            if (prevKeys[i] <= target) {
              best = prevKeys[i];
              break;
            }
          }
          if (best) {
            var gap = Math.round((new Date(target + "T12:00:00") - new Date(best + "T12:00:00")) / 86400000);
            if (gap >= 0 && gap <= 4) pv = prevMap[best];
          }
        }
        if (pv == null) return;
        var pct = deltaPct(cv, pv);
        if (pct != null) map[String(r.key)] = pct;
      });
      return map;
    }
    var n = Math.min(cur.length, prev.length);
    for (var j = 0; j < n; j++) {
      var cv2 = Number(cur[j].value);
      var pv2 = Number(prev[j].value);
      var pct2 = deltaPct(cv2, pv2);
      if (pct2 != null) map[String(cur[j].key)] = pct2;
    }
    return map;
  }

  function appendDeltaColumns(cols) {
    var out = [];
    (cols || []).forEach(function (col) {
      out.push(col);
      var cmp = col && col.compare;
      if (!cmp || cmp.available === false || !(cmp.series || []).length) return;
      if (!windowCovered(cmp.series, cmp.start, cmp.end)) return;
      out.push({
        key: String(col.key) + ":dlt",
        label: (col.label || col.metric || "Δ") + " Δ",
        shortLabel: "Δ",
        color: "#64748b",
        metric: col.metric,
        map: alignedDeltaMap(col.series || [], cmp.series, cmp.mode),
        isDelta: true,
        compare: null,
      });
    });
    return out;
  }

  function kpiText(compare) {
    if (!compare || compare.available === false || compare.delta_pct == null || !Number.isFinite(Number(compare.delta_pct))) {
      return {
        text: "—",
        cls: "is-flat",
        title: (compare && compare.missing_reason) || MISSING,
      };
    }
    var d = Number(compare.delta_pct);
    var range = (compare.start && compare.end) ? (compare.start + " → " + compare.end) : "";
    return {
      text: (d >= 0 ? "↑ " : "↓ ") + Math.abs(d).toFixed(1) + "%",
      cls: d >= 0 ? "is-up" : "is-down",
      title: label(compare.mode) + (range ? " · " + range : ""),
    };
  }

  global.SeoPeriodCompare = {
    bounds: bounds,
    windowCovered: windowCovered,
    label: label,
    fromSeries: fromSeries,
    unavailable: unavailable,
    appendDeltaColumns: appendDeltaColumns,
    kpiText: kpiText,
    deltaPct: deltaPct,
    seriesTotal: seriesTotal,
  };
})(window);
