/**
 * Plotly scatter traces: line segments green when y >= series mean, red when below.
 */
(function (global) {
  function seriesMean(ys) {
    var sum = 0;
    var n = 0;
    for (var i = 0; i < ys.length; i++) {
      var y = Number(ys[i]);
      if (!isFinite(y)) continue;
      sum += y;
      n += 1;
    }
    return n ? sum / n : 0;
  }

  function valueSide(y, mean) {
    return Number(y) >= mean ? "above" : "below";
  }

  function crossPoint(xs, i, t, mean) {
    var crossY = mean;
    var crossX;
    if (typeof xs[0] === "number") {
      crossX = xs[i - 1] + t * (xs[i] - xs[i - 1]);
    } else {
      crossX = xs[i];
    }
    return { x: crossX, y: crossY };
  }

  function mzMeanSplitLineTraces(xs, ys, opts) {
    opts = opts || {};
    if (!xs || !ys || xs.length < 2 || ys.length < 2 || xs.length !== ys.length) {
      return [];
    }
    var mean = opts.mean != null ? Number(opts.mean) : seriesMean(ys);
    if (!isFinite(mean)) mean = 0;

    var dark = !!opts.dark;
    var green = opts.green || (dark ? "#34d399" : "#10b981");
    var red = opts.red || (dark ? "#fb7185" : "#ef4444");
    var greenFill = opts.greenFill || (dark ? "rgba(52,211,153,0.18)" : "rgba(16,185,129,0.2)");
    var redFill = opts.redFill || (dark ? "rgba(251,113,133,0.18)" : "rgba(239,68,68,0.2)");
    var width = opts.lineWidth != null ? opts.lineWidth : 2;
    var mode = opts.mode || "lines";
    var traces = [];

    var segX = [xs[0]];
    var segY = [ys[0]];
    var segSide = valueSide(ys[0], mean);

    function flush() {
      if (segX.length < 2) {
        segX = [];
        segY = [];
        return;
      }
      var color = segSide === "above" ? green : red;
      var trace = {
        x: segX.slice(),
        y: segY.slice(),
        type: "scatter",
        mode: mode,
        connectgaps: opts.connectgaps !== false,
        line: { color: color, width: width, dash: opts.lineDash || undefined },
        showlegend: opts.showlegend === true && traces.length === 0,
      };
      if (opts.name && traces.length === 0) trace.name = opts.name;
      if (opts.yaxis) trace.yaxis = opts.yaxis;
      if (opts.fill) {
        trace.fill = "tozeroy";
        trace.fillcolor = segSide === "above" ? greenFill : redFill;
      }
      if (opts.markerSize) {
        trace.marker = { color: color, size: opts.markerSize, line: { width: 0 } };
      }
      if (opts.hoverinfo) trace.hoverinfo = opts.hoverinfo;
      traces.push(trace);
      segX = [];
      segY = [];
    }

    for (var i = 1; i < ys.length; i++) {
      var y0 = Number(ys[i - 1]);
      var y1 = Number(ys[i]);
      var s0 = valueSide(y0, mean);
      var s1 = valueSide(y1, mean);
      if (s0 !== s1 && y0 !== y1) {
        var t = (mean - y0) / (y1 - y0);
        if (!isFinite(t)) t = 0.5;
        t = Math.max(0, Math.min(1, t));
        var cross = crossPoint(xs, i, t, mean);
        segX.push(cross.x);
        segY.push(cross.y);
        flush();
        segSide = s1;
        segX = [cross.x, xs[i]];
        segY = [cross.y, ys[i]];
      } else {
        segX.push(xs[i]);
        segY.push(ys[i]);
        segSide = s1;
      }
    }
    flush();
    return traces;
  }

  global.mzMeanSplitLineTraces = mzMeanSplitLineTraces;
  global.mzSeriesMean = seriesMean;
})(typeof window !== "undefined" ? window : globalThis);
