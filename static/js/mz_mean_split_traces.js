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
    var ms = window.seoMatteMeanSplit ? window.seoMatteMeanSplit() : null;
    var green = opts.green || (ms ? ms.green : dark ? "#4a8f73" : "#047857");
    var red = opts.red || (ms ? ms.red : dark ? "#a85a66" : "#b91c3c");
    var greenFill = opts.greenFill || (ms ? ms.greenFill : dark ? "rgba(74,143,115,0.14)" : "rgba(4,120,87,0.16)");
    var redFill = opts.redFill || (ms ? ms.redFill : dark ? "rgba(168,90,102,0.14)" : "rgba(185,28,60,0.14)");
    var width = opts.lineWidth != null ? opts.lineWidth : 2;
    var mode = opts.mode || "lines";
    var traces = [];
    var legendGroup =
      opts.legendgroup != null && opts.legendgroup !== ""
        ? String(opts.legendgroup)
        : opts.name != null && opts.name !== ""
          ? String(opts.name)
          : null;

    function withLegendGroup(tr) {
      if (legendGroup) tr.legendgroup = legendGroup;
      return tr;
    }

    var segX = [xs[0]];
    var segY = [ys[0]];
    var segSide = valueSide(ys[0], mean);

    function baselineYs() {
      var out = [];
      for (var bi = 0; bi < segX.length; bi++) out.push(mean);
      return out;
    }

    function flush() {
      if (segX.length < 2) {
        segX = [];
        segY = [];
        return;
      }
      var color = segSide === "above" ? green : red;
      var fillColor = segSide === "above" ? greenFill : redFill;
      var xCopy = segX.slice();
      var yCopy = segY.slice();
      var baseCopy = baselineYs();

      if (opts.fill) {
        /* Yahoo tarzı: yeşilde orta çizginin üstü, kırmızıda altı dolu */
        if (segSide === "above") {
          traces.push(withLegendGroup({
            x: xCopy,
            y: baseCopy,
            type: "scatter",
            mode: "lines",
            line: { width: 0, color: "rgba(0,0,0,0)" },
            hoverinfo: "skip",
            showlegend: false,
          }));
          var aboveTrace = {
            x: xCopy,
            y: yCopy,
            type: "scatter",
            mode: mode,
            connectgaps: opts.connectgaps !== false,
            line: { color: color, width: width, dash: opts.lineDash || undefined },
            fill: "tonexty",
            fillcolor: fillColor,
            showlegend: opts.showlegend === true && traces.length === 0,
          };
          if (opts.name && traces.length === 0) aboveTrace.name = opts.name;
          if (opts.yaxis) aboveTrace.yaxis = opts.yaxis;
          if (opts.markerSize) {
            aboveTrace.marker = { color: color, size: opts.markerSize, line: { width: 0 } };
          }
          if (opts.hoverinfo) aboveTrace.hoverinfo = opts.hoverinfo;
          traces.push(withLegendGroup(aboveTrace));
        } else {
          var belowLine = {
            x: xCopy,
            y: yCopy,
            type: "scatter",
            mode: mode,
            connectgaps: opts.connectgaps !== false,
            line: { color: color, width: width, dash: opts.lineDash || undefined },
            showlegend: opts.showlegend === true && traces.length === 0,
          };
          if (opts.name && traces.length === 0) belowLine.name = opts.name;
          if (opts.yaxis) belowLine.yaxis = opts.yaxis;
          if (opts.markerSize) {
            belowLine.marker = { color: color, size: opts.markerSize, line: { width: 0 } };
          }
          if (opts.hoverinfo) belowLine.hoverinfo = opts.hoverinfo;
          traces.push(withLegendGroup(belowLine));
          traces.push(withLegendGroup({
            x: xCopy,
            y: baseCopy,
            type: "scatter",
            mode: "lines",
            line: { width: 0, color: "rgba(0,0,0,0)" },
            fill: "tonexty",
            fillcolor: fillColor,
            hoverinfo: "skip",
            showlegend: false,
          }));
        }
      } else {
        var trace = {
          x: xCopy,
          y: yCopy,
          type: "scatter",
          mode: mode,
          connectgaps: opts.connectgaps !== false,
          line: { color: color, width: width, dash: opts.lineDash || undefined },
          showlegend: opts.showlegend === true && traces.length === 0,
        };
        if (opts.name && traces.length === 0) trace.name = opts.name;
        if (opts.yaxis) trace.yaxis = opts.yaxis;
        if (opts.markerSize) {
          trace.marker = { color: color, size: opts.markerSize, line: { width: 0 } };
        }
        if (opts.hoverinfo) trace.hoverinfo = opts.hoverinfo;
        traces.push(withLegendGroup(trace));
      }
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
