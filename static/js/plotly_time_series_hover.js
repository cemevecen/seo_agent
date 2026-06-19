/**
 * Site geneli: zaman serisi / çoklu seri Plotly grafiklerinde x-unified hover
 * (hoverdistance -1, dikey şerit spike, marker hit-test bypass).
 */
(function (global) {
  "use strict";

  function isDarkTheme() {
    var el = global.document && global.document.documentElement;
    if (!el) return false;
    return el.classList.contains("dark") || el.classList.contains("midnight");
  }

  function spikeColor() {
    return isDarkTheme() ? "#71717a" : "#94a3b8";
  }

  function layoutPatch() {
    return {
      hovermode: "x unified",
      hoverdistance: -1,
      spikedistance: -1,
      xaxis: {
        showspikes: true,
        spikethickness: 1,
        spikecolor: spikeColor(),
        spikedash: "dot",
        spikesnap: "cursor",
        spikemode: "across",
      },
    };
  }

  function shouldApply(layout, traces) {
    layout = layout || {};
    traces = traces || [];
    if (layout._seoSkipTimeSeriesHover) return false;
    if (layout.hovermode === false) return false;
    if (!traces.length) return false;
    var onlyNonCartesian = traces.every(function (t) {
      if (!t) return true;
      var ty = t.type || "scatter";
      return (
        ty === "pie" ||
        ty === "histogram" ||
        ty === "histogram2d" ||
        ty === "heatmap" ||
        ty === "sankey" ||
        ty === "parcoords" ||
        ty === "indicator"
      );
    });
    if (onlyNonCartesian) return false;
    return traces.some(function (t) {
      if (!t) return false;
      var ty = t.type || "scatter";
      if (ty === "pie" || ty === "sankey" || ty === "parcoords") return false;
      if (t.yref === "paper") return false;
      return t.x && t.x.length > 1;
    });
  }

  function mergeLayout(layout) {
    var patch = layoutPatch();
    var out = Object.assign({}, layout || {});
    if (!out.hovermode || String(out.hovermode).indexOf("x") < 0) {
      out.hovermode = patch.hovermode;
    }
    if (out.hoverdistance == null) out.hoverdistance = patch.hoverdistance;
    if (out.spikedistance == null) out.spikedistance = patch.spikedistance;
    out.xaxis = Object.assign({}, patch.xaxis, out.xaxis || {});
    return out;
  }

  function expandMarkerDecorations(traces) {
    if (!traces || !traces.length) return traces;
    var out = [];
    traces.forEach(function (t) {
      if (!t || t.type !== "scatter" || t.yref === "paper" || t.hoverinfo === "skip") {
        out.push(t);
        return;
      }
      var mode = String(t.mode || "lines");
      var hasMarkers = mode.indexOf("markers") >= 0;
      if (!hasMarkers) {
        out.push(t);
        return;
      }
      var lineMode = mode
        .replace(/\+?markers\+?/g, "")
        .replace(/^\++|\++$/g, "")
        .trim();
      if (!lineMode) lineMode = "lines";
      var markerCopy = Object.assign({}, t.marker || {});
      var group =
        t.legendgroup != null && t.legendgroup !== ""
          ? String(t.legendgroup)
          : t.name != null && t.name !== ""
            ? String(t.name)
            : "__seo_tr_" + out.length;
      var lineTrace = Object.assign({}, t, { mode: lineMode, legendgroup: group });
      delete lineTrace.marker;
      if (markerCopy.size > 0) {
        out.push(lineTrace);
        out.push({
          x: t.x,
          y: t.y,
          type: "scatter",
          mode: "markers",
          marker: markerCopy,
          hoverinfo: "skip",
          showlegend: false,
          legendgroup: group,
          connectgaps: t.connectgaps,
          yaxis: t.yaxis,
          xaxis: t.xaxis,
          name: t.name || "",
        });
      } else {
        out.push(lineTrace);
      }
    });
    return out;
  }

  function plotDragSurface(gd) {
    if (!gd || !gd.querySelector) return gd;
    return (
      gd.querySelector(".nsewdrag") ||
      gd.querySelector(".drag") ||
      gd.querySelector(".draglayer") ||
      gd
    );
  }

  function syncColumnHover(gd, ev) {
    if (!gd || !global.Plotly || !Plotly.Fx || typeof Plotly.Fx.hover !== "function") return;
    var fl = gd._fullLayout;
    if (!fl || !fl.hovermode || String(fl.hovermode).indexOf("x") < 0) return;
    var surface = plotDragSurface(gd);
    if (!surface) return;
    var bb = surface.getBoundingClientRect();
    var xPx = ev.clientX - bb.left;
    var yPx = ev.clientY - bb.top;
    if (xPx < 0 || yPx < 0 || xPx > bb.width || yPx > bb.height) {
      try {
        Plotly.Fx.unhover(gd);
      } catch (_) {
        /* ignore */
      }
      return;
    }
    var hoverEvt = {
      type: "mousemove",
      target: surface,
      clientX: ev.clientX,
      clientY: ev.clientY,
      preventDefault: function () {},
      stopPropagation: function () {},
    };
    try {
      Plotly.Fx.hover(gd, hoverEvt);
    } catch (_) {
      /* ignore */
    }
  }

  function bindColumnHover(gd, layout) {
    if (!gd) return;
    if (gd._seoColHoverHandler) {
      gd.removeEventListener("mousemove", gd._seoColHoverHandler, true);
      gd._seoColHoverHandler = null;
    }
    if (gd._seoColLeaveHandler) {
      gd.removeEventListener("mouseleave", gd._seoColLeaveHandler, true);
      gd._seoColLeaveHandler = null;
    }
    var hm = (layout && layout.hovermode) || (gd._fullLayout && gd._fullLayout.hovermode);
    if (!hm || String(hm).indexOf("x") < 0) return;
    gd._seoColHoverHandler = function (ev) {
      syncColumnHover(gd, ev);
    };
    gd._seoColLeaveHandler = function () {
      try {
        if (Plotly.Fx && Plotly.Fx.unhover) Plotly.Fx.unhover(gd);
      } catch (_) {
        /* ignore */
      }
    };
    gd.addEventListener("mousemove", gd._seoColHoverHandler, true);
    gd.addEventListener("mouseleave", gd._seoColLeaveHandler, true);
  }

  function patchNewPlot() {
    if (!global.Plotly || global.Plotly.__seoTimeSeriesHoverPatched) return;
    global.Plotly.__seoTimeSeriesHoverPatched = true;
    var orig = global.Plotly.newPlot;
    global.Plotly.newPlot = function (gd, traces, layout, config) {
      traces = traces || [];
      layout = layout || {};
      var apply = shouldApply(layout, traces);
      if (apply) {
        traces = expandMarkerDecorations(traces.slice());
        layout = mergeLayout(layout);
      }
      var p = orig.call(global.Plotly, gd, traces, layout, config);
      if (!p || typeof p.then !== "function") return p;
      return p.then(function (gdOut) {
        var root = gdOut || (typeof gd === "string" ? global.document.getElementById(gd) : gd);
        if (apply && root) bindColumnHover(root, layout);
        return gdOut;
      });
    };
  }

  function install() {
    patchNewPlot();
  }

  global.SeoPlotlyTimeSeriesHover = {
    shouldApply: shouldApply,
    mergeLayout: mergeLayout,
    expandMarkerDecorations: expandMarkerDecorations,
    bindColumnHover: bindColumnHover,
    install: install,
  };

  if (global.Plotly) {
    install();
  } else if (global.document) {
    var attempts = 0;
    var timer = global.setInterval(function () {
      attempts += 1;
      if (global.Plotly) {
        global.clearInterval(timer);
        install();
      } else if (attempts > 200) {
        global.clearInterval(timer);
      }
    }, 50);
  }
})(typeof window !== "undefined" ? window : globalThis);
