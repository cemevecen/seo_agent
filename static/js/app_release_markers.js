/**
 * App Store / Play sürüm yayınları — zaman eksenli grafiklerde alt şerit işaretleri.
 */
(function (global) {
  "use strict";

  var SINCE_DEFAULT = "2025-01-01";

  function escHtml(s) {
    return String(s == null ? "" : s)
      .replace(/&/g, "&amp;")
      .replace(/</g, "&lt;")
      .replace(/>/g, "&gt;")
      .replace(/"/g, "&quot;");
  }

  function parseAt(iso) {
    if (!iso) return null;
    try {
      var d = new Date(iso);
      return isNaN(d.getTime()) ? null : d;
    } catch (e) {
      return null;
    }
  }

  function formatTr(iso) {
    var d = parseAt(iso);
    if (!d) return String(iso || "—");
    return d.toLocaleString("tr-TR", {
      day: "2-digit",
      month: "short",
      year: "numeric",
      hour: "2-digit",
      minute: "2-digit",
    });
  }

  function filterInRange(releases, xMin, xMax) {
    if (!releases || !releases.length) return [];
    return releases.filter(function (r) {
      var d = parseAt(r.released_at);
      if (!d) return false;
      if (xMin && d < xMin) return false;
      if (xMax && d > xMax) return false;
      return true;
    });
  }

  function inferXRangeFromPlot(el) {
    if (!el || !el.data || !el.data.length) return { min: null, max: null };
    var xs = [];
    el.data.forEach(function (tr) {
      if (!tr.x || tr.yref === "paper") return;
      (tr.x || []).forEach(function (x) {
        var d = x instanceof Date ? x : parseAt(x) || new Date(x);
        if (!isNaN(d.getTime())) xs.push(d);
      });
    });
    if (!xs.length) return { min: null, max: null };
    xs.sort(function (a, b) {
      return a - b;
    });
    return { min: xs[0], max: xs[xs.length - 1] };
  }

  function buildReleaseTrace(releases, opts) {
    opts = opts || {};
    if (!releases || !releases.length) return null;
    var yPaper = typeof opts.yPaper === "number" ? opts.yPaper : 0.035;
    var xs = [];
    var ys = [];
    var custom = [];
    releases.forEach(function (r) {
      xs.push(r.released_at);
      ys.push(yPaper);
      custom.push([r.version || "—", formatTr(r.released_at)]);
    });
    return {
      type: "scatter",
      mode: "markers",
      x: xs,
      y: ys,
      yref: "paper",
      marker: {
        symbol: "square",
        size: opts.markerSize || 9,
        color: opts.markerColor || "#94a3b8",
        line: { width: 1, color: "rgba(255,255,255,0.35)" },
      },
      hoverinfo: "skip",
      showlegend: false,
      name: "Sürüm",
    };
  }

  function applyToPlotly(el, releases, opts) {
    if (!el || !global.Plotly || !releases || !releases.length) {
      return Promise.resolve();
    }
    opts = opts || {};
    var range = inferXRangeFromPlot(el);
    var filtered = filterInRange(releases, range.min, range.max);
    if (!filtered.length) return Promise.resolve();
    var trace = buildReleaseTrace(filtered, opts);
    if (!trace) return Promise.resolve();
    var layoutPatch = { margin: { b: opts.marginBottom || 56 } };
    return global.Plotly.addTraces(el, trace).then(function () {
      return global.Plotly.relayout(el, layoutPatch);
    });
  }

  function releasesForPlatform(store, platform) {
    store = store || {};
    if (platform === "android") return store.android || [];
    if (platform === "ios") return store.ios || [];
    return [];
  }

  function decorate(el, platform, store) {
    var list = releasesForPlatform(store, platform);
    if (!list.length) return Promise.resolve();
    return applyToPlotly(el, list, {
      markerColor: platform === "android" ? "#22c55e" : "#6366f1",
    });
  }

  function markerStripHtml(releases, range) {
    if (!releases || !releases.length || !range || !range.start || !range.end) {
      return "";
    }
    var start = range.start instanceof Date ? range.start : parseAt(range.start);
    var end = range.end instanceof Date ? range.end : parseAt(range.end);
    if (!start || !end || end <= start) return "";
    var span = end.getTime() - start.getTime();
    var items = filterInRange(releases, start, end);
    if (!items.length) return "";
    var dots = items
      .map(function (r) {
        var d = parseAt(r.released_at);
        if (!d) return "";
        var pct = ((d.getTime() - start.getTime()) / span) * 100;
        pct = Math.max(1.5, Math.min(98.5, pct));
        var title = "v" + (r.version || "—") + " · " + formatTr(r.released_at);
        return (
          '<span class="app-rel-marker" style="left:' +
          pct.toFixed(2) +
          '%" title="' +
          escHtml(title) +
          '" data-tip="' +
          escHtml(title) +
          '" role="button" tabindex="0" aria-label="' +
          escHtml(title) +
          '"></span>'
        );
      })
      .join("");
    return '<div class="app-rel-marker-track relative mt-0.5 h-2.5 w-full">' + dots + "</div>";
  }

  function load(product, since) {
    var q =
      "/api/app/version-releases?product=" +
      encodeURIComponent(product || "doviz") +
      "&since=" +
      encodeURIComponent(since || SINCE_DEFAULT);
    return fetch(q, { cache: "no-store", credentials: "same-origin" }).then(function (r) {
      return r.json();
    });
  }

  function mergeFromIntelPayload(payload, target) {
    target = target || {};
    var vr = payload && payload.version_releases;
    if (vr && (vr.ios || vr.android)) {
      target.ios = vr.ios || [];
      target.android = vr.android || [];
      target.since = vr.since;
      return target;
    }
    return target;
  }

  function initMarkerTouchTips() {
    if (initMarkerTouchTips._bound) return;
    initMarkerTouchTips._bound = true;
    document.addEventListener(
      "click",
      function (e) {
        var m = e.target && e.target.closest ? e.target.closest(".app-rel-marker") : null;
        if (!m) {
          document.querySelectorAll(".app-rel-marker--show-tip").forEach(function (el) {
            el.classList.remove("app-rel-marker--show-tip");
          });
          return;
        }
        e.stopPropagation();
        var open = m.classList.contains("app-rel-marker--show-tip");
        document.querySelectorAll(".app-rel-marker--show-tip").forEach(function (el) {
          el.classList.remove("app-rel-marker--show-tip");
        });
        if (!open) m.classList.add("app-rel-marker--show-tip");
      },
      true
    );
  }
  initMarkerTouchTips();

  global.AppReleaseMarkers = {
    SINCE_DEFAULT: SINCE_DEFAULT,
    parseAt: parseAt,
    formatTr: formatTr,
    filterInRange: filterInRange,
    buildReleaseTrace: buildReleaseTrace,
    applyToPlotly: applyToPlotly,
    decorate: decorate,
    markerStripHtml: markerStripHtml,
    load: load,
    mergeFromIntelPayload: mergeFromIntelPayload,
    releasesForPlatform: releasesForPlatform,
  };
})(typeof window !== "undefined" ? window : globalThis);
