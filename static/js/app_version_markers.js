/**
 * Uygulama sürüm yayın işaretçileri (iOS / Android).
 *
 * Plotly grafiklerinde yatay (tarih) ekseninde sürüm yayın tarihlerini
 * nokta + ince dikey kılavuz çizgisi olarak gösterir. Üzerine gelince
 * sürüm notu (versiyon / build / tarih) tooltip olarak çıkar.
 *
 * Kullanım:  window.AppVersionMarkers.decorate(plotlyEl, 'ios'|'android'|'all')
 * Grafik tarih ekseni kullanmıyorsa (hafta/ay etiketi vb.) veya aralıkta
 * sürüm yoksa sessizce atlar.
 */
(function () {
  "use strict";

  var CACHE = null;
  var PROMISE = null;

  function load() {
    if (CACHE) return Promise.resolve(CACHE);
    if (PROMISE) return PROMISE;
    PROMISE = fetch("/api/app/version-releases", {
      headers: { Accept: "application/json" },
    })
      .then(function (r) {
        return r.ok ? r.json() : { ios: [], android: [] };
      })
      .then(function (d) {
        CACHE = { ios: (d && d.ios) || [], android: (d && d.android) || [] };
        return CACHE;
      })
      .catch(function () {
        CACHE = { ios: [], android: [] };
        return CACHE;
      });
    return PROMISE;
  }

  function releasesFor(data, platform) {
    if (platform === "ios") return data.ios || [];
    if (platform === "android") return data.android || [];
    return (data.ios || []).concat(data.android || []); // genel: ikisi birden
  }

  function dateMs(s) {
    if (!s) return NaN;
    var t = Date.parse(String(s).slice(0, 10) + "T00:00:00");
    return isNaN(t) ? NaN : t;
  }

  // Grafikteki mevcut serilerin x dizilerinden tarih aralığını bul.
  function rangeFromElement(el) {
    var data = (el && el.data) || [];
    var min = Infinity;
    var max = -Infinity;
    for (var i = 0; i < data.length; i++) {
      var xs = data[i] && data[i].x;
      if (!xs || !xs.length) continue;
      for (var j = 0; j < xs.length; j++) {
        var t = dateMs(xs[j]);
        if (isNaN(t)) continue;
        if (t < min) min = t;
        if (t > max) max = t;
      }
    }
    if (min === Infinity) return null;
    return [min, max];
  }

  function colorFor(platform) {
    if (platform === "ios") return "#0ea5e9"; // sky
    if (platform === "android") return "#16a34a"; // green
    return "#8b5cf6"; // violet
  }

  function symbolFor(platform) {
    return platform === "ios" ? "diamond" : "triangle-up";
  }

  function nameFor(platform) {
    if (platform === "ios") return "iOS sürüm";
    if (platform === "android") return "Android sürüm";
    return "Sürüm";
  }

  function buildGroup(list, platform, traces, shapes) {
    if (!list.length) return;
    var color = colorFor(platform);
    var x = [];
    var text = [];
    list.forEach(function (r) {
      x.push(r.date);
      var when = r.datetime || r.date;
      text.push(
        "<b>" + (r.label || platform + " " + (r.version || "")) + "</b><br>" + when
      );
      shapes.push({
        type: "line",
        xref: "x",
        yref: "paper",
        x0: r.date,
        x1: r.date,
        y0: 0,
        y1: 1,
        line: { color: color, width: 1, dash: "dot" },
        opacity: 0.16,
        layer: "below",
        __vm: true,
      });
    });
    traces.push({
      x: x,
      y: x.map(function () {
        return 0.02;
      }),
      yaxis: "y3",
      type: "scatter",
      mode: "markers",
      name: nameFor(platform),
      marker: {
        symbol: symbolFor(platform),
        size: 11,
        color: color,
        line: { color: "#ffffff", width: 1.2 },
      },
      text: text,
      hovertemplate: "%{text}<extra></extra>",
      hoverlabel: { align: "left" },
      cliponaxis: false,
      showlegend: false,
      __vm: true,
    });
  }

  function buildArtifacts(releases, range, combined) {
    var inRange = releases.filter(function (r) {
      var t = dateMs(r.date);
      if (isNaN(t)) return false;
      if (range && (t < range[0] - 864e5 || t > range[1] + 864e5)) return false;
      return true;
    });
    if (!inRange.length) return null;

    var traces = [];
    var shapes = [];
    if (combined) {
      buildGroup(
        inRange.filter(function (r) {
          return r.platform === "ios";
        }),
        "ios",
        traces,
        shapes
      );
      buildGroup(
        inRange.filter(function (r) {
          return r.platform === "android";
        }),
        "android",
        traces,
        shapes
      );
    } else {
      buildGroup(inRange, inRange[0].platform || "all", traces, shapes);
    }
    return { traces: traces, shapes: shapes };
  }

  function decorate(el, platform, opts) {
    opts = opts || {};
    if (!window.Plotly || !el) return Promise.resolve(false);
    var combined =
      platform === "all" || platform === "combined" || platform === "genel" || !platform;
    return load()
      .then(function (data) {
        var releases = releasesFor(data, combined ? "all" : platform);
        if (!releases.length) return false;
        var range = opts.range || rangeFromElement(el);
        if (!range) return false; // tarih ekseni değil -> atla
        var art = buildArtifacts(releases, range, combined);
        if (!art) return false;
        var existing = (el.layout && el.layout.shapes) || [];
        existing = existing.filter(function (s) {
          return !s.__vm; // önceki sürüm çizgilerini temizle (idempotent)
        });
        return Plotly.relayout(el, {
          yaxis3: {
            overlaying: "y",
            range: [0, 1],
            visible: false,
            fixedrange: true,
            showgrid: false,
            zeroline: false,
          },
          shapes: existing.concat(art.shapes),
        })
          .then(function () {
            return Plotly.addTraces(el, art.traces);
          })
          .then(function () {
            return true;
          });
      })
      .catch(function () {
        return false;
      });
  }

  window.AppVersionMarkers = { load: load, decorate: decorate };
})();
