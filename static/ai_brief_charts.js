/**
 * AI günlük özet: GA4 / PageSpeed / Search Console özet grafikleri (Plotly).
 * base.html + htmx:afterSwap üzerinden tetiklenir; HTMX ile gelen kısmi sayfada inline betik çalışmasa bile grafikler çizilir.
 */
(function () {
  function resolveVisualPack(byDomain, rawDom) {
    var d = (rawDom || "").toLowerCase().trim();
    if (!d || !byDomain) {
      return null;
    }
    if (byDomain[d]) {
      return byDomain[d];
    }
    var noWww = d.replace(/^www\./, "");
    if (noWww !== d && byDomain[noWww]) {
      return byDomain[noWww];
    }
    var withWww = d.indexOf("www.") === 0 ? d : "www." + d;
    if (withWww !== d && byDomain[withWww]) {
      return byDomain[withWww];
    }
    for (var k in byDomain) {
      if (!Object.prototype.hasOwnProperty.call(byDomain, k)) {
        continue;
      }
      var nk = String(k).toLowerCase().replace(/^www\./, "");
      if (nk === noWww) {
        return byDomain[k];
      }
    }
    return null;
  }

  function initAiBriefVisualCharts(attempt) {
    attempt = attempt || 0;
    var root = document.getElementById("ai-brief-layout-root");
    if (!root) {
      return;
    }
    if (!window.Plotly) {
      if (attempt < 40) {
        window.setTimeout(function () {
          initAiBriefVisualCharts(attempt + 1);
        }, 150);
      }
      return;
    }
    var jsonEl = document.getElementById("ai-brief-visual-json");
    if (!jsonEl) {
      return;
    }
    var text = (jsonEl.textContent || "").trim();
    if (!text) {
      return;
    }
    var payload;
    try {
      payload = JSON.parse(text);
    } catch (e) {
      return;
    }
    var byDomain = (payload && payload.by_domain) || {};
    var th = window.seoPlotlyTheme
      ? window.seoPlotlyTheme()
      : { grid: "#e2e8f0", tick: "#64748b", legend: "#475569", paper: "rgba(0,0,0,0)", plot: "rgba(0,0,0,0)" };

    root.querySelectorAll(".ai-site-row[data-ai-brief-pillar]").forEach(function (row) {
      var pillar = (row.getAttribute("data-ai-brief-pillar") || "").toLowerCase();
      var dom = (row.getAttribute("data-ai-brief-site") || "").toLowerCase().trim();
      var slot = row.querySelector(".ai-brief-visual-slot");
      if (!slot) {
        return;
      }
      slot.querySelectorAll(".ai-brief-plot").forEach(function (el) {
        try {
          window.Plotly.purge(el);
        } catch (e) {}
      });
      slot.innerHTML = "";
      slot.classList.add("hidden");
      var pack = resolveVisualPack(byDomain, dom);
      if (!pack) {
        return;
      }
      var uid = dom.replace(/[^a-z0-9]+/gi, "-").replace(/^-|-$/g, "") || "site";
      var rid = Math.random().toString(36).slice(2, 8);

      if (pillar === "ga4") {
        var g = pack.ga4 || {};
        var bars = g.session_bars || [];
        var pieSpec = g.organic_pie;
        var hasBars = bars && bars.length;
        var hasPie = pieSpec && pieSpec.values && pieSpec.values.length === 2;
        if (!hasBars && !hasPie) {
          return;
        }
        slot.classList.remove("hidden");
        if (hasBars) {
          var barDiv = document.createElement("div");
          barDiv.className = "ai-brief-plot mb-2 min-h-[220px] w-full min-w-0";
          barDiv.id = "ai-brief-ga4-bar-" + uid + "-" + rid;
          slot.appendChild(barDiv);
          var traces = bars.map(function (t) {
            return {
              type: "bar",
              name: t.name,
              x: g.period_labels || [],
              y: (t.y || []).map(function (v) {
                return typeof v === "number" ? v : 0;
              }),
              marker: { opacity: 0.9 },
            };
          });
          window.Plotly.newPlot(
            barDiv,
            traces,
            {
              barmode: "group",
              margin: { l: 52, r: 12, t: 32, b: 48 },
              paper_bgcolor: th.paper,
              plot_bgcolor: th.plot,
              title: { text: "Oturumlar — çubuk (1 / 7 / 30 gün)", font: { size: 12, color: th.tick } },
              font: { color: th.tick, size: 11 },
              xaxis: { tickfont: { color: th.tick, size: 10 }, gridcolor: th.grid },
              yaxis: {
                tickfont: { color: th.tick, size: 10 },
                gridcolor: th.grid,
                title: { text: "Oturum", font: { size: 11, color: th.tick } },
              },
              showlegend: true,
              legend: { orientation: "h", y: -0.22, font: { size: 10, color: th.legend } },
            },
            { displayModeBar: false, responsive: true }
          );

          var lineDiv = document.createElement("div");
          lineDiv.className = "ai-brief-plot mb-2 min-h-[200px] w-full min-w-0";
          lineDiv.id = "ai-brief-ga4-line-" + uid + "-" + rid;
          slot.appendChild(lineDiv);
          var lineTraces = bars.map(function (t) {
            return {
              type: "scatter",
              mode: "lines+markers",
              name: t.name,
              x: g.period_labels || [],
              y: (t.y || []).map(function (v) {
                return typeof v === "number" ? v : 0;
              }),
              line: { width: 2.5, shape: "linear" },
              marker: { size: 9 },
            };
          });
          window.Plotly.newPlot(
            lineDiv,
            lineTraces,
            {
              margin: { l: 52, r: 12, t: 32, b: 44 },
              paper_bgcolor: th.paper,
              plot_bgcolor: th.plot,
              title: { text: "Oturumlar — çizgi", font: { size: 12, color: th.tick } },
              font: { color: th.tick, size: 11 },
              xaxis: { tickfont: { color: th.tick, size: 10 }, gridcolor: th.grid },
              yaxis: {
                tickfont: { color: th.tick, size: 10 },
                gridcolor: th.grid,
                title: { text: "Oturum", font: { size: 11, color: th.tick } },
              },
              showlegend: true,
              legend: { orientation: "h", y: -0.2, font: { size: 10, color: th.legend } },
            },
            { displayModeBar: false, responsive: true }
          );
        }
        if (hasPie) {
          var pieDiv = document.createElement("div");
          pieDiv.className =
            "ai-brief-plot min-h-[200px] w-full min-w-0 max-w-[20rem] justify-self-center sm:max-w-none";
          pieDiv.id = "ai-brief-ga4-pie-" + uid + "-" + rid;
          slot.appendChild(pieDiv);
          window.Plotly.newPlot(
            pieDiv,
            [
              {
                type: "pie",
                labels: pieSpec.labels,
                values: pieSpec.values,
                hole: 0.45,
                textinfo: "label+percent",
                insidetextorientation: "horizontal",
              },
            ],
            {
              margin: { l: 8, r: 8, t: 36, b: 8 },
              paper_bgcolor: th.paper,
              title: { text: pieSpec.title || "Organik pay", font: { size: 12, color: th.tick } },
              font: { color: th.tick, size: 11 },
              showlegend: false,
            },
            { displayModeBar: false, responsive: true }
          );
        }
      } else if (pillar === "pagespeed") {
        var ps = pack.pagespeed || {};
        if (ps.mobil == null && ps.masaustu == null) {
          return;
        }
        slot.classList.remove("hidden");
        var psEl = document.createElement("div");
        psEl.className = "ai-brief-plot min-h-[180px] w-full min-w-0";
        psEl.id = "ai-brief-ps-" + uid + "-" + rid;
        slot.appendChild(psEl);
        var px = ["Mobil", "Masaüstü"];
        var py = [ps.mobil != null ? ps.mobil : 0, ps.masaustu != null ? ps.masaustu : 0];
        window.Plotly.newPlot(
          psEl,
          [
            {
              type: "bar",
              x: px,
              y: py,
              marker: { color: ["#7c3aed", "#0d9488"] },
              text: py.map(String),
              textposition: "outside",
            },
          ],
          {
            margin: { l: 44, r: 16, t: 30, b: 48 },
            paper_bgcolor: th.paper,
            plot_bgcolor: th.plot,
            title: { text: "PageSpeed (güncel skor)", font: { size: 12, color: th.tick } },
            font: { color: th.tick, size: 11 },
            yaxis: {
              range: [0, 100],
              title: { text: "Skor", font: { size: 11, color: th.tick } },
              gridcolor: th.grid,
            },
            xaxis: { tickfont: { color: th.tick } },
          },
          { displayModeBar: false, responsive: true }
        );
      } else if (pillar === "search_console") {
        var sc = pack.search_console_7d || {};
        var cl = Number(sc.clicks) || 0;
        var im = Number(sc.impressions) || 0;
        if (cl <= 0 && im <= 0) {
          return;
        }
        slot.classList.remove("hidden");
        var scEl = document.createElement("div");
        scEl.className = "ai-brief-plot min-h-[200px] w-full min-w-0";
        scEl.id = "ai-brief-sc-" + uid + "-" + rid;
        slot.appendChild(scEl);
        var sub =
          (sc.position > 0 ? " · ort. konum " + sc.position : "") + (sc.ctr > 0 ? " · CTR %" + sc.ctr : "");
        window.Plotly.newPlot(
          scEl,
          [
            {
              type: "bar",
              x: ["Tıklama", "Gösterim"],
              y: [cl, im],
              marker: { color: ["#2563eb", "#94a3b8"] },
              text: [String(cl), String(im)],
              textposition: "auto",
            },
          ],
          {
            margin: { l: 56, r: 16, t: 36, b: 48 },
            paper_bgcolor: th.paper,
            plot_bgcolor: th.plot,
            title: { text: "Search Console (7 gün)" + sub, font: { size: 11, color: th.tick } },
            font: { color: th.tick, size: 11 },
            yaxis: {
              type: "log",
              title: { text: "Adet (log ölçek)", font: { size: 11, color: th.tick } },
              gridcolor: th.grid,
            },
            xaxis: { tickfont: { color: th.tick } },
            showlegend: false,
          },
          { displayModeBar: false, responsive: true }
        );
      }
    });
    window.requestAnimationFrame(function () {
      root.querySelectorAll(".ai-brief-plot").forEach(function (el) {
        try {
          if (window.Plotly) {
            window.Plotly.Plots.resize(el);
          }
        } catch (e) {}
      });
    });
  }

  window.seoInitAiBriefVisualCharts = initAiBriefVisualCharts;

  document.addEventListener("DOMContentLoaded", function () {
    window.setTimeout(function () {
      initAiBriefVisualCharts(0);
    }, 0);
  });

  document.addEventListener("seo-theme-change", function () {
    var root = document.getElementById("ai-brief-layout-root");
    if (!root || !window.Plotly) {
      return;
    }
    var th = window.seoPlotlyTheme ? window.seoPlotlyTheme() : {};
    root.querySelectorAll(".ai-brief-plot").forEach(function (el) {
      try {
        window.Plotly.relayout(el, {
          paper_bgcolor: th.paper,
          plot_bgcolor: th.plot,
          font: { color: th.tick },
          "xaxis.gridcolor": th.grid,
          "yaxis.gridcolor": th.grid,
        });
      } catch (e) {}
    });
  });
})();
