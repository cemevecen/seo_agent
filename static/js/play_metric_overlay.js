/**
 * Play / GA4 / Virgül / Market metrik overlay — /android ile aynı katalog, ad-virgul grafikleri.
 */
(function (global) {
  "use strict";

  var SERIES_COLORS = global.seoMatteMarketOverlayPalette
    ? global.seoMatteMarketOverlayPalette()
    : [
        "#2563EB", "#F59E0B", "#10B981", "#EF4444", "#8B5CF6",
        "#06B6D4", "#F97316", "#84CC16", "#EC4899", "#64748B",
      ];

  var METRIC_GROUPS = [
    {
      label: "Quality",
      items: [
        { key: "anrs", label: "ANR" },
        { key: "crashes", label: "Crash" },
      ],
    },
    {
      label: "Growth",
      items: [
        { key: "device_acquisition", label: "Device acquisition" },
        { key: "user_acquisition", label: "User acquisition" },
        { key: "user_lost", label: "User lost" },
        { key: "active_devices", label: "Active devices" },
        { key: "active_users", label: "Active users" },
        { key: "dau", label: "Daily active users (DAU)" },
        { key: "dau_mau", label: "DAU/MAU" },
      ],
    },
    {
      label: "Store",
      items: [
        { key: "ar2_visitors", label: "Store visitors" },
        { key: "ar2_acquisitions", label: "Store acquisitions" },
        { key: "store_listing_conversion", label: "Store listing conversion" },
      ],
    },
    {
      label: "Revenue",
      items: [{ key: "revenue", label: "Revenue" }],
    },
    {
      label: "Rating",
      items: [{ key: "rating", label: "Google Play rating + distribution" }],
    },
    {
      label: "GA4 (Android)",
      items: [
        { key: "ga4:sessions", label: "GA4 · Sessions" },
        { key: "ga4:users", label: "GA4 · Users" },
        { key: "ga4:engaged_sessions", label: "GA4 · Engaged sessions" },
        { key: "ga4:new_users", label: "GA4 · New users" },
        { key: "ga4:avg_session", label: "GA4 · Avg. session" },
        { key: "ga4:page_views", label: "GA4 · Page views" },
      ],
    },
    {
      label: "Virgül (Android)",
      items: [
        { key: "virgul:net_revenue", label: "Virgül · Net revenue (TL)" },
        { key: "virgul:ad_request", label: "Virgül · Ad request" },
        { key: "virgul:matched_request", label: "Virgül · Matched request" },
        { key: "virgul:impression", label: "Virgül · Impression" },
        { key: "virgul:click", label: "Virgül · Click" },
        { key: "virgul:ad_request_ecpm", label: "Virgül · Ad request eCPM (TL)" },
        { key: "virgul:ad_ecpm", label: "Virgül · Ad impression eCPM (TL)" },
        { key: "virgul:viewability_pct", label: "Virgül · Viewability (%)" },
        { key: "virgul:ctr_pct", label: "Virgül · CTR (%)" },
        { key: "virgul:coverage_pct", label: "Virgül · Coverage (%)" },
      ],
    },
    {
      label: "Market",
      items: [
        { key: "market:usd_try", label: "USD/TRY close" },
        { key: "market:eur_try", label: "EUR/TRY close" },
        { key: "market:gram_altin", label: "Gold gram close" },
        { key: "market:ceyrek_altin", label: "Quarter gold close" },
        { key: "market:gram_gumus", label: "Silver gram close" },
        { key: "market:bist100", label: "BIST 100 close" },
        { key: "market:brent", label: "Brent close" },
        { key: "market:bitcoin", label: "Bitcoin close" },
        { key: "market:all_indexed", label: "All (range start=100)" },
      ],
    },
  ];

  var LABEL_BY_KEY = {};
  METRIC_GROUPS.forEach(function (g) {
    (g.items || []).forEach(function (it) {
      LABEL_BY_KEY[it.key] = it.label;
    });
  });

  var MARKET_KEYS = [
    "usd_try", "eur_try", "gram_altin", "ceyrek_altin",
    "gram_gumus", "bist100", "brent", "bitcoin",
  ];

  var cache = {};

  function rootEl(controlId) {
    if (controlId) {
      var byId = document.getElementById(controlId);
      if (byId && byId.getAttribute("data-play-metric-overlay-root") != null) return byId;
    }
    return document.querySelector("[data-play-metric-overlay-root]");
  }

  function storageKeyForRoot(root) {
    return "play-metric-overlay-keys-v1-" + ((root && root.getAttribute("data-overlay-storage-key")) || "play");
  }

  function readStored(root) {
    try {
      var raw = global.localStorage.getItem(storageKeyForRoot(root));
      if (!raw) return [];
      var arr = JSON.parse(raw);
      return Array.isArray(arr) ? arr.filter(Boolean) : [];
    } catch (e) {
      return [];
    }
  }

  function writeStored(root, keys) {
    try {
      global.localStorage.setItem(storageKeyForRoot(root), JSON.stringify(keys || []));
    } catch (e) {
      /* ignore */
    }
  }

  function panelForRoot(root) {
    if (!root) return null;
    var rid = root.id;
    if (rid) {
      var docked = document.querySelector(
        '[data-play-metric-overlay-panel][data-play-metric-overlay-for="' + rid + '"]'
      );
      if (docked) return docked;
    }
    return root.querySelector("[data-play-metric-overlay-panel]");
  }

  function selectedFromDom(root) {
    if (!root) return [];
    var panel = panelForRoot(root);
    if (!panel) return [];
    var keys = [];
    panel.querySelectorAll("input[type=checkbox]:checked").forEach(function (cb) {
      if (cb.value) keys.push(cb.value);
    });
    return keys;
  }

  function metricLabel(key) {
    return LABEL_BY_KEY[key] || key;
  }

  function updateTriggerLabel(root) {
    if (!root) return;
    var labelEl = root.querySelector("[data-play-metric-overlay-label]");
    if (!labelEl) return;
    var keys = selectedFromDom(root);
    if (!keys.length) keys = readStored(root);
    if (!keys.length) {
      labelEl.textContent = "Metric: off";
      return;
    }
    if (keys.length === 1) {
      labelEl.textContent = "Metric: " + (metricLabel(keys[0]) || keys[0]);
      return;
    }
    labelEl.textContent = "Metric: " + keys.length + " series";
  }

  function modes(controlId) {
    var root = rootEl(controlId);
    if (!root) return [];
    var keys = selectedFromDom(root);
    if (!keys.length) keys = readStored(root);
    return expandMarketSelection(keys);
  }

  function isGa4Key(k) {
    return String(k || "").indexOf("ga4:") === 0;
  }
  function isVirgulKey(k) {
    return String(k || "").indexOf("virgul:") === 0;
  }
  function isMarketKey(k) {
    return String(k || "").indexOf("market:") === 0;
  }

  function expandMarketSelection(keys) {
    var out = [];
    var seen = {};
    (keys || []).forEach(function (k) {
      if (k === "market:all_indexed") {
        MARKET_KEYS.forEach(function (sk) {
          var mk = "market:" + sk;
          if (!seen[mk]) {
            seen[mk] = true;
            out.push(mk);
          }
        });
      } else if (!seen[k]) {
        seen[k] = true;
        out.push(k);
      }
    });
    return out;
  }

  function clearCache() {
    cache = {};
  }

  function normalizeDateKey(d) {
    if (d == null || d === "") return "";
    var s = String(d).trim();
    if (s.length >= 10 && s.charAt(4) === "-") return s.slice(0, 10);
    var parsed = Date.parse(s);
    if (!isNaN(parsed)) return new Date(parsed).toISOString().slice(0, 10);
    return s.slice(0, 10);
  }

  function valueMapFromSeries(series) {
    var m = {};
    (series || []).forEach(function (pt) {
      if (!pt || pt.key == null) return;
      var k = normalizeDateKey(pt.key);
      if (k) m[k] = Number(pt.value);
    });
    return m;
  }

  function cacheKey(metricKey, startIso, endIso, platform) {
    return (platform || "android") + "|" + (startIso || "") + "|" + (endIso || "") + "|" + metricKey;
  }

  async function fetchMarketSeries(metricKey, startIso, endIso) {
    if (!global.SeoMarketOverlay || !SeoMarketOverlay.ensureOverlay) {
      throw new Error("Market overlay unavailable");
    }
    var sk = String(metricKey).slice("market:".length);
    var payload = await SeoMarketOverlay.ensureOverlay(startIso, endIso);
    var pts = SeoMarketOverlay.pointsForSeries(payload, sk);
    var label = SeoMarketOverlay.seriesLabel
      ? SeoMarketOverlay.seriesLabel(payload, sk, false)
      : metricLabel(metricKey);
    return { label: label, series: pts || [] };
  }

  async function fetchGa4Series(metricKey, startIso, endIso, platform) {
    var qs = new URLSearchParams({
      start: startIso || "",
      end: endIso || "",
      metric: metricKey,
      project: "doviz",
      profile: platform || "android",
    });
    var r = await fetch("/api/play-analytics/ga4-series?" + qs.toString(), {
      credentials: "same-origin",
      cache: "no-store",
    });
    var data = await r.json().catch(function () { return {}; });
    if (!r.ok) throw new Error(data.message || data.detail || "GA4 HTTP " + r.status);
    if (data.configured === false || (data.ok === false && !Array.isArray(data.series))) {
      throw new Error(data.message || "No GA4 series");
    }
    return { label: data.label || metricLabel(metricKey), series: data.series || [] };
  }

  async function fetchVirgulSeries(metricKey, startIso, endIso, platform) {
    var qs = new URLSearchParams({
      start: startIso || "",
      end: endIso || "",
      metric: metricKey,
      project: "doviz",
      branch: platform || "android",
    });
    var r = await fetch("/api/play-analytics/virgul-series?" + qs.toString(), {
      credentials: "same-origin",
      cache: "no-store",
    });
    var data = await r.json().catch(function () { return {}; });
    if (!r.ok) throw new Error(data.message || data.detail || "Virgül HTTP " + r.status);
    if (data.configured === false || (data.ok === false && !Array.isArray(data.series))) {
      throw new Error(data.message || "No Virgül series");
    }
    return { label: data.label || metricLabel(metricKey), series: data.series || [] };
  }

  async function fetchPlaySeries(metricKey, startIso, endIso) {
    var qs = new URLSearchParams({
      start: startIso || "",
      end: endIso || "",
      metric: metricKey,
      breakdown: "date",
      dim: "overview",
      compare: "",
    });
    var r = await fetch("/api/play-analytics/query?" + qs.toString(), {
      credentials: "same-origin",
      cache: "no-store",
    });
    var data = await r.json().catch(function () { return {}; });
    if (!r.ok) throw new Error(data.detail || data.message || "Play HTTP " + r.status);
    return { label: metricLabel(metricKey), series: data.series || [] };
  }

  async function fetchMetricSeries(metricKey, startIso, endIso, platform) {
    var ck = cacheKey(metricKey, startIso, endIso, platform);
    if (cache[ck]) return cache[ck];
    var p;
    if (isMarketKey(metricKey)) p = fetchMarketSeries(metricKey, startIso, endIso);
    else if (isGa4Key(metricKey)) p = fetchGa4Series(metricKey, startIso, endIso, platform);
    else if (isVirgulKey(metricKey)) p = fetchVirgulSeries(metricKey, startIso, endIso, platform);
    else p = fetchPlaySeries(metricKey, startIso, endIso);
    cache[ck] = p;
    return p;
  }

  function pickFreeYaxisId(layout, traces) {
    var used = { y: true };
    if (layout) {
      Object.keys(layout).forEach(function (k) {
        if (k === "yaxis") used.y = true;
        var m = /^yaxis(\d+)$/.exec(k);
        if (m) used["y" + m[1]] = true;
      });
    }
    (traces || []).forEach(function (t) {
      used[t.yaxis || "y"] = true;
    });
    for (var n = 2; n <= 8; n++) {
      if (!used["y" + n]) return "y" + n;
    }
    return "y8";
  }

  function layoutKeyForYaxis(yaxisId) {
    var id = yaxisId || "y5";
    if (id === "y") return "yaxis";
    var n = id.replace(/^y/, "");
    return n ? "yaxis" + n : "yaxis";
  }

  function hiddenOverlayYaxisLayout() {
    return {
      overlaying: "y",
      side: "right",
      showgrid: false,
      zeroline: false,
      automargin: false,
      showticklabels: false,
      showline: false,
      ticks: "",
      title: { text: "" },
    };
  }

  function visibleOverlayYaxisLayout(lineColor, title) {
    return {
      title: { text: title || "Metric", font: { size: 10, color: lineColor } },
      tickfont: { size: 10, color: lineColor },
      overlaying: "y",
      side: "right",
      showgrid: false,
      zeroline: false,
      automargin: true,
      nticks: 5,
    };
  }

  async function apply(traces, layout, dateKeys, platform, overlayKeys, opts) {
    var keys = expandMarketSelection(overlayKeys || []);
    if (!keys.length || !platform || !dateKeys || !dateKeys.length || !traces || !layout) {
      return false;
    }
    opts = opts || {};
    var startIso = opts.start || normalizeDateKey(dateKeys[0]);
    var endIso = opts.end || normalizeDateKey(dateKeys[dateKeys.length - 1]);
    var added = false;
    var colorIdx = 0;
    var axisCount = 0;
    for (var i = 0; i < keys.length; i++) {
      var mk = keys[i];
      try {
        var pack = await fetchMetricSeries(mk, startIso, endIso, platform);
        var vals = valueMapFromSeries(pack.series);
        var ys = dateKeys.map(function (d) {
          var k = normalizeDateKey(d);
          return vals[k] != null && Number.isFinite(vals[k]) ? vals[k] : null;
        });
        if (!ys.some(function (v) { return v != null; })) continue;
        var lineColor = SERIES_COLORS[colorIdx % SERIES_COLORS.length];
        colorIdx += 1;
        var traceYaxis = pickFreeYaxisId(layout, traces);
        var layoutKey = layoutKeyForYaxis(traceYaxis);
        var axisTitle = pack.label || metricLabel(mk);
        var showAxisChrome = axisCount === 0;
        traces.push({
          x: dateKeys,
          y: ys,
          type: "scatter",
          mode: "lines",
          name: axisTitle,
          visible: true,
          yaxis: traceYaxis,
          line: { color: lineColor, width: 2 },
          connectgaps: false,
          hovertemplate: axisTitle + ": %{y:,.4~g}<extra></extra>",
        });
        layout[layoutKey] = Object.assign(
          showAxisChrome ? visibleOverlayYaxisLayout(lineColor, "Metric") : hiddenOverlayYaxisLayout(),
          layout[layoutKey] || {}
        );
        axisCount += 1;
        added = true;
      } catch (e) {
        /* skip unavailable metric */
      }
    }
    if (!added) return false;
    layout.showlegend = true;
    return true;
  }

  function buildPanelHtml() {
    var html = "";
    METRIC_GROUPS.forEach(function (g) {
      html +=
        '<p class="px-2.5 pb-0.5 pt-1.5 text-[9px] font-bold uppercase tracking-wide text-slate-500 dark:text-zinc-400">' +
        g.label +
        "</p>";
      (g.items || []).forEach(function (it) {
        html +=
          '<label class="flex cursor-pointer items-start gap-2 px-2.5 py-1.5 text-left text-[11px] font-medium leading-snug text-slate-700 hover:bg-slate-100 dark:text-zinc-200 dark:hover:bg-zinc-800" data-play-metric-overlay-option="' +
          it.key +
          '">' +
          '<input type="checkbox" class="mt-0.5 shrink-0 rounded border-slate-300 text-indigo-600 focus:ring-indigo-500 dark:border-zinc-600" value="' +
          it.key +
          '" />' +
          '<span class="min-w-0 flex-1">' +
          it.label +
          "</span></label>";
      });
    });
    return html;
  }

  function dockPanel(panel) {
    if (!panel || panel.dataset.playMetricOverlayDocked === "1") return;
    panel._playMetricHome = { parent: panel.parentNode, next: panel.nextSibling };
    document.body.appendChild(panel);
    panel.dataset.playMetricOverlayDocked = "1";
  }

  function undockPanel(panel) {
    if (!panel || panel.dataset.playMetricOverlayDocked !== "1") return;
    var home = panel._playMetricHome;
    if (home && home.parent) {
      if (home.next) home.parent.insertBefore(panel, home.next);
      else home.parent.appendChild(panel);
    }
    delete panel._playMetricHome;
    delete panel.dataset.playMetricOverlayDocked;
  }

  function resetPanelPosition(panel) {
    if (!panel) return;
    panel.style.position = "";
    panel.style.top = "";
    panel.style.left = "";
    panel.style.right = "";
    panel.style.zIndex = "";
    panel.style.width = "";
    panel.style.maxWidth = "";
    panel.style.maxHeight = "";
    panel.style.overflowY = "";
  }

  function positionPanel(trigger, panel) {
    if (!trigger || !panel) return;
    dockPanel(panel);
    var margin = 8;
    var maxW = Math.max(260, Math.min(360, window.innerWidth - margin * 2));
    panel.style.width = maxW + "px";
    panel.style.maxWidth = maxW + "px";
    panel.style.maxHeight = Math.min(Math.round(window.innerHeight * 0.72), 380) + "px";
    panel.style.overflowY = "auto";
    var w = panel.offsetWidth || maxW;
    var r = trigger.getBoundingClientRect();
    var left = Math.round(Math.min(r.right - w, window.innerWidth - w - margin));
    if (left < margin) left = margin;
    var top = Math.round(r.bottom + 4);
    var panelH = panel.offsetHeight || 320;
    if (top + panelH > window.innerHeight - margin) {
      top = Math.max(margin, Math.round(r.top - panelH - 4));
    }
    panel.style.position = "fixed";
    panel.style.top = top + "px";
    panel.style.left = left + "px";
    panel.style.right = "auto";
    panel.style.zIndex = "10000";
  }

  function closePanel(root, trigger, panel) {
    if (!panel) return;
    panel.classList.add("hidden");
    resetPanelPosition(panel);
    undockPanel(panel);
    if (trigger) trigger.setAttribute("aria-expanded", "false");
  }

  function bindRoot(root, onChange) {
    if (!root || root.dataset.playMetricOverlayBound === "1") return;
    root.dataset.playMetricOverlayBound = "1";
    var panel = panelForRoot(root);
    var trigger = root.querySelector("[data-play-metric-overlay-trigger]");
    if (panel && !panel.dataset.playMetricBuilt) {
      panel.innerHTML = buildPanelHtml();
      panel.dataset.playMetricBuilt = "1";
    }
    if (panel && root.id) panel.setAttribute("data-play-metric-overlay-for", root.id);
    var stored = readStored(root);
    if (panel) {
      panel.querySelectorAll("input[type=checkbox]").forEach(function (cb) {
        cb.checked = stored.indexOf(cb.value) >= 0;
      });
    }
    updateTriggerLabel(root);
    function fire() {
      writeStored(root, selectedFromDom(root));
      updateTriggerLabel(root);
      clearCache();
      if (typeof onChange === "function") onChange();
      var attr = root.getAttribute("data-overlay-on-change");
      if (attr && typeof global[attr] === "function") global[attr]();
    }
    if (panel) {
      panel.querySelectorAll("input[type=checkbox]").forEach(function (cb) {
        cb.addEventListener("change", fire);
      });
    }
    if (trigger && panel) {
      trigger.addEventListener("click", function (ev) {
        ev.preventDefault();
        ev.stopPropagation();
        global.__playMetricOverlayIgnoreCloseUntil = Date.now() + 280;
        if (panel.classList.contains("hidden")) {
          panel.classList.remove("hidden");
          positionPanel(trigger, panel);
          trigger.setAttribute("aria-expanded", "true");
        } else {
          closePanel(root, trigger, panel);
        }
      });
      panel.addEventListener("click", function (ev) {
        ev.stopPropagation();
      });
    }
    if (!global.__playMetricOverlayDocClose) {
      global.__playMetricOverlayDocClose = true;
      document.addEventListener("click", function (ev) {
        if (global.__playMetricOverlayIgnoreCloseUntil && Date.now() < global.__playMetricOverlayIgnoreCloseUntil) {
          return;
        }
        var target = ev.target;
        document.querySelectorAll("[data-play-metric-overlay-root]").forEach(function (r) {
          var p = panelForRoot(r);
          var t = r.querySelector("[data-play-metric-overlay-trigger]");
          if (target && (r.contains(target) || (p && p.contains(target)))) return;
          closePanel(r, t, p);
        });
      });
      global.addEventListener("resize", function () {
        document.querySelectorAll("[data-play-metric-overlay-panel]").forEach(function (p) {
          if (p.classList.contains("hidden")) return;
          var forId = p.getAttribute("data-play-metric-overlay-for");
          var r = forId ? document.getElementById(forId) : null;
          var t = r && r.querySelector("[data-play-metric-overlay-trigger]");
          if (t) positionPanel(t, p);
        });
      });
    }
  }

  function bindWhenReady(controlId, onChange) {
    function tryBind() {
      var root = rootEl(controlId);
      if (!root) return false;
      bindRoot(root, onChange);
      return true;
    }
    if (tryBind()) return;
    var n = 0;
    var t = global.setInterval(function () {
      n += 1;
      if (tryBind() || n > 80) global.clearInterval(t);
    }, 100);
  }

  function autoBindPlayMetricOverlays() {
    document.querySelectorAll("[data-play-metric-overlay-root]").forEach(function (root) {
      bindRoot(root, null);
    });
  }

  global.PlayMetricOverlay = {
    METRIC_GROUPS: METRIC_GROUPS,
    modes: modes,
    clearCache: clearCache,
    apply: apply,
    bindWhenReady: bindWhenReady,
    autoBindPlayMetricOverlays: autoBindPlayMetricOverlays,
    metricLabel: metricLabel,
  };
})(window);
