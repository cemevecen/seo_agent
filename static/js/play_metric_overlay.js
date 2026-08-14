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

  var METRIC_GROUPS_ANDROID = [
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
  ];

  var METRIC_GROUPS_IOS = [
    {
      label: "Acquisition",
      items: [
        { key: "units", label: "First-time downloads" },
        { key: "redownloads", label: "Redownloads" },
        { key: "total_downloads", label: "Total downloads" },
        { key: "installs", label: "Installs" },
        { key: "impressions", label: "Impressions" },
        { key: "page_views", label: "Product page views" },
        { key: "conversion_rate", label: "Conversion rate (%)" },
        { key: "active_devices", label: "Active devices" },
        { key: "uninstalls", label: "Uninstalls" },
        { key: "crashes", label: "Crashes" },
      ],
    },
    {
      label: "Revenue / subscriptions",
      items: [
        { key: "iap", label: "In-app purchases" },
        { key: "paying_users", label: "Paying users" },
        { key: "proceeds", label: "Proceeds (USD)" },
        { key: "sales", label: "Sales" },
        { key: "active_subscriptions", label: "Active subscriptions" },
        { key: "subscription_renewals", label: "Subscription renewals" },
        { key: "subscription_churned", label: "Subscription churn" },
        { key: "free_trials", label: "Free trials" },
      ],
    },
    {
      label: "GA4 (iOS)",
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
      label: "Virgül (iOS)",
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
  ];

  var GA4_OVERLAY_ITEMS = [
    { key: "ga4:sessions", label: "GA4 · Sessions" },
    { key: "ga4:users", label: "GA4 · Users" },
    { key: "ga4:engaged_sessions", label: "GA4 · Engaged sessions" },
    { key: "ga4:new_users", label: "GA4 · New users" },
    { key: "ga4:avg_session", label: "GA4 · Avg. session" },
    { key: "ga4:page_views", label: "GA4 · Page views" },
  ];
  var VIRGUL_OVERLAY_ITEMS = [
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
  ];
  var METRIC_GROUPS_WEB = [
    { label: "GA4 (Web)", items: GA4_OVERLAY_ITEMS },
    { label: "Virgül (Web)", items: VIRGUL_OVERLAY_ITEMS },
  ];
  var METRIC_GROUPS_MWEB = [
    { label: "GA4 (MWeb)", items: GA4_OVERLAY_ITEMS },
    { label: "Virgül (MWeb)", items: VIRGUL_OVERLAY_ITEMS },
  ];

  var MARKET_OVERLAY_ITEMS = [
    { key: "market:usd_try", label: "USD/TRY close" },
    { key: "market:eur_try", label: "EUR/TRY close" },
    { key: "market:gram_altin", label: "Gold gram close" },
    { key: "market:ceyrek_altin", label: "Quarter gold close" },
    { key: "market:gram_gumus", label: "Silver gram close" },
    { key: "market:bist100", label: "BIST 100 close" },
    { key: "market:brent", label: "Brent close" },
    { key: "market:bitcoin", label: "Bitcoin close" },
    { key: "market:all_indexed", label: "All (range start=100)" },
  ];
  var METRIC_GROUPS = METRIC_GROUPS_ANDROID;
  var XDATA_SKIP = { appVersion: 1 };
  var XDATA_PLATFORMS = ["android", "ios", "web", "mweb"];
  var XDATA_ITEMS = { android: [], ios: [], web: [], mweb: [] };
  var xdataLoadPromise = null;
  var DROPPED_OVERLAY_KEYS = { dau: 1, dau_mau: 1, active_users: 1 };

  function seedXdataFromWindow() {
    var pack = global.SEO_XDATA_METRIC_OPTIONS;
    if (!pack || typeof pack !== "object") return false;
    var any = false;
    XDATA_PLATFORMS.forEach(function (plat) {
      var arr = pack[plat];
      if (!Array.isArray(arr) || !arr.length) return;
      XDATA_ITEMS[plat] = arr
        .map(function (o) {
          if (!o) return null;
          var key = o.value || o.key;
          if (!key) return null;
          if (String(key).indexOf("xdata:") !== 0) key = "xdata:" + key;
          var col = String(key).slice("xdata:".length);
          if (XDATA_SKIP[col]) return null;
          return { key: String(key), label: o.label || key };
        })
        .filter(Boolean);
      if (XDATA_ITEMS[plat].length) any = true;
    });
    if (any) rebuildLabelIndex();
    return any;
  }

  var LABEL_BY_KEY = {};
  function rebuildLabelIndex() {
    LABEL_BY_KEY = {};
    METRIC_GROUPS_ANDROID.concat(METRIC_GROUPS_IOS, METRIC_GROUPS_WEB, METRIC_GROUPS_MWEB).forEach(function (g) {
      (g.items || []).forEach(function (it) {
        LABEL_BY_KEY[it.key] = it.label;
      });
    });
    XDATA_PLATFORMS.forEach(function (plat) {
      (XDATA_ITEMS[plat] || []).forEach(function (it) {
        LABEL_BY_KEY[it.key] = it.label;
      });
    });
    MARKET_OVERLAY_ITEMS.forEach(function (it) {
      LABEL_BY_KEY[it.key] = it.label;
    });
  }
  rebuildLabelIndex();
  seedXdataFromWindow();

  function xdataItemsFromMeta(meta, platform) {
    var cols = ((meta && meta.columns_by_platform) || {})[platform] || [];
    var labels = (meta && meta.labels) || {};
    return cols
      .filter(function (k) {
        return k && !XDATA_SKIP[k];
      })
      .map(function (k) {
        return { key: "xdata:" + k, label: labels[k] || k };
      });
  }

  function ensureXdataItems(done) {
    seedXdataFromWindow();
    if (XDATA_PLATFORMS.every(function (p) { return (XDATA_ITEMS[p] || []).length; })) {
      if (typeof done === "function") done();
      return;
    }
    if (xdataLoadPromise) {
      xdataLoadPromise.then(function () {
        if (typeof done === "function") done();
      });
      return;
    }
    xdataLoadPromise = fetch("/api/empower-intel/meta", {
      credentials: "same-origin",
      cache: "no-store",
    })
      .then(function (r) {
        return r.json();
      })
      .then(function (meta) {
        XDATA_PLATFORMS.forEach(function (plat) {
          XDATA_ITEMS[plat] = xdataItemsFromMeta(meta, plat);
        });
        rebuildLabelIndex();
      })
      .catch(function () {
        XDATA_PLATFORMS.forEach(function (plat) {
          XDATA_ITEMS[plat] = XDATA_ITEMS[plat] || [];
        });
      })
      .then(function () {
        if (typeof done === "function") done();
      });
  }

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
    var base = (root && root.getAttribute("data-overlay-storage-key")) || "play";
    var plat = platformForRoot(root);
    return "play-metric-overlay-keys-v2-" + base + "-" + plat;
  }

  function normalizeOverlayPlatform(raw) {
    var p = String(raw || "").toLowerCase();
    if (p === "desktop") return "web";
    if (p === "web" || p === "mweb" || p === "ios" || p === "android") return p;
    return "android";
  }

  function platformForRoot(root) {
    return normalizeOverlayPlatform((root && root.getAttribute("data-overlay-platform")) || "android");
  }

  function groupsForPlatform(platform) {
    var plat = normalizeOverlayPlatform(platform);
    var base = METRIC_GROUPS_ANDROID;
    var xitems = XDATA_ITEMS.android;
    if (plat === "ios") {
      base = METRIC_GROUPS_IOS;
      xitems = XDATA_ITEMS.ios;
    } else if (plat === "web") {
      base = METRIC_GROUPS_WEB;
      xitems = XDATA_ITEMS.web;
    } else if (plat === "mweb") {
      base = METRIC_GROUPS_MWEB;
      xitems = XDATA_ITEMS.mweb;
    }
    if (!xitems || !xitems.length) {
      return base.concat([{ label: "Market", items: MARKET_OVERLAY_ITEMS }]);
    }
    var out = [];
    var inserted = false;
    if (plat === "web" || plat === "mweb") {
      out.push({ label: "X-Data", items: xitems });
      inserted = true;
    }
    base.forEach(function (g) {
      out.push(g);
      if (!inserted && (g.label === "Rating" || g.label === "Revenue / subscriptions")) {
        out.push({ label: "X-Data", items: xitems });
        inserted = true;
      }
    });
    if (!inserted) out.push({ label: "X-Data", items: xitems });
    out.push({ label: "Market", items: MARKET_OVERLAY_ITEMS });
    return out;
  }

  function isDroppedOverlayKey(k, platform) {
    if (DROPPED_OVERLAY_KEYS[k]) return true;
    if (platform === "ios" && k === "sessions") return true;
    return false;
  }

  function readStored(root) {
    var plat = platformForRoot(root);
    try {
      var raw = global.localStorage.getItem(storageKeyForRoot(root));
      if (!raw) return [];
      var arr = JSON.parse(raw);
      return Array.isArray(arr)
        ? arr.filter(function (k) {
            return k && !isDroppedOverlayKey(k, plat);
          })
        : [];
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
    var prefix = (root.getAttribute("data-overlay-label-prefix") || "Metric").trim() || "Metric";
    var keys = selectedFromDom(root);
    if (!keys.length) keys = readStored(root);
    if (!keys.length) {
      labelEl.textContent = prefix + ": off";
      return;
    }
    if (keys.length === 1) {
      labelEl.textContent = prefix + ": " + (metricLabel(keys[0]) || keys[0]);
      return;
    }
    labelEl.textContent = prefix + ": " + keys.length + " series";
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
  function isXdataKey(k) {
    return String(k || "").indexOf("xdata:") === 0;
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
      profile: normalizeOverlayPlatform(platform),
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
    var branch = normalizeOverlayPlatform(platform);
    if (branch === "web") branch = "desktop";
    var qs = new URLSearchParams({
      start: startIso || "",
      end: endIso || "",
      metric: metricKey,
      project: "doviz",
      branch: branch || "android",
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

  async function fetchXdataSeries(metricKey, startIso, endIso, platform) {
    var qs = new URLSearchParams({
      start: startIso || "",
      end: endIso || "",
      metric: metricKey,
      project: "doviz",
      platform: normalizeOverlayPlatform(platform),
    });
    var r = await fetch("/api/empower-intel/series?" + qs.toString(), {
      credentials: "same-origin",
      cache: "no-store",
    });
    var data = await r.json().catch(function () { return {}; });
    if (!r.ok) throw new Error(data.message || data.detail || "X-Data HTTP " + r.status);
    if (data.configured === false || (data.ok === false && !Array.isArray(data.series))) {
      throw new Error(data.message || "No X-Data series");
    }
    return { label: data.label || metricLabel(metricKey), series: data.series || [] };
  }

  async function fetchAscSeries(metricKey, startIso, endIso) {
    var qs = new URLSearchParams({
      start: startIso || "",
      end: endIso || "",
      metric: metricKey,
      breakdown: "date",
      dim: "overview",
      segment: "all",
    });
    var r = await fetch("/api/asc-metrics/query?" + qs.toString(), {
      credentials: "same-origin",
      cache: "no-store",
    });
    var data = await r.json().catch(function () { return {}; });
    if (!r.ok) throw new Error(data.message || data.detail || "ASC HTTP " + r.status);
    if (data.configured === false || (data.ok === false && !Array.isArray(data.series))) {
      throw new Error(data.message || "No ASC series");
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
    else if (isXdataKey(metricKey)) p = fetchXdataSeries(metricKey, startIso, endIso, platform);
    else if (isGa4Key(metricKey)) p = fetchGa4Series(metricKey, startIso, endIso, platform);
    else if (isVirgulKey(metricKey)) p = fetchVirgulSeries(metricKey, startIso, endIso, platform);
    else if (platform === "ios") p = fetchAscSeries(metricKey, startIso, endIso);
    else if (platform === "android") p = fetchPlaySeries(metricKey, startIso, endIso);
    else p = Promise.resolve({ label: metricLabel(metricKey), series: [] });
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
    var colorIdx = Number(opts.colorOffset) || 0;
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
        if (opts.namePrefix) axisTitle = String(opts.namePrefix) + axisTitle;
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
          showAxisChrome ? visibleOverlayYaxisLayout(lineColor, opts.axisTitle || "Metric") : hiddenOverlayYaxisLayout(),
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

  function escHtml(s) {
    return String(s || "")
      .replace(/&/g, "&amp;")
      .replace(/</g, "&lt;")
      .replace(/>/g, "&gt;")
      .replace(/"/g, "&quot;");
  }

  function buildPanelHtml(platform) {
    var html =
      '<button type="button" data-play-metric-overlay-off class="flex w-full items-center gap-2 px-2.5 py-1.5 text-left text-[11px] font-semibold text-slate-600 hover:bg-slate-100 dark:text-zinc-300 dark:hover:bg-zinc-800">' +
      '<span class="inline-flex w-3.5 shrink-0 justify-center text-indigo-600 dark:text-indigo-400" data-play-metric-overlay-off-mark aria-hidden="true"></span>' +
      '<span class="min-w-0 flex-1">Off</span></button>';
    groupsForPlatform(platform).forEach(function (g) {
      html +=
        '<p class="px-2.5 pb-0.5 pt-1.5 text-[9px] font-bold uppercase tracking-wide text-slate-500 dark:text-zinc-400">' +
        escHtml(g.label) +
        "</p>";
      (g.items || []).forEach(function (it) {
        html +=
          '<label class="flex cursor-pointer items-start gap-2 px-2.5 py-1.5 text-left text-[11px] font-medium leading-snug text-slate-700 hover:bg-slate-100 dark:text-zinc-200 dark:hover:bg-zinc-800" data-play-metric-overlay-option="' +
          escHtml(it.key) +
          '">' +
          '<input type="checkbox" class="mt-0.5 shrink-0 rounded border-slate-300 text-indigo-600 focus:ring-indigo-500 dark:border-zinc-600" value="' +
          escHtml(it.key) +
          '" />' +
          '<span class="min-w-0 flex-1">' +
          escHtml(it.label) +
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

  function applyStoredChecks(panel, stored) {
    if (!panel) return;
    var want = stored || [];
    panel.querySelectorAll("input[type=checkbox]").forEach(function (cb) {
      cb.checked = want.indexOf(cb.value) >= 0;
    });
    syncOffMark(panel);
  }

  function syncOffMark(panel) {
    if (!panel) return;
    var mark = panel.querySelector("[data-play-metric-overlay-off-mark]");
    if (!mark) return;
    var any = panel.querySelectorAll("input[type=checkbox]:checked").length > 0;
    mark.textContent = any ? "" : "✓";
  }

  function fillPanel(root) {
    if (!root) return null;
    var plat = platformForRoot(root);
    var panel = panelForRoot(root);
    if (!panel) return null;
    panel.innerHTML = buildPanelHtml(plat);
    panel.dataset.playMetricBuilt = "1";
    root.dataset.playMetricPlatformReady = plat;
    root.dataset.playMetricXdataReady = (XDATA_ITEMS[plat] || []).length ? "1" : "0";
    if (root.id) panel.setAttribute("data-play-metric-overlay-for", root.id);
    applyStoredChecks(panel, readStored(root));
    return panel;
  }

  function wirePanelInputs(panel, fire) {
    if (!panel || typeof fire !== "function") return;
    panel.querySelectorAll("input[type=checkbox]").forEach(function (cb) {
      cb.addEventListener("change", function () {
        syncOffMark(panel);
        fire();
      });
    });
    var offBtn = panel.querySelector("[data-play-metric-overlay-off]");
    if (offBtn) {
      offBtn.addEventListener("click", function (ev) {
        ev.preventDefault();
        panel.querySelectorAll("input[type=checkbox]").forEach(function (cb) {
          cb.checked = false;
        });
        syncOffMark(panel);
        fire();
      });
    }
  }

  function refillIfXdataArrived(root) {
    if (!root) return;
    var plat = platformForRoot(root);
    if (root.dataset.playMetricXdataReady === "1") return;
    if (!(XDATA_ITEMS[plat] || []).length) return;
    var panel = fillPanel(root);
    if (typeof root._playMetricFire === "function") wirePanelInputs(panel, root._playMetricFire);
    updateTriggerLabel(root);
  }

  async function fetchSelectedSeries(controlId, startIso, endIso, platform) {
    var keys = modes(controlId);
    var plat = normalizeOverlayPlatform(platform);
    var out = [];
    for (var i = 0; i < keys.length; i++) {
      try {
        var pack = await fetchMetricSeries(keys[i], startIso, endIso, plat);
        var src = "play";
        if (isXdataKey(keys[i])) src = "xdata";
        else if (isGa4Key(keys[i])) src = "ga4";
        else if (isVirgulKey(keys[i])) src = "virgul";
        else if (isMarketKey(keys[i])) src = "market";
        else if (plat === "ios") src = "asc";
        out.push({
          metric: keys[i],
          label: pack.label || metricLabel(keys[i]),
          series: pack.series || [],
          source: src,
          platform: plat,
          crossPeer: true,
          onChart: true
        });
      } catch (e) {
        /* skip unavailable */
      }
    }
    return out;
  }

  function setLabelPrefix(controlId, prefix) {
    var root = rootEl(controlId);
    if (!root) return;
    root.setAttribute("data-overlay-label-prefix", prefix || "Metric");
    updateTriggerLabel(root);
  }

  function setPlatform(controlId, platform) {
    var root = rootEl(controlId);
    if (!root) return;
    var plat = normalizeOverlayPlatform(platform);
    seedXdataFromWindow();
    root.setAttribute("data-overlay-platform", plat);
    var samePlat = root.dataset.playMetricPlatformReady === plat;
    var xdataOk = root.dataset.playMetricXdataReady === "1" || !(XDATA_ITEMS[plat] || []).length;
    if (samePlat && xdataOk && panelForRoot(root) && panelForRoot(root).dataset.playMetricBuilt) {
      updateTriggerLabel(root);
    } else {
      var panel = fillPanel(root);
      updateTriggerLabel(root);
      clearCache();
      if (typeof root._playMetricFire === "function") {
        wirePanelInputs(panel, root._playMetricFire);
      }
    }
    ensureXdataItems(function () {
      if (platformForRoot(root) !== plat) return;
      refillIfXdataArrived(root);
    });
  }

  function bindRoot(root, onChange) {
    if (!root) return;
    if (root.dataset.playMetricOverlayBound === "1") {
      ensureXdataItems(function () {
        refillIfXdataArrived(root);
      });
      return;
    }
    root.dataset.playMetricOverlayBound = "1";
    seedXdataFromWindow();
    var panel = fillPanel(root);
    var trigger = root.querySelector("[data-play-metric-overlay-trigger]");
    updateTriggerLabel(root);
    function fire() {
      writeStored(root, selectedFromDom(root));
      updateTriggerLabel(root);
      clearCache();
      if (typeof onChange === "function") onChange();
      var attr = root.getAttribute("data-overlay-on-change");
      if (attr && typeof global[attr] === "function") global[attr]();
    }
    root._playMetricFire = fire;
    wirePanelInputs(panel, fire);
    if (trigger && panel) {
      trigger.addEventListener("click", function (ev) {
        ev.preventDefault();
        ev.stopPropagation();
        global.__playMetricOverlayIgnoreCloseUntil = Date.now() + 280;
        var p = panelForRoot(root);
        if (!p) return;
        if (p.classList.contains("hidden")) {
          p.classList.remove("hidden");
          positionPanel(trigger, p);
          trigger.setAttribute("aria-expanded", "true");
        } else {
          closePanel(root, trigger, p);
        }
      });
      panel.addEventListener("click", function (ev) {
        ev.stopPropagation();
      });
    }
    ensureXdataItems(function () {
      refillIfXdataArrived(root);
    });
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
    METRIC_GROUPS_ANDROID: METRIC_GROUPS_ANDROID,
    METRIC_GROUPS_IOS: METRIC_GROUPS_IOS,
    METRIC_GROUPS_WEB: METRIC_GROUPS_WEB,
    METRIC_GROUPS_MWEB: METRIC_GROUPS_MWEB,
    modes: modes,
    clearCache: clearCache,
    apply: apply,
    bindWhenReady: bindWhenReady,
    autoBindPlayMetricOverlays: autoBindPlayMetricOverlays,
    metricLabel: metricLabel,
    setPlatform: setPlatform,
    setLabelPrefix: setLabelPrefix,
    fetchSelectedSeries: fetchSelectedSeries,
    platformForRoot: platformForRoot,
    normalizeOverlayPlatform: normalizeOverlayPlatform,
  };

  if (global.document) {
    if (global.document.readyState === "loading") {
      global.document.addEventListener("DOMContentLoaded", autoBindPlayMetricOverlays);
    } else {
      autoBindPlayMetricOverlays();
    }
  }
})(window);
