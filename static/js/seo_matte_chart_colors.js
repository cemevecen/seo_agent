/**
 * Mat (düşük parlaklık) grafik / spark renkleri — GA4, SC, Ad, Notification, Realtime.
 */
(function (global) {
  function isDark() {
    var el = global.document && global.document.documentElement;
    if (!el) return false;
    return el.classList.contains("dark") || el.classList.contains("midnight");
  }

  function pick(light, dark) {
    return isDark() ? dark : light;
  }

  function seoMatteChartColors() {
    var d = isDark();
    return {
      positive: d ? "#4a8f73" : "#047857",
      negative: d ? "#a85a66" : "#b91c3c",
      neutral: d ? "#71717a" : "#64748b",
      compare: d ? "#b87333" : "#c2410c",
      compareAlt: d ? "#8b7aa8" : "#5b5f9e",
      primary: d ? "#6b8aad" : "#4b6a9b",
      secondary: d ? "#3d8b6e" : "#0f766e",
      tertiary: d ? "#8b7aa8" : "#6d5b9e",
      quaternary: d ? "#b87333" : "#b45309",
      accentRose: d ? "#a86b7f" : "#9d4d6a",
      sky: d ? "#5b7c99" : "#4a6f8c",
      skyBright: d ? "#6b8aad" : "#3d6db5",
      fillPositive: d ? "rgba(74,143,115,0.14)" : "rgba(4,120,87,0.16)",
      fillNegative: d ? "rgba(168,90,102,0.14)" : "rgba(185,28,60,0.14)",
      fillCompare: d ? "rgba(184,115,51,0.12)" : "rgba(194,65,12,0.14)",
    };
  }

  function seoMatteGa4TrendLines() {
    var c = seoMatteChartColors();
    return {
      sessions: c.primary,
      users: c.secondary,
      engaged: c.tertiary,
      engagementRate: c.quaternary,
      scPosition: c.accentRose,
    };
  }

  function seoMatteScTrendLines() {
    var c = seoMatteChartColors();
    return {
      clicks: c.skyBright,
      impressions: c.tertiary,
      ctr: c.positive,
      position: c.compare,
    };
  }

  function seoMattePlatformColors() {
    var d = isDark();
    return {
      android: d ? "#4a8f73" : "#15803d",
      ios: d ? "#a85a66" : "#b91c3c",
      desktop: d ? "#7a7da8" : "#5b5f9e",
      mobileweb: d ? "#a67c3d" : "#b8732e",
    };
  }

  function seoMatteAdCompareColors() {
    var c = seoMatteChartColors();
    return {
      primary: c.compareAlt,
      sparkCompare: c.sky,
      compare: c.negative,
      rev: c.positive,
      revCmp: c.compare,
      imp: c.sky,
      impCmp: pick("#0369a1", "#5b7c99"),
      barPrimary: c.sky,
      barCompare: c.compare,
    };
  }

  function seoMatteMeanSplit() {
    var c = seoMatteChartColors();
    return {
      green: c.positive,
      red: c.negative,
      greenFill: c.fillPositive,
      redFill: c.fillNegative,
    };
  }

  function seoMatteHeatmapScale() {
    return [
      [0, "#6b2d2d"],
      [0.14, "#a85a66"],
      [0.28, "#b87333"],
      [0.42, "#a89a4a"],
      [0.57, "#8a9a5a"],
      [0.71, "#5a9a78"],
      [0.85, "#3d8b6e"],
      [1, "#1e4d3a"],
    ];
  }

  function seoMatteMarketOverlayPalette() {
    return [
      "#7a3d52",
      "#4a6f8c",
      "#9a7a2e",
      "#3d8b6e",
      "#b87333",
      "#7a5a8a",
      "#3d7a8a",
      "#9f4a52",
    ];
  }

  function seoMatteSeriesPalette() {
    var d = isDark();
    return d
      ? ["#6b8aad", "#3d8b6e", "#8b7aa8", "#b87333", "#a86b7f", "#5b7c99", "#a85a66"]
      : ["#4b6a9b", "#0f766e", "#6d5b9e", "#b45309", "#9d4d6a", "#4a6f8c", "#b91c3c"];
  }

  global.seoMatteChartColors = seoMatteChartColors;
  global.seoMatteGa4TrendLines = seoMatteGa4TrendLines;
  global.seoMatteScTrendLines = seoMatteScTrendLines;
  global.seoMattePlatformColors = seoMattePlatformColors;
  global.seoMatteAdCompareColors = seoMatteAdCompareColors;
  global.seoMatteMeanSplit = seoMatteMeanSplit;
  global.seoMatteHeatmapScale = seoMatteHeatmapScale;
  global.seoMatteMarketOverlayPalette = seoMatteMarketOverlayPalette;
  global.seoMatteSeriesPalette = seoMatteSeriesPalette;
  global.seoMatteIsDark = isDark;
  global.seoMattePick = pick;
})(typeof window !== "undefined" ? window : globalThis);
