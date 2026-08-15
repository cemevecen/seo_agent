/**
 * Mat (düşük parlaklık) grafik / spark renkleri — GA4, SC, Ad, Notification, Realtime.
 */
(function (global) {
  function themeId() {
    var el = global.document && global.document.documentElement;
    if (!el) return "light";
    if (el.classList.contains("charcoal")) return "charcoal";
    if (el.classList.contains("midnight")) return "midnight";
    if (el.classList.contains("dark")) return "dark";
    return "light";
  }

  function isDark() {
    var id = themeId();
    return id === "charcoal" || id === "midnight" || id === "dark";
  }

  function isCharcoal() {
    return themeId() === "charcoal";
  }

  function pick(light, dark, charcoal) {
    if (!isDark()) return light;
    if (charcoal != null && isCharcoal()) return charcoal;
    return dark;
  }

  function seoMatteChartColors() {
    var d = isDark();
    var ch = isCharcoal();
    /* Charcoal: slightly brighter ink than midnight, still matte (no neon) */
    return {
      positive: pick("#047857", "#4a8f73", "#5a9f83"),
      negative: pick("#b91c3c", "#a85a66", "#b86a74"),
      neutral: pick("#64748b", "#71717a", "#8b8b93"),
      compare: pick("#c2410c", "#b87333", "#c4844a"),
      compareAlt: pick("#5b5f9e", "#8b7aa8", "#9a8ab6"),
      primary: pick("#4b6a9b", "#6b8aad", "#7a99b8"),
      secondary: pick("#0f766e", "#3d8b6e", "#4d9b7e"),
      tertiary: pick("#6d5b9e", "#8b7aa8", "#9a8ab6"),
      quaternary: pick("#b45309", "#b87333", "#c4844a"),
      accentRose: pick("#9d4d6a", "#a86b7f", "#b87b8f"),
      sky: pick("#4a6f8c", "#5b7c99", "#6b8ca8"),
      skyBright: pick("#3d6db5", "#6b8aad", "#7a99b8"),
      fillPositive: pick("rgba(4,120,87,0.16)", "rgba(74,143,115,0.14)", "rgba(90,159,131,0.16)"),
      fillNegative: pick("rgba(185,28,60,0.14)", "rgba(168,90,102,0.14)", "rgba(184,106,116,0.15)"),
      fillCompare: pick("rgba(194,65,12,0.14)", "rgba(184,115,51,0.12)", "rgba(196,132,74,0.14)"),
      _theme: ch ? "charcoal" : d ? "dark" : "light",
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

  /** /ad ana seriler: mürekkep siyah; karşı dönem de aynı ink (kesikli stil şablonda). */
  function seoMatteAdCompareColors() {
    var c = seoMatteChartColors();
    /* Dark: asla #f4f4f5 / beyaz — mat zinc */
    var ink = pick("#0a0a0a", "#a1a1aa");
    var inkSoft = pick("#525252", "#71717a");
    return {
      primary: ink,
      sparkCompare: c.sky,
      compare: ink,
      rev: ink,
      revCmp: ink,
      imp: ink,
      impCmp: ink,
      barPrimary: ink,
      barCompare: inkSoft,
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

  /** Piyasa overlay — mavi / yeşil / teal ağırlıklı. */
  function seoMatteMarketOverlayPalette() {
    return [
      "#1d4ed8",
      "#0f766e",
      "#2563eb",
      "#059669",
      "#0284c7",
      "#14b8a6",
      "#1e40af",
      "#047857",
      "#0e7490",
      "#4338ca",
    ];
  }

  /** Empower overlay — sarı / amber / gri. */
  function seoMatteEmpowerOverlayPalette() {
    return [
      "#ca8a04",
      "#78716c",
      "#eab308",
      "#a8a29e",
      "#d97706",
      "#57534e",
      "#b45309",
      "#a1a1aa",
    ];
  }

  function seoMatteSeriesPalette() {
    if (isCharcoal()) {
      return ["#7a99b8", "#4d9b7e", "#9a8ab6", "#c4844a", "#b87b8f", "#6b8ca8", "#b86a74"];
    }
    var d = isDark();
    return d
      ? ["#6b8aad", "#3d8b6e", "#8b7aa8", "#b87333", "#a86b7f", "#5b7c99", "#a85a66"]
      : ["#4b6a9b", "#0f766e", "#6d5b9e", "#b45309", "#9d4d6a", "#4a6f8c", "#b91c3c"];
  }

  /** Core Web Vitals / GSC traffic-light series (Poor / NI / Good). */
  function seoMatteCwvColors() {
    var d = isDark();
    return {
      poor: d ? "#a85a66" : "#C53929",
      needs: d ? "#b87333" : "#F09300",
      good: d ? "#4a8f73" : "#0B8043",
      poorFill: d ? "rgba(168,90,102,0.12)" : "rgba(197,57,41,0.08)",
      needsFill: d ? "rgba(184,115,51,0.12)" : "rgba(240,147,0,0.08)",
      goodFill: d ? "rgba(74,143,115,0.12)" : "rgba(11,128,67,0.08)",
      amp: d
        ? ["#6b5f8a", "#7a6b9a", "#8b7aa8", "#5b5278", "#4a4568", "#71717a"]
        : ["#7c3aed", "#a78bfa", "#c4b5fd", "#8b5cf6", "#6d28d9", "#ddd6fe"],
    };
  }

  /** App / store quality gauges — matte red→green. */
  function seoMatteQualityGauge() {
    var d = isDark();
    return d
      ? ["#a85a66", "#b87333", "#a89a4a", "#3d8b6e", "#4a8f73"]
      : ["#ef4444", "#f97316", "#eab308", "#22c55e", "#10b981"];
  }

  global.seoThemeId = themeId;
  global.seoIsCharcoal = isCharcoal;
  global.seoMatteChartColors = seoMatteChartColors;
  global.seoMatteGa4TrendLines = seoMatteGa4TrendLines;
  global.seoMatteScTrendLines = seoMatteScTrendLines;
  global.seoMattePlatformColors = seoMattePlatformColors;
  global.seoMatteAdCompareColors = seoMatteAdCompareColors;
  global.seoMatteMeanSplit = seoMatteMeanSplit;
  global.seoMatteHeatmapScale = seoMatteHeatmapScale;
  global.seoMatteMarketOverlayPalette = seoMatteMarketOverlayPalette;
  global.seoMatteEmpowerOverlayPalette = seoMatteEmpowerOverlayPalette;
  global.seoMatteSeriesPalette = seoMatteSeriesPalette;
  global.seoMatteCwvColors = seoMatteCwvColors;
  global.seoMatteQualityGauge = seoMatteQualityGauge;
  global.seoMatteIsDark = isDark;
  global.seoMattePick = pick;
})(typeof window !== "undefined" ? window : globalThis);
