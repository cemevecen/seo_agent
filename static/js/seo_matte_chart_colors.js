/**
 * Mat (düşük parlaklık) grafik / spark renkleri — GA4, SC, Ad, Notification, Realtime.
 */
(function (global) {
  function themeId() {
    var el = global.document && global.document.documentElement;
    if (!el) return "light";
    if (el.classList.contains("charcoal") || el.classList.contains("dark")) return "charcoal";
    return "light";
  }

  function isDark() {
    return themeId() === "charcoal";
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
    /* Charcoal: matte dark ink (no neon) */
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

  function _hexToRgb(hex) {
    var h = String(hex || "").replace("#", "").trim();
    if (h.length === 3) h = h[0] + h[0] + h[1] + h[1] + h[2] + h[2];
    if (h.length !== 6) return null;
    var n = parseInt(h, 16);
    if (isNaN(n)) return null;
    return { r: (n >> 16) & 255, g: (n >> 8) & 255, b: n & 255 };
  }

  function _rgbToHex(r, g, b) {
    function clamp(v) {
      return Math.max(0, Math.min(255, Math.round(v)));
    }
    function byte(v) {
      var s = clamp(v).toString(16);
      return s.length === 1 ? "0" + s : s;
    }
    return "#" + byte(r) + byte(g) + byte(b);
  }

  function _mixHex(a, b, t) {
    var A = _hexToRgb(a);
    var B = _hexToRgb(b);
    if (!A || !B) return b || a || "#64748b";
    t = Math.max(0, Math.min(1, Number(t) || 0));
    return _rgbToHex(A.r + (B.r - A.r) * t, A.g + (B.g - A.g) * t, A.b + (B.b - A.b) * t);
  }

  function _rgbToHsl(r, g, b) {
    r /= 255;
    g /= 255;
    b /= 255;
    var max = Math.max(r, g, b);
    var min = Math.min(r, g, b);
    var h = 0;
    var s = 0;
    var l = (max + min) / 2;
    if (max !== min) {
      var d = max - min;
      s = l > 0.5 ? d / (2 - max - min) : d / (max + min);
      switch (max) {
        case r:
          h = ((g - b) / d + (g < b ? 6 : 0)) / 6;
          break;
        case g:
          h = ((b - r) / d + 2) / 6;
          break;
        default:
          h = ((r - g) / d + 4) / 6;
          break;
      }
    }
    return { h: h * 360, s: s, l: l };
  }

  function _hslToRgb(h, s, l) {
    h = ((h % 360) + 360) % 360;
    s = Math.max(0, Math.min(1, s));
    l = Math.max(0, Math.min(1, l));
    var c = (1 - Math.abs(2 * l - 1)) * s;
    var x = c * (1 - Math.abs(((h / 60) % 2) - 1));
    var m = l - c / 2;
    var r = 0;
    var g = 0;
    var b = 0;
    if (h < 60) {
      r = c;
      g = x;
    } else if (h < 120) {
      r = x;
      g = c;
    } else if (h < 180) {
      g = c;
      b = x;
    } else if (h < 240) {
      g = x;
      b = c;
    } else if (h < 300) {
      r = x;
      b = c;
    } else {
      r = c;
      b = x;
    }
    return { r: (r + m) * 255, g: (g + m) * 255, b: (b + m) * 255 };
  }

  /**
   * Area fill — stroke ile aynı hue olmasın (hue kaydır + soft).
   * Charcoal'da biraz daha görünür.
   */
  function seoMatteAreaFillFromStroke(strokeHex) {
    var rgb = _hexToRgb(strokeHex);
    if (!rgb) return isCharcoal() ? "rgba(113,113,122,0.22)" : "rgba(100,116,139,0.14)";
    var hsl = _rgbToHsl(rgb.r, rgb.g, rgb.b);
    var h2 = hsl.h + (isCharcoal() ? 52 : 48);
    var s2 = Math.max(0.18, Math.min(0.55, hsl.s * (isCharcoal() ? 0.42 : 0.38)));
    var l2 = isCharcoal()
      ? Math.max(0.28, Math.min(0.48, hsl.l * 0.72 + 0.12))
      : Math.max(0.42, Math.min(0.72, hsl.l * 0.55 + 0.28));
    var out = _hslToRgb(h2, s2, l2);
    return _rgbToHex(out.r, out.g, out.b);
  }

  /**
   * Metrik satırı ısı rampası — stroke/legend renginden ayrı, charcoal'da daha parlak.
   * index: 0 ANR-mavi ailesi, 1 crash-amber, 2 sessions-yeşil, …
   */
  function seoMatteSeriesHeatRamp(seriesIndex) {
    var i = Math.abs(Number(seriesIndex) || 0) % 7;
    if (isCharcoal()) {
      return [
        { lo: "#243447", hi: "#7dd3fc" }, /* sky */
        { lo: "#3a2a18", hi: "#fdba74" }, /* amber */
        { lo: "#1a3a2c", hi: "#86efac" }, /* green */
        { lo: "#3a2430", hi: "#f9a8d4" }, /* pink */
        { lo: "#2a2438", hi: "#c4b5fd" }, /* violet */
        { lo: "#1e3338", hi: "#67e8f9" }, /* cyan */
        { lo: "#3a2820", hi: "#fca5a5" }, /* rose */
      ][i];
    }
    if (isDark()) {
      return [
        { lo: "#1e293b", hi: "#6b8aad" },
        { lo: "#292016", hi: "#b87333" },
        { lo: "#162820", hi: "#4a8f73" },
        { lo: "#2a1820", hi: "#a86b7f" },
        { lo: "#221c30", hi: "#8b7aa8" },
        { lo: "#162428", hi: "#5b9aa8" },
        { lo: "#2a1c1c", hi: "#a85a66" },
      ][i];
    }
    return [
      { lo: "#dbeafe", hi: "#1d4ed8" },
      { lo: "#ffedd5", hi: "#c2410c" },
      { lo: "#dcfce7", hi: "#047857" },
      { lo: "#fce7f3", hi: "#be185d" },
      { lo: "#ede9fe", hi: "#6d28d9" },
      { lo: "#cffafe", hi: "#0e7490" },
      { lo: "#fee2e2", hi: "#b91c1c" },
    ][i];
  }

  /** Isı hücresi rengi (opak karışım — düşük opacity + charcoal mud yok) */
  function seoMatteHeatCellColor(seriesIndex, t) {
    var ramp = seoMatteSeriesHeatRamp(seriesIndex);
    var tt = Math.max(0, Math.min(1, Number(t) || 0));
    /* Charcoal: düşük değerler de okunsun */
    if (isCharcoal()) tt = 0.18 + tt * 0.82;
    else if (isDark()) tt = 0.12 + tt * 0.88;
    return _mixHex(ramp.lo, ramp.hi, tt);
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
  global.seoMatteAreaFillFromStroke = seoMatteAreaFillFromStroke;
  global.seoMatteSeriesHeatRamp = seoMatteSeriesHeatRamp;
  global.seoMatteHeatCellColor = seoMatteHeatCellColor;
  global.seoMatteMarketOverlayPalette = seoMatteMarketOverlayPalette;
  global.seoMatteEmpowerOverlayPalette = seoMatteEmpowerOverlayPalette;
  global.seoMatteSeriesPalette = seoMatteSeriesPalette;
  global.seoMatteCwvColors = seoMatteCwvColors;
  global.seoMatteQualityGauge = seoMatteQualityGauge;
  global.seoMatteIsDark = isDark;
  global.seoMattePick = pick;
})(typeof window !== "undefined" ? window : globalThis);
