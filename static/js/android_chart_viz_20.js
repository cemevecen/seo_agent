(function () {
  var root = document.getElementById("pa-viz20-list");
  if (!root) return;

  var MOCKS = [
    { id: "funnel", n: 1, title: "Funnel", blurb: "Store ziyaret → install → day-7 active — adım adım düşüş ve kayıp yüzdesi.", data: "Play listing page views → install → DAU" },
    { id: "waterfall", n: 2, title: "Waterfall", blurb: "Haftalık sessions değişimini segmentlere ayırır: push, organic, crash etkisi.", data: "Sessions haftalık Δ — segment katkıları" },
    { id: "heatmap", n: 3, title: "Heatmap (Gün × Saat)", blurb: "Oturum yoğunluğunu gün ve saat ızgarasında renk yoğunluğu ile gösterir.", data: "GA4 sessions by hour" },
    { id: "cohort", n: 4, title: "Cohort retention", blurb: "Install cohort → hafta N retention; hücre rengi ile kalıcılık okunur.", data: "Install cohort → week-N retention" },
    { id: "treemap", n: 5, title: "Treemap", blurb: "Crash / ANR hacmini issue veya sürüm bazında alan payı ile dağıtır.", data: "Issue title veya version → event share" },
    { id: "bump", n: 6, title: "Bump chart", blurb: "Kategori sıralamasının zaman içindeki yer değiştirmesi (rank lines).", data: "Play Store kategori sıralaması" },
    { id: "combo", n: 7, title: "Dual-axis combo", blurb: "Sessions çubuk + revenue çizgi — iki Y ekseni, aynı tarih ekseni.", data: "Virgül net revenue + GA4 sessions" },
    { id: "stacked100", n: 8, title: "Stacked %100 area", blurb: "Traffic source / device model payının zamana göre %100 yığılmış alanı.", data: "Source / device mix over time" },
    { id: "boxplot", n: 9, title: "Box plot", blurb: "Sürüm bazlı günlük crash rate dağılımı — median, quartile, outlier.", data: "Version → daily crash rate distribution" },
    { id: "scatter", n: 10, title: "Scatter (bubble)", blurb: "Crash rate vs ANR rate; bubble boyutu = kullanıcı hacmi.", data: "Version veya release karşılaştırması" },
    { id: "calendar", n: 11, title: "Calendar heatmap", blurb: "GitHub-style günlük takvim — renk = install / crash yoğunluğu.", data: "Günlük installs veya crashes" },
    { id: "sankey", n: 12, title: "Sankey", blurb: "Store → Install → D7 → D30 akışı; churn dalları görünür.", data: "Acquisition → retention kaybı" },
    { id: "horizon", n: 13, title: "Horizon chart", blurb: "4–6 metrik aynı düşük yükseklikte — horizon band katmanları.", data: "Crashes, ANR, sessions compact" },
    { id: "barrace", n: 14, title: "Bar race", blurb: "Haftalık top crash issue — animasyonlu sıralama değişimi.", data: "Top crash issues by week" },
    { id: "marimekko", n: 15, title: "Marimekko", blurb: "Device × OS payı; sütun genişliği traffic, yükseklik segment.", data: "Device model kırılımı" },
    { id: "control", n: 16, title: "Control chart (SPC)", blurb: "Günlük crash-free % — UCL/LCL ve anomali alarm noktaları.", data: "Daily crash-free % · Shewhart" },
    { id: "pareto", n: 17, title: "Pareto", blurb: "Top issue events + kümülatif % çizgisi — 80/20 odak.", data: "Vitals / Crashlytics issue listesi" },
    { id: "multiples", n: 18, title: "Small multiples", blurb: "2×2 mini grafik — aynı metrik, device / OS / version / country.", data: "Breakdown grid" },
    { id: "timeline", n: 19, title: "Timeline / Gantt", blurb: "Release tarihleri + crash spike overlay — olay bağlamı.", data: "Release + incident overlay" },
    { id: "matrix", n: 20, title: "Comparison matrix", blurb: "Metrik × dönem ısı tablosu — hücre rengi ile Δ karşılaştırma.", data: "Metric × period heat table" },
  ];

  function palette() {
    var dark =
      document.documentElement.classList.contains("dark") ||
      document.documentElement.classList.contains("midnight");
    if (dark) {
      return {
        bg: "#18181b",
        stroke: "#52525b",
        text: "#fafafa",
        muted: "#a1a1aa",
        soft: "#27272a",
        soft2: "#3f3f46",
        blue: "#38bdf8",
        green: "#4ade80",
        orange: "#fb923c",
        red: "#f87171",
        purple: "#c084fc",
      };
    }
    return {
      bg: "#ffffff",
      stroke: "#cbd5e1",
      text: "#0f172a",
      muted: "#64748b",
      soft: "#f1f5f9",
      soft2: "#e2e8f0",
      blue: "#3b82f6",
      green: "#22c55e",
      orange: "#f97316",
      red: "#ef4444",
      purple: "#a855f7",
    };
  }

  function esc(s) {
    return String(s == null ? "" : s)
      .replace(/&/g, "&amp;")
      .replace(/</g, "&lt;")
      .replace(/>/g, "&gt;")
      .replace(/"/g, "&quot;");
  }

  function frame(W, H, p) {
    return (
      '<rect x="8" y="8" width="' +
      (W - 16) +
      '" height="' +
      (H - 16) +
      '" rx="10" fill="' +
      p.soft +
      '" stroke="' +
      p.stroke +
      '"/>'
    );
  }

  function cap(x, y, label, p) {
    return (
      '<text x="' +
      x +
      '" y="' +
      y +
      '" fill="' +
      p.muted +
      '" font-size="10" font-family="ui-sans-serif,system-ui">' +
      esc(label) +
      "</text>"
    );
  }

  function renderChart(id, wide) {
    var p = palette();
    var W = wide ? 560 : 340;
    var H = wide ? 240 : 190;
    var f = frame(W, H, p);
    var svg =
      '<svg viewBox="0 0 ' +
      W +
      " " +
      H +
      '" role="img" aria-hidden="true">' +
      f;

    if (id === "funnel") {
      var rows = [
        { label: "Store ziyaret", w: W - 80, pct: "100%" },
        { label: "Install", w: (W - 80) * 0.62, pct: "62% ▼38%" },
        { label: "Day-7 active", w: (W - 80) * 0.31, pct: "31% ▼50%" },
      ];
      svg += cap(24, 28, "Funnel · Store → Install → Active", p);
      rows.forEach(function (r, i) {
        var y = 44 + i * 48;
        svg +=
          '<text x="24" y="' +
          (y + 14) +
          '" fill="' +
          p.text +
          '" font-size="11" font-weight="600">' +
          esc(r.label) +
          '</text><rect x="24" y="' +
          (y + 20) +
          '" width="' +
          r.w +
          '" height="22" rx="4" fill="' +
          p.blue +
          '" opacity="' +
          (0.35 + i * 0.15) +
          '"/><text x="' +
          (24 + r.w + 8) +
          '" y="' +
          (y + 36) +
          '" fill="' +
          p.muted +
          '" font-size="10">' +
          esc(r.pct) +
          "</text>";
      });
    } else if (id === "waterfall") {
      var bars = [
        { x: 40, w: 70, h: 80, up: true, label: "120k" },
        { x: 120, w: 36, h: 28, up: true, label: "+8k" },
        { x: 168, w: 48, h: 42, up: true, label: "+12k" },
        { x: 228, w: 32, h: 24, up: false, label: "-5k" },
        { x: 272, w: 70, h: 88, up: true, label: "135k" },
      ];
      var cursor = 130;
      svg += cap(24, 28, "Waterfall · Sessions haftalık Δ", p);
      bars.forEach(function (b, i) {
        var y = b.up ? cursor - b.h : cursor;
        if (i < bars.length - 1) cursor = b.up ? cursor - b.h : cursor + b.h;
        var fill = b.up ? p.green : p.red;
        svg +=
          '<rect x="' +
          b.x +
          '" y="' +
          y +
          '" width="' +
          b.w +
          '" height="' +
          b.h +
          '" fill="' +
          fill +
          '" opacity="0.55" rx="3"/><text x="' +
          (b.x + 4) +
          '" y="' +
          (y - 4) +
          '" fill="' +
          p.muted +
          '" font-size="9">' +
          esc(b.label) +
          "</text>";
      });
      svg +=
        '<line x1="32" y1="140" x2="' +
        (W - 32) +
        '" y2="140" stroke="' +
        p.stroke +
        '"/>';
    } else if (id === "heatmap") {
      var cols = 6;
      var rowsN = 5;
      var vals = [
        0.2, 0.5, 0.9, 0.7, 0.3, 0.15, 0.1, 0.4, 0.85, 0.95, 0.6, 0.2, 0.15, 0.35, 0.8, 0.9, 0.75,
        0.25, 0.55, 0.7, 0.88, 0.5, 0.2, 0.1, 0.65, 0.92, 0.78, 0.4, 0.15, 0.08,
      ];
      var dayLabels = ["Pzt", "Sal", "Çar", "Per", "Cmt"];
      svg += cap(24, 26, "Gün × Saat oturum yoğunluğu", p);
      ["00", "04", "08", "12", "16", "20"].forEach(function (h, ci) {
        svg +=
          '<text x="' +
          (72 + ci * 38) +
          '" y="44" fill="' +
          p.muted +
          '" font-size="9">' +
          h +
          "</text>";
      });
      for (var ri = 0; ri < rowsN; ri++) {
        svg +=
          '<text x="24" y="' +
          (58 + ri * 24) +
          '" fill="' +
          p.muted +
          '" font-size="9">' +
          dayLabels[ri] +
          "</text>";
        for (var ci = 0; ci < cols; ci++) {
          var v = vals[ri * cols + ci] || 0.3;
          svg +=
            '<rect x="' +
            (68 + ci * 38) +
            '" y="' +
            (46 + ri * 24) +
            '" width="32" height="18" rx="3" fill="' +
            p.blue +
            '" opacity="' +
            (0.15 + v * 0.75) +
            '"/>';
        }
      }
    } else if (id === "cohort") {
      var headers = ["Cohort", "W0", "W1", "W2", "W3", "W4"];
      var data = [
        ["14 Tem", "100%", "42%", "28%", "22%", "19%"],
        ["07 Tem", "100%", "39%", "25%", "20%", "17%"],
      ];
      var opacities = [1, 0.72, 0.55, 0.42, 0.35];
      svg += cap(24, 26, "Cohort retention · hafta 0→8", p);
      headers.forEach(function (h, i) {
        svg +=
          '<text x="' +
          (24 + i * 52) +
          '" y="48" fill="' +
          p.muted +
          '" font-size="9" font-weight="700">' +
          h +
          "</text>";
      });
      data.forEach(function (row, ri) {
        row.forEach(function (cell, ci) {
          if (ci === 0) {
            svg +=
              '<text x="24" y="' +
              (72 + ri * 28) +
              '" fill="' +
              p.text +
              '" font-size="10">' +
              esc(cell) +
              "</text>";
            return;
          }
          var op = opacities[ci - 1] || 0.3;
          svg +=
            '<rect x="' +
            (24 + ci * 52) +
            '" y="' +
            (58 + ri * 28) +
            '" width="44" height="20" rx="4" fill="' +
            p.green +
            '" opacity="' +
            op * 0.65 +
            '"/><text x="' +
            (28 + ci * 52) +
            '" y="' +
            (72 + ri * 28) +
            '" fill="' +
            p.text +
            '" font-size="9">' +
            cell +
            "</text>";
        });
      });
    } else if (id === "treemap") {
      svg += cap(24, 26, "Crash / ANR hacim dağılımı", p);
      svg +=
        '<rect x="24" y="40" width="180" height="72" rx="4" fill="' +
        p.red +
        '" opacity="0.45"/><text x="32" y="62" fill="' +
        p.text +
        '" font-size="10" font-weight="700">NullPointer</text><text x="32" y="78" fill="' +
        p.muted +
        '" font-size="10">34%</text><rect x="210" y="40" width="100" height="72" rx="4" fill="' +
        p.orange +
        '" opacity="0.5"/><text x="218" y="62" fill="' +
        p.text +
        '" font-size="10" font-weight="700">ANR svc</text><text x="218" y="78" fill="' +
        p.muted +
        '" font-size="10">22%</text><rect x="24" y="120" width="90" height="48" rx="4" fill="' +
        p.blue +
        '" opacity="0.45"/><text x="32" y="140" fill="' +
        p.text +
        '" font-size="10">v290 · 28%</text><rect x="120" y="120" width="90" height="48" rx="4" fill="' +
        p.purple +
        '" opacity="0.4"/><text x="128" y="140" fill="' +
        p.text +
        '" font-size="10">v289 · 15%</text><rect x="216" y="120" width="94" height="48" rx="4" fill="' +
        p.soft2 +
        '" stroke="' +
        p.stroke +
        '"/><text x="224" y="140" fill="' +
        p.muted +
        '" font-size="10">Diğer 11%</text>';
    } else if (id === "bump") {
      svg += cap(24, 26, "Kategori rank · Finance vs Entertainment", p);
      [1, 5, 10].forEach(function (r, i) {
        svg +=
          '<text x="20" y="' +
          (52 + i * 44) +
          '" fill="' +
          p.muted +
          '" font-size="9">' +
          r +
          "</text>";
      });
      svg +=
        '<polyline points="60,52 180,68 300,84" fill="none" stroke="' +
        p.blue +
        '" stroke-width="2.5"/><polyline points="60,96 180,80 300,52" fill="none" stroke="' +
        p.green +
        '" stroke-width="2.5"/><circle cx="300" cy="84" r="4" fill="' +
        p.blue +
        '"/><circle cx="300" cy="52" r="4" fill="' +
        p.green +
        '"/><text x="310" y="88" fill="' +
        p.blue +
        '" font-size="9">Finance</text><text x="310" y="56" fill="' +
        p.green +
        '" font-size="9">Entertainment</text>';
      ["Tem", "Ağu", "Eyl"].forEach(function (m, i) {
        svg +=
          '<text x="' +
          (60 + i * 120) +
          '" y="' +
          (H - 24) +
          '" fill="' +
          p.muted +
          '" font-size="9">' +
          m +
          "</text>";
      });
    } else if (id === "combo") {
      var cBars = [48, 62, 55, 70, 58, 68, 74];
      svg += cap(24, 26, "Sessions (sol) + Revenue TL (sağ)", p);
      cBars.forEach(function (h, i) {
        svg +=
          '<rect x="' +
          (36 + i * 38) +
          '" y="' +
          (150 - h) +
          '" width="24" height="' +
          h +
          '" fill="' +
          p.green +
          '" opacity="0.45" rx="2"/>';
      });
      svg +=
        '<polyline points="48,118 86,108 124,112 162,95 200,100 238,88 276,82" fill="none" stroke="' +
        p.orange +
        '" stroke-width="2.5"/><text x="' +
        (W - 48) +
        '" y="90" fill="' +
        p.orange +
        '" font-size="9">Revenue</text><text x="24" y="90" fill="' +
        p.green +
        '" font-size="9">Sessions</text>';
    } else if (id === "stacked100") {
      var bands = [
        { label: "Organic", color: p.green, h: 48 },
        { label: "Paid", color: p.blue, h: 36 },
        { label: "Direct", color: p.orange, h: 28 },
      ];
      var sy = 150;
      svg += cap(24, 26, "Traffic source mix · %100 stacked", p);
      bands.forEach(function (b) {
        sy -= b.h;
        svg +=
          '<rect x="40" y="' +
          sy +
          '" width="' +
          (W - 80) +
          '" height="' +
          b.h +
          '" fill="' +
          b.color +
          '" opacity="0.5"/><text x="48" y="' +
          (sy + b.h / 2 + 4) +
          '" fill="' +
          p.text +
          '" font-size="10">' +
          esc(b.label) +
          "</text>";
      });
    } else if (id === "boxplot") {
      var boxes = [
        { x: 48, w: 80, med: 0.4, label: "v290" },
        { x: 148, w: 56, med: 0.28, label: "v289" },
        { x: 228, w: 40, med: 0.18, label: "v288" },
      ];
      svg += cap(24, 26, "Sürüm bazlı crash rate dağılımı", p);
      boxes.forEach(function (b) {
        var top = 60;
        var plotH = 90;
        var yMed = top + plotH * (1 - b.med);
        svg +=
          '<line x1="' +
          (b.x + b.w / 2) +
          '" y1="' +
          top +
          '" x2="' +
          (b.x + b.w / 2) +
          '" y2="' +
          (top + plotH) +
          '" stroke="' +
          p.muted +
          '"/><rect x="' +
          b.x +
          '" y="' +
          (yMed - 16) +
          '" width="' +
          b.w +
          '" height="32" fill="' +
          p.blue +
          '" opacity="0.35" rx="3"/><rect x="' +
          (b.x + b.w * 0.3) +
          '" y="' +
          (yMed - 6) +
          '" width="' +
          b.w * 0.4 +
          '" height="12" fill="' +
          p.blue +
          '" opacity="0.7"/><text x="' +
          b.x +
          '" y="' +
          (top + plotH + 16) +
          '" fill="' +
          p.muted +
          '" font-size="9">' +
          b.label +
          "</text>";
      });
      svg +=
        '<circle cx="' +
        (boxes[0].x + boxes[0].w + 12) +
        '" cy="72" r="4" fill="' +
        p.red +
        '" opacity="0.8"/><text x="' +
        (boxes[0].x + boxes[0].w + 20) +
        '" y="76" fill="' +
        p.muted +
        '" font-size="8">outlier</text>';
    } else if (id === "scatter") {
      var dots = [
        { x: 120, y: 100, r: 14, label: "v290" },
        { x: 200, y: 130, r: 9, label: "v289" },
        { x: 160, y: 150, r: 6, label: "v288" },
      ];
      svg += cap(24, 26, "Crash rate vs ANR · bubble = users", p);
      svg +=
        '<line x1="40" y1="' +
        (H - 36) +
        '" x2="' +
        (W - 24) +
        '" y2="' +
        (H - 36) +
        '" stroke="' +
        p.stroke +
        '"/><line x1="40" y1="44" x2="40" y2="' +
        (H - 36) +
        '" stroke="' +
        p.stroke +
        '"/>';
      dots.forEach(function (d) {
        svg +=
          '<circle cx="' +
          d.x +
          '" cy="' +
          d.y +
          '" r="' +
          d.r +
          '" fill="' +
          p.blue +
          '" opacity="0.45" stroke="' +
          p.blue +
          '"/><text x="' +
          (d.x + d.r + 4) +
          '" y="' +
          (d.y + 4) +
          '" fill="' +
          p.text +
          '" font-size="9">' +
          d.label +
          "</text>";
      });
    } else if (id === "calendar") {
      svg += cap(24, 26, "Tem 2026 · günlük installs", p);
      for (var ci = 0; ci < 28; ci++) {
        var col = ci % 7;
        var row = Math.floor(ci / 7);
        var op = 0.15 + ((ci * 17) % 10) / 12;
        svg +=
          '<rect x="' +
          (24 + col * 42) +
          '" y="' +
          (44 + row * 22) +
          '" width="36" height="16" rx="3" fill="' +
          p.green +
          '" opacity="' +
          op +
          '"/>';
      }
    } else if (id === "sankey") {
      svg += cap(24, 26, "Store → Install → D7 → D30", p);
      svg +=
        '<rect x="24" y="70" width="18" height="80" rx="3" fill="' +
        p.blue +
        '" opacity="0.5"/><rect x="120" y="58" width="18" height="56" rx="3" fill="' +
        p.green +
        '" opacity="0.5"/><rect x="216" y="68" width="18" height="40" rx="3" fill="' +
        p.orange +
        '" opacity="0.5"/><rect x="312" y="74" width="18" height="28" rx="3" fill="' +
        p.purple +
        '" opacity="0.5"/><path d="M42 90 C80 90 80 86 120 86" fill="none" stroke="' +
        p.blue +
        '" stroke-width="14" opacity="0.25"/><path d="M138 86 C170 86 170 88 216 88" fill="none" stroke="' +
        p.green +
        '" stroke-width="10" opacity="0.25"/><path d="M234 88 C270 88 270 88 312 88" fill="none" stroke="' +
        p.orange +
        '" stroke-width="8" opacity="0.25"/>';
      ["Store", "Install", "D7", "D30"].forEach(function (l, i) {
        svg +=
          '<text x="' +
          (24 + i * 96) +
          '" y="' +
          (H - 28) +
          '" fill="' +
          p.muted +
          '" font-size="9">' +
          l +
          "</text>";
      });
    } else if (id === "horizon") {
      var series = [
        { label: "Crashes", color: p.blue, path: "M24,100 Q80,80 140,95 T260,88 T340,102" },
        { label: "ANR", color: p.orange, path: "M24,130 Q90,118 150,128 T260,120 T340,132" },
        { label: "Sessions", color: p.green, path: "M24,160 Q70,150 130,158 T260,152 T340,162" },
      ];
      svg += cap(24, 26, "Horizon bands · compact multi-metric", p);
      series.forEach(function (s, i) {
        svg +=
          '<text x="24" y="' +
          (58 + i * 36) +
          '" fill="' +
          s.color +
          '" font-size="10" font-weight="600">' +
          s.label +
          '</text><path d="' +
          s.path +
          '" fill="none" stroke="' +
          s.color +
          '" stroke-width="2" opacity="0.7"/><rect x="24" y="' +
          (62 + i * 36) +
          '" width="' +
          (W - 48) +
          '" height="22" fill="' +
          s.color +
          '" opacity="0.08" rx="4"/>';
      });
    } else if (id === "barrace") {
      var raceRows = [
        { label: "NullPointer MainActivity", w: 0.92, val: "842" },
        { label: "ANR Binder", w: 0.68, val: "612" },
        { label: "OOM Glide", w: 0.45, val: "401" },
      ];
      svg += cap(24, 26, "Top crash issues · bar race", p);
      raceRows.forEach(function (r, i) {
        svg +=
          '<text x="24" y="' +
          (52 + i * 44) +
          '" fill="' +
          p.muted +
          '" font-size="9">' +
          (i + 1) +
          '.</text><text x="36" y="' +
          (52 + i * 44) +
          '" fill="' +
          p.text +
          '" font-size="10">' +
          esc(r.label) +
          '</text><rect x="36" y="' +
          (58 + i * 44) +
          '" width="' +
          (W - 100) * r.w +
          '" height="18" rx="3" fill="' +
          p.red +
          '" opacity="' +
          (0.35 + i * 0.1) +
          '"/><text x="' +
          (W - 44) +
          '" y="' +
          (72 + i * 44) +
          '" fill="' +
          p.muted +
          '" font-size="9">' +
          r.val +
          "</text>";
      });
    } else if (id === "marimekko") {
      var mCols = [
        { w: 120, segs: [0.55, 0.45], label: "Samsung" },
        { w: 80, segs: [0.4, 0.6], label: "Xiaomi" },
        { w: 56, segs: [0.7, 0.3], label: "Pixel" },
      ];
      var mx = 32;
      svg += cap(24, 26, "Device × OS · genişlik ∝ traffic", p);
      mCols.forEach(function (c) {
        svg +=
          '<text x="' +
          (mx + 4) +
          '" y="48" fill="' +
          p.muted +
          '" font-size="9">' +
          c.label +
          "</text>";
        var my = 56;
        c.segs.forEach(function (s, si) {
          var mh = s * 100;
          svg +=
            '<rect x="' +
            mx +
            '" y="' +
            my +
            '" width="' +
            c.w +
            '" height="' +
            mh +
            '" fill="' +
            (si === 0 ? p.blue : p.green) +
            '" opacity="0.45"/>';
          my += mh;
        });
        mx += c.w + 4;
      });
      svg += cap(32, H - 24, "And14 · And13", p);
    } else if (id === "control") {
      svg += cap(24, 26, "Crash-free % · UCL / LCL", p);
      svg +=
        '<line x1="32" y1="52" x2="' +
        (W - 24) +
        '" y2="52" stroke="' +
        p.red +
        '" stroke-dasharray="4 3" opacity="0.6"/><line x1="32" y1="130" x2="' +
        (W - 24) +
        '" y2="130" stroke="' +
        p.red +
        '" stroke-dasharray="4 3" opacity="0.6"/><polyline points="40,100 90,98 140,96 190,94 240,102 290,97 340,99" fill="none" stroke="' +
        p.green +
        '" stroke-width="2"/><circle cx="240" cy="102" r="5" fill="' +
        p.red +
        '"/><text x="248" y="106" fill="' +
        p.red +
        '" font-size="8">alarm</text>';
    } else if (id === "pareto") {
      var items = [
        { label: "NullPointer…", pct: 34, cum: 34 },
        { label: "ANR service", pct: 22, cum: 56 },
        { label: "OOM Glide", pct: 14, cum: 70 },
      ];
      svg += cap(24, 26, "Pareto · events + kümülatif %", p);
      items.forEach(function (it, i) {
        svg +=
          '<text x="24" y="' +
          (52 + i * 36) +
          '" fill="' +
          p.text +
          '" font-size="9">' +
          esc(it.label) +
          '</text><rect x="120" y="' +
          (40 + i * 36) +
          '" width="' +
          it.pct * 2.2 +
          '" height="16" fill="' +
          p.blue +
          '" opacity="0.5"/><text x="' +
          (120 + it.pct * 2.2 + 6) +
          '" y="' +
          (52 + i * 36) +
          '" fill="' +
          p.muted +
          '" font-size="8">' +
          it.pct +
          "% · kum " +
          it.cum +
          "%</text>";
      });
      svg +=
        '<polyline points="120,40 195,70 240,106" fill="none" stroke="' +
        p.orange +
        '" stroke-width="2"/>';
    } else if (id === "multiples") {
      var panels = ["Device", "OS", "Version", "Country"];
      svg += cap(24, 26, "Small multiples · 2×2 breakdown", p);
      panels.forEach(function (title, i) {
        var col = i % 2;
        var row = Math.floor(i / 2);
        var px = 28 + col * 152;
        var py = 44 + row * 72;
        svg +=
          '<rect x="' +
          px +
          '" y="' +
          py +
          '" width="140" height="64" rx="6" fill="' +
          p.bg +
          '" stroke="' +
          p.stroke +
          '"/><text x="' +
          (px + 8) +
          '" y="' +
          (py + 16) +
          '" fill="' +
          p.muted +
          '" font-size="9" font-weight="700">' +
          title +
          '</text><polyline points="' +
          (px + 12) +
          "," +
          (py + 50) +
          " " +
          (px + 40) +
          "," +
          (py + 38) +
          " " +
          (px + 68) +
          "," +
          (py + 44) +
          " " +
          (px + 96) +
          "," +
          (py + 32) +
          " " +
          (px + 124) +
          "," +
          (py + 40) +
          '" fill="none" stroke="' +
          p.blue +
          '" stroke-width="2" opacity="0.75"/>';
      });
    } else if (id === "timeline") {
      svg += cap(24, 26, "Release + incident overlay", p);
      svg +=
        '<line x1="32" y1="100" x2="' +
        (W - 24) +
        '" y2="100" stroke="' +
        p.stroke +
        '"/><rect x="48" y="82" width="160" height="12" rx="4" fill="' +
        p.blue +
        '" opacity="0.45"/><text x="48" y="76" fill="' +
        p.text +
        '" font-size="9">v9.5.10 · 10 Ağu</text><rect x="48" y="108" width="120" height="12" rx="4" fill="' +
        p.green +
        '" opacity="0.4"/><text x="48" y="132" fill="' +
        p.text +
        '" font-size="9">v9.5.9 · 28 Tem</text><line x1="140" y1="68" x2="140" y2="124" stroke="' +
        p.red +
        '" stroke-dasharray="3 2"/><line x1="260" y1="68" x2="260" y2="124" stroke="' +
        p.orange +
        '" stroke-dasharray="3 2"/>';
    } else if (id === "matrix") {
      var metrics = ["Sessions", "Crashes", "ANR", "Revenue"];
      var periods = ["W-3", "W-2", "W-1", "Now"];
      svg += cap(24, 26, "Metrik × dönem · Δ ısı tablosu", p);
      periods.forEach(function (pLabel, ci) {
        svg +=
          '<text x="' +
          (88 + ci * 56) +
          '" y="44" fill="' +
          p.muted +
          '" font-size="9">' +
          pLabel +
          "</text>";
      });
      metrics.forEach(function (m, ri) {
        var y = 52 + ri * 32;
        svg +=
          '<text x="24" y="' +
          (y + 16) +
          '" fill="' +
          p.text +
          '" font-size="9">' +
          m +
          "</text>";
        periods.forEach(function (_, ci) {
          var op = 0.2 + ((ri + ci) % 5) * 0.14;
          var tone = (ri + ci) % 3 === 0 ? p.red : (ri + ci) % 3 === 1 ? p.green : p.blue;
          svg +=
            '<rect x="' +
            (84 + ci * 56) +
            '" y="' +
            y +
            '" width="48" height="24" rx="4" fill="' +
            tone +
            '" opacity="' +
            op +
            '"/>';
        });
      });
    }

    return svg + "</svg>";
  }

  function paintChart(el) {
    if (!el || el.getAttribute("data-rendered") === "1") return;
    var id = el.getAttribute("data-chart-id");
    if (!id) return;
    el.innerHTML = renderChart(id, true);
    el.setAttribute("data-rendered", "1");
  }

  function repaintOpenCharts() {
    root.querySelectorAll(".pa-viz20-chart[data-rendered='1']").forEach(function (el) {
      el.removeAttribute("data-rendered");
      paintChart(el);
    });
  }

  MOCKS.forEach(function (mock) {
    var details = document.createElement("details");
    details.className = "pa-viz20-drop group";
    details.id = "pa-viz20-" + mock.id;

    details.innerHTML =
      '<summary>' +
      '<div class="flex min-w-0 items-center gap-2">' +
      '<span class="pa-viz20-chevron shrink-0 text-slate-400" aria-hidden="true">▸</span>' +
      '<div class="min-w-0">' +
      '<p class="truncate text-sm font-semibold text-slate-800 dark:text-zinc-100">' +
      esc("#" + mock.n + " · " + mock.title) +
      "</p>" +
      '<p class="truncate text-[11px] text-slate-500 dark:text-zinc-400">' +
      esc(mock.blurb) +
      "</p>" +
      "</div>" +
      "</div>" +
      '<span class="pa-viz20-badge pa-viz20-badge-closed">kapalı</span>' +
      "</summary>" +
      '<div class="pa-viz20-body">' +
      '<div class="pa-viz20-chart" data-chart-id="' +
      esc(mock.id) +
      '" role="img" aria-label="' +
      esc(mock.title + " mockup chart") +
      '"></div>' +
      '<div class="pa-viz20-meta">' +
      "<span><strong>Veri:</strong> " +
      esc(mock.data) +
      "</span>" +
      "<span><strong>Platform:</strong> Android</span>" +
      "</div>" +
      "</div>";

    details.addEventListener("toggle", function () {
      if (details.open) paintChart(details.querySelector(".pa-viz20-chart"));
    });

    root.appendChild(details);
  });

  window.addEventListener("seo-theme-change", repaintOpenCharts);
})();
