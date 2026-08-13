(function () {
  var HEIGHT_KEY = "paChartHeight";
  var COMPRESS_KEY = "paChartCompress";
  var DEFAULT_HEIGHT = "2";
  var DEFAULT_COMPRESS = "1";
  var PAD_Y = 30;
  var HEIGHT_BASE = { "1": 260, "2": 200, "3": 150 };
  var COMPRESS_FACTOR = { "1": 1, "2": 1.28, "3": 1.62 };

  function readStored(key, allowed, fallback) {
    try {
      var v = localStorage.getItem(key);
      if (allowed.indexOf(v) >= 0) return v;
    } catch (_) {}
    return fallback;
  }

  function effectiveHeight(h, c) {
    var base = HEIGHT_BASE[h] || HEIGHT_BASE[DEFAULT_HEIGHT];
    var factor = COMPRESS_FACTOR[c] || COMPRESS_FACTOR[DEFAULT_COMPRESS];
    return Math.max(80, Math.round(base * factor));
  }

  function collectTargets() {
    return [
      {
        wrap: document.getElementById("pa-chart-wrap"),
        heightRoot: document.getElementById("pa-chart-height"),
        compressRoot: document.getElementById("pa-chart-compress"),
        svgId: "pa-chart",
      },
      {
        wrap: document.getElementById("ia-chart-wrap"),
        heightRoot: document.getElementById("ia-chart-height"),
        compressRoot: document.getElementById("ia-chart-compress"),
        svgId: "ia-chart",
      },
    ].filter(function (t) {
      return t.wrap && t.heightRoot;
    });
  }

  var targets = collectTargets();
  if (!targets.length) return;

  var height = readStored(HEIGHT_KEY, ["1", "2", "3"], DEFAULT_HEIGHT);
  var compress = readStored(COMPRESS_KEY, ["1", "2", "3"], DEFAULT_COMPRESS);
  var layoutSyncing = false;
  var layoutRaf = null;
  var moTimer = null;
  var lastWidths = new WeakMap();

  function syncGroup(root, attr, value) {
    if (!root) return;
    Array.prototype.forEach.call(
      root.querySelectorAll("[" + attr + "]"),
      function (btn) {
        var on = btn.getAttribute(attr) === value;
        btn.classList.toggle("is-active", on);
        btn.setAttribute("aria-pressed", on ? "true" : "false");
      }
    );
  }

  function syncUi() {
    targets.forEach(function (t) {
      syncGroup(t.heightRoot, "data-chart-height", height);
      syncGroup(t.compressRoot, "data-chart-compress", compress);
    });
  }

  function siblingsAboveSvg(wrap, svg) {
    var total = 0;
    var kids = wrap.children;
    for (var i = 0; i < kids.length; i++) {
      var child = kids[i];
      if (child === svg) break;
      if (child.id === "pa-tooltip" || child.id === "ia-tooltip") continue;
      total += child.offsetHeight || 0;
    }
    return total;
  }

  function innerWidth(wrap) {
    var cs = window.getComputedStyle(wrap);
    var pad =
      (parseFloat(cs.paddingLeft) || 0) + (parseFloat(cs.paddingRight) || 0);
    return Math.max(1, wrap.clientWidth - pad);
  }

  function applySettings() {
    var eff = effectiveHeight(height, compress);
    targets.forEach(function (t) {
      t.wrap.setAttribute("data-chart-height", height);
      t.wrap.setAttribute("data-chart-compress", compress);
      t.wrap.style.setProperty("--pa-chart-effective-h", String(eff));

      var card = t.wrap.closest("#pa-chart-card, #ia-chart-card");
      if (card) {
        card.setAttribute("data-chart-height", height);
        card.setAttribute("data-chart-compress", compress);
        card.style.setProperty("--pa-chart-effective-h", String(eff));
      }
    });
    syncUi();
    try {
      localStorage.setItem(HEIGHT_KEY, height);
      localStorage.setItem(COMPRESS_KEY, compress);
    } catch (_) {}
  }

  function syncLayout() {
    if (layoutSyncing) return;
    layoutSyncing = true;

    var eff = effectiveHeight(height, compress);
    targets.forEach(function (t) {
      var svg = document.getElementById(t.svgId);
      if (!svg || !t.wrap.contains(svg)) return;

      t.wrap.classList.add("pa-chart-layout-sync");

      var w = innerWidth(t.wrap);
      var chartH = Math.max(48, Math.round((w * eff) / 720));
      var above = siblingsAboveSvg(t.wrap, svg);
      var wrapH = above + chartH + PAD_Y * 2;

      t.wrap.style.boxSizing = "border-box";
      t.wrap.style.paddingTop = PAD_Y + "px";
      t.wrap.style.paddingBottom = PAD_Y + "px";

      svg.style.width = "100%";
      svg.style.height = chartH + "px";
      svg.style.maxHeight = chartH + "px";
      svg.style.minHeight = chartH + "px";
      svg.style.marginTop = "0";
      svg.style.marginBottom = "0";

      t.wrap.style.height = wrapH + "px";
      t.wrap.style.minHeight = wrapH + "px";
      t.wrap.style.maxHeight = wrapH + "px";

      var card = t.wrap.closest("#pa-chart-card, #ia-chart-card");
      if (card) {
        card.style.height = "auto";
        card.style.minHeight = "0";
        card.style.maxHeight = "none";
      }

      lastWidths.set(t.wrap, w);
    });

    requestAnimationFrame(function () {
      targets.forEach(function (t) {
        t.wrap.classList.remove("pa-chart-layout-sync");
      });
      layoutSyncing = false;
    });
  }

  function applyAll() {
    applySettings();
    syncLayout();
    scheduleLayoutSync();
  }

  function scheduleLayoutSync() {
    if (layoutRaf) cancelAnimationFrame(layoutRaf);
    layoutRaf = requestAnimationFrame(function () {
      layoutRaf = requestAnimationFrame(function () {
        layoutRaf = null;
        syncLayout();
      });
    });
  }

  function scheduleLayoutFromMutation() {
    if (moTimer) clearTimeout(moTimer);
    moTimer = setTimeout(function () {
      moTimer = null;
      scheduleLayoutSync();
    }, 16);
  }

  function onWrapResize(entries) {
    if (layoutSyncing) return;
    for (var i = 0; i < entries.length; i++) {
      var entry = entries[i];
      var w = entry.contentRect.width;
      var prev = lastWidths.get(entry.target) || 0;
      if (Math.abs(w - prev) > 0.5) {
        scheduleLayoutSync();
        return;
      }
    }
  }

  applyAll();
  window.addEventListener("resize", scheduleLayoutSync);
  window.paSyncChartLayout = scheduleLayoutSync;

  if (typeof ResizeObserver !== "undefined") {
    var ro = new ResizeObserver(onWrapResize);
    targets.forEach(function (t) {
      ro.observe(t.wrap);
      lastWidths.set(t.wrap, innerWidth(t.wrap));
    });
  }

  if (typeof MutationObserver !== "undefined") {
    targets.forEach(function (t) {
      var svg = document.getElementById(t.svgId);
      if (!svg) return;
      new MutationObserver(scheduleLayoutFromMutation).observe(svg, {
        childList: true,
        subtree: true,
      });
    });
  }

  targets.forEach(function (t) {
    t.heightRoot.addEventListener("click", function (ev) {
      var btn =
        ev.target && ev.target.closest
          ? ev.target.closest("[data-chart-height]")
          : null;
      if (!btn || !t.heightRoot.contains(btn)) return;
      var next = btn.getAttribute("data-chart-height") || DEFAULT_HEIGHT;
      if (next !== "1" && next !== "2" && next !== "3") return;
      height = next;
      applyAll();
    });
    if (!t.compressRoot) return;
    t.compressRoot.addEventListener("click", function (ev) {
      var btn =
        ev.target && ev.target.closest
          ? ev.target.closest("[data-chart-compress]")
          : null;
      if (!btn || !t.compressRoot.contains(btn)) return;
      var next = btn.getAttribute("data-chart-compress") || DEFAULT_COMPRESS;
      if (next !== "1" && next !== "2" && next !== "3") return;
      compress = next;
      applyAll();
    });
  });
})();
