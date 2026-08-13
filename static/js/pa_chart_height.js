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
  var syncTimer = null;

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

  function syncLayout() {
    var eff = effectiveHeight(height, compress);
    targets.forEach(function (t) {
      var svg = document.getElementById(t.svgId);
      if (!svg || !t.wrap.contains(svg)) return;

      var w = innerWidth(t.wrap);
      var chartH = Math.max(48, Math.round((w * eff) / 720));

      svg.style.width = "100%";
      svg.style.height = chartH + "px";
      svg.style.maxHeight = chartH + "px";
      svg.style.minHeight = chartH + "px";

      var above = siblingsAboveSvg(t.wrap, svg);
      var wrapH = above + chartH + PAD_Y * 2;
      t.wrap.style.height = wrapH + "px";
      t.wrap.style.minHeight = wrapH + "px";
      t.wrap.style.maxHeight = wrapH + "px";

      var card = t.wrap.closest("#pa-chart-card, #ia-chart-card");
      if (card) {
        card.style.height = "auto";
        card.style.minHeight = "0";
        card.style.maxHeight = "none";
      }
    });
  }

  function applyToDom() {
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
    syncLayout();
    requestAnimationFrame(syncLayout);
    try {
      localStorage.setItem(HEIGHT_KEY, height);
      localStorage.setItem(COMPRESS_KEY, compress);
    } catch (_) {}
  }

  function scheduleSync() {
    if (syncTimer) clearTimeout(syncTimer);
    syncTimer = setTimeout(function () {
      syncTimer = null;
      applyToDom();
    }, 40);
  }

  applyToDom();
  window.addEventListener("resize", scheduleSync);
  window.paSyncChartLayout = scheduleSync;

  if (typeof ResizeObserver !== "undefined") {
    targets.forEach(function (t) {
      var ro = new ResizeObserver(scheduleSync);
      ro.observe(t.wrap);
      var card = t.wrap.closest("#pa-chart-card, #ia-chart-card");
      if (card) ro.observe(card);
    });
  }

  if (typeof MutationObserver !== "undefined") {
    targets.forEach(function (t) {
      var svg = document.getElementById(t.svgId);
      if (!svg) return;
      new MutationObserver(scheduleSync).observe(svg, {
        childList: true,
        subtree: true,
        attributes: true,
        attributeFilter: ["viewBox", "height", "width"],
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
      applyToDom();
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
      applyToDom();
    });
  });
})();
