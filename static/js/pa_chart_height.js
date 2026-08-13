(function () {
  var HEIGHT_KEY = "paChartHeight";
  var COMPRESS_KEY = "paChartCompress";
  var DEFAULT_HEIGHT = "2";
  var DEFAULT_COMPRESS = "1";

  function readStored(key, allowed, fallback) {
    try {
      var v = localStorage.getItem(key);
      if (allowed.indexOf(v) >= 0) return v;
    } catch (_) {}
    return fallback;
  }

  function collectTargets() {
    return [
      {
        wrap: document.getElementById("pa-chart-wrap"),
        heightRoot: document.getElementById("pa-chart-height"),
        compressRoot: document.getElementById("pa-chart-compress"),
      },
      {
        wrap: document.getElementById("ia-chart-wrap"),
        heightRoot: document.getElementById("ia-chart-height"),
        compressRoot: document.getElementById("ia-chart-compress"),
      },
    ].filter(function (t) {
      return t.wrap && t.heightRoot;
    });
  }

  var targets = collectTargets();
  if (!targets.length) return;

  var height = readStored(HEIGHT_KEY, ["1", "2", "3"], DEFAULT_HEIGHT);
  var compress = readStored(COMPRESS_KEY, ["1", "2", "3"], DEFAULT_COMPRESS);

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

  function applyToDom() {
    targets.forEach(function (t) {
      t.wrap.setAttribute("data-chart-height", height);
      t.wrap.setAttribute("data-chart-compress", compress);
    });
    syncUi();
    try {
      localStorage.setItem(HEIGHT_KEY, height);
      localStorage.setItem(COMPRESS_KEY, compress);
    } catch (_) {}
  }

  applyToDom();

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
