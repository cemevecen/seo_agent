(function () {
  var STORAGE_KEY = "paChartHeight";
  var DEFAULT_HEIGHT = "2";

  function readStoredHeight() {
    try {
      var v = localStorage.getItem(STORAGE_KEY);
      if (v === "1" || v === "2" || v === "3") return v;
    } catch (_) {}
    return DEFAULT_HEIGHT;
  }

  function collectPairs() {
    return [
      {
        wrap: document.getElementById("pa-chart-wrap"),
        root: document.getElementById("pa-chart-height"),
      },
      {
        wrap: document.getElementById("ia-chart-wrap"),
        root: document.getElementById("ia-chart-height"),
      },
    ].filter(function (pair) {
      return pair.wrap && pair.root;
    });
  }

  var pairs = collectPairs();
  if (!pairs.length) return;

  var height = readStoredHeight();

  function syncUi() {
    pairs.forEach(function (pair) {
      Array.prototype.forEach.call(
        pair.root.querySelectorAll("[data-chart-height]"),
        function (btn) {
          var on = btn.getAttribute("data-chart-height") === height;
          btn.classList.toggle("is-active", on);
          btn.setAttribute("aria-pressed", on ? "true" : "false");
        }
      );
    });
  }

  function apply(next) {
    if (next !== "1" && next !== "2" && next !== "3") return;
    height = next;
    pairs.forEach(function (pair) {
      pair.wrap.setAttribute("data-chart-height", height);
    });
    syncUi();
    try {
      localStorage.setItem(STORAGE_KEY, height);
    } catch (_) {}
  }

  apply(height);

  pairs.forEach(function (pair) {
    pair.root.addEventListener("click", function (ev) {
      var btn =
        ev.target && ev.target.closest
          ? ev.target.closest("[data-chart-height]")
          : null;
      if (!btn || !pair.root.contains(btn)) return;
      apply(btn.getAttribute("data-chart-height") || DEFAULT_HEIGHT);
    });
  });
})();
