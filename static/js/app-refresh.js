/**
 * Ana ekran / standalone Safari: sayfa yenileme butonu + pull-to-refresh.
 */
(function () {
  "use strict";

  function isStandaloneDisplay() {
    try {
      if (window.navigator.standalone === true) return true;
      if (window.matchMedia("(display-mode: standalone)").matches) return true;
      if (window.matchMedia("(display-mode: fullscreen)").matches) return true;
    } catch (e) {
      /* ignore */
    }
    return false;
  }

  function isHorizontalScrollArea(el) {
    for (var node = el; node && node !== document.documentElement; node = node.parentElement) {
      if (!node || node.nodeType !== 1) continue;
      var st = window.getComputedStyle(node);
      var ox = st.overflowX;
      if (ox !== "auto" && ox !== "scroll" && ox !== "overlay") continue;
      if (node.scrollWidth > node.clientWidth + 4) return true;
    }
    return false;
  }

  function scrollTop() {
    return (
      window.scrollY ||
      document.documentElement.scrollTop ||
      document.body.scrollTop ||
      0
    );
  }

  function triggerPageRefresh() {
    var ev;
    try {
      ev = new CustomEvent("pc:page-refresh", { cancelable: true, bubbles: true });
      document.dispatchEvent(ev);
      if (ev.defaultPrevented) return;
    } catch (e2) {
      /* CustomEvent yoksa doğrudan reload */
    }
    window.location.reload();
  }

  function wireRefreshButton() {
    var btn = document.getElementById("header-page-refresh");
    if (!btn || btn.dataset.wired === "1") return;
    btn.dataset.wired = "1";
    btn.addEventListener("click", function () {
      btn.disabled = true;
      triggerPageRefresh();
    });
    if (isStandaloneDisplay() || window.matchMedia("(max-width: 767px)").matches) {
      btn.classList.remove("hidden");
    }
  }

  function wirePullToRefresh() {
    if (!isStandaloneDisplay()) return;

    var indicator = document.getElementById("pc-ptr-indicator");
    if (!indicator) return;

    var startY = 0;
    var pulling = false;
    var currentPull = 0;
    var threshold = 72;
    var maxPull = 120;

    function setPull(px) {
      currentPull = Math.max(0, Math.min(px, maxPull));
      indicator.style.transform = "translateY(" + (currentPull - 48) + "px)";
      indicator.style.opacity = String(Math.min(1, currentPull / threshold));
      indicator.setAttribute("aria-hidden", currentPull < 8 ? "true" : "false");
      if (currentPull >= threshold) {
        indicator.setAttribute("data-ptr-ready", "1");
      } else {
        indicator.removeAttribute("data-ptr-ready");
      }
    }

    function resetPull() {
      pulling = false;
      currentPull = 0;
      indicator.classList.remove("pc-ptr-active", "pc-ptr-loading");
      indicator.removeAttribute("data-ptr-ready");
      setPull(0);
    }

    document.addEventListener(
      "touchstart",
      function (ev) {
        if (ev.touches.length !== 1) return;
        if (scrollTop() > 2) return;
        if (isHorizontalScrollArea(ev.target)) return;
        startY = ev.touches[0].clientY;
        pulling = true;
      },
      { passive: true }
    );

    document.addEventListener(
      "touchmove",
      function (ev) {
        if (!pulling || ev.touches.length !== 1) return;
        var dy = ev.touches[0].clientY - startY;
        if (dy <= 0) {
          resetPull();
          return;
        }
        if (scrollTop() > 2) {
          resetPull();
          return;
        }
        setPull(dy);
        indicator.classList.add("pc-ptr-active");
        if (dy > 12) ev.preventDefault();
      },
      { passive: false }
    );

    document.addEventListener(
      "touchend",
      function () {
        if (!pulling) return;
        if (currentPull >= threshold) {
          indicator.classList.add("pc-ptr-loading");
          triggerPageRefresh();
          return;
        }
        resetPull();
      },
      { passive: true }
    );
  }

  function init() {
    wireRefreshButton();
    wirePullToRefresh();
  }

  if (document.readyState === "loading") {
    document.addEventListener("DOMContentLoaded", init);
  } else {
    init();
  }
})();
