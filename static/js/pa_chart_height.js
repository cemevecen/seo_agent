(function () {
  var HEIGHT_KEY = "paChartHeight";
  var COMPRESS_KEY = "paChartCompress";
  var MARGIN_L_KEY = "paChartMarginL";
  var MARGIN_R_KEY = "paChartMarginR";
  var CUSTOM_H_KEY = "paChartCustomH";
  var DEFAULT_HEIGHT = "2";
  var DEFAULT_COMPRESS = "1";
  var PAD_Y = 30;
  var VIEW_H = 260;
  var VIEW_W = 720;
  var MIN_WIDTH_PCT = 15;
  var MIN_CHART_H = 72;
  var MAX_CHART_H = 720;
  var HEIGHT_BASE = { "1": 260, "2": 200, "3": 150 };
  var COMPRESS_DIVISOR = { "1": 1, "2": 1.28, "3": 1.62 };

  function readStored(key, allowed, fallback) {
    try {
      var v = localStorage.getItem(key);
      if (allowed.indexOf(v) >= 0) return v;
    } catch (_) {}
    return fallback;
  }

  function readStoredNumber(key, fallback) {
    try {
      var v = parseFloat(localStorage.getItem(key));
      if (isFinite(v) && v >= 0) return v;
    } catch (_) {}
    return fallback;
  }

  function clamp(n, lo, hi) {
    return Math.max(lo, Math.min(hi, n));
  }

  function effectiveHeight(h, c) {
    var base = HEIGHT_BASE[h] || HEIGHT_BASE[DEFAULT_HEIGHT];
    var divisor = COMPRESS_DIVISOR[c] || COMPRESS_DIVISOR[DEFAULT_COMPRESS];
    return Math.max(48, Math.min(VIEW_H, Math.round(base / divisor)));
  }

  function collectTargets() {
    return [
      {
        wrap: document.getElementById("pa-chart-wrap"),
        heightRoot: document.getElementById("pa-chart-height"),
        compressRoot: document.getElementById("pa-chart-compress"),
        svgId: "pa-chart",
        cardSel: "#pa-chart-card",
        tipIds: ["pa-tooltip"],
      },
      {
        wrap: document.getElementById("ia-chart-wrap"),
        heightRoot: document.getElementById("ia-chart-height"),
        compressRoot: document.getElementById("ia-chart-compress"),
        svgId: "ia-chart",
        cardSel: "#ia-chart-card",
        tipIds: ["ia-tooltip"],
      },
      {
        wrap: document.getElementById("sd-chart-wrap"),
        heightRoot: document.getElementById("sd-chart-height"),
        compressRoot: document.getElementById("sd-chart-compress"),
        svgId: "sd-chart",
        cardSel: "#sd-chart-card",
        tipIds: ["sd-tooltip"],
      },
    ].filter(function (t) {
      return t.wrap && t.heightRoot;
    });
  }

  var targets = collectTargets();
  if (!targets.length) return;

  var height = readStored(HEIGHT_KEY, ["1", "2", "3"], DEFAULT_HEIGHT);
  var compress = readStored(COMPRESS_KEY, ["1", "2", "3"], DEFAULT_COMPRESS);
  var marginL = readStoredNumber(MARGIN_L_KEY, 0);
  var marginR = readStoredNumber(MARGIN_R_KEY, 0);
  var customChartH = readStoredNumber(CUSTOM_H_KEY, 0);
  var layoutSyncing = false;
  var layoutRaf = null;
  var moTimer = null;
  var lastWidths = new WeakMap();
  var dragState = null;
  var heightDragState = null;

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

  function chartStageForSvg(svg) {
    return svg ? svg.closest(".pa-chart-stage") : null;
  }

  function siblingsAboveSvg(wrap, svg) {
    var stage = chartStageForSvg(svg);
    var stop = stage || svg;
    var total = 0;
    var kids = wrap.children;
    var tipSkip = { "pa-tooltip": 1, "ia-tooltip": 1, "sd-tooltip": 1 };
    for (var i = 0; i < kids.length; i++) {
      var child = kids[i];
      if (child === stop) break;
      if (child.id && tipSkip[child.id]) continue;
      total += child.offsetHeight || 0;
    }
    return total;
  }

  function innerWidth(wrap) {
    var cs = window.getComputedStyle(wrap);
    var pad =
      (parseFloat(cs.paddingLeft) || 0) + (parseFloat(cs.paddingRight) || 0);
    var w = wrap.clientWidth - pad;
    if (w > 1) return w;
    var card = wrap.closest("#pa-chart-card, #ia-chart-card, #sd-chart-card");
    if (card) {
      var csCard = window.getComputedStyle(card);
      var padCard =
        (parseFloat(csCard.paddingLeft) || 0) +
        (parseFloat(csCard.paddingRight) || 0);
      w = card.clientWidth - padCard - pad;
    }
    return Math.max(1, w);
  }

  function normalizeMargins() {
    marginL = clamp(marginL, 0, 100 - MIN_WIDTH_PCT);
    marginR = clamp(marginR, 0, 100 - MIN_WIDTH_PCT);
    if (marginL + marginR > 100 - MIN_WIDTH_PCT) {
      var overflow = marginL + marginR - (100 - MIN_WIDTH_PCT);
      if (marginL >= marginR) marginL = Math.max(0, marginL - overflow);
      else marginR = Math.max(0, marginR - overflow);
    }
  }

  function persistMargins() {
    try {
      localStorage.setItem(MARGIN_L_KEY, String(Math.round(marginL * 100) / 100));
      localStorage.setItem(MARGIN_R_KEY, String(Math.round(marginR * 100) / 100));
    } catch (_) {}
  }

  function persistCustomHeight() {
    try {
      if (customChartH > 0) {
        localStorage.setItem(CUSTOM_H_KEY, String(Math.round(customChartH)));
      } else {
        localStorage.removeItem(CUSTOM_H_KEY);
      }
    } catch (_) {}
  }

  function computedChartHeight(w) {
    var eff = effectiveHeight(height, compress);
    return Math.max(MIN_CHART_H, Math.round((w * eff) / VIEW_W));
  }

  function resolveChartHeight(w) {
    if (customChartH > 0) {
      return clamp(customChartH, MIN_CHART_H, MAX_CHART_H);
    }
    return computedChartHeight(w);
  }

  function clearCustomHeight() {
    customChartH = 0;
    persistCustomHeight();
    targets.forEach(function (t) {
      if (t.wrap) t.wrap.removeAttribute("data-chart-custom-h");
    });
  }

  function applyMargins() {
    normalizeMargins();
    var narrow = typeof window !== "undefined" && window.innerWidth < 640;
    var mlUse = narrow ? 0 : marginL;
    var mrUse = narrow ? 0 : marginR;
    targets.forEach(function (t) {
      if (!t.wrap) return;
      var ml = Math.round(mlUse * 10) / 10;
      var mr = Math.round(mrUse * 10) / 10;
      t.wrap.setAttribute("data-chart-margin-l", String(ml));
      t.wrap.setAttribute("data-chart-margin-r", String(mr));
      t.wrap.style.setProperty("--pa-chart-margin-l", ml + "%");
      t.wrap.style.setProperty("--pa-chart-margin-r", mr + "%");
      if (t.viewport) {
        t.viewport.style.marginLeft = ml + "%";
        t.viewport.style.marginRight = mr + "%";
      }
    });
  }

  function decorateEdgeHandle(handle) {
    if (!handle) return;
    handle.innerHTML = "";
    handle.setAttribute("data-pa-chip", "1");
    handle.removeAttribute("title");
  }

  function decorateHeightHandle(handle) {
    if (!handle) return;
    handle.innerHTML = "";
    handle.setAttribute("data-pa-chip", "1");
    handle.removeAttribute("title");
  }

  function ensureChartStage(t) {
    var svg = document.getElementById(t.svgId);
    if (!svg || !t.wrap.contains(svg)) return null;

    var stage = chartStageForSvg(svg);
    if (!stage) {
      stage = document.createElement("div");
      stage.className = "pa-chart-stage";

      var viewport = document.createElement("div");
      viewport.className = "pa-chart-viewport";

      var leftHandle = document.createElement("button");
      leftHandle.type = "button";
      leftHandle.className = "pa-chart-edge-handle pa-chart-edge-handle--left";
      leftHandle.setAttribute("aria-label", "Grafiği soldan genişlet veya daralt");

      var rightHandle = document.createElement("button");
      rightHandle.type = "button";
      rightHandle.className = "pa-chart-edge-handle pa-chart-edge-handle--right";
      rightHandle.setAttribute("aria-label", "Grafiği sağdan genişlet veya daralt");

      var bottomHandle = document.createElement("button");
      bottomHandle.type = "button";
      bottomHandle.className = "pa-chart-height-handle pa-chart-height-handle--bottom";
      bottomHandle.setAttribute("aria-label", "Grafiği dikey genişlet veya daralt");

      t.wrap.insertBefore(stage, svg);
      viewport.appendChild(svg);
      stage.appendChild(viewport);
      stage.appendChild(leftHandle);
      stage.appendChild(rightHandle);
      stage.appendChild(bottomHandle);

      bindEdgeDrag(t, leftHandle, "left");
      bindEdgeDrag(t, rightHandle, "right");
      bindHeightDrag(t, bottomHandle);
    }

    t.stage = stage;
    t.viewport = stage.querySelector(".pa-chart-viewport");
    t.handleLeft = stage.querySelector(".pa-chart-edge-handle--left");
    t.handleRight = stage.querySelector(".pa-chart-edge-handle--right");
    t.handleBottom = stage.querySelector(".pa-chart-height-handle--bottom");
    if (!t.handleBottom) {
      var bottomHandle = document.createElement("button");
      bottomHandle.type = "button";
      bottomHandle.className = "pa-chart-height-handle pa-chart-height-handle--bottom";
      bottomHandle.setAttribute("aria-label", "Grafiği dikey genişlet veya daralt");
      stage.appendChild(bottomHandle);
      t.handleBottom = bottomHandle;
      bindHeightDrag(t, bottomHandle);
    }
    decorateEdgeHandle(t.handleLeft);
    decorateEdgeHandle(t.handleRight);
    decorateHeightHandle(t.handleBottom);
    return stage;
  }

  function pointerX(ev) {
    if (ev.touches && ev.touches.length) return ev.touches[0].clientX;
    if (ev.changedTouches && ev.changedTouches.length) return ev.changedTouches[0].clientX;
    return ev.clientX;
  }

  function pointerY(ev) {
    if (ev.touches && ev.touches.length) return ev.touches[0].clientY;
    if (ev.changedTouches && ev.changedTouches.length) return ev.changedTouches[0].clientY;
    return ev.clientY;
  }

  function bindEdgeDrag(t, handle, side) {
    handle.addEventListener("dblclick", function (ev) {
      ev.preventDefault();
      if (side === "left") marginL = 0;
      else marginR = 0;
      applyMargins();
      persistMargins();
      scheduleLayoutSync();
    });

    function onMove(ev) {
      if (!dragState || dragState.handle !== handle) return;
      ev.preventDefault();
      var wrapW = dragState.wrapW || innerWidth(t.wrap);
      var dx = pointerX(ev) - dragState.startX;
      var dPct = (dx / wrapW) * 100;
      if (side === "left") {
        marginL = clamp(
          dragState.startMarginL + dPct,
          0,
          100 - dragState.startMarginR - MIN_WIDTH_PCT
        );
      } else {
        marginR = clamp(
          dragState.startMarginR - dPct,
          0,
          100 - dragState.startMarginL - MIN_WIDTH_PCT
        );
      }
      applyMargins();
    }

    function onEnd() {
      if (!dragState || dragState.handle !== handle) return;
      handle.classList.remove("is-dragging");
      t.wrap.classList.remove("pa-chart-edge-dragging");
      dragState = null;
      document.removeEventListener("mousemove", onMove);
      document.removeEventListener("mouseup", onEnd);
      document.removeEventListener("touchmove", onMove);
      document.removeEventListener("touchend", onEnd);
      document.removeEventListener("touchcancel", onEnd);
      persistMargins();
      scheduleLayoutSync();
    }

    function onStart(ev) {
      if (ev.type === "mousedown" && ev.button !== 0) return;
      ev.preventDefault();
      dragState = {
        handle: handle,
        side: side,
        startX: pointerX(ev),
        startMarginL: marginL,
        startMarginR: marginR,
        wrapW: innerWidth(t.wrap),
      };
      handle.classList.add("is-dragging");
      t.wrap.classList.add("pa-chart-edge-dragging");
      document.addEventListener("mousemove", onMove);
      document.addEventListener("mouseup", onEnd);
      document.addEventListener("touchmove", onMove, { passive: false });
      document.addEventListener("touchend", onEnd);
      document.addEventListener("touchcancel", onEnd);
    }

    handle.addEventListener("mousedown", onStart);
    handle.addEventListener("touchstart", onStart, { passive: false });
  }

  function bindHeightDrag(t, handle) {
    handle.addEventListener("dblclick", function (ev) {
      ev.preventDefault();
      clearCustomHeight();
      scheduleLayoutSync();
    });

    function onMove(ev) {
      if (!heightDragState || heightDragState.handle !== handle) return;
      ev.preventDefault();
      var dy = pointerY(ev) - heightDragState.startY;
      customChartH = clamp(
        heightDragState.startChartH + dy,
        MIN_CHART_H,
        MAX_CHART_H
      );
      if (t.wrap) {
        t.wrap.setAttribute("data-chart-custom-h", String(Math.round(customChartH)));
      }
      syncLayout();
    }

    function onEnd() {
      if (!heightDragState || heightDragState.handle !== handle) return;
      handle.classList.remove("is-dragging");
      t.wrap.classList.remove("pa-chart-height-dragging");
      heightDragState = null;
      document.removeEventListener("mousemove", onMove);
      document.removeEventListener("mouseup", onEnd);
      document.removeEventListener("touchmove", onMove);
      document.removeEventListener("touchend", onEnd);
      document.removeEventListener("touchcancel", onEnd);
      persistCustomHeight();
      scheduleLayoutSync();
    }

    function onStart(ev) {
      if (ev.type === "mousedown" && ev.button !== 0) return;
      ev.preventDefault();
      var w = innerWidth(t.wrap);
      var currentH =
        customChartH > 0 ? customChartH : computedChartHeight(w);
      heightDragState = {
        handle: handle,
        startY: pointerY(ev),
        startChartH: currentH,
      };
      customChartH = currentH;
      handle.classList.add("is-dragging");
      t.wrap.classList.add("pa-chart-height-dragging");
      document.addEventListener("mousemove", onMove);
      document.addEventListener("mouseup", onEnd);
      document.addEventListener("touchmove", onMove, { passive: false });
      document.addEventListener("touchend", onEnd);
      document.addEventListener("touchcancel", onEnd);
    }

    handle.addEventListener("mousedown", onStart);
    handle.addEventListener("touchstart", onStart, { passive: false });
  }

  function applySettings() {
    var eff = effectiveHeight(height, compress);
    targets.forEach(function (t) {
      t.wrap.setAttribute("data-chart-height", height);
      t.wrap.setAttribute("data-chart-compress", compress);
      t.wrap.style.setProperty("--pa-chart-effective-h", String(eff));

      var card = t.wrap.closest("#pa-chart-card, #ia-chart-card, #sd-chart-card");
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

  function clearSizeLocks(el) {
    if (!el) return;
    el.style.height = "";
    el.style.minHeight = "";
    el.style.maxHeight = "";
  }

  /** preserveAspectRatio=none yatay/dikey farklı ölçekler; eksen metnini düzleştir */
  function fixAxisLabelDistortion(svg) {
    if (!svg) return;
    var vb = svg.viewBox && svg.viewBox.baseVal;
    var vw = vb && vb.width ? vb.width : VIEW_W;
    var vh = vb && vb.height ? vb.height : VIEW_H;
    var rect = svg.getBoundingClientRect();
    if (!rect.width || !rect.height) return;
    var sx = rect.width / vw;
    var sy = rect.height / vh;
    var labels = svg.querySelectorAll(".pa-axis-label");
    if (!labels.length) return;
    if (Math.abs(sx - sy) < 0.015) {
      labels.forEach(function (el) {
        el.removeAttribute("transform");
      });
      return;
    }
    var rx = sy / sx;
    labels.forEach(function (el) {
      var x = parseFloat(el.getAttribute("x"));
      var y = parseFloat(el.getAttribute("y"));
      if (!isFinite(x) || !isFinite(y)) return;
      el.setAttribute(
        "transform",
        "translate(" +
          x +
          " " +
          y +
          ") scale(" +
          rx +
          " 1) translate(" +
          -x +
          " " +
          -y +
          ")"
      );
    });
  }

  function syncLayout() {
    if (layoutSyncing) return;
    layoutSyncing = true;

    applyMargins();

    targets.forEach(function (t) {
      var svg = document.getElementById(t.svgId);
      if (!svg || !t.wrap.contains(svg)) return;

      ensureChartStage(t);

      t.wrap.classList.add("pa-chart-layout-sync");

      var w = innerWidth(t.wrap);
      var chartH = resolveChartHeight(w);
      var wrapH = PAD_Y + chartH + PAD_Y;

      if (customChartH > 0) {
        t.wrap.setAttribute("data-chart-custom-h", String(Math.round(customChartH)));
      } else {
        t.wrap.removeAttribute("data-chart-custom-h");
      }

      t.wrap.style.boxSizing = "border-box";
      t.wrap.style.paddingTop = PAD_Y + "px";
      t.wrap.style.paddingBottom = PAD_Y + "px";
      t.wrap.style.width = "100%";
      t.wrap.style.height = wrapH + "px";
      t.wrap.style.minHeight = wrapH + "px";
      t.wrap.style.maxHeight = wrapH + "px";

      if (t.stage) {
        t.stage.style.width = "100%";
        t.stage.style.position = "relative";
        t.stage.style.flex = "0 0 auto";
        t.stage.style.height = chartH + "px";
        t.stage.style.minHeight = chartH + "px";
        t.stage.style.maxHeight = chartH + "px";
      }

      if (t.viewport) {
        t.viewport.style.width = "auto";
        t.viewport.style.height = chartH + "px";
        t.viewport.style.minHeight = chartH + "px";
        t.viewport.style.maxHeight = chartH + "px";
      }

      svg.style.display = "block";
      svg.style.width = "100%";
      svg.style.height = chartH + "px";
      svg.style.minHeight = chartH + "px";
      svg.style.maxHeight = chartH + "px";
      svg.style.marginTop = "0";
      svg.style.marginBottom = "0";
      svg.setAttribute("preserveAspectRatio", "none");

      var card = t.wrap.closest("#pa-chart-card, #ia-chart-card, #sd-chart-card");
      if (card) {
        card.style.display = "flex";
        card.style.flexDirection = "column";
        clearSizeLocks(card);
        card.style.width = "100%";
      }

      fixAxisLabelDistortion(svg);
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

  function onResize(entries) {
    if (layoutSyncing || dragState || heightDragState) return;
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

  targets.forEach(function (t) {
    ensureChartStage(t);
  });
  applyAll();
  window.addEventListener("resize", scheduleLayoutSync);
  window.paSyncChartLayout = scheduleLayoutSync;

  if (typeof ResizeObserver !== "undefined") {
    var ro = new ResizeObserver(onResize);
    targets.forEach(function (t) {
      ro.observe(t.wrap);
      var card = t.wrap.closest("#pa-chart-card, #ia-chart-card, #sd-chart-card");
      if (card) ro.observe(card);
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

  if (document.readyState === "loading") {
    document.addEventListener("DOMContentLoaded", scheduleLayoutSync);
  } else {
    scheduleLayoutSync();
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
      clearCustomHeight();
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
      clearCustomHeight();
      applyAll();
    });
  });
})();
