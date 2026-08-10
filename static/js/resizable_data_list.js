/**
 * Kapalı/açık tablo listeleri: max N satır yüksekliği, kısa dönemde küçülür,
 * alttan sürükleyerek büyütülebilir. Virgül / Android / iOS ortak.
 *
 * Sayfa sonunda (iOS gibi altta başka blok yokken) sürükleme: viewport
 * kenarında otomatik büyüt + window scroll — handle ekran dışına kaçmaz.
 */
(function (global) {
  "use strict";

  var STYLE_ID = "seo-rdl-style";
  var DEFAULTS = {
    maxRows: 20,
    rowH: 30,
    headH: 34,
    pad: 4,
    minH: 72,
    /* Manuel sürüklemede tavan; tablo daha uzunsa contentHeight ile yükselir */
    maxH: 2400,
    storageKey: "",
  };

  function injectStyles() {
    if (document.getElementById(STYLE_ID)) return;
    var style = document.createElement("style");
    style.id = STYLE_ID;
    style.textContent =
      ".rdl-shell{position:relative;display:flex;flex-direction:column;min-width:0;width:100%;" +
      "border-radius:0.75rem;overflow:hidden;}" +
      ".rdl-scroll{min-width:0;width:100%;overflow:auto;-webkit-overflow-scrolling:touch;" +
      "overscroll-behavior:contain;flex:1 1 auto;}" +
      ".rdl-scroll>table{width:max-content;max-width:none;min-width:100%;border-collapse:collapse;}" +
      ".rdl-handle{flex:0 0 auto;height:14px;cursor:ns-resize;touch-action:none;" +
      "display:flex;align-items:center;justify-content:center;" +
      "background:linear-gradient(to bottom,transparent,rgba(148,163,184,.18));" +
      "border-top:1px solid rgba(148,163,184,.35);user-select:none;}" +
      ".rdl-handle::after{content:'';width:2.25rem;height:3px;border-radius:9999px;" +
      "background:rgba(100,116,139,.55);}" +
      "html.dark .rdl-handle{background:linear-gradient(to bottom,transparent,rgba(63,63,70,.45));" +
      "border-top-color:rgba(63,63,70,.8);}" +
      "html.dark .rdl-handle::after{background:rgba(161,161,170,.55);}" +
      ".rdl-handle:hover::after,.rdl-handle.is-dragging::after{background:rgba(14,165,233,.85);}" +
      ".rdl-shell.is-dragging{user-select:none;}" +
      ".rdl-drag-room{flex:0 0 auto;width:100%;height:min(42vh,22rem);pointer-events:none;}" +
      "details.rdl-dropdown>summary{list-style:none;cursor:pointer;}" +
      "details.rdl-dropdown>summary::-webkit-details-marker{display:none;}" +
      "details.rdl-dropdown>summary .rdl-chevron{display:inline-block;transition:transform .15s ease;}" +
      "details.rdl-dropdown[open]>summary .rdl-chevron{transform:rotate(90deg);}";
    document.head.appendChild(style);
  }

  function optsOf(shell, overrides) {
    var o = Object.assign({}, DEFAULTS, overrides || {});
    if (shell && shell.dataset) {
      if (shell.dataset.rdlMaxRows) o.maxRows = parseInt(shell.dataset.rdlMaxRows, 10) || o.maxRows;
      if (shell.dataset.rdlStorageKey) o.storageKey = shell.dataset.rdlStorageKey;
      if (shell.dataset.rdlMaxH) {
        var mh = parseInt(shell.dataset.rdlMaxH, 10);
        if (mh > 0) o.maxH = mh;
      }
    }
    return o;
  }

  function contentHeight(shell, opts) {
    var scroll = shell.querySelector(".rdl-scroll");
    if (!scroll) return opts.minH;
    var table = scroll.querySelector("table");
    if (!table) return opts.minH;
    return Math.max(opts.minH, Math.ceil(table.getBoundingClientRect().height) + opts.pad);
  }

  /** Sürükleme tavanı: yapılandırılan maxH veya tablonun tam yüksekliği (hangisi büyükse). */
  function dragCeiling(shell, opts) {
    return Math.max(opts.maxH, contentHeight(shell, opts));
  }

  function autoHeight(shell, rowCount, opts) {
    var n = Math.max(0, Number(rowCount) || 0);
    if (n <= 0) return opts.minH;
    var visible = Math.min(n, opts.maxRows);
    var byRows = opts.headH + visible * opts.rowH + opts.pad;
    var byContent = contentHeight(shell, opts);
    // 1–2 hafta gibi kısa dilim: içeriğe göre küçül; uzun dilim: max 20 satır viewport
    var h = n <= opts.maxRows
      ? Math.min(byContent || byRows, byRows + 12)
      : opts.headH + opts.maxRows * opts.rowH + opts.pad;
    return Math.max(opts.minH, Math.min(opts.maxH, Math.round(h)));
  }

  function readManual(shell, opts) {
    if (shell._rdlManualH != null && shell._rdlManualH > 0) return shell._rdlManualH;
    if (!opts.storageKey) return null;
    try {
      var v = parseInt(localStorage.getItem(opts.storageKey) || "", 10);
      return v > 0 ? v : null;
    } catch (e) {
      return null;
    }
  }

  function writeManual(shell, opts, h) {
    shell._rdlManualH = h;
    if (!opts.storageKey) return;
    try {
      localStorage.setItem(opts.storageKey, String(h));
    } catch (e) {}
  }

  function clearManual(shell, opts) {
    shell._rdlManualH = null;
    if (!opts.storageKey) return;
    try {
      localStorage.removeItem(opts.storageKey);
    } catch (e) {}
  }

  function applyScrollHeight(shell, h) {
    var scroll = shell.querySelector(".rdl-scroll");
    if (!scroll) return;
    scroll.style.height = h + "px";
    scroll.style.maxHeight = h + "px";
  }

  /** Sayfa sonunda sürüklemek için boşluk — handle viewport dibine yapışmasın. */
  function ensureDragRoom(shell) {
    if (!shell || !shell.parentNode) return;
    var next = shell.nextElementSibling;
    if (next && next.classList && next.classList.contains("rdl-drag-room")) return;
    var room = document.createElement("div");
    room.className = "rdl-drag-room";
    room.setAttribute("aria-hidden", "true");
    shell.parentNode.insertBefore(room, shell.nextSibling);
  }

  function keepHandleInView(handle) {
    if (!handle) return;
    var hr = handle.getBoundingClientRect();
    var pad = 36;
    var overflow = hr.bottom - (window.innerHeight - pad);
    if (overflow > 0) {
      window.scrollBy(0, Math.ceil(overflow));
    }
  }

  function fit(shell, rowCount, overrides) {
    if (!shell) return;
    injectStyles();
    ensureDragRoom(shell);
    var opts = optsOf(shell, overrides);
    var auto = autoHeight(shell, rowCount, opts);
    shell._rdlAutoH = auto;
    shell._rdlRowCount = rowCount;
    var manual = readManual(shell, opts);
    var ceiling = dragCeiling(shell, opts);
    // Manuel yükseklik yalnızca auto'dan büyükse (kullanıcı genişletti); dar dönemde küçülmeye izin ver
    var h = manual != null && manual > auto ? Math.min(ceiling, manual) : auto;
    applyScrollHeight(shell, h);
    shell.setAttribute("data-rdl-fitted", "1");
  }

  function bind(shell, overrides) {
    if (!shell || shell._rdlBound) return shell;
    injectStyles();
    ensureDragRoom(shell);
    var opts = optsOf(shell, overrides);
    shell._rdlBound = true;
    shell.classList.add("rdl-shell");
    var scroll = shell.querySelector(".rdl-scroll");
    if (scroll) scroll.classList.add("rdl-scroll");
    var handle = shell.querySelector("[data-rdl-handle], .rdl-handle");
    if (!handle) {
      handle = document.createElement("div");
      handle.className = "rdl-handle";
      handle.setAttribute("data-rdl-handle", "");
      handle.setAttribute("role", "separator");
      handle.setAttribute("aria-orientation", "horizontal");
      handle.setAttribute("aria-label", "Liste yüksekliğini sürükleyerek ayarla");
      handle.title = "Alt kenardan sürükleyerek listeyi büyüt / küçült";
      shell.appendChild(handle);
    } else {
      handle.classList.add("rdl-handle");
    }

    var dragging = false;
    var startY = 0;
    var startH = 0;
    var edgeExtra = 0;
    var lastClientY = 0;
    var edgeRaf = 0;

    function applyFromPointer(clientY) {
      var optsNow = optsOf(shell, overrides);
      var dy = clientY - startY;
      var ceiling = dragCeiling(shell, optsNow);
      var next = Math.max(optsNow.minH, Math.min(ceiling, startH + dy + edgeExtra));
      applyScrollHeight(shell, next);
      writeManual(shell, optsNow, next);
      keepHandleInView(handle);
    }

    function edgeTick() {
      edgeRaf = 0;
      if (!dragging) return;
      var edge = 72;
      var room = window.innerHeight - lastClientY;
      if (room < edge) {
        var step = Math.max(8, Math.round((edge - room) * 0.9));
        var optsNow = optsOf(shell, overrides);
        var ceiling = dragCeiling(shell, optsNow);
        var scrollEl = shell.querySelector(".rdl-scroll");
        var cur = scrollEl ? scrollEl.getBoundingClientRect().height : startH;
        if (cur + step <= ceiling || cur < ceiling) {
          edgeExtra += step;
          applyFromPointer(lastClientY);
          window.scrollBy(0, step);
        }
      } else {
        keepHandleInView(handle);
      }
      edgeRaf = requestAnimationFrame(edgeTick);
    }

    function onMove(clientY) {
      if (!dragging) return;
      lastClientY = clientY;
      applyFromPointer(clientY);
    }

    function onUp() {
      if (!dragging) return;
      dragging = false;
      edgeExtra = 0;
      if (edgeRaf) {
        cancelAnimationFrame(edgeRaf);
        edgeRaf = 0;
      }
      shell.classList.remove("is-dragging");
      handle.classList.remove("is-dragging");
      document.removeEventListener("pointermove", onPointerMove);
      document.removeEventListener("pointerup", onUp);
      document.removeEventListener("pointercancel", onUp);
    }

    function onPointerMove(ev) {
      onMove(ev.clientY);
      ev.preventDefault();
    }

    handle.addEventListener("pointerdown", function (ev) {
      if (ev.button != null && ev.button !== 0) return;
      var scrollEl = shell.querySelector(".rdl-scroll");
      if (!scrollEl) return;
      dragging = true;
      edgeExtra = 0;
      startY = ev.clientY;
      lastClientY = ev.clientY;
      startH = scrollEl.getBoundingClientRect().height || shell._rdlAutoH || DEFAULTS.minH;
      shell.classList.add("is-dragging");
      handle.classList.add("is-dragging");
      try {
        handle.setPointerCapture(ev.pointerId);
      } catch (e) {}
      document.addEventListener("pointermove", onPointerMove, { passive: false });
      document.addEventListener("pointerup", onUp);
      document.addEventListener("pointercancel", onUp);
      if (edgeRaf) cancelAnimationFrame(edgeRaf);
      edgeRaf = requestAnimationFrame(edgeTick);
      ev.preventDefault();
    });

    // Çift tık: kullanıcı boyutunu sıfırla → otomatik yüksekliğe dön
    handle.addEventListener("dblclick", function () {
      clearManual(shell, optsOf(shell, overrides));
      fit(shell, shell._rdlRowCount != null ? shell._rdlRowCount : 0, overrides);
    });

    return shell;
  }

  function bindDetails(detailsEl, shell, getRowCount, overrides) {
    if (!detailsEl || !shell) return;
    bind(shell, overrides);
    function refit() {
      var n = typeof getRowCount === "function" ? getRowCount() : getRowCount;
      fit(shell, n, overrides);
    }
    detailsEl.addEventListener("toggle", function () {
      if (detailsEl.open) {
        requestAnimationFrame(function () {
          requestAnimationFrame(refit);
        });
      }
    });
    if (detailsEl.open) refit();
  }

  global.SeoResizableDataList = {
    injectStyles: injectStyles,
    bind: bind,
    fit: fit,
    bindDetails: bindDetails,
    clearManual: function (shell, overrides) {
      if (!shell) return;
      clearManual(shell, optsOf(shell, overrides));
    },
    DEFAULTS: DEFAULTS,
  };
})(typeof window !== "undefined" ? window : this);
