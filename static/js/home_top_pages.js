/**
 * Ana sayfa — Top 25 sayfa paneli (tam genişlik, site kartları bağımsız).
 */
(function () {
  'use strict';

  function closeRoot(root) {
    if (!root) return;
    root.querySelectorAll('[data-home-top-pages-panel]').forEach(function (panel) {
      panel.classList.remove('is-open');
      panel.hidden = true;
    });
    root.querySelectorAll('[data-home-top-pages-open]').forEach(function (btn) {
      btn.classList.remove('is-active');
      btn.setAttribute('aria-expanded', 'false');
    });
  }

  function openPanel(root, panelId) {
    closeRoot(root);
    var panel = root.querySelector('[data-home-top-pages-panel="' + panelId + '"]');
    var btn = root.querySelector('[data-home-top-pages-open="' + panelId + '"]');
    if (!panel) return;
    panel.hidden = false;
    panel.classList.add('is-open');
    if (btn) {
      btn.classList.add('is-active');
      btn.setAttribute('aria-expanded', 'true');
    }
  }

  document.addEventListener('click', function (ev) {
    var closeBtn = ev.target.closest('[data-home-top-pages-close]');
    if (closeBtn) {
      var closeRootEl = closeBtn.closest('[data-home-top-pages-root]');
      closeRoot(closeRootEl);
      return;
    }
    var openBtn = ev.target.closest('[data-home-top-pages-open]');
    if (!openBtn) return;
    var root = openBtn.closest('[data-home-top-pages-root]');
    if (!root) return;
    var id = openBtn.getAttribute('data-home-top-pages-open');
    if (!id) return;
    if (openBtn.classList.contains('is-active')) {
      closeRoot(root);
    } else {
      openPanel(root, id);
    }
  });
})();
