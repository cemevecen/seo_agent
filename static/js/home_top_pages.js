/**
 * Ana sayfa — Top 25 / Spark KPI paneli (tam genişlik, site kartları bağımsız).
 * Aynı panel_id birden fazla tetikleyicide olabilir (ör. SC clicks + position Spark);
 * hepsi birlikte active olur; açık panele tekrar basınca kapanır.
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

  function setTriggersActive(root, panelId, active) {
    root.querySelectorAll('[data-home-top-pages-open="' + panelId + '"]').forEach(function (btn) {
      btn.classList.toggle('is-active', active);
      btn.setAttribute('aria-expanded', active ? 'true' : 'false');
    });
  }

  function openPanel(root, panelId) {
    closeRoot(root);
    var panel = root.querySelector('[data-home-top-pages-panel="' + panelId + '"]');
    if (!panel) return;
    panel.hidden = false;
    panel.classList.add('is-open');
    setTriggersActive(root, panelId, true);
  }

  function panelIsOpen(root, panelId) {
    var panel = root.querySelector('[data-home-top-pages-panel="' + panelId + '"]');
    return !!(panel && panel.classList.contains('is-open') && !panel.hidden);
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
    if (panelIsOpen(root, id)) {
      closeRoot(root);
    } else {
      openPanel(root, id);
    }
  });
})();
