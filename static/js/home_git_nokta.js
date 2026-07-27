/**
 * Ana sayfa git.nokta — boards yıldızları + platform chip tab'leri.
 */
(function (global) {
  var PRODUCT_ACCENT = { doviz: 'sky', sinemalar: 'violet' };

  function escapeHtml(s) {
    return String(s || '')
      .replace(/&/g, '&amp;')
      .replace(/</g, '&lt;')
      .replace(/>/g, '&gt;')
      .replace(/"/g, '&quot;');
  }

  function chipActiveClass(productId) {
    return PRODUCT_ACCENT[productId] === 'violet'
      ? 'bg-violet-600 text-white ring-violet-600'
      : 'bg-sky-600 text-white ring-sky-600';
  }

  function chipIdleClass() {
    return 'bg-white text-slate-600 ring-slate-200 hover:bg-slate-50 dark:bg-slate-900 dark:text-slate-300 dark:ring-slate-700 dark:hover:bg-slate-800';
  }

  function renderList(root, productId, colId, entries) {
    var list = root.querySelector('[data-gn-list="' + productId + '-' + colId + '"]');
    var count = root.querySelector('[data-gn-count="' + productId + '-' + colId + '"]');
    if (count) count.textContent = String(entries.length);
    if (!list) return;
    if (!entries.length) {
      list.innerHTML =
        '<li class="flex flex-1 items-center justify-center rounded-lg border border-dashed border-slate-200 px-2 py-6 dark:border-slate-700" aria-hidden="true"></li>';
      return;
    }
    list.innerHTML = entries
      .map(function (item) {
        var title = escapeHtml(item.title || 'Issue #' + item.issue_iid);
        var titleHtml = item.web_url
          ? '<a href="' +
            escapeHtml(item.web_url) +
            '" target="_blank" rel="noopener noreferrer" class="min-w-0 text-xs font-semibold leading-snug text-slate-900 underline-offset-2 hover:underline dark:text-slate-100">' +
            title +
            '</a>'
          : '<p class="min-w-0 text-xs font-semibold leading-snug text-slate-900 dark:text-slate-100">' +
            title +
            '</p>';
        return (
          '<li class="rounded-lg bg-white p-2.5 shadow-sm ring-1 ring-slate-200/90 dark:bg-slate-900 dark:ring-slate-700">' +
          '<div class="flex items-start justify-between gap-2">' +
          titleHtml +
          '<span class="shrink-0 rounded-md bg-amber-50 px-1.5 py-0.5 text-[9px] font-bold uppercase tracking-wide text-amber-700 ring-1 ring-amber-200 dark:bg-amber-950/40 dark:text-amber-300 dark:ring-amber-800">★</span>' +
          '</div>' +
          '<div class="mt-1.5 flex flex-wrap items-center gap-2 text-[11px] text-slate-500 dark:text-slate-400">' +
          (item.issue_iid ? '<span class="font-mono tabular-nums">#' + escapeHtml(item.issue_iid) + '</span>' : '') +
          (item.source_label
            ? '<span class="rounded bg-slate-100 px-1 py-0.5 text-[9px] font-bold uppercase dark:bg-slate-800">' +
              escapeHtml(item.source_label) +
              '</span>'
            : '') +
          '</div></li>'
        );
      })
      .join('');
  }

  function renderProduct(root, productId, items) {
    var section = root.querySelector('[data-gn-product="' + productId + '"]');
    if (!section) return;
    var chip = section.getAttribute('data-gn-active-chip') || 'web';
    var filtered = (items || []).filter(function (it) {
      return it.product === productId && it.platform === chip;
    });
    ['open', 'doing', 'testing', 'closed'].forEach(function (colId) {
      var entries = filtered.filter(function (it) {
        return (it.board_list || 'open') === colId;
      });
      renderList(root, productId, colId, entries);
    });
  }

  function setActiveChip(root, productId, chipId) {
    var section = root.querySelector('[data-gn-product="' + productId + '"]');
    if (!section) return;
    section.setAttribute('data-gn-active-chip', chipId);
    var buttons = section.querySelectorAll('[data-gn-chip]');
    buttons.forEach(function (btn) {
      var active = btn.getAttribute('data-gn-chip-id') === chipId;
      btn.setAttribute('aria-selected', active ? 'true' : 'false');
      btn.className =
        'gn-chip inline-flex items-center rounded-full px-2.5 py-1 text-[11px] font-bold ring-1 transition ' +
        (active ? chipActiveClass(productId) : chipIdleClass());
    });
    renderProduct(root, productId, root._gnItems || []);
  }

  async function mount(root) {
    if (!root || root.getAttribute('data-gn-wired') === '1') return;
    root.setAttribute('data-gn-wired', '1');

    var loadingEl = root.querySelector('[data-gn-loading]');
    var errorEl = root.querySelector('[data-gn-error]');
    var errorMsg = root.querySelector('[data-gn-error-msg]');
    var bodyEl = root.querySelector('[data-gn-body]');

    function showError(msg) {
      if (loadingEl) loadingEl.classList.add('hidden');
      if (errorEl) errorEl.classList.remove('hidden');
      if (errorMsg) errorMsg.textContent = msg || 'Yıldızlı maddeler alınamadı.';
      if (bodyEl) {
        bodyEl.classList.remove('opacity-60', 'pointer-events-none');
        bodyEl.removeAttribute('aria-busy');
      }
    }

    function showReady() {
      if (loadingEl) loadingEl.classList.add('hidden');
      if (errorEl) errorEl.classList.add('hidden');
      if (bodyEl) {
        bodyEl.classList.remove('opacity-60', 'pointer-events-none');
        bodyEl.removeAttribute('aria-busy');
      }
    }

    root.addEventListener('click', function (ev) {
      var btn = ev.target && ev.target.closest ? ev.target.closest('[data-gn-chip]') : null;
      if (!btn || !root.contains(btn)) return;
      var productId = btn.getAttribute('data-gn-product-id');
      var chipId = btn.getAttribute('data-gn-chip-id');
      if (productId && chipId) setActiveChip(root, productId, chipId);
    });

    try {
      var res = await fetch('/api/boards/stars', { credentials: 'same-origin', cache: 'no-store' });
      if (!res.ok) throw new Error('HTTP ' + res.status);
      var data = await res.json();
      root._gnItems = data.items || [];
      ['doviz', 'sinemalar'].forEach(function (pid) {
        renderProduct(root, pid, root._gnItems);
      });
      showReady();
    } catch (err) {
      console.warn('home git.nokta stars load failed', err);
      showError((err && err.message) || 'Yıldızlı maddeler yüklenemedi.');
      ['doviz', 'sinemalar'].forEach(function (pid) {
        ['open', 'doing', 'testing', 'closed'].forEach(function (cid) {
          renderList(root, pid, cid, []);
        });
      });
    }
  }

  function mountFrom(el) {
    if (!el) return;
    var root = el.id === 'home-git-nokta' ? el : el.querySelector && el.querySelector('#home-git-nokta');
    if (root) mount(root);
  }

  document.body.addEventListener('htmx:afterSwap', function (e) {
    mountFrom(e.detail && e.detail.target);
  });
  document.body.addEventListener('htmx:afterSettle', function (e) {
    mountFrom(e.detail && e.detail.target);
  });

  if (document.readyState === 'loading') {
    document.addEventListener('DOMContentLoaded', function () {
      mountFrom(document);
    });
  } else {
    mountFrom(document);
  }

  global.PcHomeGitNokta = { mount: mount, mountFrom: mountFrom };
})(window);
