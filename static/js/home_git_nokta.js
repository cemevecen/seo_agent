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

  function orderKey(productId, platformId, boardList) {
    return [productId, platformId, boardList].join(':');
  }

  function sortBySavedOrder(entries, savedOrder) {
    if (!Array.isArray(savedOrder) || !savedOrder.length) return entries.slice();
    var rank = {};
    savedOrder.forEach(function (iid, idx) {
      rank[String(iid)] = idx;
    });
    return entries.slice().sort(function (a, b) {
      var ra = rank[String(a.issue_iid)];
      var rb = rank[String(b.issue_iid)];
      if (ra != null && rb != null) return ra - rb;
      if (ra != null) return -1;
      if (rb != null) return 1;
      var ta = Date.parse(a.starred_at || '') || 0;
      var tb = Date.parse(b.starred_at || '') || 0;
      return tb - ta;
    });
  }

  function markScrollable(list) {
    if (!list) return;
    var scrollable = list.scrollHeight > list.clientHeight + 2;
    list.setAttribute('data-gn-scrollable', scrollable ? '1' : '0');
  }

  function renderList(root, productId, colId, entries) {
    var list = root.querySelector('[data-gn-list="' + productId + '-' + colId + '"]');
    var count = root.querySelector('[data-gn-count="' + productId + '-' + colId + '"]');
    if (count) count.textContent = String(entries.length);
    if (!list) return;
    if (!entries.length) {
      list.innerHTML =
        '<li class="flex flex-1 items-center justify-center rounded-lg border border-dashed border-slate-200 px-2 py-6 dark:border-slate-700" aria-hidden="true"></li>';
      list.setAttribute('data-gn-scrollable', '0');
      return;
    }
    list.innerHTML = entries
      .map(function (item) {
        var title = escapeHtml(item.title || 'Issue #' + item.issue_iid);
        var titleHtml = item.web_url
          ? '<a href="' +
            escapeHtml(item.web_url) +
            '" target="_blank" rel="noopener noreferrer" class="gn-issue-title min-w-0 text-slate-900 underline-offset-2 hover:underline dark:text-slate-100">' +
            title +
            '</a>'
          : '<p class="gn-issue-title min-w-0 text-slate-900 dark:text-slate-100">' +
            title +
            '</p>';
        return (
          '<li class="shrink-0 rounded-lg bg-white p-2.5 shadow-sm ring-1 ring-slate-200/90 dark:bg-slate-900 dark:ring-slate-700 cursor-grab" data-gn-iid="' + escapeHtml(item.issue_iid) + '">' +
          '<div class="flex items-start gap-2">' +
          titleHtml +
          '</div>' +
          '<div class="gn-issue-meta mt-1.5 flex flex-wrap items-center gap-2 text-slate-500 dark:text-slate-400">' +
          (item.issue_iid ? '<span class="font-mono tabular-nums">#' + escapeHtml(item.issue_iid) + '</span>' : '') +
          (item.source_label
            ? '<span class="rounded bg-slate-100 px-1.5 py-0.5 home-label dark:bg-slate-800">' +
              escapeHtml(item.source_label) +
              '</span>'
            : '') +
          '</div></li>'
        );
      })
      .join('');
    requestAnimationFrame(function () {
      markScrollable(list);
    });
  }

  function iidsFromListEl(listEl) {
    return Array.from(listEl.querySelectorAll('[data-gn-iid]'))
      .map(function (el) { return parseInt(el.getAttribute('data-gn-iid') || '', 10); })
      .filter(function (n) { return !isNaN(n); });
  }

  async function saveOrder(root, productId, platformId, boardList, issueIids) {
    var res = await fetch('/api/boards/stars/order', {
      method: 'PUT',
      credentials: 'same-origin',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({
        product: productId,
        platform: platformId,
        board_list: boardList,
        issue_iids: issueIids,
      }),
    });
    if (!res.ok) throw new Error('Sıralama kaydedilemedi');
    root._gnOrders = root._gnOrders || {};
    root._gnOrders[orderKey(productId, platformId, boardList)] = issueIids.slice();
  }

  function initSortables(root) {
    if (typeof Sortable === 'undefined') return;
    root.querySelectorAll('[data-gn-list]').forEach(function (listEl) {
      if (listEl._gnSortable) return;
      listEl._gnSortable = Sortable.create(listEl, {
        animation: 150,
        draggable: '[data-gn-iid]',
        ghostClass: 'sortable-ghost',
        dragClass: 'sortable-drag',
        onEnd: function () {
          var productId = (listEl.getAttribute('data-gn-list') || '').split('-')[0];
          var boardList = listEl.getAttribute('data-gn-board-list') || '';
          var section = listEl.closest('[data-gn-product]');
          var platformId = section ? (section.getAttribute('data-gn-active-chip') || 'web') : 'web';
          var issueIids = iidsFromListEl(listEl);
          if (!productId || !boardList || !issueIids.length) return;
          saveOrder(root, productId, platformId, boardList, issueIids).catch(function (err) {
            console.warn('home git.nokta order save failed', err);
          });
        },
      });
    });
  }

  function renderAll(root) {
    ['doviz', 'sinemalar'].forEach(function (pid) {
      renderProduct(root, pid, root._gnItems || []);
    });
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
      entries = sortBySavedOrder(entries, (root._gnOrders || {})[orderKey(productId, chip, colId)]);
      renderList(root, productId, colId, entries);
    });
    initSortables(root);
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
        'gn-chip inline-flex items-center rounded-full px-2.5 py-1 home-meta font-bold ring-1 transition ' +
        (active ? chipActiveClass(productId) : chipIdleClass());
    });
    renderProduct(root, productId, root._gnItems || []);
  }

  function setSyncStatus(root, msg, tone) {
    var el = root.querySelector('[data-gn-sync-status]');
    if (!el) return;
    if (!msg) {
      el.classList.add('hidden');
      el.textContent = '';
      return;
    }
    el.classList.remove('hidden');
    el.textContent = msg;
    el.className =
      'home-meta mt-1 ' +
      (tone === 'error'
        ? 'text-rose-600 dark:text-rose-400'
        : tone === 'ok'
          ? 'text-emerald-700 dark:text-emerald-400'
          : 'text-slate-500 dark:text-slate-400');
  }

  function setRefreshBusy(root, busy) {
    var btn = root.querySelector('[data-gn-refresh]');
    var icon = root.querySelector('[data-gn-refresh-icon]');
    var label = root.querySelector('[data-gn-refresh-label]');
    if (btn) btn.disabled = !!busy;
    if (icon) {
      if (busy) icon.classList.add('animate-spin');
      else icon.classList.remove('animate-spin');
    }
    if (label) label.textContent = busy ? 'Yenileniyor…' : 'Yenile';
  }

  function applyStarsPayload(root, data) {
    if (!data) return;
    root._gnItems = data.items || [];
    root._gnOrders = data.orders || {};
    renderAll(root);
  }

  async function fetchStars(url, timeoutMs) {
    var res = await fetch(url, {
      credentials: 'same-origin',
      cache: 'no-store',
      signal: AbortSignal.timeout ? AbortSignal.timeout(timeoutMs) : undefined,
    });
    if (!res.ok) throw new Error('HTTP ' + res.status);
    return res.json();
  }

  async function forceRefreshFromGitlab(root) {
    if (root._gnRefreshing) return;
    root._gnRefreshing = true;
    setRefreshBusy(root, true);
    setSyncStatus(root, 'GitLab ile senkronize ediliyor…', 'info');
    var bodyEl = root.querySelector('[data-gn-body]');
    if (bodyEl) {
      bodyEl.classList.add('opacity-60');
      bodyEl.setAttribute('aria-busy', 'true');
    }
    try {
      // manual=1 → sunucu daha uzun timeout ile Closed dahil tüm yıldızları çeker
      var data = await fetchStars('/api/boards/stars?refresh=1&manual=1', 60000);
      applyStarsPayload(root, data);
      var meta = data.refresh || {};
      var updated = typeof meta.updated === 'number' ? meta.updated : null;
      var errs = Array.isArray(meta.errors) ? meta.errors : [];
      if (meta.ok === false && meta.error) {
        setSyncStatus(root, 'Kısmi: ' + meta.error, 'error');
      } else if (errs.length) {
        setSyncStatus(
          root,
          'Güncellendi (' + (updated != null ? updated + ' değişiklik' : 'ok') + '), bazı issue’lar eksik.',
          'error'
        );
      } else {
        var now = new Date();
        var hh = String(now.getHours()).padStart(2, '0');
        var mm = String(now.getMinutes()).padStart(2, '0');
        setSyncStatus(
          root,
          'GitLab ile güncellendi · ' +
            hh +
            ':' +
            mm +
            (updated != null ? ' · ' + updated + ' değişiklik' : ''),
          'ok'
        );
      }
      try {
        document.body.dispatchEvent(
          new CustomEvent('pc:git-nokta-stars-refreshed', { detail: { items: root._gnItems } })
        );
      } catch (_) { /* ignore */ }
    } catch (err) {
      console.warn('home git.nokta manual refresh failed', err);
      setSyncStatus(root, 'Yenileme başarısız: ' + ((err && err.message) || 'bağlantı'), 'error');
    } finally {
      root._gnRefreshing = false;
      setRefreshBusy(root, false);
      if (bodyEl) {
        bodyEl.classList.remove('opacity-60');
        bodyEl.removeAttribute('aria-busy');
      }
    }
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
      var refreshBtn = ev.target && ev.target.closest ? ev.target.closest('[data-gn-refresh]') : null;
      if (refreshBtn && root.contains(refreshBtn)) {
        ev.preventDefault();
        forceRefreshFromGitlab(root);
        return;
      }
      var btn = ev.target && ev.target.closest ? ev.target.closest('[data-gn-chip]') : null;
      if (!btn || !root.contains(btn)) return;
      var productId = btn.getAttribute('data-gn-product-id');
      var chipId = btn.getAttribute('data-gn-chip-id');
      if (productId && chipId) setActiveChip(root, productId, chipId);
    });

    try {
      // Önce DB snapshot (anında); GitLab sync ayrı ve timeout'lu
      var data = await fetchStars('/api/boards/stars?refresh=0', 12000);
      applyStarsPayload(root, data);
      showReady();

      // Arka planda soft sync — UI'yi bloklamaz
      fetchStars('/api/boards/stars?refresh=1', 25000)
        .then(function (data2) {
          if (!data2 || !root.isConnected) return;
          applyStarsPayload(root, data2);
        })
        .catch(function (softErr) {
          console.warn('home git.nokta soft refresh skipped', softErr);
        });
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

  global.PcHomeGitNokta = {
    mount: mount,
    mountFrom: mountFrom,
    refresh: function (el) {
      var root = el && el.id === 'home-git-nokta' ? el : (el && el.querySelector && el.querySelector('#home-git-nokta')) || document.getElementById('home-git-nokta');
      if (root) forceRefreshFromGitlab(root);
    },
  };
})(window);
