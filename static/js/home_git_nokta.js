/**
 * Ana sayfa git.nokta — boards sekmesi ile aynı: tarayıcı → git.nokta.com (VPN).
 */
(function (global) {
  function escapeHtml(s) {
    return String(s || '')
      .replace(/&/g, '&amp;')
      .replace(/</g, '&lt;')
      .replace(/>/g, '&gt;')
      .replace(/"/g, '&quot;');
  }

  function labelNames(issue) {
    var raw = issue.labels || [];
    var out = [];
    for (var i = 0; i < raw.length; i++) {
      var lab = raw[i];
      if (typeof lab === 'string') out.push(lab);
      else if (lab && lab.name) out.push(String(lab.name));
    }
    return out;
  }

  function hasLabel(issue, name) {
    var target = String(name || '').toLowerCase();
    return labelNames(issue).some(function (n) {
      return String(n).toLowerCase() === target;
    });
  }

  function classify(issue) {
    if (String(issue.state || '').toLowerCase() === 'closed') return 'closed';
    if (hasLabel(issue, 'Doing')) return 'doing';
    if (hasLabel(issue, 'Testing')) return 'testing';
    return 'open';
  }

  function parseTs(issue, preferClosed) {
    var raw = preferClosed
      ? issue.closed_at || issue.updated_at || issue.created_at
      : issue.updated_at || issue.created_at;
    var t = Date.parse(raw || '');
    return isNaN(t) ? 0 : t;
  }

  function fmtDate(issue, preferClosed) {
    var raw = preferClosed
      ? issue.closed_at || issue.updated_at || issue.created_at
      : issue.updated_at || issue.created_at;
    var d = new Date(raw || '');
    if (isNaN(d.getTime())) return '';
    var dd = String(d.getDate()).padStart(2, '0');
    var mm = String(d.getMonth() + 1).padStart(2, '0');
    return dd + '.' + mm + '.' + d.getFullYear();
  }

  function topEntries(issues, status, limit) {
    var preferClosed = status === 'closed';
    return issues
      .filter(function (i) {
        return classify(i) === status;
      })
      .sort(function (a, b) {
        return parseTs(b, preferClosed) - parseTs(a, preferClosed);
      })
      .slice(0, limit);
  }

  function renderList(root, productId, colId, entries) {
    var list = root.querySelector('[data-gn-list="' + productId + '-' + colId + '"]');
    var count = root.querySelector('[data-gn-count="' + productId + '-' + colId + '"]');
    if (count) count.textContent = String(entries.length);
    if (!list) return;
    if (!entries.length) {
      list.innerHTML =
        '<li class="flex flex-1 items-center justify-center rounded-lg border border-dashed border-slate-200 px-2 py-6 text-center text-[11px] text-slate-400 dark:border-slate-700 dark:text-slate-500">Madde yok</li>';
      return;
    }
    list.innerHTML = entries
      .map(function (item) {
        var title = escapeHtml(item.title || 'Issue #' + item.iid);
        var titleHtml = item.web_url
          ? '<a href="' +
            escapeHtml(item.web_url) +
            '" target="_blank" rel="noopener noreferrer" class="min-w-0 text-xs font-semibold leading-snug text-slate-900 underline-offset-2 hover:underline dark:text-slate-100">' +
            title +
            '</a>'
          : '<p class="min-w-0 text-xs font-semibold leading-snug text-slate-900 dark:text-slate-100">' +
            title +
            '</p>';
        var dateLabel = fmtDate(item, colId === 'closed');
        return (
          '<li class="rounded-lg bg-white p-2.5 shadow-sm ring-1 ring-slate-200/90 dark:bg-slate-900 dark:ring-slate-700">' +
          '<div class="flex items-start justify-between gap-2">' +
          titleHtml +
          '<span class="shrink-0 rounded-md bg-slate-100 px-1.5 py-0.5 text-[9px] font-bold uppercase tracking-wide text-slate-600 dark:bg-slate-800 dark:text-slate-300">' +
          escapeHtml(item._source_label || 'GitLab') +
          '</span></div>' +
          '<div class="mt-1.5 flex flex-wrap items-center gap-2 text-[11px] text-slate-500 dark:text-slate-400">' +
          (item.iid ? '<span class="font-mono tabular-nums">#' + escapeHtml(item.iid) + '</span>' : '') +
          (dateLabel ? '<span class="tabular-nums">' + escapeHtml(dateLabel) + '</span>' : '') +
          '</div></li>'
        );
      })
      .join('');
  }

  async function fetchIssues(baseUrl, token, path, state) {
    var enc = encodeURIComponent(path);
    var oneYearAgo = new Date();
    oneYearAgo.setFullYear(oneYearAgo.getFullYear() - 1);
    var url =
      baseUrl +
      '/projects/' +
      enc +
      '/issues?state=' +
      state +
      '&updated_after=' +
      encodeURIComponent(oneYearAgo.toISOString()) +
      '&order_by=updated_at&sort=desc&per_page=100&page=1';
    var res = await fetch(url, { headers: { 'PRIVATE-TOKEN': token }, cache: 'no-store' });
    if (!res.ok) throw new Error(path + ' HTTP ' + res.status);
    var data = await res.json();
    return Array.isArray(data) ? data : [];
  }

  async function mount(root) {
    if (!root || root.getAttribute('data-gn-wired') === '1') return;
    root.setAttribute('data-gn-wired', '1');

    var token = root.getAttribute('data-token') || '';
    var baseUrl = root.getAttribute('data-base-url') || 'https://git.nokta.com/api/v4';
    var limit = parseInt(root.getAttribute('data-limit') || '3', 10) || 3;
    var projects = [];
    try {
      projects = JSON.parse(root.getAttribute('data-projects') || '[]');
    } catch (e) {
      projects = [];
    }

    var loadingEl = root.querySelector('[data-gn-loading]');
    var errorEl = root.querySelector('[data-gn-error]');
    var errorMsg = root.querySelector('[data-gn-error-msg]');
    var bodyEl = root.querySelector('[data-gn-body]');

    function showError(msg) {
      if (loadingEl) loadingEl.classList.add('hidden');
      if (errorEl) errorEl.classList.remove('hidden');
      if (errorMsg) errorMsg.textContent = msg || 'GitLab maddeleri alınamadı.';
      if (bodyEl) {
        bodyEl.classList.remove('opacity-60', 'pointer-events-none');
        bodyEl.removeAttribute('aria-busy');
      }
    }

    function showReady() {
      if (loadingEl) loadingEl.classList.add('hidden');
      if (bodyEl) {
        bodyEl.classList.remove('opacity-60', 'pointer-events-none');
        bodyEl.removeAttribute('aria-busy');
      }
    }

    function emptyAll() {
      ['doviz', 'sinemalar'].forEach(function (pid) {
        ['open', 'doing', 'testing', 'closed'].forEach(function (cid) {
          renderList(root, pid, cid, []);
        });
      });
    }

    if (!token) {
      showError('GITLAB_PRIVATE_TOKEN tanımlı değil — boards sekmesi ile aynı token gerekli.');
      emptyAll();
      return;
    }

    try {
      var tasks = [];
      projects.forEach(function (p) {
        tasks.push(
          fetchIssues(baseUrl, token, p.path, 'opened').then(function (rows) {
            return rows.map(function (r) {
              r._product = p.product;
              r._source_label = p.source_label;
              return r;
            });
          })
        );
        tasks.push(
          fetchIssues(baseUrl, token, p.path, 'closed').then(function (rows) {
            return rows.map(function (r) {
              r._product = p.product;
              r._source_label = p.source_label;
              return r;
            });
          })
        );
      });
      var chunks = await Promise.all(tasks);
      var byProduct = { doviz: [], sinemalar: [] };
      chunks.forEach(function (rows) {
        rows.forEach(function (issue) {
          if (byProduct[issue._product]) byProduct[issue._product].push(issue);
        });
      });
      ['doviz', 'sinemalar'].forEach(function (pid) {
        ['open', 'doing', 'testing', 'closed'].forEach(function (cid) {
          renderList(root, pid, cid, topEntries(byProduct[pid] || [], cid, limit));
        });
      });
      showReady();
    } catch (err) {
      console.warn('home git.nokta load failed', err);
      showError((err && err.message) || 'GitLab isteği başarısız (VPN?).');
      emptyAll();
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
