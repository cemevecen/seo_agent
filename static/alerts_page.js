/* alerts_page.js */

// CSS inject — alert-details gizle/göster
(function () {
  if (document.getElementById('alerts-component-style')) return;
  var s = document.createElement('style');
  s.id = 'alerts-component-style';
  s.textContent = '.alert-details{display:none!important;}.alert-details.show{display:block!important;}';
  document.head.appendChild(s);
})();

// ─── State ────────────────────────────────────────────────────────────────────
var _alertType = 'all';
var _alertPeriod = 7;
var _compCache = {};

// ─── Filter ───────────────────────────────────────────────────────────────────
function applyAlertsFilters() {
  var container = document.getElementById('alerts-container');
  var filterSelect = document.getElementById('site-filter');
  var view = document.getElementById('alerts-view');
  if (!container) return;

  var selectedDomain = filterSelect ? filterSelect.value : '';
  var cutoff = new Date();
  cutoff.setDate(cutoff.getDate() - _alertPeriod);

  var cards = container.querySelectorAll('.alert-card');
  var categoryCounts = {};
  var visible = 0;

  // Tip filtresi hariç say — boş chip'ler gizlensin
  cards.forEach(function (card) {
    var domain = card.getAttribute('data-domain') || '';
    var category = (card.getAttribute('data-alert-category') || 'other').trim();
    var isExternal = card.getAttribute('data-is-external') === 'true';
    var triggeredRaw = card.getAttribute('data-triggered-at') || '';
    var triggeredAt = triggeredRaw ? new Date(triggeredRaw) : null;
    var periodOk = !triggeredAt || isNaN(triggeredAt) || triggeredAt >= cutoff;

    var domainOk;
    if (selectedDomain === '__external__') {
      domainOk = isExternal;
    } else if (!selectedDomain) {
      domainOk = !isExternal;
    } else {
      domainOk = domain === selectedDomain;
    }
    if (domainOk && periodOk) {
      categoryCounts[category] = (categoryCounts[category] || 0) + 1;
    }
  });

  // Boş kategorilerin chip'lerini gizle
  if (view) {
    view.querySelectorAll('.alert-type-tab').forEach(function (btn) {
      var filter = btn.getAttribute('data-alert-filter') || 'all';
      if (filter === 'all') { btn.style.display = ''; return; }
      btn.style.display = categoryCounts[filter] ? '' : 'none';
    });
  }
  // Aktif tip boşsa → "Tümü"ye dön
  if (_alertType !== 'all' && !categoryCounts[_alertType]) {
    _alertType = 'all';
    setActiveTypeTab('all');
  }

  cards.forEach(function (card) {
    var domain = card.getAttribute('data-domain') || '';
    var category = (card.getAttribute('data-alert-category') || 'other').trim();
    var isExternal = card.getAttribute('data-is-external') === 'true';
    var triggeredRaw = card.getAttribute('data-triggered-at') || '';
    var triggeredAt = triggeredRaw ? new Date(triggeredRaw) : null;
    var periodOk = !triggeredAt || isNaN(triggeredAt) || triggeredAt >= cutoff;

    var domainOk;
    if (selectedDomain === '__external__') {
      domainOk = isExternal;
    } else if (!selectedDomain) {
      domainOk = !isExternal;
    } else {
      domainOk = domain === selectedDomain;
    }

    var typeOk = _alertType === 'all' || category === _alertType;
    var show = domainOk && typeOk && periodOk;
    card.style.display = show ? '' : 'none';
    if (show) visible++;
  });

  // CTR / Impression / Pozisyon sekmelerinde en olumsuzdan en olumluya sırala
  var scTabs = ['ctr', 'impression', 'position'];
  if (scTabs.indexOf(_alertType) !== -1) {
    var sortable = Array.from(cards).filter(function (c) { return c.style.display !== 'none'; });
    sortable.sort(function (a, b) {
      var sa = parseFloat(a.getAttribute('data-sort-score') || '0');
      var sb = parseFloat(b.getAttribute('data-sort-score') || '0');
      return sb - sa; // yüksek score = daha olumsuz → önce
    });
    sortable.forEach(function (c) { container.appendChild(c); });
  }

  var empty = container.querySelector('.alerts-empty-state');
  if (visible === 0) {
    if (!empty) {
      empty = document.createElement('div');
      empty.className = 'alerts-empty-state rounded-xl border border-dashed border-slate-300 dark:border-slate-600 px-4 py-8 text-center text-sm text-slate-500 dark:text-slate-400';
      container.appendChild(empty);
    }
    empty.textContent = 'Bu filtreye uygun alarm kaydı bulunamadı.';
    empty.style.display = '';
  } else if (empty) {
    empty.style.display = 'none';
  }
}

// ─── Tab helpers ──────────────────────────────────────────────────────────────
function setActiveTypeTab(activeFilter) {
  var view = document.getElementById('alerts-view');
  if (!view) return;
  view.querySelectorAll('.alert-type-tab').forEach(function (btn) {
    var f = btn.getAttribute('data-alert-filter');
    var isActive = f === activeFilter;
    var inactiveCls = (btn.getAttribute('data-inactive-cls') || '').split(' ').filter(Boolean);

    // Sadece renk + ring sınıflarını temizle (hover: ve diğerleri korunsun)
    var toRemove = Array.from(btn.classList).filter(function (c) {
      return (c.startsWith('bg-') || c.startsWith('text-') ||
              c.startsWith('dark:bg-') || c.startsWith('dark:text-') ||
              c === 'ring-2' || c === 'ring-offset-0' ||
              c.startsWith('ring-slate') || c.startsWith('dark:ring-slate'));
    });
    toRemove.forEach(function (c) { btn.classList.remove(c); });

    if (isActive) {
      btn.classList.add('bg-slate-900', 'text-white', 'ring-2', 'ring-offset-0', 'ring-slate-400', 'dark:ring-slate-500');
    } else {
      inactiveCls.forEach(function (c) { btn.classList.add(c); });
    }
  });
}

function setActivePeriodTab(period) {
  var view = document.getElementById('alerts-view');
  if (!view) return;
  view.querySelectorAll('.alert-period-tab').forEach(function (btn) {
    var p = parseInt(btn.getAttribute('data-period'), 10);
    // Tüm renk sınıflarını temizle
    btn.classList.remove('bg-slate-900', 'text-white', 'bg-slate-100', 'dark:bg-slate-800/70', 'text-slate-600', 'dark:text-slate-300', 'hover:bg-slate-200', 'dark:hover:bg-slate-700');
    if (p === period) {
      btn.classList.add('bg-slate-900', 'text-white');
    } else {
      btn.classList.add('bg-slate-100', 'dark:bg-slate-800/70', 'text-slate-600', 'dark:text-slate-300', 'hover:bg-slate-200', 'dark:hover:bg-slate-700');
    }
  });
}

// ─── Comparison button helpers ────────────────────────────────────────────────
function setComparisonBtnActive(card, activeType) {
  card.querySelectorAll('.comparison-btn').forEach(function (b) {
    var isActive = b.getAttribute('data-comparison') === activeType;
    b.classList.toggle('comp-active', isActive);
    b.classList.toggle('comp-inactive', !isActive);
  });
}

// ─── Detail toggle ────────────────────────────────────────────────────────────
function toggleAlertDetail(toggleBtn) {
  var card = toggleBtn.closest('.alert-card');
  if (!card) return;
  var details = card.querySelector('.alert-details');
  if (!details) return;
  var alertId = card.getAttribute('data-alert-id');
  if (details.classList.contains('show')) {
    details.classList.remove('show');
    toggleBtn.textContent = 'Detayına Bak';
  } else {
    details.classList.add('show');
    toggleBtn.textContent = 'Gizle';
    if (alertId) loadAlertDetails(String(alertId), card);
  }
}

// ─── API: comparison verisi yükle ────────────────────────────────────────────
async function loadAlertDetails(alertId, card) {
  try {
    var [dailyData, weeklyData] = await Promise.all([
      loadComparisonData(alertId, 'daily'),
      loadComparisonData(alertId, 'weekly'),
    ]);

    var weeklyUsable = !weeklyData || !weeklyData.comparison || weeklyData.comparison.has_meaningful_data !== false;
    var dailyUsable = !dailyData || !dailyData.comparison || dailyData.comparison.has_meaningful_data !== false;

    // Disabled state
    var weeklyBtn = card.querySelector('.comparison-btn[data-comparison="weekly"]');
    var dailyBtn = card.querySelector('.comparison-btn[data-comparison="daily"]');
    if (weeklyBtn) {
      weeklyBtn.classList.toggle('comp-disabled', !weeklyUsable);
    }
    if (dailyBtn) {
      dailyBtn.classList.toggle('comp-disabled', !dailyUsable);
    }

    var initialType = (_alertPeriod === 1)
      ? (dailyUsable ? 'daily' : 'weekly')
      : (weeklyUsable ? 'weekly' : 'daily');

    setComparisonBtnActive(card, initialType);
    renderComparisonData(alertId, initialType);
  } catch (err) {
    var div = document.getElementById('comparison-info-' + alertId);
    if (div) div.textContent = 'Hata: ' + err.message;
  }
}

async function loadComparisonData(alertId, comparisonType) {
  var resp = await fetch('/api/alert-details/' + alertId + '?comparison=' + comparisonType);
  if (!resp.ok) throw new Error('HTTP ' + resp.status);
  var data = await resp.json();
  if (!_compCache[alertId]) _compCache[alertId] = {};
  _compCache[alertId][comparisonType] = data;
  return data;
}

function renderComparisonData(alertId, comparisonType) {
  var cached = _compCache[alertId] && _compCache[alertId][comparisonType];
  var div = document.getElementById('comparison-info-' + alertId);
  if (!div || !cached) return;

  var comparison = cached.comparison || {};
  var typeLabel = comparisonType === 'daily' ? 'Dünle Karşılaştırma' : 'Geçen Hafta Aynı Gün ile Karşılaştırma';
  var isDark = document.documentElement.classList.contains('dark');

  var wrapBg = isDark ? 'rgba(15,23,42,0.55)' : 'rgba(240,249,255,0.72)';
  var wrapBorder = isDark ? 'rgba(63,63,70,0.7)' : 'rgba(186,230,253,0.95)';
  var toneStyle = isDark
    ? {
        blue:  'background:rgba(24,24,27,0.8);border-color:rgba(63,63,70,0.7);color:#e4e4e7;',
        slate: 'background:rgba(24,24,27,0.55);border-color:rgba(39,39,42,0.7);color:#d4d4d8;',
        red:   'background:rgba(76,5,25,0.38);border-color:rgba(136,19,55,0.6);color:#fda4af;',
        green: 'background:rgba(2,44,34,0.38);border-color:rgba(6,95,70,0.6);color:#6ee7b7;',
      }
    : {
        blue:  'background:#f0f9ff;border-color:#e0f2fe;color:#0f172a;',
        slate: 'background:#f8fafc;border-color:#f1f5f9;color:#0f172a;',
        red:   'background:#fff1f2;border-color:#ffe4e6;color:#be123c;',
        green: 'background:#ecfdf5;border-color:#d1fae5;color:#047857;',
      };

  var html = '<div class="rounded-2xl border p-3.5 sm:p-4" style="background:' + wrapBg + ';border-color:' + wrapBorder + ';">';
  html += '<p class="mb-2.5 text-[10px] font-semibold uppercase tracking-[0.16em] text-slate-500 dark:text-slate-400">' + typeLabel + '</p>';

  if (comparison.message) {
    html += '<p class="mb-3 text-sm font-semibold text-slate-900 dark:text-slate-100 sm:text-base">' + comparison.message + '</p>';
  }

  if (Array.isArray(comparison.cards) && comparison.cards.length > 0) {
    html += '<div class="grid grid-cols-2 gap-2 sm:grid-cols-3">';
    comparison.cards.forEach(function (c) {
      var ts = toneStyle[c.tone] || toneStyle.slate;
      html += '<div class="rounded-xl border px-3 py-2.5" style="' + ts + '">';
      html += '<p class="text-[10px] font-semibold uppercase tracking-[0.13em] opacity-75">' + (c.label || '') + '</p>';
      html += '<p class="mt-1 text-base font-bold leading-none sm:text-lg">' + (c.value || 'N/A') + '</p>';
      if (c.detail) html += '<p class="mt-1 text-[11px] leading-4 opacity-80">' + c.detail + '</p>';
      html += '</div>';
    });
    html += '</div>';
  } else if (!comparison.message) {
    html += '<p class="text-sm text-slate-500 dark:text-slate-400">Karşılaştırma verisi bulunamadı.</p>';
  }

  html += '</div>';
  div.innerHTML = html;
}

// ─── Refresh button ───────────────────────────────────────────────────────────
var _refreshBound = false;
function bindRefreshButton() {
  if (_refreshBound) return;
  _refreshBound = true;
  document.body.addEventListener('click', async function (e) {
    var btn = e.target.closest('#refresh-alerts-button');
    if (!btn) return;
    var view = document.getElementById('alerts-view');
    if (!view || !view.contains(btn)) return;

    var panel = document.getElementById('alerts-progress-panel');
    var titleEl = document.getElementById('alerts-progress-title');
    var detailEl = document.getElementById('alerts-progress-detail');
    var percentEl = document.getElementById('alerts-progress-percent');
    var barEl = document.getElementById('alerts-progress-bar');

    var origText = btn.innerHTML;
    btn.disabled = true;
    btn.textContent = 'Yenileniyor…';

    if (panel) panel.classList.remove('hidden');

    function _reloadAlerts() {
      window.location.href = '/alerts?t=' + Date.now();
    }

    function _setProgress(p, t, d) {
      if (percentEl) percentEl.textContent = p + '%';
      if (barEl) barEl.style.width = p + '%';
      if (titleEl) titleEl.textContent = t;
      if (detailEl) detailEl.textContent = d || '';
    }

    _setProgress(5, 'Yenileme başlatılıyor…', 'Sunucuya istek gönderiliyor.');

    try {
      var resp = await fetch('/alerts/refresh', { method: 'POST', headers: { Accept: 'application/json' } });
      if (!resp.ok) { _reloadAlerts(); return; }
    } catch (err) {
      _reloadAlerts();
      return;
    }

    // Countdown — arka plan taraması tamamlanana kadar bekle
    var TOTAL_SECS = 45;
    var elapsed = 0;
    var steps = [
      { at: 0,  pct: 10, title: 'Site listesi hazırlanıyor',             detail: 'Alert yenileme arka planda başladı.' },
      { at: 5,  pct: 25, title: 'Search Console verileri alınıyor',       detail: 'Sorgu bazlı karşılaştırma yapılıyor.' },
      { at: 12, pct: 45, title: 'CTR ve pozisyon hesaplanıyor',           detail: 'Değişim eşikleri kontrol ediliyor.' },
      { at: 22, pct: 65, title: 'Uyarılar oluşturuluyor',                 detail: 'Kayıtlar veritabanına yazılıyor.' },
      { at: 32, pct: 80, title: 'Diğer siteler işleniyor…',               detail: 'Lütfen bekleyin.' },
      { at: 40, pct: 90, title: 'Tamamlanmak üzere…',                     detail: 'Sonuçlar hazırlanıyor.' },
    ];
    var countdownTimer = setInterval(function () {
      elapsed++;
      var step = null;
      for (var i = steps.length - 1; i >= 0; i--) {
        if (elapsed >= steps[i].at) { step = steps[i]; break; }
      }
      if (step) _setProgress(step.pct, step.title, step.detail);
      if (elapsed >= TOTAL_SECS) {
        clearInterval(countdownTimer);
        _setProgress(100, 'Tamamlandı ✓', 'Sayfa yenileniyor…');
        setTimeout(_reloadAlerts, 600);
      }
    }, 1000);
  });
}

// ─── Main event delegation ────────────────────────────────────────────────────
var _mainBound = false;
function bindMainDelegation() {
  if (_mainBound) return;
  _mainBound = true;

  document.body.addEventListener('click', function (e) {
    var view = document.getElementById('alerts-view');
    if (!view) return;

    // Detay toggle
    var toggleBtn = e.target.closest('.toggle-details');
    if (toggleBtn && view.contains(toggleBtn)) {
      e.preventDefault();
      toggleAlertDetail(toggleBtn);
      return;
    }

    // Comparison type switch
    var compBtn = e.target.closest('.comparison-btn');
    if (compBtn && view.contains(compBtn)) {
      e.preventDefault();
      if (compBtn.classList.contains('comp-disabled')) return;
      var card = compBtn.closest('.alert-card');
      var alertId = card && card.getAttribute('data-alert-id');
      var compType = compBtn.getAttribute('data-comparison');
      if (!alertId || !compType) return;
      setComparisonBtnActive(card, compType);
      if (_compCache[alertId] && _compCache[alertId][compType]) {
        renderComparisonData(alertId, compType);
      } else {
        var infoDiv = document.getElementById('comparison-info-' + alertId);
        if (infoDiv) infoDiv.textContent = 'Yükleniyor…';
        loadComparisonData(alertId, compType).then(function () {
          renderComparisonData(alertId, compType);
        }).catch(function (err) {
          if (infoDiv) infoDiv.textContent = 'Hata: ' + err.message;
        });
      }
      return;
    }

    // Type chip
    var typeTab = e.target.closest('.alert-type-tab');
    if (typeTab && view.contains(typeTab)) {
      e.preventDefault();
      _alertType = typeTab.getAttribute('data-alert-filter') || 'all';
      setActiveTypeTab(_alertType);
      applyAlertsFilters();
      return;
    }

    // Period chip
    var periodTab = e.target.closest('.alert-period-tab');
    if (periodTab && view.contains(periodTab)) {
      e.preventDefault();
      _alertPeriod = parseInt(periodTab.getAttribute('data-period'), 10) || 7;
      setActivePeriodTab(_alertPeriod);
      applyAlertsFilters();
      return;
    }
  });

  // Site dropdown
  document.body.addEventListener('change', function (e) {
    if (e.target.id !== 'site-filter') return;
    var view = document.getElementById('alerts-view');
    if (!view || !view.contains(e.target)) return;
    applyAlertsFilters();
  });
}

// ─── Init ─────────────────────────────────────────────────────────────────────
function initAlertsPage() {
  if (!document.getElementById('alerts-view')) return;
  bindMainDelegation();
  bindRefreshButton();
  setActiveTypeTab(_alertType);
  setActivePeriodTab(_alertPeriod);
  applyAlertsFilters();

  // URL'den seçili alert'i aç
  var root = document.getElementById('alerts-view');
  var selectedId = root && root.getAttribute('data-selected-alert-id');
  if (selectedId && selectedId.trim()) {
    setTimeout(function () {
      var card = document.querySelector('.alert-card[data-alert-id="' + selectedId.trim() + '"]');
      if (!card) return;
      _alertType = 'all';
      setActiveTypeTab('all');
      var filterSelect = document.getElementById('site-filter');
      if (filterSelect) filterSelect.value = '';
      applyAlertsFilters();
      card.style.display = '';
      var details = card.querySelector('.alert-details');
      if (details && !details.classList.contains('show')) {
        var btn = card.querySelector('.toggle-details');
        if (btn) toggleAlertDetail(btn);
      }
      card.scrollIntoView({ behavior: 'smooth', block: 'center' });
    }, 150);
  }
}

window.seoInitAlertsPage = initAlertsPage;

if (document.readyState === 'loading') {
  document.addEventListener('DOMContentLoaded', initAlertsPage);
} else {
  initAlertsPage();
}

document.addEventListener('htmx:afterSwap', function () {
  if (document.getElementById('alerts-view')) {
    setTimeout(initAlertsPage, 0);
  }
});
