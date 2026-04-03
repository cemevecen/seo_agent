/* alerts_page.js — basit, tek-listener implementation */

// Inject CSS once
(function () {
  if (document.getElementById('alerts-component-style')) return;
  var s = document.createElement('style');
  s.id = 'alerts-component-style';
  s.textContent = [
    '.alert-details { display: none !important; }',
    '.alert-details.show { display: block !important; }',
  ].join('\n');
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
  if (!container) return;

  var selectedDomain = filterSelect ? filterSelect.value : '';
  var cutoff = new Date();
  cutoff.setDate(cutoff.getDate() - _alertPeriod);

  var cards = container.querySelectorAll('.alert-card');
  var visible = 0;

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
      domainOk = !isExternal; // "Tüm Siteler" → external'ları gizle
    } else {
      domainOk = domain === selectedDomain;
    }

    var typeOk = _alertType === 'all' || category === _alertType;
    var show = domainOk && typeOk && periodOk;
    card.style.display = show ? '' : 'none';
    if (show) visible++;
  });

  var empty = container.querySelector('.alerts-empty-state');
  if (visible === 0) {
    if (!empty) {
      empty = document.createElement('div');
      empty.className = 'alerts-empty-state rounded-2xl border border-dashed border-slate-300 dark:border-slate-600 p-8 text-center text-slate-500 dark:text-slate-400';
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

    // Tüm mevcut renk/ring sınıflarını temizle
    var toRemove = Array.from(btn.classList).filter(function (c) {
      return c.startsWith('bg-') || c.startsWith('text-') || c.startsWith('dark:bg-') ||
             c.startsWith('dark:text-') || c === 'ring-2' || c === 'ring-offset-0' ||
             c.startsWith('ring-slate') || c.startsWith('dark:ring-slate') || c === 'hover:bg-slate-200';
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
    btn.classList.remove('bg-slate-900', 'text-white', 'bg-slate-100', 'dark:bg-slate-800/70', 'text-slate-600', 'dark:text-slate-300');
    if (p === period) {
      btn.classList.add('bg-slate-900', 'text-white');
    } else {
      btn.classList.add('bg-slate-100', 'dark:bg-slate-800/70', 'text-slate-600', 'dark:text-slate-300');
    }
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
    toggleBtn.textContent = 'Detay';
  } else {
    details.classList.add('show');
    toggleBtn.textContent = 'Gizle';
    if (alertId) loadAlertDetails(String(alertId), card);
  }
}

// ─── API: load comparison data ────────────────────────────────────────────────
async function loadAlertDetails(alertId, card) {
  try {
    var [dailyData, weeklyData] = await Promise.all([
      loadComparisonData(alertId, 'daily'),
      loadComparisonData(alertId, 'weekly'),
    ]);

    var weeklyUsable = !weeklyData || !weeklyData.comparison || weeklyData.comparison.has_meaningful_data !== false;
    var dailyUsable = !dailyData || !dailyData.comparison || dailyData.comparison.has_meaningful_data !== false;

    var weeklyBtn = card.querySelector('.comparison-btn[data-comparison="weekly"]');
    var dailyBtn = card.querySelector('.comparison-btn[data-comparison="daily"]');
    if (weeklyBtn) {
      weeklyBtn.dataset.disabled = weeklyUsable ? 'false' : 'true';
      weeklyBtn.classList.toggle('opacity-50', !weeklyUsable);
      weeklyBtn.classList.toggle('cursor-not-allowed', !weeklyUsable);
    }
    if (dailyBtn) {
      dailyBtn.dataset.disabled = dailyUsable ? 'false' : 'true';
      dailyBtn.classList.toggle('opacity-50', !dailyUsable);
      dailyBtn.classList.toggle('cursor-not-allowed', !dailyUsable);
    }

    var initialType = (_alertPeriod === 1)
      ? (dailyUsable ? 'daily' : 'weekly')
      : (weeklyUsable ? 'weekly' : 'daily');

    card.querySelectorAll('.comparison-btn').forEach(function (b) {
      var isActive = b.getAttribute('data-comparison') === initialType;
      b.classList.toggle('bg-blue-100', isActive);
      b.classList.toggle('text-blue-700', isActive);
      b.classList.toggle('hover:bg-blue-200', isActive);
      b.classList.toggle('dark:bg-sky-950/50', isActive);
      b.classList.toggle('dark:text-sky-300', isActive);
      b.classList.toggle('dark:hover:bg-sky-900/60', isActive);
      b.classList.toggle('bg-slate-100', !isActive);
      b.classList.toggle('dark:bg-slate-800/70', !isActive);
      b.classList.toggle('text-slate-700', !isActive);
      b.classList.toggle('dark:text-slate-200', !isActive);
    });

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
  var toneMap = {
    blue: 'bg-sky-50 dark:bg-slate-900/60 border-sky-100 dark:border-slate-600 text-slate-900 dark:text-slate-100',
    slate: 'bg-slate-50 dark:bg-slate-900/50 border-slate-100 dark:border-slate-800 text-slate-900 dark:text-slate-100',
    red: 'bg-rose-50 dark:bg-rose-950/40 border-rose-100 dark:border-rose-900/50 text-rose-700 dark:text-rose-300',
    green: 'bg-emerald-50 dark:bg-emerald-950/35 border-emerald-100 dark:border-emerald-900/50 text-emerald-700 dark:text-emerald-300',
  };

  var html = '<div class="rounded-2xl border border-sky-100 dark:border-slate-600 bg-sky-50/70 dark:bg-slate-900/70 p-4 sm:p-5">';
  html += '<p class="text-xs font-semibold tracking-[0.16em] text-slate-500 dark:text-slate-400 uppercase mb-3">' + typeLabel + '</p>';

  if (comparison.message) {
    html += '<p class="text-base sm:text-lg font-semibold text-slate-900 dark:text-slate-100 mb-4">' + comparison.message + '</p>';
  }

  if (Array.isArray(comparison.cards) && comparison.cards.length > 0) {
    html += '<div class="grid gap-3 md:grid-cols-3">';
    comparison.cards.forEach(function (c) {
      var tc = toneMap[c.tone] || toneMap.slate;
      html += '<div class="rounded-2xl border p-4 ' + tc + '">';
      html += '<p class="text-[11px] font-semibold uppercase tracking-[0.18em] text-slate-500 dark:text-slate-400">' + (c.label || '') + '</p>';
      html += '<p class="mt-2 text-2xl font-bold leading-none">' + (c.value || 'N/A') + '</p>';
      if (c.detail) html += '<p class="mt-2 text-sm leading-5 text-slate-600 dark:text-slate-300">' + c.detail + '</p>';
      html += '</div>';
    });
    html += '</div>';
  } else if (!comparison.message) {
    html += '<p class="text-slate-600 dark:text-slate-300">Karşılaştırma verisi bulunamadı</p>';
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

    var origText = btn.textContent;
    btn.disabled = true;
    btn.textContent = 'Yenileniyor…';

    if (panel) panel.classList.remove('hidden');

    var steps = [
      { p: 20, t: 'Site listesi hazırlanıyor', d: 'Alert yenileme akışı başlatılıyor.' },
      { p: 45, t: 'Search Console verileri karşılaştırılıyor', d: 'CTR, pozisyon ve impression değişimleri hesaplanıyor.' },
      { p: 75, t: 'Uyarılar hesaplanıyor', d: 'Kayıtlar veritabanına yazılıyor.' },
      { p: 90, t: 'Liste yenileniyor', d: 'Yeni sonuç ile liste tekrar çizilecek.' },
    ];

    var si = 0;
    function tick() {
      if (si >= steps.length) return;
      var s = steps[si++];
      if (titleEl) titleEl.textContent = s.t;
      if (detailEl) detailEl.textContent = s.d;
      if (percentEl) percentEl.textContent = s.p + '%';
      if (barEl) barEl.style.width = s.p + '%';
    }
    tick();
    var timer = setInterval(tick, 1200);

    try {
      var resp = await fetch('/api/alerts/refresh', { method: 'POST' });
      clearInterval(timer);
      if (resp.ok) {
        if (percentEl) percentEl.textContent = '100%';
        if (barEl) barEl.style.width = '100%';
        if (titleEl) titleEl.textContent = 'Tamamlandı';
        var html = await resp.text();
        var container = document.getElementById('alerts-container');
        if (container && html.trim()) {
          // HTMX ile değil, doğrudan inner container'ı güncelle
          var tmpDiv = document.createElement('div');
          tmpDiv.innerHTML = html;
          var newContainer = tmpDiv.querySelector('#alerts-container');
          if (newContainer) container.innerHTML = newContainer.innerHTML;
        }
        applyAlertsFilters();
      } else {
        if (titleEl) titleEl.textContent = 'Hata oluştu';
      }
    } catch (err) {
      clearInterval(timer);
      if (titleEl) titleEl.textContent = 'Hata: ' + err.message;
    }

    setTimeout(function () {
      btn.disabled = false;
      btn.textContent = origText;
      if (panel) panel.classList.add('hidden');
    }, 1500);
  });
}

// ─── Main event delegation ────────────────────────────────────────────────────
var _mainBound = false;
function bindMainDelegation() {
  if (_mainBound) return;
  _mainBound = true;

  // Click delegation
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
      if (compBtn.dataset.disabled === 'true') return;
      var card = compBtn.closest('.alert-card');
      var alertId = card && card.getAttribute('data-alert-id');
      var compType = compBtn.getAttribute('data-comparison');
      if (!alertId || !compType) return;
      // Active stil
      card.querySelectorAll('.comparison-btn').forEach(function (b) {
        var isActive = b.getAttribute('data-comparison') === compType;
        b.classList.toggle('bg-slate-900', isActive);
        b.classList.toggle('text-white', isActive);
        b.classList.toggle('bg-slate-100', !isActive);
        b.classList.toggle('text-slate-700', !isActive);
        b.classList.toggle('dark:bg-slate-700', !isActive);
        b.classList.toggle('dark:text-slate-200', !isActive);
      });
      // Cache varsa direkt render, yoksa fetch et
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

  // Auto-open selected alert from URL
  var root = document.getElementById('alerts-view');
  var selectedId = root && root.getAttribute('data-selected-alert-id');
  if (selectedId && selectedId.trim()) {
    setTimeout(function () {
      var card = document.querySelector('.alert-card[data-alert-id="' + selectedId.trim() + '"]');
      if (!card) return;
      var btn = card.querySelector('.toggle-details');
      if (btn) toggleAlertDetail(btn);
      card.scrollIntoView({ behavior: 'smooth', block: 'center' });
    }, 100);
  }
}

window.seoInitAlertsPage = initAlertsPage;

// Sayfa yüklenince çalıştır
if (document.readyState === 'loading') {
  document.addEventListener('DOMContentLoaded', initAlertsPage);
} else {
  initAlertsPage();
}

// HTMX swap sonrası da çalıştır
document.addEventListener('htmx:afterSwap', function () {
  if (document.getElementById('alerts-view')) {
    setTimeout(initAlertsPage, 0);
  }
});
