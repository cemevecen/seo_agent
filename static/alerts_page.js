/* Component Isolation Styles */
let style = document.getElementById('alerts-component-style');
if (!style) {
  style = document.createElement('style');
  style.id = 'alerts-component-style';
  style.textContent = `
  body {
    overflow-x: hidden;
  }
  
  .alert-card {
    display: block;
    position: relative;
    z-index: 1;
    overflow: visible;
    width: 100%;
    margin: 0;
    border-radius: 0.5rem;
  }
  
  .alert-details {
    display: none !important;
    position: relative;
    z-index: 0;
    width: 100%;
    overflow: visible;
  }
  
  .alert-details.show {
    display: block !important;
  }
  
  #alerts-container {
    display: flex;
    flex-direction: column;
    gap: 0.75rem;
    position: relative;
    z-index: 1;
    overflow: visible;
  }
  
  .rounded-3xl.bg-white {
    position: relative;
    z-index: 1;
    overflow: visible;
  }
`;
  document.head.appendChild(style);
}

function initializeAlertsView() {
  const root = document.getElementById('alerts-view');
  if (!root || root.dataset.seoAlertsBound === '1') {
    return;
  }
  initializeFilter();
  initializeRefreshButton();

  document.querySelectorAll('.toggle-details').forEach(btn => {
    btn.addEventListener('click', function(e) {
      e.preventDefault();
      e.stopPropagation();
      
      const card = this.closest('.alert-card');
      const details = card.querySelector('.alert-details');
      const alertId = card.dataset.alertId;
      
      if (details.classList.contains('show')) {
        // Hide
        details.classList.remove('show');
        this.textContent = 'Detay';
      } else {
        // Show
        details.classList.add('show');
        loadAlertDetails(alertId, card);
        this.textContent = 'Gizle';
      }
    });
  });
  
  document.querySelectorAll('.comparison-btn').forEach(btn => {
    btn.addEventListener('click', async function(e) {
      e.preventDefault();
      e.stopPropagation();
      if (this.dataset.disabled === 'true') {
        return;
      }
      
      const alertId = this.dataset.alertId;
      const comparisonType = this.dataset.comparison;
      const card = this.closest('.alert-card');
      
      // Button styling
      const allBtns = card.querySelectorAll('.comparison-btn');
      allBtns.forEach(b => {
        if (b.dataset.comparison === comparisonType) {
          b.classList.remove('bg-slate-100', 'dark:bg-slate-800/70', 'text-slate-700', 'dark:text-slate-200', 'hover:bg-slate-200', 'dark:hover:bg-slate-700');
          b.classList.add('bg-blue-100', 'text-blue-700', 'hover:bg-blue-200', 'dark:bg-sky-950/50', 'dark:text-sky-300', 'dark:hover:bg-sky-900/60', 'active');
        } else {
          b.classList.add('bg-slate-100', 'dark:bg-slate-800/70', 'text-slate-700', 'dark:text-slate-200', 'hover:bg-slate-200', 'dark:hover:bg-slate-700');
          b.classList.remove('bg-blue-100', 'text-blue-700', 'hover:bg-blue-200', 'dark:bg-sky-950/50', 'dark:text-sky-300', 'dark:hover:bg-sky-900/60', 'active');
        }
      });
      
      // Load and render the selected comparison type.
      await loadComparisonData(alertId, comparisonType);
      renderComparisonData(alertId, comparisonType);
    });
  });
  root.dataset.seoAlertsBound = '1';
}

window.seoInitAlertsPage = function seoInitAlertsPage() {
  if (!document.getElementById('alerts-view')) {
    return;
  }
  initializeAlertsView();
  window.setTimeout(autoOpenSelectedAlertFromRoute, 80);
};

(function () {
  function runAlertsInit() {
    if (typeof window.seoInitAlertsPage !== 'function') {
      return;
    }
    if (!document.getElementById('alerts-view')) {
      return;
    }
    window.seoInitAlertsPage();
  }
  function onFirstPaint() {
    runAlertsInit();
  }
  if (document.readyState === 'loading') {
    document.addEventListener('DOMContentLoaded', onFirstPaint);
  } else {
    onFirstPaint();
  }
  /* HTMX: afterSwap, #content hedefinde bile event.target genelde swap edilen cocuk dugumdur (id=content degil). */
  function scheduleAlertsInitFromHtmx() {
    if (!document.getElementById('alerts-view')) {
      return;
    }
    window.setTimeout(runAlertsInit, 0);
  }
  document.body.addEventListener('htmx:afterSwap', scheduleAlertsInitFromHtmx);
  document.body.addEventListener('htmx:afterSettle', scheduleAlertsInitFromHtmx);
})();

function initializeRefreshButton() {
  const button = document.getElementById('refresh-alerts-button');
  if (!button) {
    return;
  }
  const panel = document.getElementById('alerts-progress-panel');
  const title = document.getElementById('alerts-progress-title');
  const detail = document.getElementById('alerts-progress-detail');
  const percent = document.getElementById('alerts-progress-percent');
  const bar = document.getElementById('alerts-progress-bar');
  const steps = [
    { percent: 16, title: 'Site listesi hazırlanıyor', detail: 'Alert yenileme akışı için tüm aktif siteler sıralanıyor.' },
    { percent: 38, title: 'Search Console verileri karşılaştırılıyor', detail: 'Her site için son kayıt verileri yeniden değerlendiriliyor.' },
    { percent: 63, title: 'Uyarılar hesaplanıyor', detail: 'CTR, pozisyon ve impression değişimleri tekrar hesaplanıyor.' },
    { percent: 84, title: 'Kayıtlar güncelleniyor', detail: 'Yeni uyarı sonuçları veritabanına yazılıyor.' },
    { percent: 94, title: 'Liste yenileniyor', detail: 'Alert listesi yeni sonuç ile tekrar çizilecek.' },
  ];
  let timer = null;
  let tailTimer = null;

  function renderProgress(step) {
    panel.classList.remove('hidden');
    title.textContent = step.title;
    detail.textContent = step.detail;
    percent.textContent = String(step.percent) + '%';
    bar.style.width = String(step.percent) + '%';
  }

  function resetTone() {
    panel.classList.remove(
      'border-rose-200',
      'bg-[linear-gradient(135deg,rgba(255,228,230,0.96),rgba(255,255,255,0.98)_52%,rgba(255,241,242,0.98))]',
      'shadow-[0_22px_55px_-34px_rgba(244,63,94,0.4)]',
      'dark:border-rose-900/60',
      'dark:bg-rose-950/45',
      'dark:shadow-[0_22px_55px_-34px_rgba(244,63,94,0.15)]',
      'dark:ring-rose-900/40'
    );
    panel.classList.add(
      'border-sky-200',
      'bg-[linear-gradient(135deg,rgba(224,242,254,0.92),rgba(255,255,255,0.98)_52%,rgba(224,231,255,0.92))]',
      'shadow-[0_22px_55px_-34px_rgba(14,165,233,0.6)]',
      'dark:border-slate-600',
      'dark:bg-slate-900/90',
      'dark:shadow-[0_22px_55px_-34px_rgba(0,0,0,0.5)]',
      'dark:ring-slate-700/80'
    );
    bar.classList.remove(
      'bg-[linear-gradient(90deg,#fb7185_0%,#ef4444_52%,#f97316_100%)]',
      'shadow-[0_0_26px_rgba(244,63,94,0.28)]',
      'dark:bg-[linear-gradient(90deg,#fb7185_0%,#f43f5e_50%,#fb923c_100%)]',
      'dark:shadow-[0_0_20px_rgba(248,113,113,0.35)]'
    );
    bar.classList.add(
      'bg-[linear-gradient(90deg,#0ea5e9_0%,#2563eb_55%,#14b8a6_100%)]',
      'shadow-[0_0_22px_rgba(14,165,233,0.28)]',
      'dark:bg-[linear-gradient(90deg,#22d3ee_0%,#38bdf8_50%,#2dd4bf_100%)]',
      'dark:shadow-[0_0_22px_rgba(34,211,238,0.32)]'
    );
  }

  button.addEventListener('click', async function() {
    const originalText = button.textContent;
    button.disabled = true;
    button.textContent = 'Yenileniyor...';
    button.classList.add('opacity-70', 'cursor-wait');
    resetTone();
    renderProgress({ percent: 8, title: 'Alert yenileme başlatıldı', detail: 'Bu işlem tüm siteler için Search Console alert metriklerini yeniden hesaplar.' });
    let stepIndex = 0;
    timer = window.setInterval(function () {
      if (stepIndex >= steps.length) {
        window.clearInterval(timer);
        timer = null;
        tailTimer = window.setInterval(function () {
          const current = parseInt(bar.style.width || '0', 10) || 0;
          const next = Math.min(current + 1, 98);
          if (next <= current) {
            window.clearInterval(tailTimer);
            tailTimer = null;
            return;
          }
          renderProgress({
            percent: next,
            title: 'Son liste güncellemesi bekleniyor',
            detail: 'Yanıttan önce kalan son adımlar tamamlandıkça ilerleme yavaşça devam ediyor.',
          });
        }, 700);
        return;
      }
      renderProgress(steps[stepIndex]);
      stepIndex += 1;
    }, 850);
    try {
      const response = await fetch('/alerts/refresh', { method: 'POST' });
      if (!response.ok) {
        throw new Error(`HTTP ${response.status}`);
      }
      if (timer) {
        window.clearInterval(timer);
      }
      if (tailTimer) {
        window.clearInterval(tailTimer);
      }
      renderProgress({ percent: 100, title: 'Uyarılar güncellendi', detail: 'Yeni alert listesi yükleniyor.' });
      setTimeout(function () {
        window.location.reload();
      }, 250);
    } catch (error) {
      console.error('Alert refresh failed:', error);
      if (timer) {
        window.clearInterval(timer);
      }
      if (tailTimer) {
        window.clearInterval(tailTimer);
      }
      panel.classList.remove(
        'border-sky-200',
        'bg-[linear-gradient(135deg,rgba(224,242,254,0.92),rgba(255,255,255,0.98)_52%,rgba(224,231,255,0.92))]',
        'shadow-[0_22px_55px_-34px_rgba(14,165,233,0.6)]',
        'dark:border-slate-600',
        'dark:bg-slate-900/90',
        'dark:shadow-[0_22px_55px_-34px_rgba(0,0,0,0.5)]',
        'dark:ring-slate-700/80'
      );
      panel.classList.add(
        'border-rose-200',
        'bg-[linear-gradient(135deg,rgba(255,228,230,0.96),rgba(255,255,255,0.98)_52%,rgba(255,241,242,0.98))]',
        'shadow-[0_22px_55px_-34px_rgba(244,63,94,0.4)]',
        'dark:border-rose-900/60',
        'dark:bg-rose-950/45',
        'dark:shadow-[0_22px_55px_-34px_rgba(244,63,94,0.15)]',
        'dark:ring-rose-900/40'
      );
      bar.classList.remove(
        'bg-[linear-gradient(90deg,#0ea5e9_0%,#2563eb_55%,#14b8a6_100%)]',
        'shadow-[0_0_22px_rgba(14,165,233,0.28)]',
        'dark:bg-[linear-gradient(90deg,#22d3ee_0%,#38bdf8_50%,#2dd4bf_100%)]',
        'dark:shadow-[0_0_22px_rgba(34,211,238,0.32)]'
      );
      bar.classList.add(
        'bg-[linear-gradient(90deg,#fb7185_0%,#ef4444_52%,#f97316_100%)]',
        'shadow-[0_0_26px_rgba(244,63,94,0.28)]',
        'dark:bg-[linear-gradient(90deg,#fb7185_0%,#f43f5e_50%,#fb923c_100%)]',
        'dark:shadow-[0_0_20px_rgba(248,113,113,0.35)]'
      );
      renderProgress({
        percent: Math.max(parseInt(bar.style.width || '16', 10) || 16, 16),
        title: 'Uyarılar yenilenemedi',
        detail: 'Search Console alert hesaplaması tamamlanamadı. Log ve SMTP bildirimlerini kontrol et.',
      });
      button.textContent = 'Yenileme Hatası';
      setTimeout(() => {
        button.textContent = originalText;
        button.disabled = false;
        button.classList.remove('opacity-70', 'cursor-wait');
      }, 1500);
    }
  });
}

async function loadAlertDetails(alertId, card) {
  try {
    console.log(`Loading alert details for ID: ${alertId}`);
    
    const response = await fetch(`/api/alert-details/${alertId}`);
    if (!response.ok) {
      throw new Error(`HTTP error! status: ${response.status}`);
    }
    
    const data = await response.json();
    console.log('Alert data loaded:', data);
    
    // Store both daily and weekly data in memory
    if (!window.comparisonCache) {
      window.comparisonCache = {};
    }
    window.comparisonCache[alertId] = {};
    
    // Paralel olarak her iki comparison type'ı yükle VE SAKLA
    const [dailyData, weeklyData] = await Promise.all([
      loadComparisonData(alertId, 'daily'),
      loadComparisonData(alertId, 'weekly')
    ]);

    const dailyUsable =
      !dailyData ||
      !dailyData.comparison ||
      dailyData.comparison.has_meaningful_data !== false;
    const weeklyUsable =
      !weeklyData ||
      !weeklyData.comparison ||
      weeklyData.comparison.has_meaningful_data !== false;
    const weeklyButton = card.querySelector('.comparison-btn[data-comparison="weekly"]');
    const dailyButton = card.querySelector('.comparison-btn[data-comparison="daily"]');

    if (weeklyButton) {
      weeklyButton.dataset.disabled = weeklyUsable ? 'false' : 'true';
      weeklyButton.classList.toggle('opacity-50', !weeklyUsable);
      weeklyButton.classList.toggle('cursor-not-allowed', !weeklyUsable);
    }
    if (dailyButton) {
      dailyButton.dataset.disabled = dailyUsable ? 'false' : 'true';
      dailyButton.classList.toggle('opacity-50', !dailyUsable);
      dailyButton.classList.toggle('cursor-not-allowed', !dailyUsable);
    }

    const pd = typeof window !== 'undefined' ? window.__alertsSelectedPeriodDays : undefined;
    const preferDaily = pd === 1;
    const initialType = preferDaily
      ? (dailyUsable ? 'daily' : weeklyUsable ? 'weekly' : 'daily')
      : (weeklyUsable ? 'weekly' : dailyUsable ? 'daily' : 'weekly');
    const allBtns = card.querySelectorAll('.comparison-btn');
    allBtns.forEach(b => {
      if (b.dataset.comparison === initialType) {
        b.classList.remove('bg-slate-100', 'dark:bg-slate-800/70', 'text-slate-700', 'dark:text-slate-200', 'hover:bg-slate-200', 'dark:hover:bg-slate-700');
        b.classList.add('bg-blue-100', 'text-blue-700', 'hover:bg-blue-200', 'dark:bg-sky-950/50', 'dark:text-sky-300', 'dark:hover:bg-sky-900/60', 'active');
      } else {
        b.classList.add('bg-slate-100', 'dark:bg-slate-800/70', 'text-slate-700', 'dark:text-slate-200', 'hover:bg-slate-200', 'dark:hover:bg-slate-700');
        b.classList.remove('bg-blue-100', 'text-blue-700', 'hover:bg-blue-200', 'dark:bg-sky-950/50', 'dark:text-sky-300', 'dark:hover:bg-sky-900/60', 'active');
      }
    });
    
    renderComparisonData(alertId, initialType);
    
  } catch (error) {
    console.error('Error loading alert details:', error);
    const comparisonDiv = document.getElementById(`comparison-info-${alertId}`);
    if (comparisonDiv) {
      comparisonDiv.textContent = `Hata: ${error.message}`;
    }
  }
}

async function loadComparisonData(alertId, comparisonType) {
  try {
    const response = await fetch(`/api/alert-details/${alertId}?comparison=${comparisonType}`);
    if (!response.ok) {
      throw new Error(`HTTP error! status: ${response.status}`);
    }
    
    const data = await response.json();
    
    // Cache data
    if (!window.comparisonCache) {
      window.comparisonCache = {};
    }
    if (!window.comparisonCache[alertId]) {
      window.comparisonCache[alertId] = {};
    }
    window.comparisonCache[alertId][comparisonType] = data;
    
    console.log(`Loaded ${comparisonType} data for alert ${alertId}:`, data);
    return data;
    
  } catch (error) {
    console.error(`Error loading ${comparisonType} comparison data:`, error);
    throw error;
  }
}

function renderComparisonData(alertId, comparisonType) {
  try {
    if (!window.comparisonCache || !window.comparisonCache[alertId] || !window.comparisonCache[alertId][comparisonType]) {
      console.warn(`No cached data for alert ${alertId}, type ${comparisonType}`);
      return;
    }
    
    const data = window.comparisonCache[alertId][comparisonType];
    const comparison = data.comparison || {};
    const comparisonDiv = document.getElementById(`comparison-info-${alertId}`);
    
    if (comparisonDiv) {
      let html = '';
      const typeLabel = comparisonType === 'daily' ? 'Dünle Karşılaştırma' : 'Geçen Hafta Aynı Gün ile Karşılaştırma';
      const toneMap = {
        blue: 'bg-sky-50 dark:bg-slate-900/60 border-sky-100 dark:border-slate-600 text-slate-900 dark:text-slate-100',
        slate: 'bg-slate-50 dark:bg-slate-900/50 border-slate-100 dark:border-slate-800 text-slate-900 dark:text-slate-100',
        red: 'bg-rose-50 dark:bg-rose-950/40 border-rose-100 dark:border-rose-900/50 text-rose-700 dark:text-rose-300',
        green: 'bg-emerald-50 dark:bg-emerald-950/35 border-emerald-100 dark:border-emerald-900/50 text-emerald-700 dark:text-emerald-300',
      };

      html += `<div class="rounded-2xl border border-sky-100 dark:border-slate-600 bg-sky-50/70 dark:bg-slate-900/70 p-4 sm:p-5">`;
      html += `<p class="text-xs font-semibold tracking-[0.16em] text-slate-500 dark:text-slate-400 uppercase mb-3">${typeLabel}</p>`;

      if (comparison.message) {
        html += `<p class="text-base sm:text-lg font-semibold text-slate-900 dark:text-slate-100 mb-4">${comparison.message}</p>`;
      }

      if (Array.isArray(comparison.cards) && comparison.cards.length > 0) {
        html += `<div class="grid gap-3 md:grid-cols-3">`;
        comparison.cards.forEach(card => {
          const toneClass = toneMap[card.tone] || toneMap.slate;
          html += `
            <div class="rounded-2xl border p-4 ${toneClass}">
              <p class="text-[11px] font-semibold uppercase tracking-[0.18em] text-slate-500 dark:text-slate-400">${card.label || ''}</p>
              <p class="mt-2 text-2xl font-bold leading-none">${card.value || 'N/A'}</p>
              ${card.detail ? `<p class="mt-2 text-sm leading-5 text-slate-600 dark:text-slate-300">${card.detail}</p>` : ''}
            </div>
          `;
        });
        html += `</div>`;
      }

      let queryDetailsArray = [];
      if (comparison.query_details) {
        if (Array.isArray(comparison.query_details)) {
          queryDetailsArray = comparison.query_details;
        } else if (typeof comparison.query_details === 'object') {
          queryDetailsArray = [comparison.query_details];
        }
      }
      
      if (queryDetailsArray.length > 0 && (!comparison.cards || comparison.cards.length === 0)) {
        html += `<div class="mt-4 overflow-x-auto"><table class="w-full text-sm border-collapse"><thead><tr class="border-b-2 border-slate-300 dark:border-slate-600"><th class="text-left px-3 py-2 font-semibold text-slate-700 dark:text-slate-200">Query</th><th class="text-center px-3 py-2 font-semibold text-slate-700 dark:text-slate-200">Eski Pozisyon</th><th class="text-center px-3 py-2 font-semibold text-slate-700 dark:text-slate-200">Yeni Pozisyon</th><th class="text-center px-3 py-2 font-semibold text-slate-700 dark:text-slate-200">Değişim</th><th class="text-center px-3 py-2 font-semibold text-slate-700 dark:text-slate-200">Durum</th></tr></thead><tbody>`;
        
        queryDetailsArray.forEach(detail => {
          const change = detail.change || 0;
          const isImprovement = detail.is_improvement;
          const isNeutral = isImprovement === null || isImprovement === undefined;
          const statusClass = isNeutral ? 'text-slate-600 dark:text-slate-300 bg-slate-50 dark:bg-slate-900/50' : isImprovement ? 'text-green-600 dark:text-emerald-400 bg-green-50 dark:bg-emerald-950/40' : 'text-red-600 dark:text-red-400 bg-red-50 dark:bg-red-950/40';
          const statusIcon = isNeutral ? '→' : isImprovement ? '↓' : '↑';
          const statusText = isNeutral ? 'Değişmedi' : isImprovement ? 'İyileşme' : 'Kötüleşme';
          const changeStr = change > 0 ? `+${change.toFixed(1)}` : change < 0 ? change.toFixed(1) : '0.0';
          
          html += `<tr class="border-b border-slate-200 dark:border-slate-700 hover:bg-slate-50 dark:bg-slate-900/50 dark:hover:bg-slate-800/80"><td class="px-3 py-2 font-medium text-slate-900 dark:text-slate-100">${detail.query || 'N/A'}</td><td class="text-center px-3 py-2 text-slate-700 dark:text-slate-200">${detail.old_position ? detail.old_position.toFixed(1) : 'N/A'}</td><td class="text-center px-3 py-2 text-slate-700 dark:text-slate-200">${detail.new_position ? detail.new_position.toFixed(1) : 'N/A'}</td><td class="text-center px-3 py-2 font-medium ${isNeutral ? 'text-slate-500 dark:text-slate-400' : isImprovement ? 'text-green-600' : 'text-red-600'}">${changeStr}</td><td class="text-center px-3 py-2"><span class="px-2 py-1 rounded text-xs font-medium ${statusClass}">${statusIcon} ${statusText}</span></td></tr>`;
        });
        
        html += `</tbody></table></div>`;
      } else if (!comparison.message && (!comparison.cards || comparison.cards.length === 0)) {
        html += '<p class="text-slate-600 dark:text-slate-300">Karşılaştırma verisi bulunamadı</p>';
      }

      html += `</div>`;
      comparisonDiv.innerHTML = html;
    }
    
  } catch (error) {
    console.error(`Error rendering ${comparisonType} data:`, error);
    const comparisonDiv = document.getElementById(`comparison-info-${alertId}`);
    if (comparisonDiv) {
      comparisonDiv.textContent = `Hata: ${error.message}`;
    }
  }
}

// Site filter — state + delegation (HTMX ile sayfa tekrar gelse bile tek listener; Tailwind safelist HTML'de)
let __alertsSelectedType = 'all';
let __alertsSelectedPeriodDays = 7;
let __alertsFilterDelegationBound = false;

function applyAlertsFilters() {
  const container = document.getElementById('alerts-container');
  const filterSelect = document.getElementById('site-filter');
  if (!container) {
    return;
  }
  const alertCards = container.querySelectorAll('.alert-card');
  const selectedDomain = filterSelect ? filterSelect.value : '';
  const cutoff = new Date();
  cutoff.setDate(cutoff.getDate() - __alertsSelectedPeriodDays);

  alertCards.forEach(card => {
    const cardDomain = card.dataset.domain;
    const cardCategory = (card.getAttribute('data-alert-category') || card.dataset.alertCategory || 'other').trim();
    const triggeredAt = card.dataset.triggeredAt ? new Date(card.dataset.triggeredAt) : null;
    const triggeredOk = triggeredAt && !Number.isNaN(triggeredAt.getTime());
    const domainMatches = selectedDomain === '' || cardDomain === selectedDomain;
    const typeMatches = __alertsSelectedType === 'all' || cardCategory === __alertsSelectedType;
    const periodMatches = !triggeredOk || triggeredAt >= cutoff;
    card.style.display = domainMatches && typeMatches && periodMatches ? 'block' : 'none';
  });

  const visibleCards = Array.from(alertCards).filter(card => card.style.display !== 'none');
  let emptyState = container.querySelector('.empty-state');
  if (visibleCards.length === 0) {
    if (!emptyState) {
      emptyState = document.createElement('div');
      emptyState.className = 'empty-state rounded-2xl border border-dashed border-slate-300 dark:border-slate-600 p-8 text-center text-slate-500 dark:text-slate-400';
      container.appendChild(emptyState);
    }
    emptyState.textContent = 'Bu filtreye uygun alarm kaydı bulunamadı.';
    emptyState.style.display = 'block';
  } else if (emptyState) {
    emptyState.style.display = 'none';
  }
}

function resetAlertTypeTabToInactive(item) {
  item.classList.remove(
    'ring-2',
    'ring-offset-0',
    'ring-offset-2',
    'ring-slate-300',
    'ring-slate-400',
    'dark:ring-slate-500',
    'dark:ring-offset-slate-900',
    'bg-slate-900',
    'text-white',
    'bg-rose-600',
    'bg-sky-600',
    'bg-amber-500'
  );
  const f = item.dataset.alertFilter;
  if (f === 'all') {
    item.classList.add('bg-slate-100', 'dark:bg-slate-800/70', 'text-slate-700', 'dark:text-slate-200');
  } else if (f === 'ctr') {
    item.classList.add('bg-rose-50', 'text-rose-700', 'dark:bg-rose-950/45', 'dark:text-rose-300');
  } else if (f === 'position') {
    item.classList.add('bg-sky-50', 'text-sky-700', 'dark:bg-sky-950/45', 'dark:text-sky-300');
  } else if (f === 'impression') {
    item.classList.add('bg-amber-50', 'text-amber-700', 'dark:bg-amber-950/40', 'dark:text-amber-300');
  }
}

function activateAlertTypeTab(tab) {
  const filterTabs = tab.closest('#alerts-view')?.querySelectorAll('.alert-type-tab');
  if (!filterTabs) {
    return;
  }
  filterTabs.forEach(resetAlertTypeTabToInactive);

  const selectedType = tab.dataset.alertFilter || 'all';
  tab.classList.remove(
    'bg-slate-100',
    'dark:bg-slate-800/70',
    'text-slate-700',
    'dark:text-slate-200',
    'bg-rose-50',
    'text-rose-700',
    'dark:bg-rose-950/45',
    'dark:text-rose-300',
    'bg-sky-50',
    'text-sky-700',
    'dark:bg-sky-950/45',
    'dark:text-sky-300',
    'bg-amber-50',
    'text-amber-700',
    'dark:bg-amber-950/40',
    'dark:text-amber-300'
  );

  if (selectedType === 'all') {
    tab.classList.add('bg-slate-900', 'text-white');
  } else if (selectedType === 'ctr') {
    tab.classList.add('bg-rose-600', 'text-white');
  } else if (selectedType === 'position') {
    tab.classList.add('bg-sky-600', 'text-white');
  } else if (selectedType === 'impression') {
    tab.classList.add('bg-amber-500', 'text-white');
  }
  /* ring-offset yok — koyu modda beyaz halka oluşmaz */
  tab.classList.add('ring-2', 'ring-offset-0', 'ring-slate-400', 'dark:ring-slate-500');
}

function syncAlertTypeTabsFromState() {
  const view = document.getElementById('alerts-view');
  if (!view) {
    return;
  }
  const tab = view.querySelector(`.alert-type-tab[data-alert-filter="${__alertsSelectedType}"]`);
  if (tab) {
    activateAlertTypeTab(tab);
  }
}

function syncPeriodTabsFromState() {
  const view = document.getElementById('alerts-view');
  if (!view) {
    return;
  }
  const periodTabs = view.querySelectorAll('.alert-period-tab');
  periodTabs.forEach(t => {
    t.classList.remove('bg-slate-900', 'text-white');
    t.classList.add('bg-slate-100', 'dark:bg-slate-800/70', 'text-slate-600', 'dark:text-slate-300');
  });
  const active = view.querySelector(`.alert-period-tab[data-period="${__alertsSelectedPeriodDays}"]`);
  if (active) {
    active.classList.remove('bg-slate-100', 'dark:bg-slate-800/70', 'text-slate-600', 'dark:text-slate-300');
    active.classList.add('bg-slate-900', 'text-white');
  }
}

function bindAlertsFilterDelegationOnce() {
  if (__alertsFilterDelegationBound) {
    return;
  }
  __alertsFilterDelegationBound = true;

  document.body.addEventListener('click', function (e) {
    const view = document.getElementById('alerts-view');
    if (!view) {
      return;
    }
    const typeTab = e.target.closest('.alert-type-tab');
    if (typeTab && view.contains(typeTab)) {
      e.preventDefault();
      __alertsSelectedType = typeTab.dataset.alertFilter || 'all';
      activateAlertTypeTab(typeTab);
      applyAlertsFilters();
      return;
    }
    const periodTab = e.target.closest('.alert-period-tab');
    if (periodTab && view.contains(periodTab)) {
      e.preventDefault();
      __alertsSelectedPeriodDays = parseInt(periodTab.dataset.period, 10) || 7;
      try {
        window.__alertsSelectedPeriodDays = __alertsSelectedPeriodDays;
      } catch (err) {}
      syncPeriodTabsFromState();
      applyAlertsFilters();
    }
  });

  document.body.addEventListener('change', function (e) {
    if (e.target.id !== 'site-filter') {
      return;
    }
    const view = document.getElementById('alerts-view');
    if (!view || !view.contains(e.target)) {
      return;
    }
    applyAlertsFilters();
  });
}

function initializeFilter() {
  bindAlertsFilterDelegationOnce();
  try {
    window.__alertsSelectedPeriodDays = __alertsSelectedPeriodDays;
  } catch (e) {}
  syncPeriodTabsFromState();
  syncAlertTypeTabsFromState();
  applyAlertsFilters();
}

function autoOpenSelectedAlertFromRoute() {
  const root = document.getElementById('alerts-view');
  const selectedAlertId = (root && root.dataset && root.dataset.selectedAlertId ? root.dataset.selectedAlertId : '').trim();
  if (!selectedAlertId) {
    return;
  }

  const card = document.querySelector(`.alert-card[data-alert-id="${selectedAlertId}"]`);
  if (!card) {
    return;
  }

  const details = card.querySelector('.alert-details');
  const button = card.querySelector('.toggle-details');
  if (!details || !button) {
    return;
  }

  card.classList.add('ring-2', 'ring-sky-200', 'dark:ring-sky-700', 'shadow-sm');
  card.scrollIntoView({ behavior: 'smooth', block: 'center' });
  if (!details.classList.contains('show')) {
    button.click();
  }
}