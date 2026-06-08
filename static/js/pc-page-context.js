/**
 * AI Talk — sayfa bağlamı toplayıcı (tüm admin sayfaları).
 * base.html içindeki widget her mesajda PcPageContext.collect() çıktısını gönderir.
 */
(function (global) {
  'use strict';

  var EXCLUDE_SELECTORS = [
    '#pc-agent-panel', '#pc-agent-btn', '#pc-agent-resize',
    'script', 'style', 'noscript', 'svg', '[aria-hidden="true"]',
  ].join(',');

  var ROUTES = [
    { prefix: '/ad', page_id: 'ad', label: 'Monetizasyon (Ad)', tool: 'page_fetch_mz_analytics' },
    { prefix: '/notification', page_id: 'notification', label: 'Notification', tool: null },
    { prefix: '/firebase', page_id: 'firebase', label: 'Firebase', tool: 'page_fetch_crashlytics_summary' },
    { prefix: '/inbox', page_id: 'inbox', label: 'Inbox', tool: 'page_fetch_inbox_threads' },
    { prefix: '/intelligence', page_id: 'intelligence', label: 'NEWS', tool: 'page_fetch_news_intelligence' },
    { prefix: '/app', page_id: 'app', label: 'App', tool: 'page_fetch_asc_analytics' },
    { prefix: '/errors', page_id: 'errors', label: 'Errors', tool: 'page_fetch_errors_summary' },
    { prefix: '/realtime', page_id: 'realtime', label: 'Realtime', tool: 'page_fetch_ga4_realtime' },
    { prefix: '/ga4', page_id: 'ga4', label: 'GA4', tool: 'page_fetch_ga4_realtime' },
    { prefix: '/search-console', page_id: 'search-console', label: 'Search Console', tool: null },
    { prefix: '/seo-audit', page_id: 'seo-audit', label: 'SEO Audit', tool: null },
    { prefix: '/tmdb-upcoming', page_id: 'tmdb-upcoming', label: 'Movie', tool: null },
    { prefix: '/boards', page_id: 'boards', label: 'GitLab Boards', tool: null },
    { prefix: '/policy', page_id: 'policy', label: 'Policy', tool: null },
    { prefix: '/alerts', page_id: 'alerts', label: 'Alerts', tool: null },
    { prefix: '/ai', page_id: 'ai', label: 'AI Brief', tool: null },
    { prefix: '/settings', page_id: 'settings', label: 'Settings', tool: null },
    { prefix: '/external', page_id: 'external', label: 'External', tool: null },
    { prefix: '/data-explorer', page_id: 'data-explorer', label: 'Data Explorer', tool: null },
    { prefix: '/public', page_id: 'public', label: 'Public', tool: null },
    { prefix: '/admin/login', page_id: 'login', label: 'Login', tool: null },
    { prefix: '/', page_id: 'home', label: 'Home', tool: 'page_fetch_home_dashboard', exact: true },
  ];

  var DEFAULT_QUICK = [
    { label: 'Sağlık', msg: 'Sistem sağlık durumunu kontrol et' },
    { label: 'Deploy', msg: 'Railway son deployment durumu nedir?' },
    { label: 'Commit', msg: 'Son commit\'leri göster ve özetle' },
  ];

  var QUICK_BY_PAGE = {
    ad: [
      { label: 'Analiz et', msg: 'Seçili filtrelerle monetizasyon verisini analiz et: KPI + trend + gelir/impression/eCPM ilişkisi. Ölçülen→gözlem→çıkarım→risk→en fazla 3 öneri formatında yaz; sadece ekranı tarif etme.' },
      { label: 'Darboğaz', msg: 'Request→match→impression→click hunisinde ve coverage/CTR ile birlikte darboğaz veya anomali var mı? Olası nedenleri ve test edilebilir aksiyonları sırala.' },
      { label: 'Karşılaştırma', msg: 'Karşılaştırma açıksa delta ve kazanan/kaybeden birimleri yorumla: hangi birimler geliri çekiyor, hangileri risk; somut öncelik listesi ver.' },
    ],
    firebase: [
      { label: 'Analiz et', msg: 'Crashlytics verisini analiz et: crash-free, günlük trend, top issue ve cihaz/OS kırılımı. Spike mı kronik mi ayır; kullanıcı etkisine göre öncelik ve 3 aksiyon öner.' },
      { label: 'Sürüm riski', msg: 'Top issue\'lar hangi sürüm/cihaz/OS ile hizalanıyor? Yayın veya hotfix önceliği için çıkarım yap.' },
      { label: 'Kritik crash', msg: 'En kritik issue için kök neden hipotezleri (kanıtla sınırlı) ve doğrulama adımlarını yaz.' },
    ],
    inbox: [
      { label: 'Sekme özeti', msg: 'Bu inbox sekmesindeki okunmamış ve cevaplanmamış mailleri özetle.' },
      { label: 'Seçili mail', msg: 'Şu an seçili mail thread\'ini özetle ve ne cevap vermeliyiz öner.' },
    ],
    intelligence: [
      { label: 'Haber özeti', msg: 'Son 12 saatin haber başlıklarını konuya göre gruplayarak özetle.' },
      { label: 'Kaynak analizi', msg: 'Seçili kaynak filtresindeki haberlerin ortak teması ne?' },
    ],
    app: [
      { label: 'ASC analiz', msg: 'App Store Connect verisini analiz et: impression, dönüşüm, indirme, redownload, proceeds. Ölçülen→çıkarım→öneri; sentetik demo kullanma.' },
      { label: 'Kazanım hunisi', msg: 'Impression → sayfa görüntüleme → indirme ilişkisinde darboğaz ve olası nedenleri yaz.' },
      { label: 'Yorumlar', msg: 'Store yorum analizi ne gösteriyor, öncelikli aksiyon ne?' },
    ],
    ga4: [
      { label: 'Trafik özeti', msg: 'Bu GA4 ekranındaki oturum/kaynak verilerini özetle.' },
      { label: 'Site listesi', msg: 'Hangi siteler var, site id listesini göster.' },
    ],
    realtime: [
      { label: 'Anlık trafik', msg: 'Realtime ekranındaki anlık kullanıcı ve alarm durumunu özetle.' },
    ],
    errors: [
      { label: 'Hata özeti', msg: 'Seçili site için 404/5xx hata özetini yorumla.' },
    ],
    'tmdb-upcoming': [
      { label: 'Takvim özeti', msg: 'Movie takviminde bu ay ve yakın dönemde öne çıkan yapımları özetle.' },
    ],
    home: [
      { label: 'Verileri özetle', msg: 'Ana sayfadaki Günün Özeti verilerini sayılarla özetle: doviz ve sinemalar için anlık kullanıcı, GA4 session, Search Console tıklama/gösterim, kritik pozisyon düşüşleri.' },
      { label: 'Dikkat çeken', msg: 'Bugün dikkat çeken tek en önemli metrik hangisi ve neden?' },
    ],
  };

  function matchRoute(path) {
    var p = path || global.location.pathname || '/';
    for (var i = 0; i < ROUTES.length; i++) {
      var r = ROUTES[i];
      if (r.exact && p === r.prefix) return r;
      if (!r.exact && p === r.prefix) return r;
      if (!r.exact && r.prefix !== '/' && p.indexOf(r.prefix) === 0) return r;
    }
    if (p.indexOf('/site/') === 0) {
      return { page_id: 'site-detail', label: 'Site', tool: 'page_fetch_ga4_realtime', prefix: p };
    }
    return { page_id: 'unknown', label: 'Panel', tool: null, prefix: p };
  }

  function findMainRoot() {
    return global.document.querySelector('main')
      || global.document.querySelector('[role="main"]')
      || global.document.querySelector('#content')
      || global.document.body;
  }

  function collectDomSnapshot(maxLen) {
    maxLen = maxLen || 5500;
    var root = findMainRoot();
    if (!root || !root.cloneNode) return '';

    var clone = root.cloneNode(true);
    EXCLUDE_SELECTORS.split(',').forEach(function (sel) {
      try {
        clone.querySelectorAll(sel.trim()).forEach(function (el) { el.remove(); });
      } catch (_) {}
    });

    var parts = [];
    var headings = clone.querySelectorAll('h1,h2,h3,h4');
    for (var h = 0; h < Math.min(headings.length, 12); h++) {
      var ht = (headings[h].innerText || '').replace(/\s+/g, ' ').trim();
      if (ht && ht.length > 2) parts.push('[' + headings[h].tagName.toLowerCase() + '] ' + ht);
    }

    var stats = clone.querySelectorAll('table, [class*="tabular"], .pc-stat, [data-stat]');
    for (var s = 0; s < Math.min(stats.length, 8); s++) {
      var st = (stats[s].innerText || '').replace(/\s+/g, ' ').trim();
      if (st && st.length > 10 && st.length < 800) parts.push(st);
    }

    var bodyText = (clone.innerText || '').replace(/\s+/g, ' ').trim();
    if (bodyText) parts.push(bodyText);

    var out = parts.join('\n').trim();
    if (out.length > maxLen) out = out.slice(0, maxLen) + '…';
    return out;
  }

  function collectQuery() {
    try {
      var params = new URLSearchParams(global.location.search);
      var obj = {};
      params.forEach(function (v, k) { obj[k] = v; });
      return obj;
    } catch (_) {
      return {};
    }
  }

  function collectCustom() {
    try {
      if (typeof global.__pcPageContext === 'function') {
        var custom = global.__pcPageContext();
        if (custom && typeof custom === 'object') return custom;
      }
    } catch (e) {
      return { __pcPageContext_error: String(e).slice(0, 200) };
    }
    return null;
  }

  function collectFilters(custom, route) {
    var filters = {};
    if (custom && custom.filters) {
      filters = Object.assign({}, custom.filters);
    }
    var q = collectQuery();
    Object.keys(q).forEach(function (k) {
      if (['product', 'site', 'site_id', 'route', 'source', 'months', 'days', 'platform', 'project', 'branch', 'stream', 'start', 'end', 'compare'].indexOf(k) >= 0) {
        filters[k] = q[k];
      }
    });
    return Object.keys(filters).length ? filters : null;
  }

  function collect() {
    var path = global.location.pathname || '/';
    var route = matchRoute(path);
    var custom = collectCustom();
    var ctx = {
      path: path,
      page_id: (custom && custom.page) || route.page_id,
      label: route.label,
      title: (global.document.title || '').trim(),
      query: collectQuery(),
      query_string: global.location.search || '',
      filters: collectFilters(custom, route),
      suggested_tool: route.tool || null,
      custom: custom,
      dom_snapshot: collectDomSnapshot(),
      collected_at: new Date().toISOString(),
    };
    if (custom && custom.thread_id) ctx.thread_id = custom.thread_id;
    if (custom && custom.site_id) ctx.site_id = custom.site_id;
    if (custom && custom.visible_text) {
      var merged = (String(custom.visible_text) + '\n---\n' + (ctx.dom_snapshot || '')).trim();
      ctx.dom_snapshot = merged.length > 9000 ? merged.slice(0, 9000) + '…' : merged;
    }
    return ctx;
  }

  function getChipLabel(ctx) {
    ctx = ctx || collect();
    var label = ctx.label || ctx.page_id || 'Panel';
    var extra = '';
    if (ctx.filters) {
      if (ctx.filters.product) extra = ctx.filters.product;
      else if (ctx.filters.route) extra = ctx.filters.route;
      else if (ctx.filters.source) extra = ctx.filters.source;
    }
    if (ctx.custom && ctx.custom.active_tab && !extra) extra = ctx.custom.active_tab;
    return extra ? (label + ' · ' + extra) : label;
  }

  function getQuickActions(ctx) {
    ctx = ctx || collect();
    var pageId = ctx.page_id || 'unknown';
    return QUICK_BY_PAGE[pageId] || DEFAULT_QUICK;
  }

  function renderQuickActions(container, ctx) {
    if (!container) return;
    var actions = getQuickActions(ctx);
    container.innerHTML = '';
    actions.forEach(function (a) {
      var btn = global.document.createElement('button');
      btn.type = 'button';
      btn.className = 'pc-agent-quick-btn rounded-full border border-violet-800/50 bg-violet-950/60 px-2.5 py-1 text-xs text-violet-300 hover:border-violet-600 hover:text-white hover:bg-violet-900/50 transition-all';
      btn.setAttribute('data-msg', a.msg);
      btn.textContent = a.label;
      btn.addEventListener('click', function () {
        if (typeof global.pcAgentQuickSend === 'function') global.pcAgentQuickSend(btn);
      });
      container.appendChild(btn);
    });
  }

  function updatePageChip(chipEl) {
    if (!chipEl) return;
    try {
      var ctx = collect();
      chipEl.textContent = getChipLabel(ctx);
      chipEl.title = ctx.path + (ctx.query_string || '');
    } catch (_) {
      chipEl.textContent = 'Panel';
    }
  }

  global.PcPageContext = {
    collect: collect,
    getChipLabel: getChipLabel,
    getQuickActions: getQuickActions,
    renderQuickActions: renderQuickActions,
    updatePageChip: updatePageChip,
    collectDomSnapshot: collectDomSnapshot,
  };
})(typeof window !== 'undefined' ? window : globalThis);
