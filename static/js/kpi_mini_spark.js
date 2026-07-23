/**
 * KPI kart mini spark (Search Console; GA4 uyumlu satır seçicileri).
 */
(function (global) {
  'use strict';

  var ROW_SEL = '[data-kpi-spark-row]';
  var SPARK_SEL = '[data-kpi-spark]';

  function sparkPeriodDays(row) {
    var raw = row && row.getAttribute('data-kpi-spark-period-days');
    var n = parseInt(raw || '0', 10);
    return n > 0 ? n : 7;
  }

  function barsLayoutClass(barCount) {
    var n = parseInt(barCount, 10) || 0;
    if (n > 45) {
      return 'kpi-mini-spark__bars--dense';
    }
    if (n > 0 && n <= 31) {
      return 'kpi-mini-spark__bars--fill';
    }
    return '';
  }

  function visibleBarCount(sparkEl, periodDays, seriesLen) {
    var pd = Math.max(1, parseInt(periodDays, 10) || 7);
    /* Kısa dönemler: siteler arası aynı çubuk sayısı (eksik günler pad edilir) */
    if (pd <= 31) {
      return pd;
    }
    var len = Math.max(0, parseInt(seriesLen, 10) || 0);
    var want = Math.min(len || pd, pd);
    var w = sparkEl && sparkEl.clientWidth ? sparkEl.clientWidth : 0;
    if (w < 8) {
      return want;
    }
    var minBar = w < 120 ? 2 : w < 180 ? 2.5 : 3;
    var gap = 1;
    var fit = Math.max(8, Math.floor((w + gap) / (minBar + gap)));
    return Math.min(want, fit);
  }

  function toneClass(tone) {
    return tone === 'up'
      ? 'kpi-mini-spark--tone-up'
      : tone === 'down'
        ? 'kpi-mini-spark--tone-down'
        : 'kpi-mini-spark--tone-flat';
  }

  function seriesHasSignal(sampled) {
    if (!sampled || !sampled.length) {
      return false;
    }
    for (var i = 0; i < sampled.length; i++) {
      if (sampled[i] > 0) {
        return true;
      }
    }
    return false;
  }

  function downsampleSeries(values, target) {
    var t = Math.max(0, parseInt(target, 10) || 0);
    if (!t) {
      return [];
    }
    if (!values || !values.length) {
      return Array(t).fill(0);
    }
    if (values.length <= t) {
      var mapped = values.map(function (v) {
        if (v == null || v === '') {
          return 0;
        }
        return parseFloat(v) || 0;
      });
      while (mapped.length < t) {
        mapped.unshift(0);
      }
      return mapped;
    }
    var out = [];
    var step = values.length / t;
    for (var i = 0; i < t; i++) {
      var start = Math.floor(i * step);
      var end = Math.min(values.length, Math.floor((i + 1) * step));
      var sum = 0;
      var n = 0;
      for (var j = start; j < end; j++) {
        var raw = values[j];
        if (raw != null && raw !== '') {
          sum += parseFloat(raw) || 0;
          n += 1;
        }
      }
      out.push(n ? sum / n : 0);
    }
    return out;
  }

  function renderPlaceholder(spark, tone, barCount) {
    var n = Math.max(4, parseInt(barCount, 10) || 7);
    spark.classList.remove(
      'kpi-mini-spark--tone-up',
      'kpi-mini-spark--tone-down',
      'kpi-mini-spark--tone-flat',
      'kpi-mini-spark--placeholder'
    );
    spark.classList.add('kpi-mini-spark--placeholder', toneClass(tone));
    var bars = document.createElement('div');
    bars.className = 'kpi-mini-spark__bars';
    var layoutCls = barsLayoutClass(n);
    if (layoutCls) {
      bars.classList.add(layoutCls);
    }
    bars.setAttribute('aria-hidden', 'true');
    for (var i = 0; i < n; i++) {
      var bar = document.createElement('span');
      bar.className = 'kpi-mini-spark__bar';
      bars.appendChild(bar);
    }
    spark.textContent = '';
    spark.appendChild(bars);
  }

  function resolveTrendRaw(row) {
    var scope =
      (row && row.closest('[data-device-panel]')) ||
      (row && row.closest('[data-sc-period-panel]')) ||
      (row && row.parentElement) ||
      row;
    if (!scope || !scope.querySelector) {
      return '';
    }
    var chart = scope.querySelector('[data-search-console-trend]');
    if (!chart) {
      return '';
    }
    var jsonId = chart.getAttribute('data-sc-trend-json-id');
    if (jsonId) {
      var node = document.getElementById(jsonId);
      if (node && node.textContent) {
        return node.textContent.trim();
      }
    }
    return chart.getAttribute('data-search-console-trend') || '';
  }

  function render(root) {
    (root || document).querySelectorAll(ROW_SEL).forEach(function (row) {
      if (row.closest('.hidden')) {
        return;
      }
      var periodDays = sparkPeriodDays(row);
      var trendRaw = resolveTrendRaw(row);
      var trend = { dates: [] };
      if (trendRaw) {
        try {
          trend = JSON.parse(trendRaw);
        } catch (err) {
          trend = { dates: [] };
        }
      }
      var seriesLen = trend && trend.dates ? trend.dates.length : 0;
      row.querySelectorAll(SPARK_SEL).forEach(function (spark) {
        var key = spark.getAttribute('data-kpi-spark-key') || 'clicks';
        var tone = spark.getAttribute('data-kpi-spark-tone') || 'flat';
        var barTarget = visibleBarCount(spark, periodDays, seriesLen);
        var sig = trendRaw + '|' + key + '|' + tone + '|' + periodDays + '|' + barTarget + '|' + spark.clientWidth;
        if (spark.dataset.kpiMiniSparkSig === sig && spark.dataset.kpiMiniSparkReady === '1') {
          return;
        }
        var series = trend && trend.dates && trend.dates.length ? trend[key] || [] : [];
        var sampled = downsampleSeries(series, barTarget || periodDays);
        if (!seriesHasSignal(sampled)) {
          renderPlaceholder(spark, tone, barTarget || periodDays);
          spark.dataset.kpiMiniSparkSig = sig + '|placeholder';
          spark.dataset.kpiMiniSparkReady = '1';
          return;
        }
        var max = 0;
        sampled.forEach(function (v) {
          if (v > max) {
            max = v;
          }
        });
        spark.classList.remove(
          'kpi-mini-spark--tone-up',
          'kpi-mini-spark--tone-down',
          'kpi-mini-spark--tone-flat',
          'kpi-mini-spark--placeholder'
        );
        spark.classList.add(toneClass(tone));
        var bars = document.createElement('div');
        bars.className = 'kpi-mini-spark__bars';
        var layoutCls = barsLayoutClass(sampled.length);
        if (layoutCls) {
          bars.classList.add(layoutCls);
        }
        bars.setAttribute('aria-hidden', 'true');
        var animStep = sampled.length > 60 ? 2 : sampled.length > 30 ? 3 : 4;
        sampled.forEach(function (v) {
          var bar = document.createElement('span');
          bar.className = 'kpi-mini-spark__bar';
          var pct = Math.max(8, Math.min(100, max > 0 ? (v / max) * 100 : 8));
          bar.style.setProperty('--kpi-spark-h', '4%');
          bar.dataset.kpiSparkTarget = String(pct);
          bars.appendChild(bar);
        });
        spark.textContent = '';
        spark.appendChild(bars);
        spark.dataset.kpiMiniSparkSig = sig;
        spark.dataset.kpiMiniSparkReady = '1';
        window.requestAnimationFrame(function () {
          bars.querySelectorAll('.kpi-mini-spark__bar').forEach(function (bar, idx) {
            window.setTimeout(function () {
              bar.style.setProperty('--kpi-spark-h', bar.dataset.kpiSparkTarget + '%');
            }, 6 + idx * animStep);
          });
        });
      });
    });
  }

  function clearCache(root) {
    (root || document).querySelectorAll(SPARK_SEL).forEach(function (el) {
      delete el.dataset.kpiMiniSparkReady;
      delete el.dataset.kpiMiniSparkSig;
    });
  }

  global.KpiMiniSpark = {
    render: render,
    clearCache: clearCache,
  };
  global.renderScKpiSparks = function (root) {
    render(root);
  };
})(window);
