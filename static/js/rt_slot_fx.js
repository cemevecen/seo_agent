/**
 * Slot tarzı geçişler — realtime / home spark KPI ve bar güncellemeleri.
 */
(function (global) {
  'use strict';

  var DURATION_MS = 420;
  var DELTA_DURATION_MS = 520;

  function esc(s) {
    return String(s == null ? '' : s)
      .replace(/&/g, '&amp;')
      .replace(/</g, '&lt;')
      .replace(/>/g, '&gt;');
  }

  function parseNum(text) {
    if (text == null) return NaN;
    var s = String(text).replace(/\s/g, '').replace(/\./g, '').replace(',', '.');
    var m = s.match(/([\d.]+)\s*K/i);
    if (m) return parseFloat(m[1]) * 1000;
    m = s.match(/([\d.]+)/);
    return m ? parseFloat(m[1]) : NaN;
  }

  function setDeltaText(el, text, opts) {
    if (!el) return;
    opts = opts || {};
    text = text == null || text === '' ? '—' : String(text);
    var prev = el.getAttribute('data-rt-slot-cur');
    if (prev === text) return;

    el.classList.add('rt-slot-delta');
    el.setAttribute('data-rt-slot-cur', text);

    var tone = opts.tone || 'flat';
    el.classList.remove('rt-slot-delta-pulse--up', 'rt-slot-delta-pulse--down', 'rt-slot-delta-pulse--flat');
    el.classList.add(
      tone === 'up' ? 'rt-slot-delta-pulse--up' : tone === 'down' ? 'rt-slot-delta-pulse--down' : 'rt-slot-delta-pulse--flat'
    );
    void el.offsetWidth;
    el.classList.add('rt-slot-delta-pulse');

    if (prev == null) {
      el.textContent = text;
      window.setTimeout(function () {
        el.classList.remove('rt-slot-delta-pulse', 'rt-slot-delta-pulse--up', 'rt-slot-delta-pulse--down', 'rt-slot-delta-pulse--flat');
      }, DELTA_DURATION_MS);
      return;
    }

    var enterFrom = tone === 'down' ? '-100%' : '100%';
    var exitTo = tone === 'down' ? '100%' : '-100%';

    var stage = document.createElement('span');
    stage.className = 'rt-slot-delta-stage';
    stage.setAttribute('aria-hidden', 'true');

    var fromEl = document.createElement('span');
    fromEl.className = 'rt-slot-delta-from';
    fromEl.textContent = prev;

    var toEl = document.createElement('span');
    toEl.className = 'rt-slot-delta-to';
    toEl.textContent = text;
    toEl.style.transform = 'translateY(' + enterFrom + ')';

    stage.appendChild(fromEl);
    stage.appendChild(toEl);
    el.textContent = '';
    el.appendChild(stage);

    window.requestAnimationFrame(function () {
      fromEl.style.transform = 'translateY(' + exitTo + ')';
      fromEl.style.opacity = '0';
      toEl.style.transform = 'translateY(0)';
      toEl.style.opacity = '1';
    });

    window.setTimeout(function () {
      el.textContent = text;
      el.classList.remove('rt-slot-delta-pulse', 'rt-slot-delta-pulse--up', 'rt-slot-delta-pulse--down', 'rt-slot-delta-pulse--flat');
    }, DELTA_DURATION_MS);
  }

  function setText(el, text) {
    if (!el) return;
    if (el.hasAttribute('data-rt-slot-delta') || el.classList.contains('rt-slot-delta')) {
      setDeltaText(el, text, {});
      return;
    }
    text = text == null || text === '' ? '—' : String(text);
    var prev = el.getAttribute('data-rt-slot-cur');
    if (prev === text) return;

    if (prev == null && !el.classList.contains('rt-slot-text')) {
      el.classList.add('rt-slot-text');
      el.textContent = '';
      text.split('').forEach(function (ch) {
        var col = document.createElement('span');
        col.className = 'rt-slot-col';
        col.innerHTML = '<span class="rt-slot-strip"><span class="rt-slot-ch">' + esc(ch) + '</span></span>';
        el.appendChild(col);
      });
      el.setAttribute('data-rt-slot-cur', text);
      return;
    }

    var prevNum = parseNum(prev);
    var nextNum = parseNum(text);
    var dir = 1;
    if (!isNaN(prevNum) && !isNaN(nextNum) && nextNum < prevNum) dir = -1;

    el.setAttribute('data-rt-slot-cur', text);
    if (!el.classList.contains('rt-slot-text')) {
      el.classList.add('rt-slot-text');
      el.textContent = '';
    }

    var cols = Array.prototype.slice.call(el.querySelectorAll('.rt-slot-col'));
    var chars = text.split('');

    while (cols.length < chars.length) {
      var colNew = document.createElement('span');
      colNew.className = 'rt-slot-col';
      colNew.innerHTML =
        '<span class="rt-slot-strip"><span class="rt-slot-ch">' + esc(chars[cols.length]) + '</span></span>';
      el.appendChild(colNew);
      cols.push(colNew);
    }
    while (cols.length > chars.length) {
      el.removeChild(cols.pop());
    }

    chars.forEach(function (ch, idx) {
      var col = cols[idx];
      var strip = col.querySelector('.rt-slot-strip');
      if (!strip) return;
      var cur = strip.querySelector('.rt-slot-ch');
      if (cur && cur.textContent === ch) return;

      var oldCh = cur ? cur.textContent : '';
      strip.innerHTML = '';
      if (oldCh) {
        var oldEl = document.createElement('span');
        oldEl.className = 'rt-slot-ch';
        oldEl.textContent = oldCh;
        strip.appendChild(oldEl);
      }
      var newEl = document.createElement('span');
      newEl.className = 'rt-slot-ch';
      newEl.textContent = ch;
      strip.appendChild(newEl);

      strip.classList.add('rt-slot-anim');
      strip.style.transform = 'translateY(0)';
      void strip.offsetWidth;
      strip.style.transform = dir < 0 ? 'translateY(100%)' : 'translateY(-100%)';

      (function (stripRef, chRef) {
        window.setTimeout(function () {
          stripRef.innerHTML = '<span class="rt-slot-ch">' + esc(chRef) + '</span>';
          stripRef.classList.remove('rt-slot-anim');
          stripRef.style.transform = '';
        }, DURATION_MS);
      })(strip, ch);
    });
  }

  function barHtml(spec, opts) {
    opts = opts || {};
    var barClass = opts.barClass || 'rt-spark-mini-bar';
    var i = spec.i;
    if (spec.empty || spec.hPx <= 2) {
      return (
        '<div class="' +
        barClass +
        ' rt-spark-mini-bar--empty rt-slot-bar" data-rt-bar-i="' +
        i +
        '" style="height:2px;background:' +
        esc(spec.bg || 'transparent') +
        ';opacity:0.45"></div>'
      );
    }
    var extra = spec.extraClass ? ' ' + spec.extraClass : '';
    return (
      '<div class="' +
      barClass +
      ' rt-slot-bar' +
      extra +
      '" data-rt-bar-i="' +
      i +
      '" style="height:' +
      spec.hPx +
      'px;background:' +
      esc(spec.bg) +
      '"></div>'
    );
  }

  function renderBars(barsEl, specs, opts) {
    if (!barsEl || !specs) return;
    opts = opts || {};
    var barClass = opts.barClass || 'rt-spark-mini-bar';
    var ready = barsEl.getAttribute('data-slot-bars-ready') === '1';
    var existing = {};
    barsEl.querySelectorAll('[data-rt-bar-i]').forEach(function (node) {
      var bar = node.classList.contains(barClass) ? node : node.querySelector('.' + barClass.split(' ')[0]);
      if (!bar) bar = node;
      var idx = bar.getAttribute('data-rt-bar-i');
      if (idx != null) existing[idx] = bar;
    });

    if (!ready || !Object.keys(existing).length) {
      barsEl.innerHTML = specs.map(function (s) { return barHtml(s, opts); }).join('');
      barsEl.setAttribute('data-slot-bars-ready', '1');
      return;
    }

    var nextIdx = {};
    specs.forEach(function (spec) {
      nextIdx[String(spec.i)] = spec;
    });

    Object.keys(existing).forEach(function (idx) {
      if (!nextIdx[idx]) {
        var oldBar = existing[idx];
        oldBar.classList.add('rt-slot-bar-exit');
        (function (el) {
          window.setTimeout(function () {
            if (el.parentNode) el.parentNode.removeChild(el);
          }, 240);
        })(oldBar);
      }
    });

    specs.forEach(function (spec) {
      var key = String(spec.i);
      var bar = existing[key];
      if (bar) {
        bar.classList.remove('rt-slot-bar-exit', 'rt-slot-bar-enter');
        if (spec.empty || spec.hPx <= 2) {
          bar.classList.add('rt-spark-mini-bar--empty');
          bar.style.height = '2px';
          bar.style.background = spec.bg || 'transparent';
          bar.style.opacity = '0.45';
        } else {
          bar.classList.remove('rt-spark-mini-bar--empty');
          bar.style.height = spec.hPx + 'px';
          bar.style.background = spec.bg;
          bar.style.opacity = '';
        }
        return;
      }
      var wrap = document.createElement('div');
      wrap.innerHTML = barHtml(spec, opts);
      var newBar = wrap.firstChild;
      newBar.classList.add('rt-slot-bar-enter');
      barsEl.appendChild(newBar);
      window.setTimeout(function () {
        newBar.classList.remove('rt-slot-bar-enter');
      }, DURATION_MS);
    });

    var kids = Array.prototype.slice.call(barsEl.children);
    specs.forEach(function (spec, order) {
      var key = String(spec.i);
      var bar = barsEl.querySelector('[data-rt-bar-i="' + key + '"]');
      if (bar && bar.parentNode === barsEl) {
        barsEl.appendChild(bar);
      }
    });
  }

  function snapshotHome(root) {
    var snap = {};
    if (!root) return snap;
    root.querySelectorAll('[data-home-rt-key]').forEach(function (card) {
      var key = card.getAttribute('data-home-rt-key');
      if (!key) return;
      var valEl = card.querySelector('[data-rt-slot-value]');
      var deltaEl = card.querySelector('[data-rt-slot-delta]');
      var line = card.querySelector('.home-rt-spark-line');
      snap[key] = {
        value: valEl ? valEl.getAttribute('data-rt-slot-cur') || valEl.textContent : '',
        delta: deltaEl ? deltaEl.getAttribute('data-rt-slot-cur') || deltaEl.textContent : '',
        spark: line ? line.getAttribute('d') || '' : '',
      };
    });
    root.querySelectorAll('[data-home-rt-total]').forEach(function (el) {
      var tk = 'total-' + el.getAttribute('data-home-rt-total');
      snap[tk] = { value: el.getAttribute('data-rt-slot-cur') || el.textContent || '' };
    });
    return snap;
  }

  function animateHome(root, prevSnap) {
    if (!root || !prevSnap) return;
    root.querySelectorAll('[data-home-rt-key]').forEach(function (card) {
      var key = card.getAttribute('data-home-rt-key');
      var prev = prevSnap[key];
      if (!prev) return;
      var valEl = card.querySelector('[data-rt-slot-value]');
      var deltaEl = card.querySelector('[data-rt-slot-delta]');
      if (valEl) {
        var newVal = (valEl.textContent || '').trim();
        if (prev.value && newVal && prev.value !== newVal) {
          valEl.removeAttribute('data-rt-slot-cur');
          valEl.classList.remove('rt-slot-text');
          valEl.textContent = prev.value;
          setText(valEl, newVal);
        } else {
          valEl.setAttribute('data-rt-slot-cur', newVal);
        }
      }
      if (deltaEl) {
        var newDelta = (deltaEl.textContent || '').trim();
        if (prev.delta && newDelta && prev.delta !== newDelta) {
          deltaEl.removeAttribute('data-rt-slot-cur');
          deltaEl.classList.remove('rt-slot-text', 'rt-slot-delta');
          deltaEl.textContent = prev.delta;
          var tone = 'flat';
          if (newDelta.indexOf('↑') !== -1 || newDelta.indexOf('+') === 0) tone = 'up';
          else if (newDelta.indexOf('↓') !== -1 || newDelta.indexOf('-') === 0) tone = 'down';
          setDeltaText(deltaEl, newDelta, { tone: tone });
        } else {
          deltaEl.setAttribute('data-rt-slot-cur', newDelta);
        }
      }
      var line = card.querySelector('.home-rt-spark-line');
      var sparkWrap = card.querySelector('.home-rt-spark');
      if (sparkWrap && line && prev.spark && line.getAttribute('d') !== prev.spark) {
        sparkWrap.classList.remove('rt-slot-spark-in');
        sparkWrap.classList.add('rt-slot-spark-fade');
        window.setTimeout(function () {
          sparkWrap.classList.remove('rt-slot-spark-fade');
          sparkWrap.classList.add('rt-slot-spark-in');
          window.setTimeout(function () {
            sparkWrap.classList.remove('rt-slot-spark-in');
          }, DURATION_MS);
        }, 180);
      }
    });
    root.querySelectorAll('[data-home-rt-total]').forEach(function (el) {
      var tk = 'total-' + el.getAttribute('data-home-rt-total');
      var prev = prevSnap[tk];
      if (!prev || !prev.value) return;
      var newVal = (el.textContent || '').trim();
      if (newVal && prev.value !== newVal) {
        el.removeAttribute('data-rt-slot-cur');
        el.classList.remove('rt-slot-text');
        el.textContent = prev.value;
        setText(el, newVal);
      } else {
        el.setAttribute('data-rt-slot-cur', newVal);
      }
    });
  }

  global.RtSlotFx = {
    setText: setText,
    setDeltaText: setDeltaText,
    renderBars: renderBars,
    snapshotHome: snapshotHome,
    animateHome: animateHome,
    barHtml: barHtml,
  };
})(typeof window !== 'undefined' ? window : this);
