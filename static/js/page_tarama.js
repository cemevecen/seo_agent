/**
 * Sayfa tarama — üstteki «Sayfayı güncelle» tüm Mac köprü / API taramalarını sırayla çalıştırır.
 */
(function () {
  "use strict";

  var BRIDGE = "http://127.0.0.1:18765";

  var JOBS = {
    play: { id: "play", label: "Play Console", kind: "bridge", path: "/sync-play", timeoutMs: 90 * 60 * 1000 },
    asc: { id: "asc", label: "App Store Connect", kind: "bridge", path: "/sync-asc", timeoutMs: 90 * 60 * 1000 },
    firebase: { id: "firebase", label: "Firebase Console", kind: "bridge", path: "/sync-firebase", timeoutMs: 40 * 60 * 1000 },
    cwv: { id: "cwv", label: "Web Vitals (GSC)", kind: "bridge", path: "/sync-gsc-cwv", timeoutMs: 90 * 60 * 1000 },
    notification: { id: "notification", label: "Notification", kind: "bridge", path: "/sync", timeoutMs: 20 * 60 * 1000 },
    news: { id: "news", label: "Haberler", kind: "bridge", path: "/sync-news?days=7", timeoutMs: 25 * 60 * 1000 },
    virgul: { id: "virgul", label: "Virgül", kind: "bridge", path: "/sync-virgul", timeoutMs: 30 * 60 * 1000 },
    market: { id: "market", label: "Piyasa", kind: "bridge", path: "/sync-market", timeoutMs: 25 * 60 * 1000 },
    links: { id: "links", label: "Backlinks (GSC)", kind: "bridge", path: "/sync-gsc-links", timeoutMs: 40 * 60 * 1000 },
    policy: { id: "policy", label: "Ad Manager Policy", kind: "bridge", path: "/sync-policy", timeoutMs: 25 * 60 * 1000 },
    noads: { id: "noads", label: "Sinemalar noAds", kind: "bridge", path: "/sync-noads", timeoutMs: 20 * 60 * 1000 },
    seo: { id: "seo", label: "SEO denetim", kind: "bridge", path: "/sync-seo-audit", timeoutMs: 50 * 60 * 1000 },
    errors: {
      id: "errors",
      label: "Hata / CSV tarama",
      kind: "poll",
      startUrl: "/api/errors/refresh-all/start",
      progressUrl: "/api/errors/refresh-all/progress",
      timeoutMs: 40 * 60 * 1000,
    },
    alerts: {
      id: "alerts",
      label: "Uyarılar (Search Console)",
      kind: "api",
      url: "/alerts/refresh",
      timeoutMs: 8 * 60 * 1000,
      waitAfterMs: 45000,
    },
  };

  var PAGES = {
    home: ["play", "asc", "firebase", "cwv", "notification", "virgul", "market"],
    android: ["play", "firebase", "market"],
    ios: ["asc", "firebase"],
    news: ["news"],
    virgul: ["virgul"],
    notification: ["notification"],
    firebase: ["firebase"],
    app: ["play", "asc", "firebase"],
    vitals: ["cwv"],
    alerts: ["alerts"],
    seo: ["seo"],
    "s-firebase": ["firebase"],
    backlinks: ["links"],
    policy: ["policy", "noads"],
    errors: ["errors"],
  };

  var running = false;

  function pageKey() {
    var el = document.querySelector("[data-page-tarama]");
    return el ? (el.getAttribute("data-page-tarama") || "").trim() : "";
  }

  function jobsFor(key) {
    var ids = PAGES[key] || [];
    return ids.map(function (id) { return JOBS[id]; }).filter(Boolean);
  }

  function $(id) {
    return document.getElementById(id);
  }

  function showOverlay(show) {
    var el = $("pc-page-tarama-overlay");
    if (!el) return;
    el.classList.toggle("hidden", !show);
    el.style.display = show ? "flex" : "none";
    el.setAttribute("aria-hidden", show ? "false" : "true");
    var closeBtn = $("pc-page-tarama-close");
    if (closeBtn && show) closeBtn.classList.add("hidden");
  }

  function setBar(pct) {
    var bar = $("pc-page-tarama-bar");
    var pctEl = $("pc-page-tarama-pct");
    if (bar) bar.style.width = Math.max(0, Math.min(100, pct)) + "%";
    if (pctEl) pctEl.textContent = Math.round(pct) + "%";
  }

  function setStatus(text) {
    var el = $("pc-page-tarama-status");
    if (el) el.textContent = text || "";
  }

  function renderSteps(steps, current) {
    var box = $("pc-page-tarama-steps");
    if (!box) return;
    box.innerHTML = steps.map(function (s, i) {
      var st = s.status || (i < current ? "ok" : i === current ? "run" : "wait");
      var mark = st === "ok" ? "✓" : st === "fail" ? "!" : st === "run" ? "…" : "·";
      var cls = st === "ok" ? "text-emerald-600 dark:text-emerald-400"
        : st === "fail" ? "text-rose-600 dark:text-rose-400"
        : st === "run" ? "text-sky-600 dark:text-sky-300 font-semibold"
        : "text-slate-400 dark:text-zinc-500";
      var extra = s.detail ? '<span class="ml-1 opacity-70">' + escapeHtml(s.detail) + "</span>" : "";
      return '<div class="flex items-start justify-between gap-2 ' + cls + '">'
        + '<span class="truncate">' + escapeHtml(mark + " " + s.label) + extra + "</span></div>";
    }).join("");
  }

  function escapeHtml(s) {
    return String(s || "")
      .replace(/&/g, "&amp;").replace(/</g, "&lt;").replace(/>/g, "&gt;");
  }

  function sleep(ms) {
    return new Promise(function (resolve) { setTimeout(resolve, ms); });
  }

  function fetchJson(url, opts, timeoutMs) {
    opts = opts || {};
    var ctrl = typeof AbortController !== "undefined" ? new AbortController() : null;
    var t = null;
    if (ctrl && timeoutMs) {
      t = setTimeout(function () { ctrl.abort(); }, timeoutMs);
    }
    var headers = Object.assign({ Accept: "application/json" }, opts.headers || {});
    return fetch(url, {
      method: opts.method || "GET",
      credentials: opts.credentials || "omit",
      mode: opts.mode,
      headers: headers,
      body: opts.body,
      signal: ctrl ? ctrl.signal : undefined,
    }).then(function (resp) {
      return resp.json().then(function (data) {
        return { resp: resp, data: data || {} };
      }).catch(function () {
        return { resp: resp, data: {} };
      });
    }).finally(function () {
      if (t) clearTimeout(t);
    });
  }

  function runBridge(job) {
    var url = BRIDGE + job.path;
    var tries = 0;
    function attempt() {
      tries += 1;
      return fetchJson(url, { method: "POST", mode: "cors" }, job.timeoutMs).then(function (out) {
        if (out.resp.status === 409 && tries < 10) {
          setStatus(job.label + " · köprü meşgul, bekleniyor…");
          return sleep(12000).then(attempt);
        }
        if (!out.resp.ok || out.data.ok === false) {
          var msg = (out.data && (out.data.message || out.data.detail)) || ("HTTP " + out.resp.status);
          throw new Error(msg);
        }
        return out.data;
      });
    }
    return attempt();
  }

  function runApi(job) {
    return fetchJson(job.url, {
      method: "POST",
      credentials: "same-origin",
    }, job.timeoutMs).then(function (out) {
      if (!out.resp.ok) {
        throw new Error((out.data && (out.data.message || out.data.detail)) || ("HTTP " + out.resp.status));
      }
      return out.data;
    });
  }

  function runPoll(job) {
    return fetchJson(job.startUrl, {
      method: "POST",
      credentials: "same-origin",
    }, 30000).then(function (out) {
      if (!out.resp.ok && out.resp.status !== 409) {
        throw new Error((out.data && (out.data.message || out.data.status)) || ("HTTP " + out.resp.status));
      }
      var started = Date.now();
      function poll() {
        if (Date.now() - started > job.timeoutMs) {
          throw new Error("Tarama zaman aşımı");
        }
        return fetchJson(job.progressUrl, { credentials: "same-origin" }, 20000).then(function (p) {
          var d = p.data || {};
          var pct = typeof d.pct === "number" ? d.pct : 0;
          var msg = d.current || (d.running ? "Taranıyor…" : "Tamamlandı");
          setStatus(job.label + " · " + msg + (d.total ? (" " + (d.done || 0) + "/" + d.total) : ""));
          if (d.error) throw new Error(d.error);
          if (d.running) return sleep(1500).then(poll);
          return d;
        });
      }
      return poll();
    });
  }

  function runJob(job) {
    var p = job.kind === "bridge" ? runBridge(job) : job.kind === "poll" ? runPoll(job) : runApi(job);
    if (!job.waitAfterMs) return p;
    return p.then(function (data) {
      var left = job.waitAfterMs;
      var tick = 2000;
      function waitTick() {
        if (left <= 0) return data;
        setStatus(job.label + " · arka plan tarama " + Math.ceil(left / 1000) + "s");
        left -= tick;
        return sleep(tick).then(waitTick);
      }
      return waitTick();
    });
  }

  function setButtonsBusy(busy) {
    document.querySelectorAll(".js-page-tarama").forEach(function (btn) {
      btn.disabled = !!busy;
    });
  }

  function finish(ok, summary) {
    running = false;
    setButtonsBusy(false);
    setStatus(summary);
    setBar(100);
    var closeBtn = $("pc-page-tarama-close");
    if (closeBtn) closeBtn.classList.toggle("hidden", !!ok);
    setTimeout(function () {
      if (ok) {
        showOverlay(false);
        try {
          window.location.reload();
        } catch (e) {}
      }
    }, ok ? 1200 : 0);
  }

  function isLocalHost() {
    var h = (location.hostname || "").toLowerCase();
    return h === "localhost" || h === "127.0.0.1";
  }

  function applyServerJobs(serverJobs, steps, jobs) {
    var byId = {};
    (serverJobs || []).forEach(function (j) { byId[j.id] = j; });
    var current = 0;
    jobs.forEach(function (job, i) {
      if (job.kind !== "bridge") return;
      var s = byId[job.id];
      if (!s) return;
      var st = s.status === "ok" ? "ok" : s.status === "fail" ? "fail"
        : (s.status === "claimed" || s.status === "running") ? "run" : "wait";
      steps[i].status = st;
      steps[i].detail = (s.detail || "").slice(0, 90);
      if (st === "run") current = i;
    });
    renderSteps(steps, current);
  }

  function runViaQueue(key, jobs, steps) {
    return fetchJson("/api/page-tarama/start", {
      method: "POST",
      credentials: "same-origin",
      headers: { "Content-Type": "application/json", Accept: "application/json" },
      body: JSON.stringify({ page: key }),
    }, 20000).then(function (out) {
      if (!out.resp.ok) {
        throw new Error((out.data && out.data.detail) || "Kuyruk başlatılamadı");
      }
      var runId = out.data.id;
      applyServerJobs(out.data.jobs, steps, jobs);
      setStatus(out.data.message || "Mac köprü bekleniyor…");
      setBar(out.data.pct || 4);
      function poll() {
        return fetchJson("/api/page-tarama/progress?run_id=" + encodeURIComponent(runId), {
          credentials: "same-origin",
        }, 20000).then(function (p) {
          var d = p.data || {};
          applyServerJobs(d.jobs, steps, jobs);
          setBar(typeof d.pct === "number" ? d.pct : 10);
          setStatus(d.message || "Taranıyor…");
          if (d.running) return sleep(1200).then(poll);
          return d;
        });
      }
      return poll();
    });
  }

  function runSequential(jobs, steps, fromIndex) {
    var i = fromIndex || 0;
    var failed = 0;
    steps.forEach(function (s) {
      if (s.status === "fail") failed += 1;
    });
    function next() {
      if (i >= jobs.length) {
        var someOk = failed < jobs.length;
        finish(someOk, failed === 0
          ? "Tüm taramalar bitti — sayfa yenileniyor…"
          : failed + " tarama hata verdi" + (someOk ? " — başarılı olanlar yükleniyor…" : ". Mac’te bridge --daemon açık olmalı."));
        return;
      }
      if (jobs[i].kind === "bridge" && (steps[i].status === "ok" || steps[i].status === "fail")) {
        i += 1;
        next();
        return;
      }
      var job = jobs[i];
      steps[i].status = "run";
      renderSteps(steps, i);
      setBar(((i + 0.15) / jobs.length) * 100);
      setStatus(job.label + " çalışıyor…");
      runJob(job)
        .then(function (data) {
          steps[i].status = "ok";
          steps[i].detail = (data && (data.message || data.status)) ? String(data.message || data.status).slice(0, 80) : "";
        })
        .catch(function (err) {
          failed += 1;
          steps[i].status = "fail";
          var msg = (err && err.message) ? err.message : String(err);
          if (/Failed to fetch|NetworkError|Load failed|abort/i.test(msg)) {
            msg = "Mac köprü yok (127.0.0.1:18765)";
          }
          steps[i].detail = msg.slice(0, 90);
        })
        .then(function () {
          renderSteps(steps, i);
          setBar(((i + 1) / jobs.length) * 100);
          i += 1;
          next();
        });
    }
    next();
  }

  function start(key) {
    key = key || pageKey();
    var jobs = jobsFor(key);
    if (!jobs.length) {
      window.alert("Bu sayfa için tarama tanımı yok.");
      return;
    }
    if (running) return;
    running = true;
    setButtonsBusy(true);
    showOverlay(true);
    var steps = jobs.map(function (j) {
      return { label: j.label, status: "wait", detail: "" };
    });
    renderSteps(steps, 0);
    setBar(2);
    setStatus("Başlatılıyor…");

    var bridgeJobs = jobs.filter(function (j) { return j.kind === "bridge"; });
    var useQueue = !isLocalHost() && bridgeJobs.length > 0;
    var chain = Promise.resolve();
    if (useQueue) {
      chain = runViaQueue(key, jobs, steps).catch(function (err) {
        jobs.forEach(function (job, i) {
          if (job.kind !== "bridge" || steps[i].status === "ok") return;
          steps[i].status = "fail";
          steps[i].detail = ((err && err.message) || "Kuyruk hatası").slice(0, 90);
        });
        renderSteps(steps, 0);
      });
    }
    chain.then(function () {
      runSequential(jobs, steps, 0);
    });
  }

  function ensureButton(root) {
    var page = root || document;
    page.querySelectorAll("[data-page-tarama-slot]").forEach(function (slot) {
      if (slot.querySelector(".js-page-tarama")) return;
      var btn = document.createElement("button");
      btn.type = "button";
      btn.className = "js-page-tarama inline-flex h-9 items-center gap-1.5 rounded-lg border border-sky-600 bg-sky-600 px-3 text-xs font-bold text-white shadow-sm hover:bg-sky-500 disabled:opacity-60";
      btn.title = "Bu sayfadaki tüm tarama kaynaklarını şimdi çalıştır";
      btn.innerHTML = '<svg class="h-3.5 w-3.5 shrink-0" fill="none" viewBox="0 0 24 24" stroke="currentColor" stroke-width="2" aria-hidden="true"><path stroke-linecap="round" stroke-linejoin="round" d="M16.023 9.348h4.992M2.985 19.644v-4.992m0 0h4.992m-4.993 0 3.181 3.183a8.25 8.25 0 0 0 13.803-3.7M4.031 9.865a8.25 8.25 0 0 1 13.803-3.7l3.181 3.182"/></svg><span class="pt-label-full">Sayfayı güncelle</span><span class="pt-label-short">Güncelle</span>';
      slot.appendChild(btn);
    });
  }

  document.addEventListener("click", function (ev) {
    if (ev.target && ev.target.id === "pc-page-tarama-close") {
      ev.preventDefault();
      showOverlay(false);
      return;
    }
    var btn = ev.target && ev.target.closest && ev.target.closest(".js-page-tarama");
    if (!btn) return;
    ev.preventDefault();
    var host = btn.closest("[data-page-tarama]");
    start(host ? host.getAttribute("data-page-tarama") : pageKey());
  });

  document.addEventListener("pc:page-refresh", function (ev) {
    var key = pageKey();
    if (!key || !PAGES[key]) return;
    if (ev && ev.preventDefault) ev.preventDefault();
    start(key);
  });

  function init() {
    ensureButton(document);
    document.body.addEventListener("htmx:afterSwap", function (ev) {
      var t = ev && ev.detail && ev.detail.target;
      ensureButton(t || document);
    });
  }

  if (document.readyState === "loading") {
    document.addEventListener("DOMContentLoaded", init);
  } else {
    init();
  }

  window.__pcPageTaramaStart = start;
  window.__pcPageTaramaPages = PAGES;
})();
