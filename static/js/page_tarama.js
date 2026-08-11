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
    news: { id: "news", label: "News", kind: "bridge", path: "/sync-news?days=7", timeoutMs: 25 * 60 * 1000 },
    virgul: { id: "virgul", label: "Virgül", kind: "bridge", path: "/sync-virgul", timeoutMs: 30 * 60 * 1000 },
    market: { id: "market", label: "Market", kind: "bridge", path: "/sync-market", timeoutMs: 25 * 60 * 1000 },
    links: { id: "links", label: "Backlinks (GSC)", kind: "bridge", path: "/sync-gsc-links", timeoutMs: 40 * 60 * 1000 },
    policy: { id: "policy", label: "Ad Manager Policy", kind: "bridge", path: "/sync-policy", timeoutMs: 25 * 60 * 1000 },
    noads: { id: "noads", label: "Sinemalar noAds", kind: "bridge", path: "/sync-noads", timeoutMs: 20 * 60 * 1000 },
    seo: { id: "seo", label: "SEO audit", kind: "bridge", path: "/sync-seo-audit", timeoutMs: 50 * 60 * 1000 },
    errors: {
      id: "errors",
      label: "Errors / CSV scan",
      kind: "poll",
      startUrl: "/api/errors/refresh-all/start",
      progressUrl: "/api/errors/refresh-all/progress",
      timeoutMs: 40 * 60 * 1000,
    },
    alerts: {
      id: "alerts",
      label: "Alerts (Search Console)",
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
  var lastQuota = { remaining: 3, retry_after_sec: 0, limit: 3, message: "" };

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
          setStatus(job.label + " · bridge busy, waiting…");
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
          throw new Error("Scan timed out");
        }
        return fetchJson(job.progressUrl, { credentials: "same-origin" }, 20000).then(function (p) {
          var d = p.data || {};
          var pct = typeof d.pct === "number" ? d.pct : 0;
          var msg = d.current || (d.running ? "Scanning…" : "Done");
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
        setStatus(job.label + " · background scan " + Math.ceil(left / 1000) + "s");
        left -= tick;
        return sleep(tick).then(waitTick);
      }
      return waitTick();
    });
  }

  function fmtRetry(sec) {
    sec = Number(sec) || 0;
    if (sec <= 60) return "1 min";
    var mins = Math.round(sec / 60);
    if (mins < 60) return mins + " min";
    return Math.max(1, Math.round(mins / 60)) + " h";
  }

  function applyQuota(data) {
    if (!data) return;
    if (typeof data.remaining === "number") lastQuota.remaining = data.remaining;
    if (typeof data.retry_after_sec === "number") lastQuota.retry_after_sec = data.retry_after_sec;
    if (typeof data.limit === "number") lastQuota.limit = data.limit;
    if (data.message) lastQuota.message = data.message;
    var left = lastQuota.remaining;
    var title = left <= 0
      ? (lastQuota.message || ("At most " + lastQuota.limit + " scans per hour. "
        + fmtRetry(lastQuota.retry_after_sec) + " later."))
      : ("Run scans on this page · " + lastQuota.limit + " per hour, " + left + " left");
    document.querySelectorAll(".js-page-tarama").forEach(function (btn) {
      btn.title = title;
      if (!running) btn.disabled = left <= 0;
    });
  }

  function refreshQuota() {
    return fetchJson("/api/page-tarama/quota", { credentials: "same-origin" }, 12000).then(function (out) {
      if (out.resp && out.resp.ok) applyQuota(out.data);
    }).catch(function () {});
  }

  function setButtonsBusy(busy) {
    document.querySelectorAll(".js-page-tarama").forEach(function (btn) {
      btn.disabled = !!busy || lastQuota.remaining <= 0;
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

  function errDetail(data, fallback) {
    var d = data && data.detail;
    if (Array.isArray(d)) {
      d = (d[0] && (d[0].msg || d[0].message)) || fallback;
    }
    return d || (data && (data.message || data.status)) || fallback;
  }

  function claimManual(page) {
    return fetchJson("/api/page-tarama/manual", {
      method: "POST",
      credentials: "same-origin",
      headers: { "Content-Type": "application/json", Accept: "application/json" },
      body: JSON.stringify({ page: page }),
    }, 20000);
  }

  function pollQueue(initial, jobs, steps) {
    var runId = initial && initial.id;
    if (!runId) return Promise.reject(new Error("No queue id"));
    applyServerJobs(initial.jobs, steps, jobs);
    setStatus(initial.message || "Waiting for Mac bridge…");
    setBar(initial.pct || 4);
    var started = Date.now();
    function poll() {
      if (Date.now() - started > 3 * 60 * 60 * 1000) {
        throw new Error("Scan timed out");
      }
      return fetchJson("/api/page-tarama/progress?run_id=" + encodeURIComponent(runId), {
        credentials: "same-origin",
      }, 20000).then(function (p) {
        if (!p.resp.ok) {
          if ((p.resp.status === 404 || p.resp.status === 502 || p.resp.status === 503)
              && Date.now() - started < 90000) {
            setStatus("Waiting for queue…");
            return sleep(1500).then(poll);
          }
          throw new Error(errDetail(p.data, "Could not read queue (HTTP " + p.resp.status + ")"));
        }
        var d = p.data || {};
        applyServerJobs(d.jobs, steps, jobs);
        setBar(typeof d.pct === "number" ? d.pct : 10);
        setStatus(d.message || "Scanning…");
        if (d.running) return sleep(1200).then(poll);
        return d;
      });
    }
    return poll();
  }

  function runSequential(jobs, steps, fromIndex, opts) {
    var i = fromIndex || 0;
    var skipBridge = !!(opts && opts.skipBridge);
    var failed = 0;
    steps.forEach(function (s) {
      if (s.status === "fail") failed += 1;
    });
    function next() {
      if (i >= jobs.length) {
        var someOk = failed < jobs.length;
        finish(someOk, failed === 0
          ? "All scans finished — refreshing page…"
          : failed + " scan(s) failed" + (someOk ? " — loading successful ones…" : ". bridge --daemon must be running on the Mac."));
        return;
      }
      if (jobs[i].kind === "bridge") {
        if (skipBridge && steps[i].status !== "ok" && steps[i].status !== "fail") {
          failed += 1;
          steps[i].status = "fail";
          steps[i].detail = steps[i].detail || "Mac queue did not complete";
        }
        if (skipBridge || steps[i].status === "ok" || steps[i].status === "fail") {
          i += 1;
          next();
          return;
        }
      }
      var job = jobs[i];
      steps[i].status = "run";
      renderSteps(steps, i);
      setBar(((i + 0.15) / jobs.length) * 100);
      setStatus(job.label + " running…");
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
            msg = "Mac bridge unavailable (127.0.0.1:18765)";
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
      window.alert("No scan defined for this page.");
      return;
    }
    if (running) return;
    if (lastQuota.remaining <= 0) {
      window.alert(lastQuota.message || "At most 3 Update page runs per hour.");
      return;
    }
    running = true;
    setButtonsBusy(true);
    showOverlay(true);
    var steps = jobs.map(function (j) {
      return { label: j.label, status: "wait", detail: "" };
    });
    renderSteps(steps, 0);
    setBar(2);
    setStatus("Starting…");

    var bridgeJobs = jobs.filter(function (j) { return j.kind === "bridge"; });
    // Canlı panelde asla 127.0.0.1 köprüye düşme — diğer usersların tarayıcısı Mac’e ulaşamaz.
    claimManual(key).then(function (out) {
      applyQuota(out.data);
      if (!out.resp.ok) {
        finish(false, errDetail(out.data, "At most 3 scans per hour"));
        return;
      }
      var useQueue = !isLocalHost() && bridgeJobs.length > 0 && out.data.id;
      if (useQueue) {
        pollQueue(out.data, jobs, steps)
          .catch(function (err) {
            jobs.forEach(function (job, i) {
              if (job.kind !== "bridge" || steps[i].status === "ok") return;
              steps[i].status = "fail";
              steps[i].detail = ((err && err.message) || "Queue error").slice(0, 90);
            });
            renderSteps(steps, 0);
          })
          .then(function () {
            runSequential(jobs, steps, 0, { skipBridge: true });
          });
        return;
      }
      runSequential(jobs, steps, 0);
    }).catch(function (err) {
      finish(false, (err && err.message) || "Could not start queue");
    });
  }

  function ensureButton(root) {
    var page = root || document;
    page.querySelectorAll("[data-page-tarama-slot]").forEach(function (slot) {
      if (slot.querySelector(".js-page-tarama")) return;
      var btn = document.createElement("button");
      btn.type = "button";
      btn.className = "js-page-tarama inline-flex h-9 items-center gap-1.5 rounded-lg border border-sky-600 bg-sky-600 px-3 text-xs font-bold text-white shadow-sm hover:bg-sky-500 disabled:opacity-60";
      btn.title = "Run all scan sources on this page now";
      btn.innerHTML = '<svg class="h-3.5 w-3.5 shrink-0" fill="none" viewBox="0 0 24 24" stroke="currentColor" stroke-width="2" aria-hidden="true"><path stroke-linecap="round" stroke-linejoin="round" d="M16.023 9.348h4.992M2.985 19.644v-4.992m0 0h4.992m-4.993 0 3.181 3.183a8.25 8.25 0 0 0 13.803-3.7M4.031 9.865a8.25 8.25 0 0 1 13.803-3.7l3.181 3.182"/></svg><span class="pt-label-full">Update page</span><span class="pt-label-short">Update</span>';
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
    refreshQuota();
    document.body.addEventListener("htmx:afterSwap", function (ev) {
      var t = ev && ev.detail && ev.detail.target;
      ensureButton(t || document);
      applyQuota(lastQuota);
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
