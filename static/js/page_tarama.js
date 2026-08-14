/**
 * Sayfa tarama — üstteki «Sayfayı güncelle» tüm Mac köprü / API taramalarını sırayla çalıştırır.
 * Progress: iş sayısı, kuyruk, aktif adım, alt-adım, süre — ayrıntılı canlı panel.
 */
(function () {
  "use strict";

  var BRIDGE = "http://127.0.0.1:18765";

  var JOBS = {
    play: { id: "play", label: "Play Console", kind: "bridge", path: "/sync-play", timeoutMs: 90 * 60 * 1000 },
    play_vitals: { id: "play_vitals", label: "Android Vitals", kind: "bridge", path: "/sync-play-vitals", timeoutMs: 45 * 60 * 1000 },
    asc: { id: "asc", label: "App Store Connect", kind: "bridge", path: "/sync-asc", timeoutMs: 90 * 60 * 1000 },
    firebase: { id: "firebase", label: "Firebase Console", kind: "bridge", path: "/sync-firebase", timeoutMs: 40 * 60 * 1000, progressPath: "/firebase-progress" },
    cwv: { id: "cwv", label: "Web Vitals (GSC)", kind: "bridge", path: "/sync-gsc-cwv", timeoutMs: 35 * 60 * 1000, progressPath: "/gsc-cwv-progress", body: { mode: "shots" } },
    notification: { id: "notification", label: "Notification", kind: "bridge", path: "/sync", timeoutMs: 20 * 60 * 1000, progressPath: "/nt-progress" },
    news: { id: "news", label: "News", kind: "bridge", path: "/sync-news?days=7", timeoutMs: 25 * 60 * 1000, progressPath: "/news-progress" },
    virgul: { id: "virgul", label: "Virgül", kind: "bridge", path: "/sync-virgul", timeoutMs: 30 * 60 * 1000 },
    revenue_targets: {
      id: "revenue_targets",
      label: "Virgül Targets",
      kind: "bridge",
      path: "/sync-revenue-targets",
      timeoutMs: 25 * 60 * 1000,
    },
    market: { id: "market", label: "Market", kind: "bridge", path: "/sync-market", timeoutMs: 25 * 60 * 1000 },
    links: { id: "links", label: "Backlinks (GSC)", kind: "bridge", path: "/sync-gsc-links", timeoutMs: 40 * 60 * 1000 },
    policy: { id: "policy", label: "Ad Manager Policy", kind: "bridge", path: "/sync-policy", timeoutMs: 25 * 60 * 1000 },
    noads: { id: "noads", label: "Sinemalar noAds", kind: "bridge", path: "/sync-noads", timeoutMs: 20 * 60 * 1000 },
    moderation: { id: "moderation", label: "Sinemalar Moderation", kind: "bridge", path: "/sync-sinemalar-moderation?which=yesterday", timeoutMs: 60 * 60 * 1000 },
    empower_intel: { id: "empower_intel", label: "Empower Intel (Döviz)", kind: "bridge", path: "/sync-empower-intel?mode=yesterday", timeoutMs: 45 * 60 * 1000 },
    empower_intel_sinemalar: { id: "empower_intel_sinemalar", label: "Empower Intel (Sinemalar)", kind: "bridge", path: "/sync-empower-intel-sinemalar?mode=yesterday", timeoutMs: 45 * 60 * 1000 },
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
    virgul: ["virgul", "revenue_targets"],
    notification: ["notification"],
    firebase: ["firebase"],
    app: ["play", "asc", "firebase"],
    vitals: ["cwv"],
    alerts: ["alerts"],
    seo: ["seo"],
    backlinks: ["links"],
    policy: ["policy", "noads"],
    moderation: ["moderation"],
    "empower-sinemalar": ["empower_intel_sinemalar"],
    "x-data": ["empower_intel"],
    errors: ["errors"],
  };

  var running = false;
  var lastQuota = { remaining: 3, retry_after_sec: 0, limit: 3, message: "", unlimited: false };
  var activeSteps = [];
  var activeTotal = 0;
  var runStartedAt = 0;
  var elapsedTimer = null;
  var lastProgressSnap = null;

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

  function escapeHtml(s) {
    return String(s || "")
      .replace(/&/g, "&amp;").replace(/</g, "&lt;").replace(/>/g, "&gt;");
  }

  function fmtElapsed(sec) {
    sec = Math.max(0, Math.floor(Number(sec) || 0));
    if (sec < 60) return sec + "s";
    var m = Math.floor(sec / 60);
    var s = sec % 60;
    if (m < 60) return m + "m " + (s < 10 ? "0" : "") + s + "s";
    var h = Math.floor(m / 60);
    m = m % 60;
    return h + "h " + (m < 10 ? "0" : "") + m + "m";
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

  function stopElapsedTimer() {
    if (elapsedTimer) {
      clearInterval(elapsedTimer);
      elapsedTimer = null;
    }
  }

  function startElapsedTimer() {
    stopElapsedTimer();
    runStartedAt = Date.now();
    elapsedTimer = setInterval(function () {
      paintMeta(lastProgressSnap);
    }, 1000);
  }

  function paintMeta(snap) {
    var meta = $("pc-page-tarama-meta");
    if (!meta) return;
    var elapsed = snap && typeof snap.elapsed_sec === "number"
      ? snap.elapsed_sec
      : Math.floor((Date.now() - (runStartedAt || Date.now())) / 1000);
    var page = (snap && snap.page) || pageKey() || "page";
    var bridge = "";
    if (snap && snap.bridge_age_sec != null) {
      bridge = " · last activity " + fmtElapsed(snap.bridge_age_sec) + " ago";
    } else if (snap && snap.running && snap.bridge_seen_at == null) {
      bridge = " · waiting for scan";
    }
    meta.textContent = "Page «" + page + "» · elapsed " + fmtElapsed(elapsed) + bridge;
  }

  function setProgressUI(opts) {
    opts = opts || {};
    var done = Math.max(0, Number(opts.done) || 0);
    var total = Math.max(0, Number(opts.total) || 0);
    var waiting = Math.max(0, Number(opts.waiting != null ? opts.waiting : Math.max(0, total - done - (opts.runningJob ? 1 : 0))));
    var pct = typeof opts.pct === "number" ? opts.pct : (total > 0 ? Math.round((100 * done) / total) : 0);
    pct = Math.max(0, Math.min(100, Math.round(pct)));
    var step = opts.step;
    var totalSteps = opts.totalSteps;
    var subPct = 0;
    if (totalSteps && totalSteps > 0 && step != null) {
      subPct = Math.max(0, Math.min(100, Math.round((100 * Number(step)) / Number(totalSteps))));
    } else if (opts.runningJob) {
      subPct = 8;
    }

    var bar = $("pc-page-tarama-bar");
    var subbar = $("pc-page-tarama-subbar");
    var pctEl = $("pc-page-tarama-pct");
    var countsEl = $("pc-page-tarama-counts");
    if (bar) {
      bar.style.width = pct + "%";
      bar.setAttribute("aria-valuenow", String(pct));
      bar.setAttribute("aria-valuemax", "100");
      bar.setAttribute("aria-valuetext", pct + "% · " + done + "/" + total + " jobs");
    }
    if (subbar) subbar.style.width = subPct + "%";
    if (pctEl) pctEl.textContent = pct + "%";
    if (countsEl) {
      var bits = [done + " done"];
      if (opts.doneOk != null || opts.doneFail != null) {
        bits = [];
        if (opts.doneOk) bits.push(opts.doneOk + " ok");
        if (opts.doneFail) bits.push(opts.doneFail + " fail");
        if (!bits.length) bits.push("0 done");
      }
      if (opts.runningJob) bits.push("1 running");
      bits.push(waiting + " waiting");
      bits.push(total + " total");
      countsEl.textContent = bits.join(" · ");
    }

    var statusEl = $("pc-page-tarama-status");
    var detailEl = $("pc-page-tarama-detail");
    var queueEl = $("pc-page-tarama-queue");
    if (statusEl) statusEl.textContent = opts.status || "";
    if (detailEl) {
      var dParts = [];
      if (opts.platform) dParts.push("Platform: " + opts.platform);
      if (opts.phase) dParts.push("Phase: " + opts.phase);
      if (step != null && totalSteps) dParts.push("Sub-step " + step + "/" + totalSteps);
      if (opts.detail) dParts.push(opts.detail);
      detailEl.textContent = dParts.length ? dParts.join(" · ") : "—";
    }
    if (queueEl) {
      var q = opts.waitingLabels || [];
      queueEl.textContent = q.length
        ? ("Queue (" + q.length + "): " + q.join(" → "))
        : (waiting > 0 ? ("Queue: " + waiting + " job(s)") : "Queue: empty");
    }
    paintMeta(opts.snap || lastProgressSnap);
  }

  function countFinished(steps) {
    var n = 0;
    (steps || []).forEach(function (s) {
      if (s && (s.status === "ok" || s.status === "fail")) n += 1;
    });
    return n;
  }

  function countByStatus(steps, st) {
    var n = 0;
    (steps || []).forEach(function (s) {
      if (s && s.status === st) n += 1;
    });
    return n;
  }

  function syncProgressFromSteps(steps, total, extra) {
    extra = extra || {};
    var done = countFinished(steps);
    var runningJob = (steps || []).some(function (s) { return s && s.status === "run"; });
    var waitingLabels = [];
    (steps || []).forEach(function (s) {
      if (s && s.status === "wait") waitingLabels.push(s.label);
    });
    var cur = null;
    (steps || []).forEach(function (s) {
      if (s && s.status === "run") cur = s;
    });
    var step = cur && cur.step != null ? cur.step : extra.step;
    var totalSteps = cur && cur.totalSteps != null ? cur.totalSteps : extra.totalSteps;
    var pct = total > 0 ? (100 * done) / total : 0;
    if (runningJob && totalSteps && totalSteps > 0 && step != null) {
      pct = (100 * (done + Math.min(0.99, Number(step) / Number(totalSteps)))) / total;
    } else if (runningJob && total > 0) {
      pct = Math.min(99, (100 * done) / total + 8);
    }
    setProgressUI({
      done: done,
      total: total != null ? total : (steps || []).length,
      waiting: countByStatus(steps, "wait"),
      waitingLabels: waitingLabels,
      runningJob: runningJob,
      doneOk: countByStatus(steps, "ok"),
      doneFail: countByStatus(steps, "fail"),
      pct: pct,
      step: step,
      totalSteps: totalSteps,
      status: extra.status || (cur ? (cur.label + " running…") : "Starting…"),
      detail: extra.detail || (cur && cur.detail) || "",
      phase: extra.phase || (cur && cur.phase) || "",
      platform: extra.platform || (cur && cur.platform) || "",
      snap: lastProgressSnap,
    });
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
      var mark = st === "ok" ? "✓" : st === "fail" ? "!" : st === "run" ? "▸" : "·";
      var cls = st === "ok" ? "text-emerald-600 dark:text-emerald-400"
        : st === "fail" ? "text-rose-600 dark:text-rose-400"
        : st === "run" ? "text-sky-700 dark:text-sky-300 font-semibold"
        : "text-slate-400 dark:text-zinc-500";
      var badge = st === "ok" ? "done" : st === "fail" ? "fail" : st === "run" ? "running" : "waiting";
      var sub = [];
      if (s.platform) sub.push(s.platform);
      if (s.phase) sub.push(s.phase);
      if (s.step != null && s.totalSteps) sub.push(s.step + "/" + s.totalSteps);
      if (s.detail) sub.push(s.detail);
      var extra = sub.length
        ? '<span class="mt-0.5 block text-[10px] font-normal opacity-75">' + escapeHtml(sub.join(" · ")) + "</span>"
        : "";
      return '<div class="flex items-start justify-between gap-2 rounded-lg px-2 py-1.5 ' +
        (st === "run" ? "bg-sky-50 dark:bg-sky-950/30 " : "") + cls + '">'
        + '<div class="min-w-0 flex-1">'
        + '<span class="truncate">' + escapeHtml(mark + " " + s.label) + "</span>"
        + extra
        + "</div>"
        + '<span class="shrink-0 rounded px-1.5 py-0.5 text-[9px] font-bold uppercase tracking-wide opacity-80">'
        + escapeHtml(badge) + "</span></div>";
    }).join("");
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

  function applyBridgeLiveProgress(job, steps, index, data) {
    if (!data || !steps[index]) return;
    var s = steps[index];
    if (data.step != null) s.step = data.step;
    if (data.total_steps != null) s.totalSteps = data.total_steps;
    if (data.phase) s.phase = data.phase;
    if (data.platform) s.platform = data.platform;
    var msg = data.message || data.sub_label || data.current || "";
    if (msg) s.detail = String(msg).slice(0, 160);
    renderSteps(steps, index);
    syncProgressFromSteps(steps, steps.length, {
      status: job.label + (msg ? (" · " + msg) : " running…"),
      detail: s.detail,
      phase: s.phase,
      platform: s.platform,
      step: s.step,
      totalSteps: s.totalSteps,
    });
  }

  function pollBridgeProgressWhile(job, steps, index, untilPromise) {
    if (!job.progressPath || !isLocalHost()) return untilPromise;
    var stop = false;
    untilPromise.then(function () { stop = true; }, function () { stop = true; });
    function tick() {
      if (stop) return;
      fetchJson(BRIDGE + job.progressPath, { mode: "cors" }, 8000)
        .then(function (out) {
          if (stop) return;
          if (out.resp.ok && out.data) applyBridgeLiveProgress(job, steps, index, out.data);
        })
        .catch(function () {})
        .then(function () {
          if (!stop) return sleep(1200).then(tick);
        });
    }
    tick();
    return untilPromise;
  }

  /** Arka planda başlayan köprü işleri (CWV/SEO/…) bitene kadar progress poll. */
  function waitBridgeUntilIdle(job, steps, index) {
    if (!job.progressPath) {
      return Promise.resolve({ ok: true, message: "Started (no progress endpoint)" });
    }
    var started = Date.now();
    var maxMs = Math.max(60000, Number(job.timeoutMs) || 90 * 60 * 1000);
    function tick() {
      if (Date.now() - started > maxMs) {
        throw new Error("Scan timed out — try again later");
      }
      return fetchJson(BRIDGE + job.progressPath, { mode: "cors" }, 8000).then(
        function (out) {
          var d = (out && out.data) || {};
          if (!(out.resp && out.resp.ok)) return sleep(2000).then(tick);
          applyBridgeLiveProgress(job, steps, index, d);
          if (d.running) return sleep(1500).then(tick);
          if (d.phase === "error" || d.ok === false) {
            throw new Error(d.message || (job.label + " failed"));
          }
          return d;
        },
        function () {
          return sleep(2000).then(tick);
        }
      );
    }
    return sleep(800).then(tick);
  }

  function runBridge(job, steps, index) {
    var url = BRIDGE + job.path;
    var tries = 0;
    function attempt() {
      tries += 1;
      var opts = { method: "POST", mode: "cors" };
      if (job.body && typeof job.body === "object") {
        opts.headers = { "Content-Type": "application/json", Accept: "application/json" };
        opts.body = JSON.stringify(job.body);
      }
      return fetchJson(url, opts, job.timeoutMs).then(function (out) {
        if (out.resp.status === 409 && tries < 10) {
          setStatus(job.label + " · scan busy, waiting…");
          return sleep(12000).then(attempt);
        }
        if (!out.resp.ok || out.data.ok === false) {
          var msg = (out.data && (out.data.message || out.data.detail)) || ("HTTP " + out.resp.status);
          throw new Error(msg);
        }
        return out.data;
      });
    }
    return attempt().then(function (data) {
      // /sync-gsc-cwv vb. hemen {started:true} döner — bitene kadar bekle
      if (data && data.started && job.progressPath) {
        return waitBridgeUntilIdle(job, steps, index).then(function (prog) {
          return Object.assign({}, data, prog || {}, {
            ok: true,
            message: (prog && prog.message) || data.message || "Done",
          });
        });
      }
      return data;
    });
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
          var msg = d.current || (d.running ? "Scanning…" : "Done");
          setProgressUI({
            done: Number(d.done) || 0,
            total: Number(d.total) || activeTotal || 1,
            waiting: Math.max(0, (Number(d.total) || 0) - (Number(d.done) || 0) - (d.running ? 1 : 0)),
            runningJob: !!d.running,
            pct: typeof d.pct === "number" ? d.pct : undefined,
            status: job.label + " · " + msg,
            detail: d.detail || (d.total ? ((d.done || 0) + "/" + d.total + " items") : ""),
            step: d.done,
            totalSteps: d.total,
            snap: lastProgressSnap,
          });
          if (d.error) throw new Error(d.error);
          if (d.running) return sleep(1500).then(poll);
          return d;
        });
      }
      return poll();
    });
  }

  function runJob(job, steps, index) {
    var p;
    if (job.kind === "bridge") {
      p = pollBridgeProgressWhile(job, steps, index, runBridge(job, steps, index));
    } else if (job.kind === "poll") {
      p = runPoll(job);
    } else {
      p = runApi(job);
    }
    if (!job.waitAfterMs) return p;
    return p.then(function (data) {
      var left = job.waitAfterMs;
      var tick = 2000;
      function waitTick() {
        if (left <= 0) return data;
        setStatus(job.label + " · background settle " + Math.ceil(left / 1000) + "s left");
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
    lastQuota.unlimited = !!data.unlimited;
    var left = lastQuota.remaining;
    var title = lastQuota.unlimited
      ? (lastQuota.message || "Unlimited Update page (admin)")
      : left <= 0
      ? (lastQuota.message || ("At most " + lastQuota.limit + " scans per hour. "
        + fmtRetry(lastQuota.retry_after_sec) + " later."))
      : ("Run scans on this page · " + lastQuota.limit + " per hour, " + left + " left");
    document.querySelectorAll(".js-page-tarama").forEach(function (btn) {
      btn.title = title;
      if (!running) btn.disabled = !lastQuota.unlimited && left <= 0;
    });
  }

  function refreshQuota() {
    return fetchJson("/api/page-tarama/quota", { credentials: "same-origin" }, 12000).then(function (out) {
      if (out.resp && out.resp.ok) applyQuota(out.data);
    }).catch(function () {});
  }

  function setButtonsBusy(busy) {
    document.querySelectorAll(".js-page-tarama").forEach(function (btn) {
      btn.disabled = !!busy || (!lastQuota.unlimited && lastQuota.remaining <= 0);
    });
  }

  function finish(ok, summary) {
    running = false;
    stopElapsedTimer();
    setButtonsBusy(false);
    setStatus(summary);
    if (activeTotal > 0) {
      syncProgressFromSteps(activeSteps, activeTotal, { status: summary });
    }
    var closeBtn = $("pc-page-tarama-close");
    // Başarısızda da kısa süre sonra kapat+yenile — popup asılı kalmasın
    if (closeBtn) closeBtn.classList.toggle("hidden", !!ok);
    setTimeout(function () {
      showOverlay(false);
      try {
        window.location.reload();
      } catch (e) {}
    }, ok ? 1200 : 2800);
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
      steps[i].detail = (s.detail || s.sub_label || "").slice(0, 160);
      steps[i].phase = s.phase || "";
      steps[i].platform = s.platform || "";
      if (s.step != null) steps[i].step = s.step;
      if (s.total_steps != null) steps[i].totalSteps = s.total_steps;
      if (st === "run") current = i;
    });
    renderSteps(steps, current);
  }

  function applyServerProgress(d, steps, jobs) {
    lastProgressSnap = d;
    applyServerJobs(d.jobs, steps, jobs);
    setProgressUI({
      done: d.done,
      total: d.total,
      waiting: d.waiting,
      waitingLabels: d.waiting_labels || [],
      runningJob: !!(d.current_job_id),
      doneOk: d.done_ok,
      doneFail: d.done_fail,
      pct: d.pct,
      step: d.current_step,
      totalSteps: d.current_total_steps,
      status: d.message || "Scanning…",
      detail: d.current_detail || d.current_sub_label || "",
      phase: d.current_phase || "",
      platform: d.current_platform || "",
      snap: d,
    });
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
    applyServerProgress(initial, steps, jobs);
    var started = Date.now();
    function poll() {
      if (Date.now() - started > 3 * 60 * 60 * 1000) {
        throw new Error("Scan timed out");
      }
      return fetchJson("/api/page-tarama/progress?run_id=" + encodeURIComponent(runId), {
        credentials: "same-origin",
      }, 60000).then(function (p) {
        if (!p.resp.ok) {
          if ((p.resp.status === 404 || p.resp.status === 502 || p.resp.status === 503)
              && Date.now() - started < 90000) {
            setStatus("Waiting for queue…");
            return sleep(1200).then(poll);
          }
          throw new Error(errDetail(p.data, "Could not read queue (HTTP " + p.resp.status + ")"));
        }
        var d = p.data || {};
        applyServerProgress(d, steps, jobs);
        // Bridge 90sn+ sessiz ve iş hâlâ queued → daemon claim etmiyor
        var bridgeAge = d.bridge_age_sec;
        var elapsedMs = Date.now() - started;
        if (d.running && elapsedMs > 100000
            && typeof bridgeAge === "number" && bridgeAge >= 90) {
          var stillQueued = (d.jobs || []).some(function (j) {
            return j && j.kind === "bridge" && (j.status === "queued" || j.status === "wait");
          });
          if (stillQueued) {
            throw new Error(
              "Automatic scan offline (last activity " + Math.round(bridgeAge) + "s ago). Try Update page later."
            );
          }
        }
        if (d.running) return sleep(900).then(poll);
        return d;
      }).catch(function (err) {
        // Railway yavaş / AbortController — tek poll hatasında tüm taramayı kesme
        var msg = (err && err.message) ? String(err.message) : String(err || "");
        if (/abort|timeout|Failed to fetch|NetworkError|Load failed|network/i.test(msg)
            && Date.now() - started < 3 * 60 * 60 * 1000) {
          setStatus("Queue slow — retrying…");
          return sleep(2500).then(poll);
        }
        throw err;
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
        syncProgressFromSteps(steps, jobs.length);
        var someOk = failed < jobs.length;
        finish(someOk, failed === 0
          ? "All scans finished — refreshing page…"
          : failed + " scan(s) failed" + (someOk ? " — loading successful ones…" : ". Try Update page again later."));
        return;
      }
      if (jobs[i].kind === "bridge") {
        if (skipBridge && steps[i].status !== "ok" && steps[i].status !== "fail") {
          failed += 1;
          steps[i].status = "fail";
          steps[i].detail = steps[i].detail || "Scan did not complete";
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
      syncProgressFromSteps(steps, jobs.length, {
        status: "Job " + (i + 1) + "/" + jobs.length + " · " + job.label + " starting…",
      });
      runJob(job, steps, i)
        .then(function (data) {
          steps[i].status = "ok";
          steps[i].detail = (data && (data.message || data.status)) ? String(data.message || data.status).slice(0, 120) : "Done";
        })
        .catch(function (err) {
          failed += 1;
          steps[i].status = "fail";
          var msg = (err && err.message) ? err.message : String(err);
          if (/Failed to fetch|NetworkError|Load failed|abort|aborted/i.test(msg)) {
            msg = "Scan unavailable — try again later";
          }
          steps[i].detail = msg.slice(0, 140);
        })
        .then(function () {
          renderSteps(steps, i);
          syncProgressFromSteps(steps, jobs.length);
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
    if (!lastQuota.unlimited && lastQuota.remaining <= 0) {
      window.alert(lastQuota.message || "At most 3 Update page runs per hour.");
      return;
    }
    running = true;
    setButtonsBusy(true);
    showOverlay(true);
    startElapsedTimer();
    var steps = jobs.map(function (j) {
      return { label: j.label, status: "wait", detail: "", phase: "", platform: "" };
    });
    activeSteps = steps;
    activeTotal = jobs.length;
    lastProgressSnap = { page: key, elapsed_sec: 0 };
    renderSteps(steps, 0);
    setProgressUI({
      done: 0,
      total: jobs.length,
      waiting: jobs.length,
      waitingLabels: jobs.map(function (j) { return j.label; }),
      runningJob: false,
      pct: 0,
      status: "Starting " + jobs.length + " scan job(s) for «" + key + "»…",
      detail: "Jobs: " + jobs.map(function (j) { return j.label; }).join(", "),
      snap: lastProgressSnap,
    });

    var bridgeJobs = jobs.filter(function (j) { return j.kind === "bridge"; });
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
              steps[i].detail = ((err && err.message) || "Queue error").slice(0, 140);
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
