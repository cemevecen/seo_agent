/**
 * Sinemalar moderasyon — özet tablo + tıklanabilir drill-down.
 */
(function () {
  "use strict";

  var DATA_EL = document.getElementById("mod-panel-data");
  if (!DATA_EL) return;

  var RAW;
  try {
    RAW = JSON.parse(DATA_EL.textContent || "{}");
  } catch (e) {
    return;
  }

  var panel = document.getElementById("mod-drill-panel");
  var titleEl = document.getElementById("mod-drill-title");
  var subEl = document.getElementById("mod-drill-sub");
  var bodyEl = document.getElementById("mod-drill-body");
  var footEl = document.getElementById("mod-drill-foot");
  var closeBtn = document.getElementById("mod-drill-close");

  function esc(s) {
    return String(s || "").replace(/&/g, "&amp;").replace(/</g, "&lt;").replace(/"/g, "&quot;");
  }

  function loadDrill(userId, username, metricType, metricLabel, count) {
    if (!panel || !bodyEl) return;
    panel.classList.remove("hidden");
    if (titleEl) titleEl.textContent = username + " · " + metricLabel;
    if (subEl) subEl.textContent = (RAW.start || "") + " → " + (RAW.end || "") + " · " + count + " iş";
    bodyEl.innerHTML = '<tr><td colspan="4" class="px-4 py-8 text-center text-slate-400">Yükleniyor…</td></tr>';
    panel.scrollIntoView({ behavior: "smooth", block: "nearest" });

    var qs = new URLSearchParams({
      start: RAW.start || "",
      end: RAW.end || "",
      user_id: String(userId),
      metric_type: metricType,
      limit: "500",
    });

    fetch("/api/sinemalar-moderation/details?" + qs.toString(), { credentials: "same-origin" })
      .then(function (r) { return r.json(); })
      .then(function (data) {
        var items = data.items || [];
        if (!items.length) {
          bodyEl.innerHTML = '<tr><td colspan="4" class="px-4 py-8 text-center text-slate-400">Kayıt yok</td></tr>';
          if (footEl) footEl.textContent = "0 kayıt";
          return;
        }
        bodyEl.innerHTML = items.map(function (it) {
          var link = it.admin_url
            ? '<a href="' + esc(it.admin_url) + '" target="_blank" rel="noopener" class="text-sky-600 hover:underline">Panel</a>'
            : "—";
          return (
            "<tr class=\"border-b border-slate-100 dark:border-slate-800\">" +
            "<td class=\"px-3 py-1.5 font-mono text-[11px] whitespace-nowrap\">" + esc(it.event_at) + "</td>" +
            "<td class=\"px-3 py-1.5 max-w-[280px] truncate\" title=\"" + esc(it.title) + "\">" + esc(it.title || "—") + "</td>" +
            "<td class=\"px-3 py-1.5 max-w-[180px] truncate text-slate-500\">" + esc(it.subtitle || "—") + "</td>" +
            "<td class=\"px-3 py-1.5 text-center\">" + link + "</td>" +
            "</tr>"
          );
        }).join("");
        if (footEl) footEl.textContent = items.length + " / " + (data.total || items.length) + " kayıt";
      })
      .catch(function () {
        bodyEl.innerHTML = '<tr><td colspan="4" class="px-4 py-8 text-center text-rose-500">Yüklenemedi</td></tr>';
      });
  }

  document.querySelectorAll(".mod-count-btn").forEach(function (btn) {
    btn.addEventListener("click", function () {
      loadDrill(
        btn.getAttribute("data-user-id"),
        btn.getAttribute("data-username"),
        btn.getAttribute("data-metric"),
        btn.getAttribute("data-metric-label"),
        btn.getAttribute("data-count")
      );
    });
  });

  if (closeBtn && panel) {
    closeBtn.addEventListener("click", function () {
      panel.classList.add("hidden");
    });
  }
})();
