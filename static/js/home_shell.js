/**
 * Ana sayfa container shell — aç/kapa (Notification / Crash / Drive ile aynı ok).
 * HTMX swap sonrası da çalışır (document delegation).
 */
(function () {
  "use strict";

  function setOpen(root, open) {
    if (!root) return;
    root.classList.toggle("is-open", open);
    var body = root.querySelector("[data-home-shell-body]");
    if (body) body.hidden = !open;
    root.querySelectorAll("[data-home-shell-toggle]").forEach(function (btn) {
      btn.setAttribute("aria-expanded", open ? "true" : "false");
    });
  }

  document.addEventListener("click", function (ev) {
    var btn = ev.target.closest("[data-home-shell-toggle]");
    if (!btn) return;
    var root = btn.closest("[data-home-shell-root]");
    if (!root) return;
    setOpen(root, !root.classList.contains("is-open"));
  });
})();
