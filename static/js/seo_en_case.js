/**
 * İngilizce etiketler: html lang=tr iken CSS uppercase her "i" → İ yapar.
 * Çözüm: metni en-US ile tamamen büyüt, text-transform kullanma.
 */
(function (g) {
  function normalizeLatin(str) {
    return String(str == null ? "" : str)
      .replace(/\u0130/g, "I")
      .replace(/\u0131/g, "i")
      .replace(/İ/g, "I")
      .replace(/ı/g, "i");
  }

  function seoEnUppercase(str) {
    return normalizeLatin(str).toLocaleUpperCase("en-US");
  }

  function shouldFixEnCapsEl(el) {
    if (!el || el.nodeType !== 1 || el.dataset.seoEnCapsApplied === "1") return false;
    if (el.getAttribute("lang") === "tr") return false;
    if (el.getAttribute("lang") === "en" || el.getAttribute("data-seo-en-caps") != null) {
      return true;
    }
    if (el.classList.contains("seo-en-caps")) return true;
    return false;
  }

  function applyEnCapsToElement(el) {
    if (!shouldFixEnCapsEl(el)) return;
    var raw = el.textContent;
    if (!raw || !String(raw).trim()) return;
    el.textContent = seoEnUppercase(raw);
    el.classList.remove("uppercase");
    el.classList.add("seo-en-caps");
    el.style.setProperty("text-transform", "none", "important");
    el.dataset.seoEnCapsApplied = "1";
  }

  function scanEnCapsRoot(root) {
    if (!root || !root.querySelectorAll) return;
    root.querySelectorAll('[lang="en"], [data-seo-en-caps], .seo-en-caps').forEach(applyEnCapsToElement);
    root.querySelectorAll("#mz-kpi-grid button > p:first-child").forEach(function (el) {
      if (el.dataset.seoEnCapsApplied === "1") return;
      el.textContent = seoEnUppercase(el.textContent);
      el.classList.remove("uppercase");
      el.classList.add("seo-en-caps");
      el.style.setProperty("text-transform", "none", "important");
      el.setAttribute("lang", "en");
      el.dataset.seoEnCapsApplied = "1";
    });
  }

  function scheduleScan(root) {
    g.requestAnimationFrame(function () {
      scanEnCapsRoot(root || document);
    });
  }

  g.seoEnUppercase = seoEnUppercase;
  g.seoEnCapsApply = applyEnCapsToElement;
  g.seoEnCapsScan = scanEnCapsRoot;

  if (typeof document !== "undefined") {
    if (document.readyState === "loading") {
      document.addEventListener("DOMContentLoaded", function () {
        scheduleScan(document);
      });
    } else {
      scheduleScan(document);
    }
    document.addEventListener("htmx:afterSettle", function (ev) {
      var t = ev && ev.detail && ev.detail.target;
      scheduleScan(t && t.nodeType === 1 ? t : document);
    });
  }
})(typeof window !== "undefined" ? window : globalThis);
