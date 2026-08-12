(function () {
  function initSearchConsoleTables(root) {
    (root || document).querySelectorAll("[data-search-console-table]").forEach(function (table) {
      if (table.closest(".hidden")) return;
      if (table.dataset.tableReady === "1") return;
      table.dataset.tableReady = "1";
      var tbody = table.querySelector("tbody");
      if (!tbody) return;
      var buttons = table.querySelectorAll("thead [data-sort-key]");
      buttons.forEach(function (button) {
        button.addEventListener("click", function () {
          var key = button.dataset.sortKey;
          var type = button.dataset.sortType || "string";
          var currentDirection = button.dataset.sortDirection === "asc" ? "asc" : "desc";
          var nextDirection = currentDirection === "asc" ? "desc" : "asc";
          buttons.forEach(function (item) {
            item.dataset.sortDirection = "";
          });
          button.dataset.sortDirection = nextDirection;
          var groups = [];
          Array.from(tbody.children).forEach(function (row) {
            if (row.hasAttribute("data-sc-sort-row") || row.dataset.query) {
              var group = [row];
              var next = row.nextElementSibling;
              if (next && next.hasAttribute("data-sc-extra-row-error")) {
                group.push(next);
              }
              groups.push({ row: row, nodes: group });
            }
          });
          groups.sort(function (leftGroup, rightGroup) {
            var left = leftGroup.row;
            var right = rightGroup.row;
            var leftValue = left.dataset[key];
            var rightValue = right.dataset[key];
            if (type === "number") {
              var leftNumber = parseFloat(leftValue);
              var rightNumber = parseFloat(rightValue);
              var leftNa = Number.isNaN(leftNumber);
              var rightNa = Number.isNaN(rightNumber);
              if (leftNa && rightNa) return 0;
              if (leftNa) return 1;
              if (rightNa) return -1;
              return nextDirection === "asc" ? leftNumber - rightNumber : rightNumber - leftNumber;
            }
            if (type === "date") {
              var leftDate = Date.parse(leftValue || "") || 0;
              var rightDate = Date.parse(rightValue || "") || 0;
              if (!leftDate && !rightDate) return 0;
              if (!leftDate) return 1;
              if (!rightDate) return -1;
              return nextDirection === "asc" ? leftDate - rightDate : rightDate - leftDate;
            }
            return nextDirection === "asc"
              ? String(leftValue || "").localeCompare(String(rightValue || ""), "tr")
              : String(rightValue || "").localeCompare(String(leftValue || ""), "tr");
          });
          groups.forEach(function (group) {
            group.nodes.forEach(function (node) {
              tbody.appendChild(node);
            });
          });
        });
      });
    });
  }

  function scExtraSetTableExpanded(root, expanded) {
    if (!root) return;
    root.querySelectorAll("[data-sc-extra-row-more]").forEach(function (tr) {
      tr.classList.toggle("sc-extra-row-visible", expanded);
    });
    var expandBtn = root.querySelector("[data-sc-extra-expand]");
    var collapseBtn = root.querySelector("[data-sc-extra-collapse]");
    if (expandBtn) expandBtn.classList.toggle("hidden", expanded);
    if (collapseBtn) collapseBtn.classList.toggle("hidden", !expanded);
    if (expanded) root.dataset.scExtraTableExpanded = "1";
    else delete root.dataset.scExtraTableExpanded;
  }

  function scExtraExpandTable(btn) {
    if (!btn) return;
    scExtraSetTableExpanded(btn.closest("[data-sc-extra-card]"), true);
  }

  function scExtraCollapseTable(btn) {
    if (!btn) return;
    var root = btn.closest("[data-sc-extra-card]");
    scExtraSetTableExpanded(root, false);
    var wrap = root && root.querySelector("[data-sc-extra-table]");
    if (wrap) wrap.scrollIntoView({ behavior: "smooth", block: "nearest" });
  }

  function onScExtrasSwap(ev) {
    var target = ev && ev.detail && ev.detail.target;
    if (!target) return;
    var root = document.getElementById("dn-sc-google-news");
    if (!root) return;
    if (target !== root && !root.contains(target)) return;
    initSearchConsoleTables(root);
    root.querySelectorAll("[data-sc-extra-card]").forEach(function (card) {
      if (card.dataset.scExtraTableExpanded === "1") scExtraSetTableExpanded(card, true);
    });
  }

  window.initSearchConsoleTables = initSearchConsoleTables;
  window.scExtraExpandTable = scExtraExpandTable;
  window.scExtraCollapseTable = scExtraCollapseTable;

  document.body.addEventListener("htmx:afterSwap", onScExtrasSwap);
  document.body.addEventListener("htmx:afterSettle", onScExtrasSwap);
})();
