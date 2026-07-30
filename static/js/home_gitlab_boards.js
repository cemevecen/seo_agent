/**
 * Ana sayfa git.nokta — boards ile aynı kanban (tüm issue'lar, yıldız yok).
 * Veri: /api/boards/project-bundle · DnD: /api/boards/move + /api/boards/reorder
 */
(function (global) {
  function homeGitlabBoards() {
    return {
      configProjects: [],
      activeProject: '',
      projectData: {},
      boardOrders: {},
      statusMsg: '',
      _wired: false,

      init() {
        if (this._wired) return;
        this._wired = true;
        try {
          var node = document.getElementById('home-gl-projects');
          this.configProjects = node ? JSON.parse(node.textContent || '[]') : [];
        } catch (e) {
          this.configProjects = [];
        }
        var root = this.$el;
        var def =
          (root && root.getAttribute('data-default-project')) ||
          (this.configProjects[0] && this.configProjects[0].path) ||
          'nokta/doviz';
        this.activeProject = def;
        this.ensureProject(def);
        this.fetchProject(def, false);
      },

      ensureProject(path) {
        if (!this.projectData[path]) {
          this.projectData[path] = {
            loading: false,
            loaded: false,
            error: null,
            board: null,
            lists: [],
            issues: [],
          };
        }
      },

      pd() {
        return this.projectData[this.activeProject] || null;
      },

      setActive(path) {
        this.activeProject = path;
        this.ensureProject(path);
        var pd = this.projectData[path];
        if (!pd.loaded && !pd.loading) this.fetchProject(path, false);
      },

      platformLabel() {
        var p = this.configProjects.find(function (x) {
          return x.path === this.activeProject;
        }.bind(this));
        return (p && p.platform) || '';
      },

      listColor(index, isClosed) {
        if (isClosed) return 'border-t-emerald-500';
        var colors = [
          'border-t-slate-400',
          'border-t-sky-500',
          'border-t-indigo-500',
          'border-t-amber-500',
          'border-t-fuchsia-500',
          'border-t-rose-500',
        ];
        return colors[index % colors.length];
      },

      listKey(lst) {
        if (!lst) return '';
        if (lst.isClosed) return '__closed__';
        if (lst.isVirtual) return '__open__';
        return (lst.label && lst.label.name) || String(lst.id || '');
      },

      labelNames(issue) {
        if (!issue || !issue.labels) return [];
        return (issue.labels || [])
          .map(function (l) {
            return typeof l === 'string' ? l : (l && l.name) || '';
          })
          .filter(Boolean);
      },

      getRawIssuesForList(path, lst) {
        var pd = this.projectData[path];
        if (!pd || !pd.issues) return [];
        var boardLabels = ((pd.board && pd.board.lists) || [])
          .map(function (l) {
            return l.label && l.label.name;
          })
          .filter(Boolean);

        if (lst.isClosed) {
          return pd.issues.filter(function (i) {
            return i.state === 'closed';
          });
        }
        if (lst.isVirtual) {
          return pd.issues.filter(function (i) {
            if (i.state === 'closed') return false;
            var labs = this.labelNames(i);
            return !labs.some(function (n) {
              return boardLabels.indexOf(n) >= 0;
            });
          }.bind(this));
        }
        var name = lst.label && lst.label.name;
        return pd.issues.filter(function (i) {
          if (i.state === 'closed') return false;
          return this.labelNames(i).indexOf(name) >= 0;
        }.bind(this));
      },

      sortIssuesForDisplay(path, listKey, issues) {
        var order = (this.boardOrders[path] && this.boardOrders[path][listKey]) || [];
        if (!order.length) {
          return issues.slice().sort(function (a, b) {
            var pa = a.relative_position != null ? a.relative_position : Number.MAX_SAFE_INTEGER;
            var pb = b.relative_position != null ? b.relative_position : Number.MAX_SAFE_INTEGER;
            if (pa !== pb) return pa - pb;
            return (a.id || 0) - (b.id || 0);
          });
        }
        var rank = {};
        order.forEach(function (iid, idx) {
          rank[String(iid)] = idx;
        });
        return issues.slice().sort(function (a, b) {
          var ra = rank[String(a.iid)];
          var rb = rank[String(b.iid)];
          if (ra != null && rb != null) return ra - rb;
          if (ra != null) return -1;
          if (rb != null) return 1;
          return (a.id || 0) - (b.id || 0);
        });
      },

      issuesFor(lst) {
        return this.sortIssuesForDisplay(
          this.activeProject,
          this.listKey(lst),
          this.getRawIssuesForList(this.activeProject, lst)
        );
      },

      async loadBoardOrders(path) {
        try {
          var res = await fetch('/api/boards/order?project_path=' + encodeURIComponent(path), {
            credentials: 'same-origin',
            cache: 'no-store',
          });
          if (!res.ok) return;
          var data = await res.json();
          this.boardOrders[path] = data.orders || {};
        } catch (e) {
          console.warn('home board order load failed', e);
        }
      },

      async saveColumnOrder(path, listKey, issueIids) {
        if (!path || !listKey) return;
        var iids = (issueIids || []).map(function (x) {
          return parseInt(x, 10);
        }).filter(function (n) {
          return !isNaN(n);
        });
        if (!this.boardOrders[path]) this.boardOrders[path] = {};
        this.boardOrders[path][listKey] = iids;
        await fetch('/api/boards/order', {
          method: 'PUT',
          credentials: 'same-origin',
          headers: { 'Content-Type': 'application/json' },
          body: JSON.stringify({
            project_path: path,
            list_key: listKey,
            issue_iids: iids,
          }),
        });
      },

      async fetchProject(path, force) {
        this.ensureProject(path);
        var pd = this.projectData[path];
        if (pd.loading) return;
        if (pd.loaded && !force) return;
        pd.loading = true;
        pd.error = null;
        this.statusMsg = '';
        try {
          await this.loadBoardOrders(path);
          var res = await fetch(
            '/api/boards/project-bundle?project_path=' + encodeURIComponent(path),
            { credentials: 'same-origin', cache: 'no-store' }
          );
          var data = await res.json().catch(function () {
            return {};
          });
          if (!res.ok || data.error) {
            throw new Error(data.error || ('HTTP ' + res.status));
          }
          var board = data.board;
          var actualLists = (board.lists || []).map(function (l) {
            return Object.assign({}, l, {
              isVirtual: false,
              isClosed: false,
            });
          });
          actualLists.unshift({
            id: 'virtual-open',
            label: { name: 'Open (Backlog)' },
            isVirtual: true,
            isClosed: false,
          });
          actualLists.push({
            id: 'virtual-closed',
            label: { name: 'Closed' },
            isVirtual: true,
            isClosed: true,
          });
          pd.board = board;
          pd.lists = actualLists;
          pd.issues = [].concat(data.opened_issues || [], data.closed_issues || []);
          pd.loaded = true;
          if (force) this.statusMsg = 'Güncellendi · ' + pd.issues.length + ' madde';
        } catch (err) {
          console.error(err);
          pd.error = (err && err.message) || 'Board yüklenemedi';
          this.statusMsg = pd.error;
        } finally {
          pd.loading = false;
        }
      },

      refreshActive() {
        if (!this.activeProject) return;
        this.projectData[this.activeProject].loaded = false;
        this.fetchProject(this.activeProject, true);
      },

      iidsFromListEl(listEl) {
        return Array.from(listEl.querySelectorAll('[data-id]'))
          .map(function (el) {
            return parseInt(el.getAttribute('data-id') || '', 10);
          })
          .filter(function (n) {
            return !isNaN(n);
          });
      },

      getReorderParamsFromList(listEl, issueIid) {
        var items = Array.from(listEl.querySelectorAll('[data-id]'));
        var idx = items.findIndex(function (el) {
          return String(el.getAttribute('data-id')) === String(issueIid);
        });
        if (idx === -1) return null;
        var params = {};
        function globalId(el) {
          var g = el && el.getAttribute('data-global-id');
          return g ? parseInt(g, 10) : null;
        }
        if (idx === 0) {
          if (items[1]) {
            var nextId = globalId(items[1]);
            if (nextId != null) params.move_before_id = nextId;
          }
        } else {
          var afterId = globalId(items[idx - 1]);
          if (afterId != null) params.move_after_id = afterId;
        }
        return params;
      },

      async apiMove(projectPath, issueIid, payload) {
        var res = await fetch('/api/boards/move', {
          method: 'POST',
          credentials: 'same-origin',
          headers: { 'Content-Type': 'application/json' },
          body: JSON.stringify(Object.assign({ project_path: projectPath, issue_iid: Number(issueIid) }, payload)),
        });
        var data = await res.json().catch(function () {
          return {};
        });
        return data.issue || null;
      },

      async apiReorder(projectPath, issueIid, params) {
        var res = await fetch('/api/boards/reorder', {
          method: 'POST',
          credentials: 'same-origin',
          headers: { 'Content-Type': 'application/json' },
          body: JSON.stringify({
            project_path: projectPath,
            issue_iid: Number(issueIid),
            move_after_id: params.move_after_id,
            move_before_id: params.move_before_id,
          }),
        });
        var data = await res.json().catch(function () {
          return {};
        });
        return data.issue || null;
      },

      applyIssueUpdate(pd, updated) {
        if (!updated || updated.iid == null) return;
        var idx = pd.issues.findIndex(function (i) {
          return String(i.iid) === String(updated.iid);
        });
        if (idx >= 0) pd.issues[idx] = Object.assign({}, pd.issues[idx], updated);
      },

      initSortable(el) {
        if (!el || el._homeGlSortable) return;
        if (typeof Sortable === 'undefined') return;
        var self = this;
        el._homeGlSortable = Sortable.create(el, {
          group: 'home-gl-shared',
          animation: 150,
          ghostClass: 'sortable-ghost',
          dragClass: 'sortable-drag',
          draggable: '[data-id]',
          onStart: function () {
            document.querySelectorAll('.home-gl-board .home-gl-sortable').forEach(function (l) {
              l.classList.add('is-dragging-over');
            });
          },
          onEnd: async function (evt) {
            document.querySelectorAll('.home-gl-board .home-gl-sortable').forEach(function (l) {
              l.classList.remove('is-dragging-over');
            });
            var itemEl = evt.item;
            var fromList = evt.from;
            var toList = evt.to;
            var issueIid = itemEl.getAttribute('data-id');
            var projectPath = toList.getAttribute('data-project');
            var pd = self.projectData[projectPath];
            if (!pd) return;
            var crossColumn = fromList !== toList;
            var isToVirtual = toList.getAttribute('data-virtual') === 'true';
            var isFromVirtual = fromList.getAttribute('data-virtual') === 'true';
            var isToClosed = toList.getAttribute('data-closed') === 'true';
            var isFromClosed = fromList.getAttribute('data-closed') === 'true';
            var fromLabel = fromList.getAttribute('data-label');
            var toLabel = toList.getAttribute('data-label');
            var toListKey = toList.getAttribute('data-list-key') || '';
            var fromListKey = fromList.getAttribute('data-list-key') || '';
            var reorderedInColumn = !crossColumn && evt.oldIndex !== evt.newIndex;

            try {
              if (crossColumn) {
                var boardLabels = ((pd.board && pd.board.lists) || [])
                  .map(function (l) {
                    return l.label && l.label.name;
                  })
                  .filter(Boolean);
                var issue = pd.issues.find(function (i) {
                  return String(i.iid) === String(issueIid);
                });
                var labelsToRemove = [];
                if (issue && issue.labels) {
                  labelsToRemove = self.labelNames(issue).filter(function (l) {
                    return boardLabels.indexOf(l) >= 0;
                  });
                } else if (fromLabel && !isFromVirtual && !isFromClosed) {
                  labelsToRemove = [fromLabel];
                }
                var addLabel = isToVirtual || isToClosed ? '' : toLabel;
                if (addLabel) {
                  labelsToRemove = labelsToRemove.filter(function (l) {
                    return l !== addLabel;
                  });
                }
                var stateEvent = null;
                if (isToClosed && !isFromClosed) stateEvent = 'close';
                else if (!isToClosed && isFromClosed) stateEvent = 'reopen';

                var updated = await self.apiMove(projectPath, issueIid, {
                  from_label: isFromVirtual || isFromClosed ? '' : fromLabel,
                  to_label: addLabel || '',
                  remove_labels: labelsToRemove,
                  state_event: stateEvent,
                });
                if (updated) {
                  self.applyIssueUpdate(pd, updated);
                } else {
                  var issueIndex = pd.issues.findIndex(function (i) {
                    return String(i.iid) === String(issueIid);
                  });
                  if (issueIndex > -1) {
                    if (stateEvent === 'close') pd.issues[issueIndex].state = 'closed';
                    if (stateEvent === 'reopen') pd.issues[issueIndex].state = 'opened';
                    var labels = self.labelNames(pd.issues[issueIndex]).filter(function (l) {
                      return boardLabels.indexOf(l) < 0;
                    });
                    if (addLabel && labels.indexOf(addLabel) < 0) labels.push(addLabel);
                    pd.issues[issueIndex].labels = labels;
                  }
                }
              }

              var toIids = self.iidsFromListEl(toList);
              await self.saveColumnOrder(projectPath, toListKey, toIids);
              if (crossColumn && fromListKey) {
                await self.saveColumnOrder(projectPath, fromListKey, self.iidsFromListEl(fromList));
              }

              if (reorderedInColumn || crossColumn) {
                var params = self.getReorderParamsFromList(toList, issueIid);
                if (params && (params.move_after_id || params.move_before_id)) {
                  var reordered = await self.apiReorder(projectPath, issueIid, params);
                  if (reordered) self.applyIssueUpdate(pd, reordered);
                }
              }
            } catch (e) {
              console.warn('home board drag failed', e);
              self.statusMsg = 'Sürükle-bırak kaydı başarısız';
              self.fetchProject(projectPath, true);
            }
          },
        });
      },
    };
  }

  function ensureAlpineAndMount() {
    if (!global.Alpine) return false;
    if (!global.Alpine.data || !global.__homeGitlabBoardsRegistered) {
      try {
        global.Alpine.data('homeGitlabBoards', homeGitlabBoards);
        global.__homeGitlabBoardsRegistered = true;
      } catch (e) {
        // Alpine already started — define on window for x-data string eval
      }
    }
    global.homeGitlabBoards = homeGitlabBoards;
    return true;
  }

  global.homeGitlabBoards = homeGitlabBoards;

  function boot() {
    ensureAlpineAndMount();
    var el = document.getElementById('home-git-nokta');
    if (el && global.Alpine && typeof global.Alpine.initTree === 'function') {
      // HTMX swap sonrası Alpine ağacını yeniden başlat
      if (!el.__x) {
        try {
          global.Alpine.initTree(el);
        } catch (e) {
          console.warn('home gl alpine init', e);
        }
      }
    }
  }

  document.body.addEventListener('htmx:afterSwap', function (e) {
    var t = e.detail && e.detail.target;
    if (!t) return;
    if (t.id === 'home-git-nokta' || (t.querySelector && t.querySelector('#home-git-nokta'))) {
      setTimeout(boot, 0);
    }
  });
  document.body.addEventListener('htmx:afterSettle', function (e) {
    var t = e.detail && e.detail.target;
    if (!t) return;
    if (t.id === 'home-git-nokta' || (t.querySelector && t.querySelector('#home-git-nokta'))) {
      setTimeout(boot, 30);
    }
  });

  if (document.readyState === 'loading') {
    document.addEventListener('DOMContentLoaded', boot);
  } else {
    boot();
  }
})(window);
