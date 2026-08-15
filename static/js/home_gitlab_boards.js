/**
 * Ana sayfa git.nokta — boards ile aynı kanban.
 * Veri + DnD: tarayıcıdan doğrudan GitLab (VPN); sıra kaydı: /api/boards/order.
 * Railway sunucusu GitLab'e ulaşamadığı için project-bundle kullanılmaz.
 */
(function (global) {
  var GITLAB_API = 'https://git.nokta.com/api/v4';
  var MAX_OPENED_PAGES = 25;
  var MAX_CLOSED_PAGES = 8;

  function homeGitlabBoards() {
    return {
      token: '',
      baseUrl: GITLAB_API,
      configProjects: [],
      activeProject: '',
      projectData: {},
      boardOrders: {},
      statusMsg: '',
      panelOpen: true,
      vpnProbing: false,
      vpnRemain: 5,
      vpnOffline: false,
      vpnOk: null,
      _vpnTimer: null,
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
        this.token = (root && root.getAttribute('data-token')) || '';
        var def =
          (root && root.getAttribute('data-default-project')) ||
          (this.configProjects[0] && this.configProjects[0].path) ||
          'nokta/doviz';
        this.activeProject = def;
        this.ensureProject(def);
        this.startVpnProbe();
        this.checkVpn();
        this.fetchProject(def, false);
        this._onHomeFocus = this.applyHomeFocus.bind(this);
        document.addEventListener('pc-home-focus', this._onHomeFocus);
        // Mevcut ana sayfa focus'una uy
        var board = document.getElementById('home-board');
        var mode = board ? board.getAttribute('data-home-focus') : 'split';
        this.applyHomeFocus({ detail: { mode: mode || 'split' } });
      },

      onDropToggle() {
        /* legacy no-op — shell uses panelOpen + is-open */
      },

      collapsePanel() {
        this.panelOpen = false;
      },

      expandPanel() {
        this.panelOpen = true;
        this.vpnOffline = false;
      },

      _clearVpnTimer() {
        if (this._vpnTimer) {
          clearInterval(this._vpnTimer);
          this._vpnTimer = null;
        }
      },

      startVpnProbe() {
        this._clearVpnTimer();
        this.vpnRemain = 5;
        this.vpnProbing = true;
        this.vpnOffline = false;
        this.vpnOk = null;
        this.expandPanel();
        var self = this;
        this._vpnTimer = setInterval(function () {
          self.vpnRemain -= 1;
          if (self.vpnRemain > 0) return;
          self.finishVpnProbe();
        }, 1000);
      },

      async checkVpn() {
        var ctrl = typeof AbortController !== 'undefined' ? new AbortController() : null;
        var timer = setTimeout(function () {
          if (ctrl) ctrl.abort();
        }, 2500);
        try {
          var opts = { headers: { 'PRIVATE-TOKEN': this.token } };
          if (ctrl) opts.signal = ctrl.signal;
          var res = await fetch(this.baseUrl + '/version', opts);
          clearTimeout(timer);
          this.vpnOk = !!(res && (res.ok || res.status === 401));
        } catch (e) {
          clearTimeout(timer);
          this.vpnOk = false;
        }
      },

      finishVpnProbe() {
        this._clearVpnTimer();
        this.vpnProbing = false;
        if (this.vpnRemain < 0) this.vpnRemain = 0;
        var pd = this.pd();
        if (pd && pd.loaded) {
          this.vpnOffline = false;
          this.expandPanel();
          return;
        }
        var unreachable = this.vpnOk === false || (pd && pd.error);
        if (unreachable) {
          this.vpnOffline = true;
          this.collapsePanel();
        }
      },

      applyHomeFocus(ev) {
        var mode = (ev && ev.detail && ev.detail.mode) || 'split';
        if (mode !== 'doviz' && mode !== 'sinemalar') return;
        var active = this.configProjects.find(function (p) {
          return p.path === this.activeProject;
        }.bind(this));
        if (active && active.product === mode) return;
        var next = this.configProjects.find(function (p) {
          return p.product === mode;
        });
        if (next && next.path) this.setActive(next.path);
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
        if (!pd.loaded && !pd.loading) {
          this.startVpnProbe();
          this.checkVpn();
          this.fetchProject(path, false);
        }
      },

      platformLabel() {
        var p = this.configProjects.find(function (x) {
          return x.path === this.activeProject;
        }.bind(this));
        return this.chipPlatformLabel(p);
      },

      chipPlatformLabel(project) {
        if (!project) return '';
        var plat = String(project.platform || '').toLowerCase();
        if (plat === 'web') return 'Web';
        if (plat === 'mweb' || plat === 'mobil' || plat === 'mobile') return 'MWEB';
        if (plat === 'ios') return 'iOS';
        if (plat === 'android') return 'Android';
        var name = String(project.name || '');
        return name
          .replace(/^Döviz\s+/i, '')
          .replace(/^Sinemalar\s+/i, '')
          .trim() || name;
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

      async fetchAllIssues(encodedPath, state) {
        var allIssues = [];
        var page = 1;
        var oneYearAgo = new Date();
        oneYearAgo.setFullYear(oneYearAgo.getFullYear() - 1);
        var updatedAfter = oneYearAgo.toISOString();
        var orderBy = state === 'opened' ? 'relative_position' : 'updated_at';
        var sort = state === 'opened' ? 'asc' : 'desc';
        var maxPages = state === 'opened' ? MAX_OPENED_PAGES : MAX_CLOSED_PAGES;
        while (page <= maxPages) {
          var url =
            this.baseUrl +
            '/projects/' +
            encodedPath +
            '/issues?state=' +
            state +
            '&updated_after=' +
            encodeURIComponent(updatedAfter) +
            '&order_by=' +
            orderBy +
            '&sort=' +
            sort +
            '&per_page=100&page=' +
            page;
          var res = await fetch(url, { headers: { 'PRIVATE-TOKEN': this.token } });
          if (!res.ok) throw new Error('Issues fetch failed (' + state + ')');
          var data = await res.json();
          allIssues = allIssues.concat(data);
          if (data.length < 100) break;
          page += 1;
        }
        return allIssues;
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
          if (!this.token) {
            throw new Error('No GitLab token — check VPN / the boards page.');
          }
          await this.loadBoardOrders(path);
          var encodedPath = encodeURIComponent(path);
          var headers = { 'PRIVATE-TOKEN': this.token };
          var boardsRes = await fetch(this.baseUrl + '/projects/' + encodedPath + '/boards', {
            headers: headers,
          });
          if (!boardsRes.ok) {
            if (boardsRes.status === 0 || boardsRes.type === 'opaque') {
              throw new Error('Could not reach GitLab — is VPN on?');
            }
            throw new Error('GitLab API error: ' + boardsRes.status + ' ' + (boardsRes.statusText || ''));
          }
          var boardsData = await boardsRes.json();
          if (!boardsData || !boardsData.length) {
            throw new Error('No active board found for this project.');
          }
          var openedIssues = await this.fetchAllIssues(encodedPath, 'opened');
          var closedIssues = await this.fetchAllIssues(encodedPath, 'closed');
          var board = boardsData[0];
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
          pd.issues = openedIssues.concat(closedIssues);
          pd.loaded = true;
          this.vpnOffline = false;
          this._clearVpnTimer();
          this.vpnProbing = false;
          this.expandPanel();
          if (force) this.statusMsg = 'Updated · ' + pd.issues.length + ' items';
        } catch (err) {
          console.error(err);
          var msg = (err && err.message) || 'Could not load board';
          if (
            /Failed to fetch|NetworkError|Load failed|network/i.test(msg) ||
            (err && err.name === 'TypeError')
          ) {
            msg = 'Could not reach GitLab — is VPN on? (same connection as boards)';
            this.vpnOk = false;
          }
          pd.error = msg;
          this.statusMsg = msg;
          if (!this.vpnProbing) {
            this.vpnOffline = true;
            this.collapsePanel();
          }
        } finally {
          pd.loading = false;
        }
      },

      refreshActive() {
        if (!this.activeProject) return;
        this.projectData[this.activeProject].loaded = false;
        this.startVpnProbe();
        this.checkVpn();
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

      getReorderParamsFromList(listEl, issueIid, projectPath) {
        var items = Array.from(listEl.querySelectorAll('[data-id]'));
        var idx = items.findIndex(function (el) {
          return String(el.getAttribute('data-id')) === String(issueIid);
        });
        if (idx === -1) return null;
        var pd = this.projectData[projectPath];
        var globalId = function (el) {
          var g = el && el.getAttribute('data-global-id');
          if (g) return parseInt(g, 10);
          if (!pd || !pd.issues) return null;
          var iss = pd.issues.find(function (i) {
            return String(i.iid) === String(el.getAttribute('data-id'));
          });
          return iss && iss.id != null ? parseInt(iss.id, 10) : null;
        };
        var params = {};
        if (idx === 0) {
          if (items[1]) {
            var nextId = globalId(items[1]);
            if (nextId != null && !isNaN(nextId)) params.move_before_id = nextId;
          }
        } else {
          var afterId = globalId(items[idx - 1]);
          if (afterId != null && !isNaN(afterId)) params.move_after_id = afterId;
        }
        return params;
      },

      async gitlabUpdateIssue(projectPath, issueIid, opts) {
        opts = opts || {};
        if (!this.token) return null;
        var enc = encodeURIComponent(projectPath);
        var url = this.baseUrl + '/projects/' + enc + '/issues/' + parseInt(issueIid, 10);
        var body = new URLSearchParams();
        if (opts.add_labels && opts.add_labels.length) {
          body.set('add_labels', opts.add_labels.join(','));
        }
        if (opts.remove_labels && opts.remove_labels.length) {
          body.set('remove_labels', opts.remove_labels.join(','));
        }
        if (opts.state_event === 'close' || opts.state_event === 'reopen') {
          body.set('state_event', opts.state_event);
        }
        if (!Array.from(body.keys()).length) return null;
        var res = await fetch(url, {
          method: 'PUT',
          headers: { 'PRIVATE-TOKEN': this.token },
          body: body,
        });
        if (!res.ok) {
          var detail = 'Could not update GitLab issue';
          try {
            var err = await res.json();
            if (err && (err.message || err.error)) detail = String(err.message || err.error);
          } catch (e) { /* ignore */ }
          throw new Error(detail);
        }
        return res.json();
      },

      async gitlabReorderIssue(projectPath, issueIid, params) {
        params = params || {};
        if (!this.token) return null;
        var enc = encodeURIComponent(projectPath);
        var iid = parseInt(issueIid, 10);
        var qs = new URLSearchParams();
        if (params.move_after_id != null) qs.set('move_after_id', String(params.move_after_id));
        if (params.move_before_id != null) qs.set('move_before_id', String(params.move_before_id));
        var q = qs.toString();
        var url =
          this.baseUrl + '/projects/' + enc + '/issues/' + iid + '/reorder' + (q ? '?' + q : '');
        var res = await fetch(url, {
          method: 'PUT',
          headers: { 'PRIVATE-TOKEN': this.token },
        });
        if (!res.ok) return null;
        if (res.status === 204) {
          var ref = await fetch(this.baseUrl + '/projects/' + enc + '/issues/' + iid, {
            headers: { 'PRIVATE-TOKEN': this.token },
          });
          return ref.ok ? ref.json() : null;
        }
        var text = await res.text();
        if (text && text.trim()) {
          try {
            return JSON.parse(text);
          } catch (e) { /* ignore */ }
        }
        return null;
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

                var updated = await self.gitlabUpdateIssue(projectPath, issueIid, {
                  add_labels: addLabel ? [addLabel] : [],
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
                var params = self.getReorderParamsFromList(toList, issueIid, projectPath);
                if (params && (params.move_after_id || params.move_before_id)) {
                  var reordered = await self.gitlabReorderIssue(projectPath, issueIid, params);
                  if (reordered) self.applyIssueUpdate(pd, reordered);
                }
              }
            } catch (e) {
              console.warn('home board drag failed', e);
              self.statusMsg = 'Drag-and-drop save failed';
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

  document.addEventListener('alpine:init', function () {
    try {
      if (global.Alpine && typeof global.Alpine.data === 'function') {
        global.Alpine.data('homeGitlabBoards', homeGitlabBoards);
        global.__homeGitlabBoardsRegistered = true;
      }
    } catch (e) { /* ignore */ }
  });

  function boot() {
    ensureAlpineAndMount();
    var el = document.getElementById('home-git-nokta');
    if (el && global.Alpine && typeof global.Alpine.initTree === 'function') {
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
