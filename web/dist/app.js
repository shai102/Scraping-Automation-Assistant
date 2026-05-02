/* app.js — Vue 3 SPA (Vue is loaded via <script> in index.html) */

const API = window.location.origin;

const app = Vue.createApp({
  data() {
    return {
      page: location.hash ? location.hash.slice(1) : 'folders',
      // Folders
      folders: [],
      showAddFolder: false,
      newFolder: { path: '', target_root: '', media_type: 'auto', data_source: 'siliconflow_tmdb', organize_mode: 'move', symlink_source: '', skip_if_scraped: false },
      editFolderVisible: false,
      editFolderData: { id: null, path: '', target_root: '', media_type: 'auto', data_source: 'siliconflow_tmdb', organize_mode: 'move', symlink_source: '', skip_if_scraped: false },
      // Symlink export
      showAddSymlink: false,
      newSymlinkFolder: { path: '', target_root: '' },
      symlinkRecords: [],
      symlinkSelectedIds: [],
      symlinkPage: 1,
      symlinkPageSize: 20,
      symlinkTotal: 0,
      symlinkGoPage: 1,
      symlinkStats: {},
      symlinkFilter: '',
      symlinkKeyword: '',
      symlinkGroupedView: false,
      symlinkGroupedRecords: [],
      symlinkExpandedGroups: {},
      // Records
      records: [],
      recordFilter: '',
      recordKeyword: '',
      recordTypeFilter: '',
      recordParseFilter: '',
      recordPage: 1,
      recordPageSize: 20,
      recordTotal: 0,
      selectedIds: [],
      recordGoPage: 1,
      // Grouped view
      groupedView: false,
      groupedRecords: [],
      expandedGroups: {},
      // Manual match
      manualMatchVisible: false,
      manualRecord: null,
      manualSeason: null,
      manualEpOffset: 0,
      manualScope: 'single',
      searchQuery: '',
      searchYear: null,
      searchMode: 'name',
      searchTmdbId: '',
      searchIsTv: true,
      searching: false,
      searchDone: false,
      candidates: [],
      selectedCandidate: null,
      // Settings
      cfg: {},
      testResult: null,
      proxyTestResult: null,
      proxyTesting: false,
      tgTestResult: null,
      ollamaModels: [],
      // API key visibility (default hidden)
      showTmdbKey: false,
      showBgmKey: false,
      showSfKey: false,
      showTgToken: false,
      // Recognition test
      recognitionTab: 'single',
      recognitionName: '',
      recognitionUseAi: false,
      recognitionBypassCache: true,
      recognitionTesting: false,
      recognitionResult: null,
      recognitionError: '',
      recognitionBatchText: '',
      recognitionBatchTesting: false,
      recognitionBatchResult: null,
      recognitionBatchError: '',
      // WebSocket
      ws: null,
      // Folder browser
      browseVisible: false,
      browseField: '',
      browseCurrent: '',
      browseParent: '',
      browseDirs: [],
      browseSelected: '',
      newKeyword: '',
      toasts: [],
      toastSeq: 1,
      confirmDialog: {
        visible: false,
        title: '',
        message: '',
        confirmText: '确认',
        cancelText: '取消',
        danger: false,
        resolver: null,
      },
    };
  },
  mounted() {
    this.loadSettings();
    this.loadFolders();
    this.loadRecords();
    this.connectWs();
    // Load page-specific data on initial mount (handles F5 refresh)
    if (this.page === 'symlink_records') {
      if (this.symlinkGroupedView) this.loadSymlinkGroupedRecords();
      else this.loadSymlinkRecords();
      this.loadSymlinkStats();
    }
    if (this.page === 'symlink_folders') { this.loadFolders(); }
    // Keep hash in sync when page changes, so F5 restores correctly
    this.$watch('page', function(val) {
      location.hash = val;
      if (val === 'symlink_records') {
        if (this.symlinkGroupedView) this.loadSymlinkGroupedRecords();
        else this.loadSymlinkRecords();
        this.loadSymlinkStats();
      }
      if (val === 'symlink_folders') { this.loadFolders(); }
    });
  },
  computed: {
    allSelected: function() {
      return this.records.length > 0 && this.selectedIds.length === this.records.length;
    },
    symlinkAllSelected: function() {
      return this.symlinkRecords.length > 0 && this.symlinkSelectedIds.length === this.symlinkRecords.length;
    },
    scrapeFolders: function() {
      return this.folders.filter(function(f) { return f.organize_mode !== 'symlink_export'; });
    },
    symlinkFolders: function() {
      return this.folders.filter(function(f) { return f.organize_mode === 'symlink_export'; });
    },
  },
  methods: {
    notify(message, type, duration) {
      var text = String(message || '').trim();
      if (!text) return;
      var toast = { id: this.toastSeq++, message: text, type: type || 'info' };
      this.toasts.push(toast);
      var self = this;
      setTimeout(function() {
        self.removeToast(toast.id);
      }, duration || 3600);
    },
    removeToast(id) {
      this.toasts = this.toasts.filter(function(t) { return t.id !== id; });
    },
    confirmAction(options) {
      var opts = options || {};
      var self = this;
      return new Promise(function(resolve) {
        self.confirmDialog = {
          visible: true,
          title: opts.title || '确认操作',
          message: opts.message || '',
          confirmText: opts.confirmText || '确认',
          cancelText: opts.cancelText || '取消',
          danger: !!opts.danger,
          resolver: resolve,
        };
      });
    },
    resolveConfirm(value) {
      var resolver = this.confirmDialog && this.confirmDialog.resolver;
      this.confirmDialog.visible = false;
      this.confirmDialog.resolver = null;
      if (resolver) resolver(!!value);
    },

    // --- API helpers ---
    async api(method, path, body) {
      var opts = { method: method, headers: { 'Content-Type': 'application/json' } };
      if (body) opts.body = JSON.stringify(body);
      var resp = await fetch(API + path, opts);
      if (!resp.ok) {
        var err = await resp.json().catch(function() { return { detail: resp.statusText }; });
        throw new Error(err.detail || resp.statusText);
      }
      return resp.json();
    },

    // --- WebSocket ---
    connectWs() {
      var self = this;
      var proto = location.protocol === 'https:' ? 'wss' : 'ws';
      try {
        this.ws = new WebSocket(proto + '://' + location.host + '/ws');
        this.ws.onmessage = function(e) {
          try {
            var msg = JSON.parse(e.data);
            if (msg.type === 'record_update') self.onRecordUpdate(msg.data);
            if (msg.type === 'symlink_update') self.onSymlinkUpdate(msg.data);
          } catch (ex) {}
        };
        this.ws.onclose = function() {
          setTimeout(function() { self.connectWs(); }, 3000);
        };
        this.ws.onerror = function() {};
        this._heartbeat = setInterval(function() {
          if (self.ws && self.ws.readyState === 1) self.ws.send('ping');
        }, 30000);
      } catch (ex) {}
    },
    onRecordUpdate(data) {
      var idx = this.records.findIndex(function(r) { return r.id === data.id; });
      if (idx >= 0) {
        // 已在当前页列表中：原地更新状态，不改变 DOM 结构
        Object.assign(this.records[idx], data);
      } else {
        // 新记录：只更新总数，不插入列表，避免批量处理时列表条数持续增长
        this.recordTotal++;
      }
      // 防抖 3s 刷新列表（含分组视图），批量结束后拉取最新分页数据
      clearTimeout(this._recordRefreshTimer);
      var self = this;
      this._recordRefreshTimer = setTimeout(function() {
        if (self.groupedView) self.loadGroupedRecords();
        else self.loadRecords();
      }, 3000);
    },
    onSymlinkUpdate(data) {
      var idx = this.symlinkRecords.findIndex(function(r) { return r.id === data.id; });
      if (idx >= 0) {
        // 已在当前页列表中：原地更新状态
        Object.assign(this.symlinkRecords[idx], data);
      } else {
        // 新记录：只更新总数
        this.symlinkTotal++;
      }
      // 防抖 3s 刷新软链接列表和统计
      clearTimeout(this._symlinkRefreshTimer);
      var self = this;
      this._symlinkRefreshTimer = setTimeout(function() {
        if (self.symlinkGroupedView) self.loadSymlinkGroupedRecords();
        else self.loadSymlinkRecords();
        self.loadSymlinkStats();
      }, 3000);
    },

    // --- Folder Browser ---
    async openBrowse(field) {
      this.browseField = field;
      this.browseSelected = '';
      var startPath = '';
      if (field === 'path') startPath = this.newFolder.path;
      else if (field === 'target_root') startPath = this.newFolder.target_root;
      else if (field === 'symlink_source') startPath = this.newFolder.symlink_source;
      else if (field === 'edit_path') startPath = this.editFolderData.path;
      else if (field === 'edit_target_root') startPath = this.editFolderData.target_root;
      else if (field === 'edit_symlink_source') startPath = this.editFolderData.symlink_source;
      else if (field === 'symlink_path') startPath = this.newSymlinkFolder.path;
      else if (field === 'symlink_target') startPath = this.newSymlinkFolder.target_root;
      try {
        var data = await this.api('POST', '/api/monitor/browse', { path: startPath || '' });
        this.browseCurrent = data.current || '';
        this.browseParent = data.parent || '';
        this.browseDirs = data.dirs || [];
      } catch (e) {
        var data2 = await this.api('POST', '/api/monitor/browse', { path: '' });
        this.browseCurrent = data2.current || '';
        this.browseParent = data2.parent || '';
        this.browseDirs = data2.dirs || [];
      }
      this.browseVisible = true;
    },
    async browseInto(path) {
      this.browseSelected = '';
      try {
        var data = await this.api('POST', '/api/monitor/browse', { path: path });
        this.browseCurrent = data.current || '';
        this.browseParent = data.parent || '';
        this.browseDirs = data.dirs || [];
      } catch (e) { this.notify(e.message, 'error'); }
    },
    browseSelect(d) {
      this.browseSelected = d.path;
    },
    async browseUp() {
      if (this.browseParent !== undefined) {
        await this.browseInto(this.browseParent);
      }
    },
    browseConfirm() {
      var chosen = this.browseSelected || this.browseCurrent;
      if (!chosen) return;
      if (this.browseField === 'path') {
        this.newFolder.path = chosen;
      } else if (this.browseField === 'target_root') {
        this.newFolder.target_root = chosen;
      } else if (this.browseField === 'symlink_source') {
        this.newFolder.symlink_source = chosen;
      } else if (this.browseField === 'edit_path') {
        this.editFolderData.path = chosen;
      } else if (this.browseField === 'edit_target_root') {
        this.editFolderData.target_root = chosen;
      } else if (this.browseField === 'edit_symlink_source') {
        this.editFolderData.symlink_source = chosen;
      } else if (this.browseField === 'symlink_path') {
        this.newSymlinkFolder.path = chosen;
      } else if (this.browseField === 'symlink_target') {
        this.newSymlinkFolder.target_root = chosen;
      }
      this.browseVisible = false;
    },

    // --- Folders ---
    async loadFolders() {
      try { this.folders = await this.api('GET', '/api/monitor/folders'); } catch (ex) {}
    },
    async addFolder() {
      try {
        await this.api('POST', '/api/monitor/folders', this.newFolder);
        this.showAddFolder = false;
        this.newFolder = { path: '', target_root: '', media_type: 'auto', data_source: 'siliconflow_tmdb', organize_mode: 'move', symlink_source: '', skip_if_scraped: false };
        this.loadFolders();
      } catch (e) { this.notify(e.message, 'error'); }
    },
    async toggleFolder(f) {
      try {
        await this.api('PUT', '/api/monitor/folders/' + f.id, { enabled: !f.enabled });
        this.loadFolders();
      } catch (e) { this.notify(e.message, 'error'); }
    },
    async deleteFolder(id) {
      if (!(await this.confirmAction({
        title: '删除监控目录',
        message: '关联的刮削记录不会删除。',
        confirmText: '删除',
        danger: true,
      }))) return;
      try { await this.api('DELETE', '/api/monitor/folders/' + id); this.loadFolders(); } catch (e) { this.notify(e.message, 'error'); }
    },
    openEditFolder(f) {
      this.editFolderData = {
        id: f.id,
        path: f.path,
        target_root: f.target_root || '',
        media_type: f.media_type || 'auto',
        data_source: f.data_source || 'siliconflow_tmdb',
        organize_mode: f.organize_mode || 'move',
        symlink_source: f.symlink_source || '',
        skip_if_scraped: f.skip_if_scraped || false,
      };
      this.editFolderVisible = true;
    },
    async saveEditFolder() {
      try {
        await this.api('PUT', '/api/monitor/folders/' + this.editFolderData.id, {
          path: this.editFolderData.path,
          target_root: this.editFolderData.target_root,
          media_type: this.editFolderData.media_type,
          data_source: this.editFolderData.data_source,
          organize_mode: this.editFolderData.organize_mode,
          symlink_source: this.editFolderData.symlink_source,
          skip_if_scraped: this.editFolderData.skip_if_scraped,
        });
        this.editFolderVisible = false;
        this.loadFolders();
      } catch (e) { this.notify(e.message, 'error'); }
    },
    async scanFolder(id) {
      try {
        var r = await this.api('POST', '/api/monitor/folders/' + id + '/scan');
        this.notify(r.message || '扫描已启动', 'success');
      } catch (e) { this.notify(e.message, 'error'); }
    },

    // --- Symlink Export ---
    async addSymlinkFolder() {
      try {
        await this.api('POST', '/api/monitor/folders', {
          path: this.newSymlinkFolder.path,
          target_root: this.newSymlinkFolder.target_root,
          media_type: 'auto',
          data_source: 'siliconflow_tmdb',
          organize_mode: 'symlink_export',
        });
        this.showAddSymlink = false;
        this.newSymlinkFolder = { path: '', target_root: '' };
        this.loadFolders();
      } catch (e) { this.notify(e.message, 'error'); }
    },
    async loadSymlinkRecords() {
      try {
        var params = new URLSearchParams({ page: this.symlinkPage, page_size: this.symlinkPageSize });
        if (this.symlinkFilter) params.set('status', this.symlinkFilter);
        if (this.symlinkKeyword) params.set('keyword', this.symlinkKeyword);
        var data = await this.api('GET', '/api/symlinks?' + params.toString());
        this.symlinkRecords = data.items || [];
        this.symlinkTotal = data.total || 0;
        this.symlinkSelectedIds = [];
      } catch (ex) {}
    },
    async loadSymlinkStats() {
      try { this.symlinkStats = await this.api('GET', '/api/symlinks/stats'); } catch (ex) {}
    },
    resetSymlinkFilter() {
      this.symlinkFilter = '';
      this.symlinkKeyword = '';
      this.symlinkPage = 1;
      if (this.symlinkGroupedView) this.loadSymlinkGroupedRecords();
      else this.loadSymlinkRecords();
    },
    toggleSymlinkGroupedView() {
      this.symlinkGroupedView = !this.symlinkGroupedView;
      this.symlinkSelectedIds = [];
      if (this.symlinkGroupedView) this.loadSymlinkGroupedRecords();
      else this.loadSymlinkRecords();
    },
    async loadSymlinkGroupedRecords() {
      try {
        var params = new URLSearchParams();
        if (this.symlinkFilter) params.set('status', this.symlinkFilter);
        if (this.symlinkKeyword) params.set('keyword', this.symlinkKeyword);
        var suffix = params.toString() ? '?' + params.toString() : '';
        var data = await this.api('GET', '/api/symlinks/grouped' + suffix);
        this.symlinkGroupedRecords = data.groups || [];
        this.symlinkExpandedGroups = {};
        this.symlinkSelectedIds = [];
      } catch (ex) {}
    },
    async toggleSymlinkGroup(g) {
      if (this.symlinkExpandedGroups[g.dir_path]) {
        delete this.symlinkExpandedGroups[g.dir_path];
        this.symlinkExpandedGroups = Object.assign({}, this.symlinkExpandedGroups);
        return;
      }
      this.symlinkExpandedGroups[g.dir_path] = { loading: true, records: [], page: 1, total: 0 };
      this.symlinkExpandedGroups = Object.assign({}, this.symlinkExpandedGroups);
      await this.loadSymlinkGroupRecords(g);
    },
    async loadSymlinkGroupRecords(g) {
      var state = this.symlinkExpandedGroups[g.dir_path];
      if (!state) return;
      state.loading = true;
      this.symlinkExpandedGroups = Object.assign({}, this.symlinkExpandedGroups);
      try {
        var params = new URLSearchParams({ dir: g.dir_path, page: state.page || 1, page_size: 50 });
        if (this.symlinkFilter) params.set('status', this.symlinkFilter);
        if (this.symlinkKeyword) params.set('keyword', this.symlinkKeyword);
        var data = await this.api('GET', '/api/symlinks?' + params.toString());
        state.records = data.items || [];
        state.total = data.total || 0;
      } catch (e) {
        state.records = [];
        state.total = 0;
      }
      state.loading = false;
      this.symlinkExpandedGroups = Object.assign({}, this.symlinkExpandedGroups);
    },
    symlinkGroupPagePrev(g) {
      var state = this.symlinkExpandedGroups[g.dir_path];
      if (!state || state.page <= 1) return;
      state.page--;
      this.loadSymlinkGroupRecords(g);
    },
    symlinkGroupPageNext(g) {
      var state = this.symlinkExpandedGroups[g.dir_path];
      if (!state) return;
      if (state.page * 50 >= state.total) return;
      state.page++;
      this.loadSymlinkGroupRecords(g);
    },
    isSymlinkGroupAllSelected(g) {
      var self = this;
      return g.ids.length > 0 && g.ids.every(function(id) { return self.symlinkSelectedIds.indexOf(id) >= 0; });
    },
    toggleSelectSymlinkGroup(g) {
      var self = this;
      if (this.isSymlinkGroupAllSelected(g)) {
        this.symlinkSelectedIds = this.symlinkSelectedIds.filter(function(id) { return g.ids.indexOf(id) < 0; });
      } else {
        var newIds = g.ids.filter(function(id) { return self.symlinkSelectedIds.indexOf(id) < 0; });
        this.symlinkSelectedIds = this.symlinkSelectedIds.concat(newIds);
      }
    },
    async deleteSymlinkRecord(id) {
      if (!(await this.confirmAction({
        title: '删除软链接记录',
        message: '确认删除该记录？',
        confirmText: '删除',
        danger: true,
      }))) return;
      try {
        await this.api('DELETE', '/api/symlinks/' + id);
        if (this.symlinkGroupedView) this.loadSymlinkGroupedRecords();
        else this.loadSymlinkRecords();
        this.loadSymlinkStats();
      } catch (e) { this.notify(e.message, 'error'); }
    },
    toggleSymlinkSelectAll(e) {
      if (e.target.checked) {
        this.symlinkSelectedIds = this.symlinkRecords.map(function(r) { return r.id; });
      } else {
        this.symlinkSelectedIds = [];
      }
    },
    async batchDeleteSymlinkSelected() {
      if (!this.symlinkSelectedIds.length) return;
      if (!(await this.confirmAction({
        title: '批量删除软链接记录',
        message: '确认删除选中的 ' + this.symlinkSelectedIds.length + ' 条记录？',
        confirmText: '删除',
        danger: true,
      }))) return;
      try {
        await this.api('POST', '/api/symlinks/batch-delete', { ids: this.symlinkSelectedIds });
        if (this.symlinkGroupedView) this.loadSymlinkGroupedRecords();
        else this.loadSymlinkRecords();
        this.loadSymlinkStats();
      } catch (e) { this.notify(e.message, 'error'); }
    },
    async retrySymlinkFailed() {
      if (!(await this.confirmAction({
        title: '重试失败软链接',
        message: '确认重试所有失败的软链接记录？',
        confirmText: '重试',
      }))) return;
      try {
        var res = await this.api('POST', '/api/symlinks/retry-failed');
        this.notify('已将 ' + (res.queued || 0) + ' 条失败记录加入重试队列，请稍候刷新查看结果。', 'success');
        if (this.symlinkGroupedView) this.loadSymlinkGroupedRecords();
        else this.loadSymlinkRecords();
        this.loadSymlinkStats();
      } catch (e) { this.notify(e.message, 'error'); }
    },
    async clearSymlinkFailed() {
      if (!(await this.confirmAction({
        title: '清除失败软链接记录',
        message: '确认清除所有失败的软链接记录？',
        confirmText: '清除',
        danger: true,
      }))) return;
      try {
        await this.api('POST', '/api/symlinks/clear-failed');
        if (this.symlinkGroupedView) this.loadSymlinkGroupedRecords();
        else this.loadSymlinkRecords();
        this.loadSymlinkStats();
      } catch (e) { this.notify(e.message, 'error'); }
    },
    async clearSymlinkAll() {
      if (!(await this.confirmAction({
        title: '清空软链接记录',
        message: '确认清空所有软链接记录？此操作不可恢复。',
        confirmText: '清空',
        danger: true,
      }))) return;
      try {
        await this.api('DELETE', '/api/symlinks/all');
        if (this.symlinkGroupedView) this.loadSymlinkGroupedRecords();
        else this.loadSymlinkRecords();
        this.loadSymlinkStats();
      } catch (e) { this.notify(e.message, 'error'); }
    },
    async deleteSymlinkGroup(g) {
      if (!(await this.confirmAction({
        title: '删除软链接分组',
        message: '确认删除「' + g.dir_name + '」内的全部 ' + g.total + ' 条软链接记录？',
        confirmText: '删除',
        danger: true,
      }))) return;
      try {
        await this.api('POST', '/api/symlinks/batch-delete', { ids: g.ids });
        this.loadSymlinkGroupedRecords();
        this.loadSymlinkStats();
      } catch (e) { this.notify(e.message, 'error'); }
    },
    async deleteSymlinkGroupRecord(g, id) {
      if (!(await this.confirmAction({
        title: '删除软链接记录',
        message: '确认删除该记录？',
        confirmText: '删除',
        danger: true,
      }))) return;
      try {
        await this.api('DELETE', '/api/symlinks/' + id);
        this.loadSymlinkGroupedRecords();
        this.loadSymlinkStats();
      } catch (e) { this.notify(e.message, 'error'); }
    },

    // --- Records ---
    async loadRecords() {
      try {
        var params = new URLSearchParams({ page: this.recordPage, page_size: this.recordPageSize });
        if (this.recordFilter) params.set('status', this.recordFilter);
        if (this.recordKeyword) params.set('keyword', this.recordKeyword);
        if (this.recordTypeFilter) params.set('media_type', this.recordTypeFilter);
        if (this.recordParseFilter) params.set('parse_source', this.recordParseFilter);
        var data = await this.api('GET', '/api/records?' + params.toString());
        this.records = data.items || [];
        this.recordTotal = data.total || 0;
        this.selectedIds = [];
      } catch (ex) {}
    },
    refreshRecords() {
      if (this.groupedView) this.loadGroupedRecords();
      else this.loadRecords();
    },
    resetRecordFilter() {
      this.recordFilter = '';
      this.recordKeyword = '';
      this.recordTypeFilter = '';
      this.recordParseFilter = '';
      this.recordPage = 1;
      if (this.groupedView) this.loadGroupedRecords();
      else this.loadRecords();
    },
    gotoPage() {
      var max = Math.ceil(this.recordTotal / this.recordPageSize) || 1;
      var p = parseInt(this.recordGoPage) || 1;
      if (p < 1) p = 1;
      if (p > max) p = max;
      this.recordPage = p;
      this.recordGoPage = p;
      this.loadRecords();
    },
    gotoSymlinkPage() {
      var max = Math.ceil(this.symlinkTotal / this.symlinkPageSize) || 1;
      var p = parseInt(this.symlinkGoPage) || 1;
      if (p < 1) p = 1;
      if (p > max) p = max;
      this.symlinkPage = p;
      this.symlinkGoPage = p;
      this.loadSymlinkRecords();
    },
    async deleteRecord(id) {
      if (!(await this.confirmAction({
        title: '删除刮削记录',
        message: '确认删除该记录？',
        confirmText: '删除',
        danger: true,
      }))) return;
      try { await this.api('DELETE', '/api/records/' + id); this.loadRecords(); } catch (e) { this.notify(e.message, 'error'); }
    },
    async retryRecord(id) {
      try { await this.api('POST', '/api/records/' + id + '/retry'); this.loadRecords(); } catch (e) { this.notify(e.message, 'error'); }
    },
    // --- Batch Operations ---
    toggleSelectAll(e) {
      if (e.target.checked) {
        this.selectedIds = this.records.map(function(r) { return r.id; });
      } else {
        this.selectedIds = [];
      }
    },
    async batchDeleteSelected() {
      if (!this.selectedIds.length) return;
      if (!(await this.confirmAction({
        title: '批量删除刮削记录',
        message: '确认删除选中的 ' + this.selectedIds.length + ' 条记录？',
        confirmText: '删除',
        danger: true,
      }))) return;
      try {
        await this.api('POST', '/api/records/batch-delete', { ids: this.selectedIds });
        if (this.groupedView) this.loadGroupedRecords();
        else this.loadRecords();
      } catch (e) { this.notify(e.message, 'error'); }
    },
    async batchRetrySelected() {
      if (!this.selectedIds.length) return;
      try {
        await this.api('POST', '/api/records/batch-retry', { ids: this.selectedIds });
        if (this.groupedView) this.loadGroupedRecords();
        else this.loadRecords();
      } catch (e) { this.notify(e.message, 'error'); }
    },
    async clearFailed() {
      if (!(await this.confirmAction({
        title: '清除失败记录',
        message: '确认清除所有失败记录？',
        confirmText: '清除',
        danger: true,
      }))) return;
      try {
        await this.api('POST', '/api/records/clear-failed');
        if (this.groupedView) this.loadGroupedRecords();
        else this.loadRecords();
      } catch (e) { this.notify(e.message, 'error'); }
    },
    async clearAll() {
      if (!(await this.confirmAction({
        title: '清空刮削记录',
        message: '确认清空所有刮削记录？此操作不可恢复。',
        confirmText: '清空',
        danger: true,
      }))) return;
      try {
        await this.api('POST', '/api/records/clear-all');
        if (this.groupedView) this.loadGroupedRecords();
        else this.loadRecords();
      } catch (e) { this.notify(e.message, 'error'); }
    },
    exportErrors() {
      var errors = this.records.filter(function(r) { return r.status === 'failed' || r.status === 'pending_manual'; });
      if (!errors.length) { this.notify('当前页无识别错误记录', 'info'); return; }
      var lines = ['文件名,状态,错误信息,原始路径'];
      errors.forEach(function(r) {
        lines.push('"' + (r.original_name || '') + '","' + (r.status || '') + '","' + (r.error_msg || '') + '","' + (r.original_path || '') + '"');
      });
      var blob = new Blob([lines.join('\n')], { type: 'text/csv;charset=utf-8' });
      var a = document.createElement('a');
      a.href = URL.createObjectURL(blob);
      a.download = 'scrape_errors.csv';
      a.click();
    },
    fileStatusClass(r) {
      if (!r.original_path) return 'badge-gray';
      return r.target_path ? 'badge-success' : 'badge-gray';
    },
    fileStatusText(r) {
      return r.target_path ? '已归档' : '未归档';
    },
    recordType(r) {
      if (r.media_type === 'episode') return '电视剧';
      if (r.media_type === 'movie') return '电影';
      return '-';
    },
    formatTime(t) {
      if (!t) return '-';
      return t.replace('T', ' ').substring(0, 19);
    },
    statusClass(s) {
      var map = { success: 'badge-success', pending_manual: 'badge-warning', processing: 'badge-processing', failed: 'badge-danger', skipped: 'badge-gray' };
      return map[s] || 'badge-gray';
    },
    statusText(s) {
      var map = { success: '成功', pending_manual: '待手动', processing: '处理中', failed: '失败', skipped: '已跳过' };
      return map[s] || s;
    },
    shortPath(p) {
      if (!p) return '';
      var parts = p.replace(/\\/g, '/').split('/');
      return parts.length > 3 ? '.../' + parts.slice(-3).join('/') : p;
    },

    // --- Recognition Test ---
    async runRecognitionTest() {
      var name = (this.recognitionName || '').trim();
      if (!name || this.recognitionTesting) return;
      this.recognitionTesting = true;
      this.recognitionError = '';
      this.recognitionResult = null;
      try {
        this.recognitionResult = await this.api('POST', '/api/recognition-test', {
          filename: name,
          use_ai: this.recognitionUseAi,
          bypass_cache: this.recognitionBypassCache,
          data_source: (this.cfg && this.cfg.data_source) || 'siliconflow_tmdb',
        });
      } catch (e) {
        this.recognitionError = e.message || '识别测试失败';
      }
      this.recognitionTesting = false;
    },
    recognitionTypeText(type) {
      if (type === 'episode') return '电视剧';
      if (type === 'movie') return '电影';
      return '自动判断';
    },
    formatRecognitionRaw(value) {
      try { return JSON.stringify(value || {}, null, 2); }
      catch (e) { return String(value || ''); }
    },
    loadRecognitionBatchSample() {
      this.recognitionBatchText = [
        'filename,expected_title,expected_year,expected_season,expected_episode,expected_provider,expected_id,media_type',
        'The.Mandalorian.S03E04.2023.WEB-DL.mkv,The Mandalorian,2023,3,4,tmdb,82856,tv',
        '[KTXP][Dungeon Meshi][01][CHS][1080P][AVC].mkv,Dungeon Meshi,2024,1,1,tmdb,,tv',
      ].join('\n');
    },
    parseRecognitionCsv(text) {
      var lines = String(text || '').split(/\r?\n/).map(function(line) { return line.trim(); }).filter(Boolean);
      if (!lines.length) return [];

      function parseLine(line) {
        var out = [], cur = '', inQuote = false;
        for (var i = 0; i < line.length; i++) {
          var ch = line[i];
          if (ch === '"') {
            if (inQuote && line[i + 1] === '"') { cur += '"'; i++; }
            else inQuote = !inQuote;
          } else if (ch === ',' && !inQuote) {
            out.push(cur.trim());
            cur = '';
          } else {
            cur += ch;
          }
        }
        out.push(cur.trim());
        return out;
      }

      var first = parseLine(lines[0]);
      var hasHeader = first.some(function(v) { return /filename|文件名|expected/i.test(v); });
      var headers = hasHeader ? first : ['filename'];
      var rows = hasHeader ? lines.slice(1) : lines;

      function normHeader(h) {
        var key = String(h || '').trim().toLowerCase();
        var map = {
          '文件名': 'filename',
          '标题': 'expected_title',
          '正确标题': 'expected_title',
          '年份': 'expected_year',
          '季': 'expected_season',
          '季数': 'expected_season',
          '集': 'expected_episode',
          '集数': 'expected_episode',
          '来源': 'expected_provider',
          '资料库': 'expected_provider',
          'id': 'expected_id',
          'tmdbid': 'expected_id',
          'tmdb_id': 'expected_id',
          '媒体类型': 'media_type',
        };
        return map[key] || key;
      }

      headers = headers.map(normHeader);
      return rows.map(function(line) {
        var cols = parseLine(line);
        if (!hasHeader && cols.length === 1) return { filename: cols[0] };
        var obj = {};
        headers.forEach(function(h, idx) { obj[h] = cols[idx] || ''; });
        return obj;
      }).filter(function(row) { return row.filename; });
    },
    async runRecognitionBatch() {
      this.recognitionBatchError = '';
      this.recognitionBatchResult = null;
      var cases = [];
      try {
        cases = this.parseRecognitionCsv(this.recognitionBatchText);
      } catch (e) {
        this.recognitionBatchError = 'CSV 解析失败：' + e.message;
        return;
      }
      if (!cases.length) {
        this.recognitionBatchError = '没有可用的测试数据';
        return;
      }
      if (cases.length > 100) {
        this.recognitionBatchError = '单次最多支持 100 条测试数据';
        return;
      }
      this.recognitionBatchTesting = true;
      try {
        this.recognitionBatchResult = await this.api('POST', '/api/recognition-test/batch', {
          cases: cases,
          bypass_cache: this.recognitionBypassCache,
          data_source: (this.cfg && this.cfg.data_source) || 'siliconflow_tmdb',
        });
      } catch (e) {
        this.recognitionBatchError = e.message || '批量识别失败';
      }
      this.recognitionBatchTesting = false;
    },
    recognitionModeLabel(mode) {
      var map = { guessit: 'guessit', local_ai: '本地 AI', online_ai: '在线 AI' };
      return map[mode] || mode;
    },
    metricPercent(metric) {
      if (!metric || !metric.evaluated) return '-';
      return String(metric.rate) + '%';
    },
    recognitionExpectationText(expected) {
      expected = expected || {};
      var parts = [];
      if (expected.title) parts.push(expected.title);
      if (expected.year) parts.push(expected.year);
      if (expected.season !== null && expected.season !== undefined) parts.push('S' + expected.season);
      if (expected.episode !== null && expected.episode !== undefined) parts.push('E' + expected.episode);
      if (expected.id) parts.push('ID:' + expected.id);
      return parts.length ? parts.join(' / ') : '未提供标准答案';
    },
    recognitionBrief(result) {
      var m = (result && result.match) || {};
      return m.title || (result && result.guessit && result.guessit.title) || '-';
    },
    recognitionResultMeta(result) {
      var m = (result && result.match) || {};
      var parts = [];
      if (m.year) parts.push(m.year);
      if (m.season !== null && m.season !== undefined) parts.push('S' + m.season);
      if (m.episode !== null && m.episode !== undefined) parts.push('E' + m.episode);
      if (m.id) parts.push('ID:' + m.id);
      return parts.join(' · ') || (result ? result.message : '-');
    },
    recognitionScoreText(result) {
      var s = (result && result.score) || {};
      if (!s.evaluated) return '未参与统计';
      return s.full_match ? '完全命中' : '未完全命中';
    },
    resultCellClass(result) {
      if (!result || result.status === 'failed') return 'is-failed';
      if (result.score && result.score.full_match) return 'is-pass';
      if (result.score && result.score.wrong_match) return 'is-wrong';
      if (result.status === 'pending_manual') return 'is-pending';
      return 'is-partial';
    },
    recognitionSearchPlan(result) {
      var plan = result && result.diagnostics && result.diagnostics.search_plan;
      if (!plan || !plan.length) return '-';
      return plan.map(function(group) { return (group || []).join(' / '); }).join(' | ');
    },

    // --- Grouped View ---
    toggleGroupedView() {
      this.groupedView = !this.groupedView;
      this.selectedIds = [];
      if (this.groupedView) {
        this.loadGroupedRecords();
      } else {
        this.loadRecords();
      }
    },
    async loadGroupedRecords() {
      try {
        var params = new URLSearchParams();
        if (this.recordFilter) params.set('status', this.recordFilter);
        if (this.recordKeyword) params.set('keyword', this.recordKeyword);
        if (this.recordTypeFilter) params.set('media_type', this.recordTypeFilter);
        if (this.recordParseFilter) params.set('parse_source', this.recordParseFilter);
        var data = await this.api('GET', '/api/records/grouped?' + params.toString());
        this.groupedRecords = data.groups || [];
      } catch (ex) {}
    },
    toggleGroup(g) {
      var key = g.dir_path;
      if (this.expandedGroups[key]) {
        delete this.expandedGroups[key];
        // Force Vue reactivity
        this.expandedGroups = Object.assign({}, this.expandedGroups);
      } else {
        this.expandedGroups[key] = { records: [], page: 1, total: 0, loading: false };
        this.expandedGroups = Object.assign({}, this.expandedGroups);
        this.loadGroupRecords(g);
      }
    },
    async loadGroupRecords(g) {
      var state = this.expandedGroups[g.dir_path];
      if (!state) return;
      state.loading = true;
      this.expandedGroups = Object.assign({}, this.expandedGroups);
      try {
        var params = new URLSearchParams({ page: state.page, page_size: 50, dir: g.dir_path });
        if (this.recordFilter) params.set('status', this.recordFilter);
        if (this.recordKeyword) params.set('keyword', this.recordKeyword);
        if (this.recordParseFilter) params.set('parse_source', this.recordParseFilter);
        var data = await this.api('GET', '/api/records?' + params.toString());
        state.records = data.items || [];
        state.total = data.total || 0;
      } catch (ex) {}
      state.loading = false;
      this.expandedGroups = Object.assign({}, this.expandedGroups);
    },
    groupPagePrev(g) {
      var state = this.expandedGroups[g.dir_path];
      if (!state || state.page <= 1) return;
      state.page--;
      this.loadGroupRecords(g);
    },
    groupPageNext(g) {
      var state = this.expandedGroups[g.dir_path];
      if (!state) return;
      if (state.page * 50 >= state.total) return;
      state.page++;
      this.loadGroupRecords(g);
    },
    isGroupAllSelected(g) {
      var self = this;
      return g.ids.length > 0 && g.ids.every(function(id) { return self.selectedIds.indexOf(id) >= 0; });
    },
    toggleSelectGroup(g) {
      var self = this;
      if (this.isGroupAllSelected(g)) {
        this.selectedIds = this.selectedIds.filter(function(id) { return g.ids.indexOf(id) < 0; });
      } else {
        var newIds = g.ids.filter(function(id) { return self.selectedIds.indexOf(id) < 0; });
        this.selectedIds = this.selectedIds.concat(newIds);
      }
    },
    async deleteGroup(g) {
      if (!(await this.confirmAction({
        title: '删除分组记录',
        message: '确认删除「' + g.dir_name + '」内的全部 ' + g.total + ' 条记录？',
        confirmText: '删除',
        danger: true,
      }))) return;
      try {
        await this.api('POST', '/api/records/batch-delete', { ids: g.ids });
        this.loadGroupedRecords();
      } catch (e) { this.notify(e.message, 'error'); }
    },
    async deleteGroupRecord(g, id) {
      if (!(await this.confirmAction({
        title: '删除刮削记录',
        message: '确认删除该记录？',
        confirmText: '删除',
        danger: true,
      }))) return;
      try {
        await this.api('DELETE', '/api/records/' + id);
        this.loadGroupedRecords();
        if (this.expandedGroups[g.dir_path]) this.loadGroupRecords(g);
      } catch (e) { this.notify(e.message, 'error'); }
    },

    // --- Manual Match ---
    openManualMatch(record) {
      this.manualRecord = record;
      this.searchQuery = record.matched_title || record.original_name.replace(/\.[^.]+$/, '');
      var ym = /(19|20)\d{2}/.exec(record.original_name);
      this.searchYear = ym ? parseInt(ym[0]) : null;
      this.candidates = [];
      this.selectedCandidate = null;
      this.searchDone = false;
      this.manualSeason = null;
      this.manualEpOffset = 0;
      this.manualScope = 'single';
      this.searchMode = 'name';
      this.searchTmdbId = '';
      // Auto-detect TV/Movie
      if (record.media_type === 'movie') this.searchIsTv = false;
      else if (record.media_type === 'episode') this.searchIsTv = true;
      else {
        var p = record.target_path || record.original_path || '';
        this.searchIsTv = /[Ss]\d{1,2}[Ee]\d{1,4}|Season\s*\d/i.test(p);
      }
      this.manualMatchVisible = true;
    },
    async searchCandidates() {
      if (!this.searchQuery.trim()) return;
      this.searching = true;
      this.searchDone = false;
      try {
        var data = await this.api('POST', '/api/records/search-candidates', {
          query: this.searchQuery.trim(),
          year: this.searchYear || null,
          is_tv: this.searchIsTv,
          source: (this.cfg && this.cfg.data_source) || 'siliconflow_tmdb',
        });
        this.candidates = data.candidates || [];
        this.selectedCandidate = null;
      } catch (e) { this.notify(e.message, 'error'); }
      this.searching = false;
      this.searchDone = true;
    },
    async applyByTmdbId() {
      if (!this.searchTmdbId.trim() || !this.manualRecord) return;
      var provider = ((this.cfg && this.cfg.data_source) || 'siliconflow_tmdb') === 'siliconflow_tmdb' ? 'tmdb' : 'bgm';
      this.searching = true;
      try {
        await this.api('POST', '/api/records/' + this.manualRecord.id + '/manual-match', {
          candidate_id: this.searchTmdbId.trim(),
          candidate_title: '',
          provider: provider,
          is_tv: this.searchIsTv,
          season_override: (this.manualSeason !== null && this.manualSeason !== '') ? parseInt(this.manualSeason) : null,
          episode_offset: parseInt(this.manualEpOffset) || 0,
          scope: this.manualScope,
        });
        this.manualMatchVisible = false;
        this.loadRecords();
      } catch (e) { this.notify('匹配失败: ' + e.message, 'error'); }
      this.searching = false;
    },
    selectCandidate(candidate) {
      this.selectedCandidate = (this.selectedCandidate && this.selectedCandidate.id === candidate.id) ? null : candidate;
    },
    async confirmManualMatch() {
      if (!this.manualRecord || !this.selectedCandidate) return;
      var candidate = this.selectedCandidate;
      var provider = ((this.cfg && this.cfg.data_source) || 'siliconflow_tmdb') === 'siliconflow_tmdb' ? 'tmdb' : 'bgm';
      this.searching = true;
      try {
        await this.api('POST', '/api/records/' + this.manualRecord.id + '/manual-match', {
          candidate_id: String(candidate.id),
          candidate_title: candidate.title,
          provider: provider,
          is_tv: this.searchIsTv,
          season_override: (this.manualSeason !== null && this.manualSeason !== '') ? parseInt(this.manualSeason) : null,
          episode_offset: parseInt(this.manualEpOffset) || 0,
          scope: this.manualScope,
        });
        this.manualMatchVisible = false;
        this.loadRecords();
      } catch (e) { this.notify('匹配失败: ' + e.message, 'error'); }
      this.searching = false;
    },

    // --- Settings ---
    async loadSettings() {
      try {
        this.cfg = await this.api('GET', '/api/settings/raw');
        if (!this.cfg.embedding_source) this.cfg.embedding_source = 'local';
        if (this.cfg.online_embedding_model === undefined) this.cfg.online_embedding_model = '';
        if (this.cfg.proxy_enabled === undefined) this.cfg.proxy_enabled = false;
        if (this.cfg.proxy_url === undefined) this.cfg.proxy_url = '';
        if (this.cfg.preserve_media_suffix === undefined) this.cfg.preserve_media_suffix = false;
        if (!this.cfg.proxy_no_proxy) {
          this.cfg.proxy_no_proxy = 'localhost,127.0.0.1,::1,0.0.0.0,host.docker.internal,*.local,10.*,192.168.*,172.16.*,172.17.*,172.18.*,172.19.*,172.20.*,172.21.*,172.22.*,172.23.*,172.24.*,172.25.*,172.26.*,172.27.*,172.28.*,172.29.*,172.30.*,172.31.*';
        }
      } catch (ex) {}
    },
    async saveSettings() {
      this.testResult = null;
      try {
        await this.api('PUT', '/api/settings', this.cfg);
        this.testResult = { ok: true, message: '配置已保存并生效' };
      } catch (e) { this.testResult = { ok: false, message: e.message }; }
    },
    async testTmdb() {
      this.testResult = null;
      try { this.testResult = await this.api('POST', '/api/settings/test-tmdb'); } catch (e) { this.testResult = { ok: false, message: e.message }; }
    },
    async testAi() {
      this.testResult = null;
      try {
        this.testResult = await this.api('POST', '/api/settings/test-ai');
        if (this.testResult.models) this.ollamaModels = this.testResult.models;
      } catch (e) { this.testResult = { ok: false, message: e.message }; }
    },
    async refreshOllamaModels() {
      this.testResult = null;
      try {
        var params = new URLSearchParams();
        if (this.cfg.ollama_url) params.set('ollama_url', this.cfg.ollama_url);
        var suffix = params.toString() ? '?' + params.toString() : '';
        var data = await this.api('GET', '/api/settings/ollama-models' + suffix);
        this.ollamaModels = data.models || [];
        this.testResult = {
          ok: this.ollamaModels.length > 0,
          message: data.message || (this.ollamaModels.length ? '已获取本地模型列表' : '未获取到本地模型')
        };
      } catch (ex) { this.testResult = { ok: false, message: ex.message }; }
    },
    async clearCache() {
      if (!(await this.confirmAction({
        title: '清除 API 缓存',
        message: '清除后所有识别结果将重新向 API 请求，不会影响已归档的文件。',
        confirmText: '清除',
        danger: true,
      }))) return;
      this.testResult = null;
      try {
        var r = await this.api('POST', '/api/settings/clear-cache');
        this.testResult = { ok: true, message: r.message };
      } catch (e) { this.testResult = { ok: false, message: e.message }; }
    },
    async testProxy() {
      this.proxyTesting = true;
      this.proxyTestResult = null;
      try {
        this.proxyTestResult = await this.api('POST', '/api/settings/test-proxy', this.cfg);
      } catch (e) {
        this.proxyTestResult = {
          ok: false,
          summary: { total: 0, success: 0, failed: 0, avg_latency_ms: null },
          proxy: {},
          results: [],
          message: e.message
        };
      }
      this.proxyTesting = false;
    },
    proxyModeText(mode) {
      var map = { manual: '手动代理', environment: '环境/系统代理', direct: '直连' };
      return map[mode] || mode || '-';
    },
    addStripKeyword() {
      var kw = (this.newKeyword || '').trim();
      if (!kw) return;
      if (!this.cfg.strip_keywords) this.cfg.strip_keywords = [];
      if (this.cfg.strip_keywords.indexOf(kw) === -1) {
        this.cfg.strip_keywords.push(kw);
      }
      this.newKeyword = '';
    },
    removeStripKeyword(idx) {
      if (this.cfg.strip_keywords) {
        this.cfg.strip_keywords.splice(idx, 1);
      }
    },
    async testTelegram() {
      this.tgTestResult = null;
      try {
        await this.api('PUT', '/api/settings', this.cfg);
        var r = await this.api('POST', '/api/settings/test-telegram');
        this.tgTestResult = r;
      } catch (e) { this.tgTestResult = { ok: false, message: e.message }; }
    },
  },
});

app.mount('#app');
