/* app.js — Vue 3 SPA (Vue is loaded via <script> in index.html) */

const API = window.location.origin;

const app = Vue.createApp({
  data() {
    return {
      page: location.hash ? location.hash.slice(1) : 'folders',
      // Folders
      folders: [],
      showAddFolder: false,
      newFolder: { path: '', target_root: '', media_type: 'auto', data_source: 'siliconflow_tmdb', organize_mode: 'move', symlink_source: '' },
      editFolderVisible: false,
      editFolderData: { id: null, path: '', target_root: '', media_type: 'auto', data_source: 'siliconflow_tmdb', organize_mode: 'move', symlink_source: '' },
      // Symlink export
      showAddSymlink: false,
      newSymlinkFolder: { path: '', target_root: '' },
      symlinkRecords: [],
      symlinkPage: 1,
      symlinkPageSize: 20,
      symlinkTotal: 0,
      symlinkStats: {},
      // Records
      records: [],
      recordFilter: '',
      recordKeyword: '',
      recordTypeFilter: '',
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
      // Settings
      cfg: {},
      testResult: null,
      tgTestResult: null,
      ollamaModels: [],
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
    };
  },
  mounted() {
    this.loadSettings();
    this.loadFolders();
    this.loadRecords();
    this.connectWs();
    // Keep hash in sync when page changes, so F5 restores correctly
    this.$watch('page', function(val) {
      location.hash = val;
      if (val === 'symlink_records') { this.loadSymlinkRecords(); this.loadSymlinkStats(); }
      if (val === 'symlink_folders') { this.loadFolders(); }
    });
  },
  computed: {
    allSelected: function() {
      return this.records.length > 0 && this.selectedIds.length === this.records.length;
    },
    scrapeFolders: function() {
      return this.folders.filter(function(f) { return f.organize_mode !== 'symlink_export'; });
    },
    symlinkFolders: function() {
      return this.folders.filter(function(f) { return f.organize_mode === 'symlink_export'; });
    },
  },
  methods: {
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
        Object.assign(this.records[idx], data);
      } else {
        this.records.unshift(data);
        this.recordTotal++;
      }
      // Refresh grouped view in background if active
      if (this.groupedView) this.loadGroupedRecords();
    },
    onSymlinkUpdate(data) {
      var idx = this.symlinkRecords.findIndex(function(r) { return r.id === data.id; });
      if (idx >= 0) {
        Object.assign(this.symlinkRecords[idx], data);
      } else {
        this.symlinkRecords.unshift(data);
        this.symlinkTotal++;
      }
      this.loadSymlinkStats();
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
      } catch (e) { alert(e.message); }
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
        this.newFolder = { path: '', target_root: '', media_type: 'auto', data_source: 'siliconflow_tmdb', organize_mode: 'move', symlink_source: '' };
        this.loadFolders();
      } catch (e) { alert(e.message); }
    },
    async toggleFolder(f) {
      try {
        await this.api('PUT', '/api/monitor/folders/' + f.id, { enabled: !f.enabled });
        this.loadFolders();
      } catch (e) { alert(e.message); }
    },
    async deleteFolder(id) {
      if (!confirm('确认删除该监控目录？关联的刮削记录不会删除。')) return;
      try { await this.api('DELETE', '/api/monitor/folders/' + id); this.loadFolders(); } catch (e) { alert(e.message); }
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
        });
        this.editFolderVisible = false;
        this.loadFolders();
      } catch (e) { alert(e.message); }
    },
    async scanFolder(id) {
      try {
        var r = await this.api('POST', '/api/monitor/folders/' + id + '/scan');
        alert(r.message || '扫描已启动');
      } catch (e) { alert(e.message); }
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
      } catch (e) { alert(e.message); }
    },
    async loadSymlinkRecords() {
      try {
        var params = new URLSearchParams({ page: this.symlinkPage, page_size: this.symlinkPageSize });
        var data = await this.api('GET', '/api/symlinks?' + params.toString());
        this.symlinkRecords = data.items || [];
        this.symlinkTotal = data.total || 0;
      } catch (ex) {}
    },
    async loadSymlinkStats() {
      try { this.symlinkStats = await this.api('GET', '/api/symlinks/stats'); } catch (ex) {}
    },
    async deleteSymlinkRecord(id) {
      if (!confirm('确认删除该记录？')) return;
      try { await this.api('DELETE', '/api/symlinks/' + id); this.loadSymlinkRecords(); this.loadSymlinkStats(); } catch (e) { alert(e.message); }
    },
    async clearSymlinkFailed() {
      if (!confirm('确认清除所有失败的软链接记录？')) return;
      try { await this.api('POST', '/api/symlinks/clear-failed'); this.loadSymlinkRecords(); this.loadSymlinkStats(); } catch (e) { alert(e.message); }
    },
    async clearSymlinkAll() {
      if (!confirm('确认清空所有软链接记录？此操作不可恢复。')) return;
      try { await this.api('DELETE', '/api/symlinks/all'); this.loadSymlinkRecords(); this.loadSymlinkStats(); } catch (e) { alert(e.message); }
    },

    // --- Records ---
    async loadRecords() {
      try {
        var params = new URLSearchParams({ page: this.recordPage, page_size: this.recordPageSize });
        if (this.recordFilter) params.set('status', this.recordFilter);
        if (this.recordKeyword) params.set('keyword', this.recordKeyword);
        if (this.recordTypeFilter) params.set('media_type', this.recordTypeFilter);
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
      this.loadRecords();
    },
    async deleteRecord(id) {
      if (!confirm('确认删除该记录？')) return;
      try { await this.api('DELETE', '/api/records/' + id); this.loadRecords(); } catch (e) { alert(e.message); }
    },
    async retryRecord(id) {
      try { await this.api('POST', '/api/records/' + id + '/retry'); this.loadRecords(); } catch (e) { alert(e.message); }
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
      if (!confirm('确认删除选中的 ' + this.selectedIds.length + ' 条记录？')) return;
      try {
        await this.api('POST', '/api/records/batch-delete', { ids: this.selectedIds });
        if (this.groupedView) this.loadGroupedRecords();
        else this.loadRecords();
      } catch (e) { alert(e.message); }
    },
    async batchRetrySelected() {
      if (!this.selectedIds.length) return;
      try {
        await this.api('POST', '/api/records/batch-retry', { ids: this.selectedIds });
        if (this.groupedView) this.loadGroupedRecords();
        else this.loadRecords();
      } catch (e) { alert(e.message); }
    },
    async clearFailed() {
      if (!confirm('确认清除所有失败记录？')) return;
      try {
        await this.api('POST', '/api/records/clear-failed');
        if (this.groupedView) this.loadGroupedRecords();
        else this.loadRecords();
      } catch (e) { alert(e.message); }
    },
    async clearAll() {
      if (!confirm('确认清空所有刮削记录？此操作不可恢复。')) return;
      try {
        await this.api('POST', '/api/records/clear-all');
        if (this.groupedView) this.loadGroupedRecords();
        else this.loadRecords();
      } catch (e) { alert(e.message); }
    },
    exportErrors() {
      var errors = this.records.filter(function(r) { return r.status === 'failed' || r.status === 'pending_manual'; });
      if (!errors.length) { alert('当前页无识别错误记录'); return; }
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
      if (!confirm('确认删除「' + g.dir_name + '」内的全部 ' + g.total + ' 条记录？')) return;
      try {
        await this.api('POST', '/api/records/batch-delete', { ids: g.ids });
        this.loadGroupedRecords();
      } catch (e) { alert(e.message); }
    },
    async deleteGroupRecord(g, id) {
      if (!confirm('确认删除该记录？')) return;
      try {
        await this.api('DELETE', '/api/records/' + id);
        this.loadGroupedRecords();
        if (this.expandedGroups[g.dir_path]) this.loadGroupRecords(g);
      } catch (e) { alert(e.message); }
    },

    // --- Manual Match ---
    openManualMatch(record) {
      this.manualRecord = record;
      this.searchQuery = record.matched_title || record.original_name.replace(/\.[^.]+$/, '');
      var ym = /(19|20)\d{2}/.exec(record.original_name);
      this.searchYear = ym ? parseInt(ym[0]) : null;
      this.candidates = [];
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
      } catch (e) { alert(e.message); }
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
      } catch (e) { alert('匹配失败: ' + e.message); }
      this.searching = false;
    },
    async applyManualMatch(candidate) {
      if (!this.manualRecord) return;
      var provider = ((this.cfg && this.cfg.data_source) || 'siliconflow_tmdb') === 'siliconflow_tmdb' ? 'tmdb' : 'bgm';
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
      } catch (e) { alert('匹配失败: ' + e.message); }
    },

    // --- Settings ---
    async loadSettings() {
      try { this.cfg = await this.api('GET', '/api/settings/raw'); } catch (ex) {}
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
      try {
        var data = await this.api('GET', '/api/settings/ollama-models');
        this.ollamaModels = data.models || [];
      } catch (ex) {}
    },
    async clearCache() {
      if (!confirm('确认清除 API 缓存？\n清除后所有识别结果将重新向 API 请求，不会影响已归档的文件。')) return;
      this.testResult = null;
      try {
        var r = await this.api('POST', '/api/settings/clear-cache');
        this.testResult = { ok: true, message: r.message };
      } catch (e) { this.testResult = { ok: false, message: e.message }; }
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
