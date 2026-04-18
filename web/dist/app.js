/* app.js — Vue 3 SPA (Vue is loaded via <script> in index.html) */

const API = window.location.origin;

const app = Vue.createApp({
  data() {
    return {
      page: location.hash ? location.hash.slice(1) : 'folders',
      // Folders
      folders: [],
      showAddFolder: false,
      newFolder: { path: '', target_root: '', media_type: 'auto', data_source: 'siliconflow_tmdb' },
      editFolderVisible: false,
      editFolderData: { id: null, path: '', target_root: '', media_type: 'auto', data_source: 'siliconflow_tmdb' },
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
      // Manual match
      manualMatchVisible: false,
      manualRecord: null,
      manualSeason: null,
      manualEpOffset: 0,
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
    };
  },
  mounted() {
    this.loadSettings();
    this.loadFolders();
    this.loadRecords();
    this.connectWs();
    // Keep hash in sync when page changes, so F5 restores correctly
    this.$watch('page', function(val) { location.hash = val; });
  },
  computed: {
    allSelected: function() {
      return this.records.length > 0 && this.selectedIds.length === this.records.length;
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
    },

    // --- Folder Browser ---
    async openBrowse(field) {
      this.browseField = field;
      this.browseSelected = '';
      var startPath = '';
      if (field === 'path') startPath = this.newFolder.path;
      else if (field === 'target_root') startPath = this.newFolder.target_root;
      else if (field === 'edit_path') startPath = this.editFolderData.path;
      else if (field === 'edit_target_root') startPath = this.editFolderData.target_root;
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
      } else if (this.browseField === 'edit_path') {
        this.editFolderData.path = chosen;
      } else if (this.browseField === 'edit_target_root') {
        this.editFolderData.target_root = chosen;
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
        this.newFolder = { path: '', target_root: '', media_type: 'auto', data_source: 'siliconflow_tmdb' };
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
      this.loadRecords();
    },
    resetRecordFilter() {
      this.recordFilter = '';
      this.recordKeyword = '';
      this.recordTypeFilter = '';
      this.recordPage = 1;
      this.loadRecords();
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
        this.loadRecords();
      } catch (e) { alert(e.message); }
    },
    async batchRetrySelected() {
      if (!this.selectedIds.length) return;
      try {
        await this.api('POST', '/api/records/batch-retry', { ids: this.selectedIds });
        this.loadRecords();
      } catch (e) { alert(e.message); }
    },
    async clearFailed() {
      if (!confirm('确认清除所有失败记录？')) return;
      try {
        await this.api('POST', '/api/records/clear-failed');
        this.loadRecords();
      } catch (e) { alert(e.message); }
    },
    async clearAll() {
      if (!confirm('确认清空所有刮削记录？此操作不可恢复。')) return;
      try {
        await this.api('POST', '/api/records/clear-all');
        this.loadRecords();
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
      if (r.matched_provider === 'tmdb' || r.matched_provider === 'bgm') {
        // Try to infer from target_path or metadata
      }
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
      this.searchMode = 'name';
      this.searchTmdbId = '';
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
