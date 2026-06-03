window.scraperAppMethodsRecords = {
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
    var result = await this.confirmAction({
      title: '删除刮削记录',
      message: '确认删除该记录？',
      confirmText: '删除',
      danger: true,
      checkboxLabel: '同时删除本地文件（目标文件及 NFO/缩略图）',
      checkboxDefault: false,
    });
    if (!result) return;
    try {
      var url = '/api/records/' + id + (result.checked ? '?delete_files=true' : '');
      await this.api('DELETE', url);
      this.loadRecords();
    } catch (e) { this.notify(e.message, 'error'); }
  },
  async retryRecord(id) {
    try { await this.api('POST', '/api/records/' + id + '/retry'); this.loadRecords(); } catch (e) { this.notify(e.message, 'error'); }
  },
  async refreshMetadata(id) {
    try {
      var res = await this.api('POST', '/api/records/' + id + '/refresh-metadata');
      if (res.updated) {
        this.notify('元数据已刷新', 'success');
      } else {
        this.notify(res.message || '元数据已是最新', 'info');
      }
      this.loadRecords();
    } catch (e) { this.notify(e.message, 'error'); }
  },
  async batchRefreshMetadata() {
    if (!this.selectedIds.length) return;
    try {
      var res = await this.api('POST', '/api/records/batch-refresh-metadata', { ids: this.selectedIds });
      this.notify(res.message || ('已刷新 ' + (res.updated || 0) + ' 条记录'), 'success');
      if (this.groupedView) this.loadGroupedRecords();
      else this.loadRecords();
    } catch (e) { this.notify(e.message, 'error'); }
  },
  toggleSelectAll(e) {
    if (e.target.checked) {
      this.selectedIds = this.records.map(function(r) { return r.id; });
    } else {
      this.selectedIds = [];
    }
  },
  async batchDeleteSelected() {
    if (!this.selectedIds.length) return;
    var result = await this.confirmAction({
      title: '批量删除刮削记录',
      message: '确认删除选中的 ' + this.selectedIds.length + ' 条记录？',
      confirmText: '删除',
      danger: true,
      checkboxLabel: '同时删除本地文件（目标文件及 NFO/缩略图）',
      checkboxDefault: false,
    });
    if (!result) return;
    try {
      await this.api('POST', '/api/records/batch-delete', { ids: this.selectedIds, delete_files: !!result.checked });
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
  toggleGroupedView() {
    this.groupedView = !this.groupedView;
    try { localStorage.setItem('scraping_records_grouped_view', this.groupedView ? '1' : '0'); } catch (e) {}
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
      title: '删除刮削分组并清理目录',
      message: '确认删除“' + g.dir_name + '”内的全部 ' + g.total + ' 条刮削记录，并同步清理该输出目录下的归档文件？\n\n目录：' + g.dir_path,
      confirmText: '删除并清理',
      danger: true,
    }))) return;
    try {
      var res = await this.api('POST', '/api/records/delete-group', { ids: g.ids, group_dir: g.dir_path });
      this.loadGroupedRecords();
      this.notify('已删除 ' + (res.deleted || 0) + ' 条记录，清理 ' + (res.files_deleted || 0) + ' 个文件' + (res.dir_deleted ? '，并删除空目录' : ''), 'success');
    } catch (e) { this.notify(e.message, 'error'); }
  },
  async deleteGroupRecord(g, id) {
    var result = await this.confirmAction({
      title: '删除刮削记录',
      message: '确认删除该记录？',
      confirmText: '删除',
      danger: true,
      checkboxLabel: '同时删除本地文件（目标文件及 NFO/缩略图）',
      checkboxDefault: false,
    });
    if (!result) return;
    try {
      var url = '/api/records/' + id + (result.checked ? '?delete_files=true' : '');
      await this.api('DELETE', url);
      this.loadGroupedRecords();
      if (this.expandedGroups[g.dir_path]) this.loadGroupRecords(g);
    } catch (e) { this.notify(e.message, 'error'); }
  },
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
};
