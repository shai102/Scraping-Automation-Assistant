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
      var maxPage = Math.ceil(this.recordTotal / this.recordPageSize) || 1;
      if (this.recordPage > maxPage) {
        this.recordPage = maxPage;
        this.recordGoPage = maxPage;
        if (this.recordTotal > 0) {
          return this.loadRecords();
        }
      }
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
        this.notify('已从 TMDB 刷新元数据', 'success');
      } else {
        this.notify(res.message || 'TMDB 元数据已是最新', 'info');
      }
      this.loadRecords();
    } catch (e) { this.notify(e.message, 'error'); }
  },
  async updateFromMetadataHub(id) {
    var confirmed = await this.confirmAction({
      title: '从 Metadata Hub 更新',
      message: '将按 TMDB ID、季号和集号，用本地 Hub 中的 NFO 与图片覆盖当前作品对应文件。此操作不会查询 TMDB。',
      confirmText: '开始更新',
    });
    if (!confirmed) return;
    try {
      var res = await this.api('POST', '/api/records/' + id + '/update-from-metadata-hub');
      this.notify(res.message || '已从 Metadata Hub 更新', 'success');
      this.refreshRecords();
    } catch (e) { this.notify(e.message, 'error'); }
  },
  async batchRefreshMetadata() {
    if (!this.selectedIds.length) return;
    try {
      var res = await this.api('POST', '/api/records/batch-refresh-metadata', { ids: this.selectedIds });
      this.notify(res.message || ('已从 TMDB 刷新 ' + (res.updated || 0) + ' 条记录'), 'success');
      if (this.groupedView) this.loadGroupedRecords();
      else this.loadRecords();
    } catch (e) { this.notify(e.message, 'error'); }
  },
  async batchUpdateFromMetadataHub() {
    if (!this.selectedIds.length) return;
    var confirmed = await this.confirmAction({
      title: '批量从 Metadata Hub 更新',
      message: '将只处理所选记录中已成功且来源为 TMDB 的项目，并用 Hub 本地文件覆盖对应 NFO 与图片。',
      confirmText: '开始更新',
    });
    if (!confirmed) return;
    try {
      var res = await this.api('POST', '/api/records/batch-update-from-metadata-hub', { ids: this.selectedIds });
      this.notify(res.message || ('已从 Metadata Hub 更新 ' + (res.updated || 0) + ' 条记录'), res.failed ? 'warning' : 'success');
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
};
