window.scraperAppMethodsSymlinks = {
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
    try { localStorage.setItem('scraping_symlink_grouped_view', this.symlinkGroupedView ? '1' : '0'); } catch (e) {}
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
    var result = await this.confirmAction({
      title: '删除软链接记录',
      message: '确认删除该记录？',
      confirmText: '删除',
      danger: true,
      checkboxLabel: '同时删除本地软链接文件',
      checkboxDefault: false,
    });
    if (!result) return;
    try {
      var url = '/api/symlinks/' + id + (result.checked ? '?delete_files=true' : '');
      await this.api('DELETE', url);
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
    var result = await this.confirmAction({
      title: '批量删除软链接记录',
      message: '确认删除选中的 ' + this.symlinkSelectedIds.length + ' 条记录？',
      confirmText: '删除',
      danger: true,
      checkboxLabel: '同时删除本地软链接文件',
      checkboxDefault: false,
    });
    if (!result) return;
    try {
      await this.api('POST', '/api/symlinks/batch-delete', { ids: this.symlinkSelectedIds, delete_files: !!result.checked });
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
      title: '删除软连接分组并清理目录',
      message: '确认删除“' + g.dir_name + '”内的全部 ' + g.total + ' 条软连接记录，并同步清理目录中的软连接文件？\n\n目录：' + g.dir_path,
      confirmText: '删除并清理',
      danger: true,
    }))) return;
    try {
      var res = await this.api('POST', '/api/symlinks/delete-group', { ids: g.ids, group_dir: g.dir_path });
      this.loadSymlinkGroupedRecords();
      this.loadSymlinkStats();
      this.notify('已删除 ' + (res.deleted || 0) + ' 条记录，清理 ' + (res.files_deleted || 0) + ' 个文件' + (res.dir_deleted ? '，并删除空目录' : ''), 'success');
    } catch (e) { this.notify(e.message, 'error'); }
  },
  async deleteSymlinkGroupRecord(g, id) {
    var result = await this.confirmAction({
      title: '删除软链接记录',
      message: '确认删除该记录？',
      confirmText: '删除',
      danger: true,
      checkboxLabel: '同时删除本地软链接文件',
      checkboxDefault: false,
    });
    if (!result) return;
    try {
      var url = '/api/symlinks/' + id + (result.checked ? '?delete_files=true' : '');
      await this.api('DELETE', url);
      this.loadSymlinkGroupedRecords();
      this.loadSymlinkStats();
    } catch (e) { this.notify(e.message, 'error'); }
  },
};
