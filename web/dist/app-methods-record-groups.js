window.scraperAppMethodsRecords = Object.assign(window.scraperAppMethodsRecords || {}, {
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
      this.expandedGroups = {};
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
});
