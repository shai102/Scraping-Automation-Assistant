window.scraperAppMethodsLogs = {
  async loadLogs() {
    this.logLoading = true;
    try {
      var params = new URLSearchParams({ limit: this.logLimit, kind: this.currentLogKind() });
      if (this.logLevel) params.set('level', this.logLevel);
      if (this.logKeyword) params.set('keyword', this.logKeyword);
      if (this.logDate) params.set('date', this.logDate);
      var data = await this.api('GET', '/api/logs?' + params.toString());
      this.logEntries = data.items || [];
      this.logPath = data.path || '';
      this.logDates = data.available_dates || [];
      this.logDate = data.selected_date || this.logDate || '';
      this.buildMetadataLogGroups();
    } catch (e) {
      this.notify(e.message, 'error');
    }
    this.logLoading = false;
  },
  refreshLogs() {
    this.loadLogs();
  },
  async clearLogs() {
    var title = this.currentLogClearLabel();
    var ok = await this.confirmAction({
      title: '清除日志',
      message: '确定要清除所选日期的' + title + '吗？此操作不可恢复。',
      danger: true,
      confirmText: '清除',
      cancelText: '取消'
    });
    if (!ok) return;
    try {
      var path = '/api/logs?kind=' + encodeURIComponent(this.currentLogKind());
      if (this.logDate) path += '&date=' + encodeURIComponent(this.logDate);
      var data = await this.api('DELETE', path);
      this.notify(data.message || '日志已清除', data.ok ? 'success' : 'error');
      if (data.ok) this.loadLogs();
    } catch (e) {
      this.notify(e.message, 'error');
    }
  },
  startLogAutoRefresh() {
    this.stopLogAutoRefresh();
    if (!this.logAutoRefreshEnabled) return;
    var self = this;
    this._logRefreshTimer = setInterval(function() {
      if (self.isLogPage(self.page) && !self.logLoading) self.loadLogs();
    }, 3000);
  },
  stopLogAutoRefresh() {
    if (this._logRefreshTimer) {
      clearInterval(this._logRefreshTimer);
      this._logRefreshTimer = null;
    }
  },
  resetLogFilter() {
    this.logLevel = '';
    this.logKeyword = '';
    this.logLimit = 200;
    this.loadLogs();
  },
  onLogDateChange() {
    this.loadLogs();
  },
  logLevelClass(level) {
    var map = { INFO: 'badge-success', WARNING: 'badge-warning', ERROR: 'badge-danger' };
    return map[String(level || '').toUpperCase()] || 'badge-gray';
  },
};
