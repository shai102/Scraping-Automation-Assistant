window.scraperAppMethodsCore = {
  isLogPage(page) {
    return page === 'logs' || page === 'app_logs' || page === 'metadata_logs';
  },
  currentLogKind() {
    if (this.page === 'app_logs') return 'app';
    if (this.page === 'metadata_logs') return 'metadata';
    return 'scrape';
  },
  currentLogTitle() {
    if (this.page === 'app_logs') return '普通日志';
    if (this.page === 'metadata_logs') return '元数据巡检日志';
    return '刮削日志';
  },
  currentLogClearLabel() {
    if (this.page === 'app_logs') return '普通日志';
    if (this.page === 'metadata_logs') return '元数据巡检日志';
    return '刮削日志';
  },
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
        checkboxLabel: opts.checkboxLabel || '',
        checkboxValue: !!opts.checkboxDefault,
        resolver: resolve,
      };
    });
  },
  resolveConfirm(value) {
    var resolver = this.confirmDialog && this.confirmDialog.resolver;
    var checked = this.confirmDialog ? !!this.confirmDialog.checkboxValue : false;
    this.confirmDialog.visible = false;
    this.confirmDialog.resolver = null;
    this.confirmDialog.checkboxLabel = '';
    this.confirmDialog.checkboxValue = false;
    if (resolver) resolver(value ? { confirmed: true, checked: checked } : false);
  },
  async api(method, path, body) {
    var opts = { method: method, headers: { 'Content-Type': 'application/json' } };
    if (body) opts.body = JSON.stringify(body);
    var resp = await fetch(window.location.origin + path, opts);
    if (!resp.ok) {
      var err = await resp.json().catch(function() { return { detail: resp.statusText }; });
      throw new Error(err.detail || resp.statusText);
    }
    return resp.json();
  },
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
      this.recordTotal++;
    }
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
      Object.assign(this.symlinkRecords[idx], data);
    } else {
      this.symlinkTotal++;
    }
    clearTimeout(this._symlinkRefreshTimer);
    var self = this;
    this._symlinkRefreshTimer = setTimeout(function() {
      if (self.symlinkGroupedView) self.loadSymlinkGroupedRecords();
      else self.loadSymlinkRecords();
      self.loadSymlinkStats();
    }, 3000);
  },
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
};
