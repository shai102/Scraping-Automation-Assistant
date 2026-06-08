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
  toggleMetadataLogGroupedView() {
    this.metadataLogGroupedView = !this.metadataLogGroupedView;
    this.metadataLogSelectedGroups = [];
    this.buildMetadataLogGroups();
  },
  buildMetadataLogGroups() {
    if (this.currentLogKind && this.currentLogKind() !== 'metadata') {
      this.metadataLogGroupedRecords = [];
      this.metadataLogSelectedGroups = [];
      return;
    }
    var groups = {};
    var order = [];
    (this.logEntries || []).forEach(function(log, idx) {
      var parsed = log.parsed || {};
      var key = metadataLogGroupKey(log);
      if (!groups[key]) {
        groups[key] = {
          key: key,
          dir_path: metadataLogGroupPath(log),
          dir_name: metadataLogGroupName(log),
          title: parsed.title || '',
          skip_rule: metadataLogSkipRuleFromLog(log),
          total: 0,
          success: 0,
          failed: 0,
          scan: 0,
          records: [],
        };
        order.push(key);
      }
      var group = groups[key];
      var skipRule = metadataLogSkipRuleFromLog(log);
      if (skipRule && (!group.skip_rule || group.skip_rule.indexOf(':') < 0)) {
        group.skip_rule = skipRule;
      }
      group.total += 1;
      if (log.kind === 'metadata_refresh') group.success += 1;
      else if (log.kind === 'metadata_refresh_failed') group.failed += 1;
      else group.scan += 1;
      group.records.push(Object.assign({ _idx: idx }, log));
    });
    Object.keys(groups).forEach(function(key) {
      if (!groups[key].skip_rule) groups[key].skip_rule = metadataLogSkipRuleFromGroup(groups[key]);
    });
    this.metadataLogGroupedRecords = order.map(function(key) { return groups[key]; });
    var validKeys = this.metadataLogGroupedRecords.map(function(group) { return group.key; });
    this.metadataLogSelectedGroups = (this.metadataLogSelectedGroups || []).filter(function(key) {
      return validKeys.indexOf(key) >= 0;
    });
  },
  toggleMetadataLogGroup(group) {
    if (!group || !group.key) return;
    if (this.metadataLogExpandedGroups[group.key]) {
      delete this.metadataLogExpandedGroups[group.key];
    } else {
      this.metadataLogExpandedGroups[group.key] = true;
    }
    this.metadataLogExpandedGroups = Object.assign({}, this.metadataLogExpandedGroups);
  },
  metadataLogGroupSelected(group) {
    return !!(group && (this.metadataLogSelectedGroups || []).indexOf(group.key) >= 0);
  },
  toggleMetadataLogSelectGroup(group) {
    if (!group || !group.key || !this.metadataLogSkipRule(group)) return;
    var selected = this.metadataLogSelectedGroups || [];
    if (selected.indexOf(group.key) >= 0) {
      this.metadataLogSelectedGroups = selected.filter(function(key) { return key !== group.key; });
    } else {
      this.metadataLogSelectedGroups = selected.concat([group.key]);
    }
  },
  metadataLogSkipRule(group) {
    if (!group) return '';
    return group.skip_rule || metadataLogSkipRuleFromGroup(group);
  },
  metadataLogSelectedSkipRules() {
    var self = this;
    var seen = {};
    var rules = [];
    (this.metadataLogGroupedRecords || []).forEach(function(group) {
      if (!self.metadataLogGroupSelected(group)) return;
      var rule = self.metadataLogSkipRule(group);
      if (!rule || seen[rule]) return;
      seen[rule] = true;
      rules.push(rule);
    });
    return rules;
  },
  async skipSelectedMetadataLogGroups() {
    var rules = this.metadataLogSelectedSkipRules();
    if (!rules.length) return;
    var confirmed = await this.confirmAction({
      title: '跳过元数据补齐',
      message: '将把选中的 ' + rules.length + ' 个作品加入“跳过自动补齐”规则。\n\n规则：' + rules.join('，') + '\n\n之后命中的作品不会再进入元数据自动补齐巡检。',
      confirmText: '跳过补齐',
    });
    if (!confirmed) return;
    try {
      var cfg = await this.api('GET', '/api/settings/raw');
      var merged = metadataLogMergeRules(cfg.metadata_refresh_skip_rules || '', rules);
      cfg.metadata_refresh_skip_rules = merged;
      await this.api('PUT', '/api/settings', cfg);
      this.cfg = Object.assign({}, this.cfg || {}, { metadata_refresh_skip_rules: merged });
      this.metadataLogSelectedGroups = [];
      this.notify('已加入跳过补齐规则：' + rules.join('，'), 'success');
      this.loadLogs();
    } catch (e) {
      this.notify(e.message, 'error');
    }
  },
  metadataLogKindText(kind) {
    var map = {
      metadata_scan_start: '巡检开始',
      metadata_scan_item: '待补齐',
      metadata_refresh: '刷新成功',
      metadata_refresh_failed: '刷新失败',
      metadata_scan_done: '巡检完成',
      general: '日志',
    };
    return map[kind] || kind || '日志';
  },
  metadataLogKindClass(kind) {
    if (kind === 'metadata_refresh') return 'badge-success';
    if (kind === 'metadata_refresh_failed') return 'badge-danger';
    if (kind === 'metadata_scan_item') return 'badge-warning';
    return 'badge-gray';
  },
  logLevelClass(level) {
    var map = { INFO: 'badge-success', WARNING: 'badge-warning', ERROR: 'badge-danger' };
    return map[String(level || '').toUpperCase()] || 'badge-gray';
  },
};

function metadataLogMergeRules(existing, rules) {
  var lines = String(existing || '').split(/[\n,，;；]+/).map(function(item) {
    return String(item || '').trim();
  }).filter(Boolean);
  var seen = {};
  var merged = [];
  lines.concat(rules || []).forEach(function(rule) {
    var value = String(rule || '').trim();
    var key = value.replace(/\s+/g, '').toLowerCase();
    if (!value || seen[key]) return;
    seen[key] = true;
    merged.push(value);
  });
  return merged.join('\n');
}

function metadataLogGroupKey(log) {
  var parsed = (log && log.parsed) || {};
  var path = String(parsed.target_path || '').trim();
  if (path && path !== '-') return 'path:' + metadataLogGroupPath(log);
  var title = String(parsed.title || '').trim();
  if (title && title !== '-') return 'title:' + title;
  return 'kind:' + (log.kind || 'general');
}

function metadataLogSkipRuleFromLog(log) {
  var parsed = (log && log.parsed) || {};
  var provider = String(parsed.provider || '').trim().toLowerCase();
  var id = String(parsed.id || '').trim();
  if (provider && id && id !== '-' && id !== 'None') return provider + ':' + id;

  var pathRule = metadataLogSkipRuleFromPath(parsed.target_path || '');
  if (pathRule) return pathRule;

  var title = String(parsed.title || '').trim();
  if (title && title !== '-') return title;
  return '';
}

function metadataLogSkipRuleFromGroup(group) {
  if (!group) return '';
  if (String(group.key || '').indexOf('kind:') === 0) return '';
  var pathRule = metadataLogSkipRuleFromPath(group.dir_path || '');
  if (pathRule) return pathRule;
  var title = String(group.title || group.dir_name || '').trim();
  if (title && title !== '-') return title;
  return '';
}

function metadataLogSkipRuleFromPath(path) {
  var text = String(path || '');
  var match = /(?:tmdbid|tmdb_id|tmdb)[=\-:\s]*(\d+)/i.exec(text);
  if (match && match[1]) return 'tmdb:' + match[1];
  match = /(?:bgmid|bgm_id|bgm)[=\-:\s]*(\d+)/i.exec(text);
  if (match && match[1]) return 'bgm:' + match[1];
  return '';
}

function metadataLogGroupPath(log) {
  var parsed = (log && log.parsed) || {};
  var path = String(parsed.target_path || '').trim();
  if (!path || path === '-') return '';
  var parts = path.split('/');
  var seasonIndex = -1;
  for (var i = 0; i < parts.length; i++) {
    if (/^Season\s+\d+$/i.test(parts[i])) {
      seasonIndex = i;
      break;
    }
  }
  if (seasonIndex > 0) return parts.slice(0, seasonIndex).join('/') || '/';
  if (parts.length > 1) return parts.slice(0, -1).join('/') || '/';
  return path;
}

function metadataLogGroupName(log) {
  var parsed = (log && log.parsed) || {};
  var title = String(parsed.title || '').trim();
  if (title && title !== '-') return title;
  var groupPath = metadataLogGroupPath(log);
  if (groupPath) {
    var parts = groupPath.split('/').filter(Boolean);
    if (parts.length) return parts[parts.length - 1];
  }
  if (log.kind === 'metadata_scan_start') return '巡检开始';
  if (log.kind === 'metadata_scan_done') return '巡检完成';
  return '其它元数据日志';
}
