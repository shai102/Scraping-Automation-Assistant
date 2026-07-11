/* app.js — Vue 3 SPA bootstrap */

var GROUPED_VIEW_KEY = 'scraping_records_grouped_view';
var SYMLINK_GROUPED_VIEW_KEY = 'scraping_symlink_grouped_view';
var CURRENT_PAGE_KEY = window.scraperAppPageKey || 'scraping_current_page';
var UI_STATE_KEY = 'scraping_ui_state_v1';
var UI_STATE_STRING_FIELDS = [
  'recordFilter',
  'recordKeyword',
  'recordTypeFilter',
  'recordParseFilter',
  'symlinkFilter',
  'symlinkKeyword',
  'logLevel',
  'logKeyword',
  'logDate',
  'recognitionTab',
  'recognitionName',
  'recognitionBatchText',
];
var UI_STATE_NUMBER_FIELDS = [
  'recordPage',
  'recordPageSize',
  'recordGoPage',
  'symlinkPage',
  'symlinkPageSize',
  'symlinkGoPage',
  'logLimit',
];
var UI_STATE_BOOLEAN_FIELDS = [
  'groupedView',
  'symlinkGroupedView',
  'logShowAnnotations',
  'logAutoRefreshEnabled',
  'metadataLogGroupedView',
  'recognitionUseAi',
  'recognitionBypassCache',
  'searchIsTv',
];

function isKnownPage(page) {
  return (window.scraperAppPages || []).indexOf(page) >= 0;
}

function normalizePageHash() {
  return String(location.hash || '').replace(/^#/, '').replace(/^\/+/, '');
}

function readUiState() {
  try {
    var raw = localStorage.getItem(UI_STATE_KEY);
    return raw ? JSON.parse(raw) || {} : {};
  } catch (e) {
    return {};
  }
}

function restoreUiState(vm) {
  var state = readUiState();
  UI_STATE_STRING_FIELDS.forEach(function(field) {
    if (typeof state[field] === 'string') vm[field] = state[field];
  });
  UI_STATE_NUMBER_FIELDS.forEach(function(field) {
    var value = Number(state[field]);
    if (Number.isFinite(value)) vm[field] = Math.max(1, Math.floor(value));
  });
  UI_STATE_BOOLEAN_FIELDS.forEach(function(field) {
    if (typeof state[field] === 'boolean') vm[field] = state[field];
  });
  if (vm.recognitionTab !== 'single' && vm.recognitionTab !== 'batch') {
    vm.recognitionTab = 'single';
  }
  if (!vm.logDate || vm.logDate === '__latest__') {
    vm.logDate = 'latest';
  }
}

function saveUiState(vm) {
  var state = {};
  UI_STATE_STRING_FIELDS.forEach(function(field) {
    state[field] = String(vm[field] == null ? '' : vm[field]);
  });
  UI_STATE_NUMBER_FIELDS.forEach(function(field) {
    state[field] = Number(vm[field]) || 1;
  });
  UI_STATE_BOOLEAN_FIELDS.forEach(function(field) {
    state[field] = !!vm[field];
  });
  try {
    localStorage.setItem(UI_STATE_KEY, JSON.stringify(state));
  } catch (e) {}
}

var app = Vue.createApp({
  data: window.scraperAppCreateData,
  mounted() {
    try {
      this.groupedView = localStorage.getItem(GROUPED_VIEW_KEY) === '1';
      this.symlinkGroupedView = localStorage.getItem(SYMLINK_GROUPED_VIEW_KEY) === '1';
    } catch (e) {}
    restoreUiState(this);
    this.loadSettings();
    this.loadFolders();
    this.connectWs();
    if (this.page === 'records') {
      if (this.groupedView) this.loadGroupedRecords();
      else this.loadRecords();
    } else {
      this.loadRecords();
    }
    if (this.page === 'symlink_records') {
      if (this.symlinkGroupedView) this.loadSymlinkGroupedRecords();
      else this.loadSymlinkRecords();
      this.loadSymlinkStats();
    }
    if (this.isLogPage(this.page)) {
      this.loadLogs();
      this.startLogAutoRefresh();
    }
    if (this.page === 'symlink_folders') {
      this.loadFolders();
    }
    if (this.page === 'system') this.loadSystemStatus();
    this._onHashChange = () => {
      var nextPage = normalizePageHash();
      if (isKnownPage(nextPage) && nextPage !== this.page) {
        this.page = nextPage;
      }
    };
    window.addEventListener('hashchange', this._onHashChange);
    this.$watch('page', function(val) {
      if (!isKnownPage(val)) val = 'folders';
      try { localStorage.setItem(CURRENT_PAGE_KEY, val); } catch (e) {}
      saveUiState(this);
      if (normalizePageHash() !== val) {
        location.hash = val;
      }
      if (val === 'records') {
        if (this.groupedView) this.loadGroupedRecords();
        else this.loadRecords();
      }
      if (val === 'symlink_records') {
        if (this.symlinkGroupedView) this.loadSymlinkGroupedRecords();
        else this.loadSymlinkRecords();
        this.loadSymlinkStats();
      }
      if (this.isLogPage(val)) {
        this.loadLogs();
        this.startLogAutoRefresh();
      } else {
        this.stopLogAutoRefresh();
      }
      if (val === 'symlink_folders') {
        this.loadFolders();
      }
      if (val === 'system') this.loadSystemStatus();
    });
    var self = this;
    UI_STATE_STRING_FIELDS
      .concat(UI_STATE_NUMBER_FIELDS)
      .concat(UI_STATE_BOOLEAN_FIELDS)
      .forEach(function(field) {
        self.$watch(field, function() {
          saveUiState(self);
        });
      });
  },
  beforeUnmount() {
    if (this._onHashChange) window.removeEventListener('hashchange', this._onHashChange);
    this.stopLogAutoRefresh();
  },
  computed: window.scraperAppComputed,
  methods: Object.assign(
    {},
    window.scraperAppMethodsCore,
    window.scraperAppMethodsFolders,
    window.scraperAppMethodsSymlinks,
    window.scraperAppMethodsRecords,
    window.scraperAppMethodsLogs,
    window.scraperAppMethodsRecognition,
    window.scraperAppMethodsSettings
  ),
});

if (window.scraperAppPageComponents) {
  Object.keys(window.scraperAppPageComponents).forEach(function(name) {
    app.component(name, window.scraperAppPageComponents[name]);
  });
}

app.mount('#app');
