/* app.js — Vue 3 SPA bootstrap */

var GROUPED_VIEW_KEY = 'scraping_records_grouped_view';
var SYMLINK_GROUPED_VIEW_KEY = 'scraping_symlink_grouped_view';

var app = Vue.createApp({
  data: window.scraperAppCreateData,
  mounted() {
    try {
      this.groupedView = localStorage.getItem(GROUPED_VIEW_KEY) === '1';
      this.symlinkGroupedView = localStorage.getItem(SYMLINK_GROUPED_VIEW_KEY) === '1';
    } catch (e) {}
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
    this.$watch('page', function(val) {
      location.hash = val;
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
    });
  },
  beforeUnmount() {
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
