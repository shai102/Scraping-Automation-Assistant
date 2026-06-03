window.scraperAppComputed = {
  allSelected: function() {
    return this.records.length > 0 && this.selectedIds.length === this.records.length;
  },
  symlinkAllSelected: function() {
    return this.symlinkRecords.length > 0 && this.symlinkSelectedIds.length === this.symlinkRecords.length;
  },
  scrapeFolders: function() {
    return this.folders.filter(function(f) { return f.organize_mode !== 'symlink_export'; });
  },
  symlinkFolders: function() {
    return this.folders.filter(function(f) { return f.organize_mode === 'symlink_export'; });
  },
};
