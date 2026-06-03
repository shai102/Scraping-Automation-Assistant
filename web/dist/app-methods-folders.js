window.scraperAppMethodsFolders = {
  async loadFolders() {
    try { this.folders = await this.api('GET', '/api/monitor/folders'); } catch (ex) {}
  },
  async addFolder() {
    try {
      await this.api('POST', '/api/monitor/folders', this.newFolder);
      this.showAddFolder = false;
      this.newFolder = { path: '', target_root: '', media_type: 'auto', data_source: 'siliconflow_tmdb', organize_mode: 'move', symlink_source: '', skip_if_scraped: false, preserve_existing_folder: false };
      this.loadFolders();
    } catch (e) { this.notify(e.message, 'error'); }
  },
  async toggleFolder(f) {
    try {
      await this.api('PUT', '/api/monitor/folders/' + f.id, { enabled: !f.enabled });
      this.loadFolders();
    } catch (e) { this.notify(e.message, 'error'); }
  },
  async deleteFolder(id) {
    if (!(await this.confirmAction({
      title: '删除监控目录',
      message: '关联的刮削记录不会删除。',
      confirmText: '删除',
      danger: true,
    }))) return;
    try { await this.api('DELETE', '/api/monitor/folders/' + id); this.loadFolders(); } catch (e) { this.notify(e.message, 'error'); }
  },
  openEditFolder(f) {
    this.editFolderData = {
      id: f.id,
      path: f.path,
      target_root: f.target_root || '',
      media_type: f.media_type || 'auto',
      data_source: f.data_source || 'siliconflow_tmdb',
      organize_mode: f.organize_mode || 'move',
      symlink_source: f.symlink_source || '',
      skip_if_scraped: f.skip_if_scraped || false,
      preserve_existing_folder: f.preserve_existing_folder || false,
    };
    this.editFolderVisible = true;
  },
  async saveEditFolder() {
    try {
      await this.api('PUT', '/api/monitor/folders/' + this.editFolderData.id, {
        path: this.editFolderData.path,
        target_root: this.editFolderData.target_root,
        media_type: this.editFolderData.media_type,
        data_source: this.editFolderData.data_source,
        organize_mode: this.editFolderData.organize_mode,
        symlink_source: this.editFolderData.symlink_source,
        skip_if_scraped: this.editFolderData.skip_if_scraped,
        preserve_existing_folder: this.editFolderData.preserve_existing_folder,
      });
      this.editFolderVisible = false;
      this.loadFolders();
    } catch (e) { this.notify(e.message, 'error'); }
  },
  async scanFolder(id) {
    try {
      var r = await this.api('POST', '/api/monitor/folders/' + id + '/scan');
      this.notify(r.message || '扫描已启动', 'success');
    } catch (e) { this.notify(e.message, 'error'); }
  },
};
