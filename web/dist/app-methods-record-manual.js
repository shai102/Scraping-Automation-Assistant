window.scraperAppMethodsRecords = Object.assign(window.scraperAppMethodsRecords || {}, {
  openManualMatch(record) {
    this.manualRecord = record;
    this.searchQuery = record.matched_title || record.original_name.replace(/\.[^.]+$/, '');
    var ym = /(19|20)\d{2}/.exec(record.original_name);
    this.searchYear = ym ? parseInt(ym[0]) : null;
    this.candidates = [];
    this.selectedCandidate = null;
    this.searchDone = false;
    this.manualSeason = null;
    this.manualEpOffset = 0;
    this.manualScope = 'single';
    this.searchMode = 'name';
    this.searchTmdbId = '';
    if (record.media_type === 'movie') this.searchIsTv = false;
    else if (record.media_type === 'episode') this.searchIsTv = true;
    else {
      var p = record.target_path || record.original_path || '';
      this.searchIsTv = /[Ss]\d{1,2}[Ee]\d{1,4}|Season\s*\d/i.test(p);
    }
    this.manualMatchVisible = true;
  },
  async searchCandidates() {
    if (!this.searchQuery.trim()) return;
    this.searching = true;
    this.searchDone = false;
    try {
      var data = await this.api('POST', '/api/records/search-candidates', {
        query: this.searchQuery.trim(),
        year: this.searchYear || null,
        is_tv: this.searchIsTv,
        source: (this.cfg && this.cfg.data_source) || 'siliconflow_tmdb',
      });
      this.candidates = data.candidates || [];
      this.selectedCandidate = null;
    } catch (e) { this.notify(e.message, 'error'); }
    this.searching = false;
    this.searchDone = true;
  },
  async applyByTmdbId() {
    if (!this.searchTmdbId.trim() || !this.manualRecord) return;
    var provider = ((this.cfg && this.cfg.data_source) || 'siliconflow_tmdb') === 'siliconflow_tmdb' ? 'tmdb' : 'bgm';
    this.searching = true;
    try {
      await this.api('POST', '/api/records/' + this.manualRecord.id + '/manual-match', {
        candidate_id: this.searchTmdbId.trim(),
        candidate_title: '',
        provider: provider,
        is_tv: this.searchIsTv,
        season_override: (this.manualSeason !== null && this.manualSeason !== '') ? parseInt(this.manualSeason) : null,
        episode_offset: parseInt(this.manualEpOffset) || 0,
        scope: this.manualScope,
      });
      this.manualMatchVisible = false;
      this.loadRecords();
    } catch (e) { this.notify('匹配失败: ' + e.message, 'error'); }
    this.searching = false;
  },
  selectCandidate(candidate) {
    this.selectedCandidate = (this.selectedCandidate && this.selectedCandidate.id === candidate.id) ? null : candidate;
  },
  async confirmManualMatch() {
    if (!this.manualRecord || !this.selectedCandidate) return;
    var candidate = this.selectedCandidate;
    var provider = ((this.cfg && this.cfg.data_source) || 'siliconflow_tmdb') === 'siliconflow_tmdb' ? 'tmdb' : 'bgm';
    this.searching = true;
    try {
      await this.api('POST', '/api/records/' + this.manualRecord.id + '/manual-match', {
        candidate_id: String(candidate.id),
        candidate_title: candidate.title,
        provider: provider,
        is_tv: this.searchIsTv,
        season_override: (this.manualSeason !== null && this.manualSeason !== '') ? parseInt(this.manualSeason) : null,
        episode_offset: parseInt(this.manualEpOffset) || 0,
        scope: this.manualScope,
      });
      this.manualMatchVisible = false;
      this.loadRecords();
    } catch (e) { this.notify('匹配失败: ' + e.message, 'error'); }
    this.searching = false;
  },
});
