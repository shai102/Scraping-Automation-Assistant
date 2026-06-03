window.scraperAppMethodsSettings = {
  async loadSettings() {
    try {
      this.cfg = await this.api('GET', '/api/settings/raw');
      if (!this.cfg.embedding_source) this.cfg.embedding_source = 'local';
      if (this.cfg.online_embedding_model === undefined) this.cfg.online_embedding_model = '';
      if (this.cfg.proxy_enabled === undefined) this.cfg.proxy_enabled = false;
      if (this.cfg.proxy_url === undefined) this.cfg.proxy_url = '';
      if (this.cfg.preserve_media_suffix === undefined) this.cfg.preserve_media_suffix = false;
      if (!this.cfg.proxy_no_proxy) {
        this.cfg.proxy_no_proxy = 'localhost,127.0.0.1,::1,0.0.0.0,host.docker.internal,*.local,10.*,192.168.*,172.16.*,172.17.*,172.18.*,172.19.*,172.20.*,172.21.*,172.22.*,172.23.*,172.24.*,172.25.*,172.26.*,172.27.*,172.28.*,172.29.*,172.30.*,172.31.*';
      }
    } catch (ex) {}
  },
  async saveSettings() {
    this.testResult = null;
    try {
      await this.api('PUT', '/api/settings', this.cfg);
      this.testResult = { ok: true, message: '配置已保存并生效' };
    } catch (e) { this.testResult = { ok: false, message: e.message }; }
  },
  async previewFilenameTemplate(isTv) {
    var template = isTv ? this.cfg.tv_format : this.cfg.movie_format;
    if (!String(template || '').trim()) {
      this.notify('请先输入命名模板', 'warning');
      return;
    }
    this.templatePreviewLoading = true;
    this.templatePreviewVisible = true;
    this.templatePreviewData = null;
    try {
      this.templatePreviewData = await this.api('POST', '/api/settings/preview-filename', {
        template: template,
        is_tv: !!isTv,
        preserve_media_suffix: !!this.cfg.preserve_media_suffix,
      });
    } catch (e) {
      this.templatePreviewData = {
        ok: false,
        is_tv: !!isTv,
        template: template,
        error: e.message,
      };
    }
    this.templatePreviewLoading = false;
  },
  async testTmdb() {
    this.testResult = null;
    try { this.testResult = await this.api('POST', '/api/settings/test-tmdb'); } catch (e) { this.testResult = { ok: false, message: e.message }; }
  },
  async testAi() {
    this.testResult = null;
    try {
      this.testResult = await this.api('POST', '/api/settings/test-ai');
      if (this.testResult.models) this.ollamaModels = this.testResult.models;
    } catch (e) { this.testResult = { ok: false, message: e.message }; }
  },
  async refreshOllamaModels() {
    this.testResult = null;
    try {
      var params = new URLSearchParams();
      if (this.cfg.ollama_url) params.set('ollama_url', this.cfg.ollama_url);
      var suffix = params.toString() ? '?' + params.toString() : '';
      var data = await this.api('GET', '/api/settings/ollama-models' + suffix);
      this.ollamaModels = data.models || [];
      this.testResult = {
        ok: this.ollamaModels.length > 0,
        message: data.message || (this.ollamaModels.length ? '已获取本地模型列表' : '未获取到本地模型')
      };
    } catch (ex) { this.testResult = { ok: false, message: ex.message }; }
  },
  async clearCache() {
    if (!(await this.confirmAction({
      title: '清除 API 缓存',
      message: '清除后所有识别结果将重新向 API 请求，不会影响已归档的文件。',
      confirmText: '清除',
      danger: true,
    }))) return;
    this.testResult = null;
    try {
      var r = await this.api('POST', '/api/settings/clear-cache');
      this.testResult = { ok: true, message: r.message };
    } catch (e) { this.testResult = { ok: false, message: e.message }; }
  },
  async testProxy() {
    this.proxyTesting = true;
    this.proxyTestResult = null;
    try {
      this.proxyTestResult = await this.api('POST', '/api/settings/test-proxy', this.cfg);
    } catch (e) {
      this.proxyTestResult = {
        ok: false,
        summary: { total: 0, success: 0, failed: 0, avg_latency_ms: null },
        proxy: {},
        results: [],
        message: e.message
      };
    }
    this.proxyTesting = false;
  },
  proxyModeText(mode) {
    var map = { manual: '手动代理', environment: '环境/系统代理', direct: '直连' };
    return map[mode] || mode || '-';
  },
  addStripKeyword() {
    var kw = (this.newKeyword || '').trim();
    if (!kw) return;
    if (!this.cfg.strip_keywords) this.cfg.strip_keywords = [];
    if (this.cfg.strip_keywords.indexOf(kw) === -1) {
      this.cfg.strip_keywords.push(kw);
    }
    this.newKeyword = '';
  },
  removeStripKeyword(idx) {
    if (this.cfg.strip_keywords) {
      this.cfg.strip_keywords.splice(idx, 1);
    }
  },
  async testTelegram() {
    this.tgTestResult = null;
    try {
      await this.api('PUT', '/api/settings', this.cfg);
      var r = await this.api('POST', '/api/settings/test-telegram');
      this.tgTestResult = r;
    } catch (e) { this.tgTestResult = { ok: false, message: e.message }; }
  },
  async testEmby() {
    this.embyTestResult = null;
    try {
      await this.api('PUT', '/api/settings', this.cfg);
      var r = await this.api('POST', '/api/settings/test-emby');
      this.embyTestResult = r;
    } catch (e) { this.embyTestResult = { ok: false, message: e.message }; }
  },
};
