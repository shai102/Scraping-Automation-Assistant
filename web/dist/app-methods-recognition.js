window.scraperAppMethodsRecognition = {
  async runRecognitionTest() {
    var name = (this.recognitionName || '').trim();
    if (!name || this.recognitionTesting) return;
    this.recognitionTesting = true;
    this.recognitionError = '';
    this.recognitionResult = null;
    try {
      this.recognitionResult = await this.api('POST', '/api/recognition-test', {
        filename: name,
        use_ai: this.recognitionUseAi,
        bypass_cache: this.recognitionBypassCache,
        data_source: (this.cfg && this.cfg.data_source) || 'siliconflow_tmdb',
      });
    } catch (e) {
      this.recognitionError = e.message || '识别测试失败';
    }
    this.recognitionTesting = false;
  },
  recognitionTypeText(type) {
    if (type === 'episode') return '电视剧';
    if (type === 'movie') return '电影';
    return '自动判断';
  },
  formatRecognitionRaw(value) {
    try { return JSON.stringify(value || {}, null, 2); }
    catch (e) { return String(value || ''); }
  },
  loadRecognitionBatchSample() {
    this.recognitionBatchText = [
      'filename,expected_title,expected_year,expected_season,expected_episode,expected_provider,expected_id,media_type',
      'The.Mandalorian.S03E04.2023.WEB-DL.mkv,The Mandalorian,2023,3,4,tmdb,82856,tv',
      '[KTXP][Dungeon Meshi][01][CHS][1080P][AVC].mkv,Dungeon Meshi,2024,1,1,tmdb,,tv',
    ].join('\n');
  },
  parseRecognitionCsv(text) {
    var lines = String(text || '').split(/\r?\n/).map(function(line) { return line.trim(); }).filter(Boolean);
    if (!lines.length) return [];

    function parseLine(line) {
      var out = [], cur = '', inQuote = false;
      for (var i = 0; i < line.length; i++) {
        var ch = line[i];
        if (ch === '"') {
          if (inQuote && line[i + 1] === '"') { cur += '"'; i++; }
          else inQuote = !inQuote;
        } else if (ch === ',' && !inQuote) {
          out.push(cur.trim());
          cur = '';
        } else {
          cur += ch;
        }
      }
      out.push(cur.trim());
      return out;
    }

    var first = parseLine(lines[0]);
    var hasHeader = first.some(function(v) { return /filename|文件名|expected/i.test(v); });
    var headers = hasHeader ? first : ['filename'];
    var rows = hasHeader ? lines.slice(1) : lines;

    function normHeader(h) {
      var key = String(h || '').trim().toLowerCase();
      var map = {
        '文件名': 'filename',
        '标题': 'expected_title',
        '正确标题': 'expected_title',
        '年份': 'expected_year',
        '季': 'expected_season',
        '季数': 'expected_season',
        '集': 'expected_episode',
        '集数': 'expected_episode',
        '来源': 'expected_provider',
        '资料库': 'expected_provider',
        'id': 'expected_id',
        'tmdbid': 'expected_id',
        'tmdb_id': 'expected_id',
        '媒体类型': 'media_type',
      };
      return map[key] || key;
    }

    headers = headers.map(normHeader);
    return rows.map(function(line) {
      var cols = parseLine(line);
      if (!hasHeader && cols.length === 1) return { filename: cols[0] };
      var obj = {};
      headers.forEach(function(h, idx) { obj[h] = cols[idx] || ''; });
      return obj;
    }).filter(function(row) { return row.filename; });
  },
  async runRecognitionBatch() {
    this.recognitionBatchError = '';
    this.recognitionBatchResult = null;
    var cases = [];
    try {
      cases = this.parseRecognitionCsv(this.recognitionBatchText);
    } catch (e) {
      this.recognitionBatchError = 'CSV 解析失败：' + e.message;
      return;
    }
    if (!cases.length) {
      this.recognitionBatchError = '没有可用的测试数据';
      return;
    }
    if (cases.length > 100) {
      this.recognitionBatchError = '单次最多支持 100 条测试数据';
      return;
    }
    this.recognitionBatchTesting = true;
    try {
      this.recognitionBatchResult = await this.api('POST', '/api/recognition-test/batch', {
        cases: cases,
        bypass_cache: this.recognitionBypassCache,
        data_source: (this.cfg && this.cfg.data_source) || 'siliconflow_tmdb',
      });
    } catch (e) {
      this.recognitionBatchError = e.message || '批量识别失败';
    }
    this.recognitionBatchTesting = false;
  },
  recognitionModeLabel(mode) {
    var map = { guessit: 'guessit', local_ai: '本地 AI', online_ai: '在线 AI' };
    return map[mode] || mode;
  },
  metricPercent(metric) {
    if (!metric || !metric.evaluated) return '-';
    return String(metric.rate) + '%';
  },
  recognitionExpectationText(expected) {
    expected = expected || {};
    var parts = [];
    if (expected.title) parts.push(expected.title);
    if (expected.year) parts.push(expected.year);
    if (expected.season !== null && expected.season !== undefined) parts.push('S' + expected.season);
    if (expected.episode !== null && expected.episode !== undefined) parts.push('E' + expected.episode);
    if (expected.id) parts.push('ID:' + expected.id);
    return parts.length ? parts.join(' / ') : '未提供标准答案';
  },
  recognitionBrief(result) {
    var m = (result && result.match) || {};
    return m.title || (result && result.guessit && result.guessit.title) || '-';
  },
  recognitionResultMeta(result) {
    var m = (result && result.match) || {};
    var parts = [];
    if (m.year) parts.push(m.year);
    if (m.season !== null && m.season !== undefined) parts.push('S' + m.season);
    if (m.episode !== null && m.episode !== undefined) parts.push('E' + m.episode);
    if (m.id) parts.push('ID:' + m.id);
    return parts.join(' · ') || (result ? result.message : '-');
  },
  recognitionScoreText(result) {
    var s = (result && result.score) || {};
    if (!s.evaluated) return '未参与统计';
    return s.full_match ? '完全命中' : '未完全命中';
  },
  resultCellClass(result) {
    if (!result || result.status === 'failed') return 'is-failed';
    if (result.score && result.score.full_match) return 'is-pass';
    if (result.score && result.score.wrong_match) return 'is-wrong';
    if (result.status === 'pending_manual') return 'is-pending';
    return 'is-partial';
  },
  recognitionSearchPlan(result) {
    var plan = result && result.diagnostics && result.diagnostics.search_plan;
    if (!plan || !plan.length) return '-';
    return plan.map(function(group) { return (group || []).join(' / '); }).join(' | ');
  },
};
