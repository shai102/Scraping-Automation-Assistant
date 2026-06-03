window.scraperAppPageComponents = Object.assign(window.scraperAppPageComponents || {}, {
  'recognition-page': {
    template: `
      <section class="recognition-page">
        <div class="recognition-shell">
          <div class="recognition-hero">
            <div class="recognition-hero-icon">🧠</div>
            <div>
              <div class="recognition-eyebrow">整理识别实验室</div>
              <h1>识别测试</h1>
              <p>输入文件名或测试集，对比 guessit、本地 AI、在线 AI 的识别率和失败原因。</p>
            </div>
          </div>
          <div class="recognition-body">
            <div class="recognition-tabs">
              <button :class="{active: $root.recognitionTab==='single'}" @click="$root.recognitionTab='single'">单条测试</button>
              <button :class="{active: $root.recognitionTab==='batch'}" @click="$root.recognitionTab='batch'">批量实验</button>
            </div>

            <div v-if="$root.recognitionTab==='single'">
              <label class="recognition-label">测试名称</label>
              <div class="recognition-search">
                <input v-model="$root.recognitionName" @keyup.enter="$root.runRecognitionTest" placeholder="例如：CLIMAX.S01E01.2026.1080p.DSNP.WEB-DL.H264.AAC-ADWeb.mkv">
                <button class="recognition-run" @click="$root.runRecognitionTest" :disabled="$root.recognitionTesting || !$root.recognitionName.trim()">{{$root.recognitionTesting?'识别中...':'开始识别'}}</button>
              </div>

              <div class="recognition-toggle-row">
                <label class="recognition-toggle">
                  <input type="checkbox" v-model="$root.recognitionUseAi">
                  <span class="toggle-track"><span class="toggle-dot"></span></span>
                  <span>
                    <strong>使用在线 AI 测试</strong>
                    <small>开启后，在线 API 解析文件名，本地 Embedding 重排候选，并由在线 API 做最终判定</small>
                  </span>
                </label>
                <label class="recognition-toggle recognition-cache-toggle">
                  <input type="checkbox" v-model="$root.recognitionBypassCache">
                  <span class="toggle-track"><span class="toggle-dot"></span></span>
                  <span>
                    <strong>绕过 API 缓存</strong>
                    <small>开启后，本页测试会实时请求 TMDB/BGM，不读取也不写入 api_cache.json；平常刮削不受影响</small>
                  </span>
                </label>
              </div>

              <div class="recognition-pills">
                <span>仅支持文件名，不支持文件夹名</span>
                <span>自动解析季集与 TMDB</span>
                <span>返回标准命名预览</span>
              </div>

              <div class="recognition-result-box">
                <div v-if="$root.recognitionError" class="msg msg-err">{{$root.recognitionError}}</div>
                <div v-else-if="$root.recognitionResult" class="recognition-result">
                  <div class="recognition-result-head">
                    <div>
                      <span :class="['badge', $root.statusClass($root.recognitionResult.status)]">{{$root.statusText($root.recognitionResult.status)}}</span>
                      <span class="recognition-message">{{$root.recognitionResult.message}}</span>
                    </div>
                    <span class="recognition-source">{{$root.recognitionResult.source==='siliconflow_tmdb'?'TMDB':'BGM'}}</span>
                  </div>

                  <div class="recognition-summary">
                    <div class="recognition-poster" v-if="$root.recognitionResult.preview && $root.recognitionResult.preview.poster">
                      <img :src="$root.recognitionResult.preview.poster" alt="">
                    </div>
                    <div class="recognition-poster recognition-poster-empty" v-else>NO POSTER</div>
                    <div class="recognition-main">
                      <h2>{{($root.recognitionResult.match && $root.recognitionResult.match.title) || $root.recognitionResult.guessit.title || '-'}}</h2>
                      <div class="recognition-meta-line">
                        <span v-if="$root.recognitionResult.match && $root.recognitionResult.match.year">{{$root.recognitionResult.match.year}}</span>
                        <span>{{$root.recognitionTypeText($root.recognitionResult.match && $root.recognitionResult.match.type)}}</span>
                        <span v-if="$root.recognitionResult.match && $root.recognitionResult.match.id">ID: {{$root.recognitionResult.match.id}}</span>
                        <span v-if="$root.recognitionResult.ai">{{$root.recognitionResult.ai.status}}</span>
                      </div>
                      <p v-if="$root.recognitionResult.match && ($root.recognitionResult.match.episode_plot || $root.recognitionResult.match.overview)">{{$root.recognitionResult.match.episode_plot || $root.recognitionResult.match.overview}}</p>
                      <div class="recognition-preview">
                        <div><strong>标准文件名</strong><span>{{($root.recognitionResult.preview && $root.recognitionResult.preview.new_name) || '-'}}</span></div>
                        <div><strong>归档路径预览</strong><span>{{($root.recognitionResult.preview && $root.recognitionResult.preview.target_path) || '-'}}</span></div>
                      </div>
                    </div>
                  </div>

                  <div class="recognition-detail-grid">
                    <div class="recognition-detail-card">
                      <h3>guessit 基础解析</h3>
                      <dl>
                        <div><dt>标题</dt><dd>{{$root.recognitionResult.guessit.title || '-'}}</dd></div>
                        <div><dt>年份</dt><dd>{{$root.recognitionResult.guessit.year || '-'}}</dd></div>
                        <div><dt>季 / 集</dt><dd>S{{$root.recognitionResult.guessit.season || '-'}} / E{{$root.recognitionResult.guessit.episode || '-'}}</dd></div>
                        <div><dt>是否建议 AI</dt><dd>{{$root.recognitionResult.guessit.needs_ai_assist?'是':'否'}}</dd></div>
                      </dl>
                    </div>
                    <div class="recognition-detail-card">
                      <h3>最终匹配</h3>
                      <dl>
                        <div><dt>来源</dt><dd>{{($root.recognitionResult.ai && $root.recognitionResult.ai.parse_source) || '-'}}</dd></div>
                        <div><dt>资料库</dt><dd>{{($root.recognitionResult.match && $root.recognitionResult.match.provider) || '-'}}</dd></div>
                        <div><dt>剧集标题</dt><dd>{{($root.recognitionResult.match && $root.recognitionResult.match.episode_title) || '-'}}</dd></div>
                        <div><dt>季 / 集</dt><dd>S{{($root.recognitionResult.match && $root.recognitionResult.match.season) || '-'}} / E{{($root.recognitionResult.match && $root.recognitionResult.match.episode) || '-'}} </dd></div>
                      </dl>
                    </div>
                  </div>

                  <details class="recognition-raw">
                    <summary>查看原始解析数据</summary>
                    <pre>{{$root.formatRecognitionRaw($root.recognitionResult)}}</pre>
                  </details>
                </div>
                <div v-else class="recognition-empty"></div>
              </div>
            </div>

            <div v-else class="recognition-batch">
              <div class="recognition-batch-head">
                <div>
                  <h2>批量测试集</h2>
                  <p>粘贴 CSV，系统会对每条文件名同时跑 guessit、本地 AI、在线 AI，并计算准确率。</p>
                </div>
                <button class="btn btn-sm" @click="$root.loadRecognitionBatchSample">填入示例</button>
              </div>
              <label class="recognition-toggle recognition-cache-toggle">
                <input type="checkbox" v-model="$root.recognitionBypassCache">
                <span class="toggle-track"><span class="toggle-dot"></span></span>
                <span>
                  <strong>绕过 API 缓存</strong>
                  <small>开启后，批量实验会实时请求 TMDB/BGM；关闭可复用缓存以减少请求量</small>
                </span>
              </label>
              <textarea v-model="$root.recognitionBatchText" class="recognition-batch-input" rows="8" placeholder="filename,expected_title,expected_year,expected_season,expected_episode,expected_provider,expected_id,media_type&#10;The.Mandalorian.S03E04.2023.WEB-DL.mkv,The Mandalorian,2023,3,4,tmdb,82856,tv"></textarea>
              <div class="recognition-batch-actions">
                <button class="recognition-run" @click="$root.runRecognitionBatch" :disabled="$root.recognitionBatchTesting || !$root.recognitionBatchText.trim()">{{$root.recognitionBatchTesting?'批量识别中...':'开始批量实验'}}</button>
                <span>最多 100 条。未填写标准答案的字段不会参与对应准确率统计。</span>
              </div>
              <div v-if="$root.recognitionBatchError" class="msg msg-err">{{$root.recognitionBatchError}}</div>

              <div v-if="$root.recognitionBatchResult" class="recognition-batch-result">
                <div class="recognition-stats">
                  <div class="recognition-stat-card" v-for="mode in $root.recognitionBatchResult.modes" :key="mode">
                    <h3>{{$root.recognitionModeLabel(mode)}}</h3>
                    <strong>{{$root.metricPercent($root.recognitionBatchResult.stats.modes[mode].full)}}</strong>
                    <span>完全命中率</span>
                    <div>标题 {{$root.metricPercent($root.recognitionBatchResult.stats.modes[mode].metrics.title)}} · ID {{$root.metricPercent($root.recognitionBatchResult.stats.modes[mode].metrics.id)}}</div>
                    <div>年份 {{$root.metricPercent($root.recognitionBatchResult.stats.modes[mode].metrics.year)}} · 季 {{$root.metricPercent($root.recognitionBatchResult.stats.modes[mode].metrics.season)}} · 集 {{$root.metricPercent($root.recognitionBatchResult.stats.modes[mode].metrics.episode)}}</div>
                    <div>待手动 {{$root.recognitionBatchResult.stats.modes[mode].pending_manual}} · 错配 {{$root.recognitionBatchResult.stats.modes[mode].wrong_match}}</div>
                  </div>
                </div>

                <div class="recognition-table-wrap">
                  <table class="table recognition-compare-table">
                    <thead>
                      <tr>
                        <th>文件名 / 标准答案</th>
                        <th v-for="mode in $root.recognitionBatchResult.modes" :key="mode">{{$root.recognitionModeLabel(mode)}}</th>
                      </tr>
                    </thead>
                    <tbody v-for="row in $root.recognitionBatchResult.rows" :key="row.index">
                      <tr>
                        <td class="cell-path">
                          <strong>{{row.filename}}</strong>
                          <div class="path-target">期望：{{$root.recognitionExpectationText(row.expected)}}</div>
                        </td>
                        <td v-for="mode in $root.recognitionBatchResult.modes" :key="mode" :class="['recognition-mode-cell', $root.resultCellClass(row.results[mode])]">
                          <div class="recognition-cell-title">{{$root.recognitionBrief(row.results[mode])}}</div>
                          <div class="recognition-cell-meta">{{$root.recognitionResultMeta(row.results[mode])}}</div>
                          <div class="recognition-cell-score">{{$root.recognitionScoreText(row.results[mode])}}</div>
                        </td>
                      </tr>
                      <tr class="recognition-detail-row">
                        <td :colspan="1 + $root.recognitionBatchResult.modes.length">
                          <details>
                            <summary>识别过程与失败原因</summary>
                            <div class="recognition-reason-grid">
                              <div v-for="mode in $root.recognitionBatchResult.modes" :key="mode" class="recognition-reason-card">
                                <h4>{{$root.recognitionModeLabel(mode)}}</h4>
                                <ul>
                                  <li v-for="reason in row.results[mode].reasons" :key="reason">{{reason}}</li>
                                </ul>
                                <div class="recognition-mini">
                                  <div>解析名：{{(row.results[mode].diagnostics && row.results[mode].diagnostics.parsed_name) || '-'}}</div>
                                  <div>搜索词：{{$root.recognitionSearchPlan(row.results[mode])}}</div>
                                </div>
                                <pre>{{$root.formatRecognitionRaw(row.results[mode])}}</pre>
                              </div>
                            </div>
                          </details>
                        </td>
                      </tr>
                    </tbody>
                  </table>
                </div>
              </div>
            </div>
          </div>
        </div>
      </section>
    `,
  },
  'tmdb-page': {
    template: `
      <section>
        <div class="page-header"><h1>TMDB 设置</h1></div>
        <div class="card">
          <div class="form-group">
            <label>TMDB API Key</label>
            <div class="input-with-btn">
              <input v-model="$root.cfg.tmdb_api_key" :type="$root.showTmdbKey ? 'text' : 'password'" placeholder="输入 TMDB API Key">
              <button class="btn btn-sm" @click="$root.showTmdbKey=!$root.showTmdbKey">{{$root.showTmdbKey?'隐藏':'显示'}}</button>
            </div>
          </div>
          <div class="form-group">
            <label>BGM API Key（可选）</label>
            <div class="input-with-btn">
              <input v-model="$root.cfg.bgm_api_key" :type="$root.showBgmKey ? 'text' : 'password'" placeholder="输入 Bangumi API Key">
              <button class="btn btn-sm" @click="$root.showBgmKey=!$root.showBgmKey">{{$root.showBgmKey?'隐藏':'显示'}}</button>
            </div>
          </div>
          <div class="form-actions">
            <button class="btn btn-primary" @click="$root.saveSettings">保存</button>
            <button class="btn" @click="$root.testTmdb">测试 TMDB 连接</button>
          </div>
          <div v-if="$root.testResult" :class="['msg', $root.testResult.ok?'msg-ok':'msg-err']">{{$root.testResult.message}}</div>
        </div>
      </section>
    `,
  },
  'ai-page': {
    template: `
      <section>
        <div class="page-header"><h1>AI 识别设置</h1></div>
        <div class="card">
          <div class="form-group">
            <label>是否启用AI识别</label>
            <div class="btn-group" style="display:flex;gap:0;margin-top:4px">
              <button :class="['btn', $root.cfg.ai_mode==='disabled'?'btn-primary':'']" style="border-radius:6px 0 0 6px;min-width:80px" @click="$root.cfg.ai_mode='disabled'">禁用</button>
              <button :class="['btn', (!$root.cfg.ai_mode||$root.cfg.ai_mode==='assist')?'btn-primary':'']" style="border-radius:0;min-width:80px" @click="$root.cfg.ai_mode='assist'">辅助识别</button>
              <button :class="['btn', $root.cfg.ai_mode==='force'?'btn-primary':'']" style="border-radius:0 6px 6px 0;min-width:80px" @click="$root.cfg.ai_mode='force'">强制使用</button>
            </div>
            <small style="color:#888;display:block;margin-top:6px" v-if="$root.cfg.ai_mode==='disabled'">禁用：只使用 guessit 解析文件名并搜索资料库，不调用 AI。</small>
            <small style="color:#888;display:block;margin-top:6px" v-else-if="!$root.cfg.ai_mode||$root.cfg.ai_mode==='assist'">辅助识别：标准命名优先走 guessit；遇到标题不可靠、番组命名或季集不清晰时自动调用 AI。记录来源只显示 guessit 或 AI，不显示混合。</small>
            <small style="color:#888;display:block;margin-top:6px" v-else>强制使用：始终由 AI 解析标题与季集；AI 失败时会直接记为失败或待处理。</small>
          </div>
          <div class="form-group">
            <label><input type="checkbox" v-model="$root.cfg.prefer_ollama"> 优先使用本地 Ollama</label>
          </div>
          <fieldset class="fieldset">
            <legend>Ollama / LM Studio（本地）</legend>
            <div class="form-group">
              <label>API 地址</label>
              <input v-model="$root.cfg.ollama_url">
            </div>
            <div class="form-row">
              <div class="form-group" style="flex:1">
                <label>模型</label>
                <select v-model="$root.cfg.ollama_model">
                  <option value="">请选择模型</option>
                  <option v-if="$root.cfg.ollama_model && $root.ollamaModels.indexOf($root.cfg.ollama_model)===-1" :value="$root.cfg.ollama_model">{{$root.cfg.ollama_model}}</option>
                  <option v-for="m in $root.ollamaModels" :key="'model-'+m" :value="m">{{m}}</option>
                </select>
              </div>
              <div class="form-group">
                <label>&nbsp;</label>
                <button class="btn btn-sm" @click="$root.refreshOllamaModels">刷新模型列表</button>
              </div>
            </div>
            <div class="form-group">
              <label>Embedding 来源</label>
              <select v-model="$root.cfg.embedding_source">
                <option value="local">本地 Ollama / LM Studio</option>
                <option value="online">在线 OpenAI 兼容</option>
              </select>
            </div>
            <div class="form-group" v-if="!$root.cfg.embedding_source || $root.cfg.embedding_source==='local'">
              <label>Embedding 模型</label>
              <select v-model="$root.cfg.embedding_model">
                <option value="">请选择 Embedding 模型</option>
                <option v-if="$root.cfg.embedding_model && $root.ollamaModels.indexOf($root.cfg.embedding_model)===-1" :value="$root.cfg.embedding_model">{{$root.cfg.embedding_model}}</option>
                <option v-for="m in $root.ollamaModels" :key="'embedding-'+m" :value="m">{{m}}</option>
              </select>
            </div>
            <div class="form-group">
              <label><input type="checkbox" v-model="$root.cfg.use_embedding_rank"> 启用 Embedding 候选重排</label>
            </div>
          </fieldset>
          <fieldset class="fieldset">
            <legend>OpenAI 兼容（SiliconFlow / DeepSeek 等）</legend>
            <div class="form-group">
              <label>API Key</label>
              <div class="input-with-btn">
                <input v-model="$root.cfg.sf_api_key" :type="$root.showSfKey ? 'text' : 'password'">
                <button class="btn btn-sm" @click="$root.showSfKey=!$root.showSfKey">{{$root.showSfKey?'隐藏':'显示'}}</button>
              </div>
            </div>
            <div class="form-group">
              <label>API URL</label>
              <input v-model="$root.cfg.sf_api_url">
            </div>
            <div class="form-group">
              <label>模型名称</label>
              <input v-model="$root.cfg.sf_model">
            </div>
            <div class="form-group" v-if="$root.cfg.embedding_source==='online'">
              <label>在线 Embedding 模型</label>
              <input v-model="$root.cfg.online_embedding_model" placeholder="例如 BAAI/bge-m3 或 openai/text-embedding-3-small">
              <small style="color:#888;display:block;margin-top:6px">复用本区域的 API URL 和 API Key，通过 /embeddings 接口进行候选重排。</small>
            </div>
          </fieldset>
          <div class="form-row">
            <div class="form-group">
              <label>Temperature</label>
              <input v-model="$root.cfg.ai_temperature" type="number" step="0.01" min="0" max="2">
            </div>
            <div class="form-group">
              <label>Top-P</label>
              <input v-model="$root.cfg.ai_top_p" type="number" step="0.01" min="0" max="1">
            </div>
          </div>
          <div class="form-actions">
            <button class="btn btn-primary" @click="$root.saveSettings">保存</button>
            <button class="btn" @click="$root.testAi">测试 AI 连接</button>
            <button class="btn btn-danger" @click="$root.clearCache">🗑 清除 API 缓存</button>
          </div>
          <div v-if="$root.testResult" :class="['msg', $root.testResult.ok?'msg-ok':'msg-err']">{{$root.testResult.message}}</div>
        </div>

        <div class="card" style="margin-top:16px">
          <div class="form-group">
            <label style="font-weight:600;font-size:15px">要删除的关键词</label>
            <div style="display:flex;gap:8px;margin-top:8px;align-items:center">
              <input v-model="$root.newKeyword" placeholder="输入关键词" @keyup.enter="$root.addStripKeyword" style="flex:1;max-width:260px">
              <button class="btn btn-primary btn-sm" @click="$root.addStripKeyword">+ 添加</button>
            </div>
            <div style="display:flex;flex-wrap:wrap;gap:8px;margin-top:10px" v-if="$root.cfg.strip_keywords&&$root.cfg.strip_keywords.length">
              <span v-for="(kw, idx) in $root.cfg.strip_keywords" :key="idx" style="display:inline-flex;align-items:center;padding:4px 10px;background:#e8f4fd;color:#1976d2;border-radius:14px;font-size:13px;border:1px solid #90cdf4">
                {{kw}}
                <span @click="$root.removeStripKeyword(idx)" style="margin-left:6px;cursor:pointer;color:#999;font-size:15px;line-height:1">&times;</span>
              </span>
            </div>
            <small style="color:#1976d2;display:block;margin-top:8px">从视频文件名中提取影视剧标题时先删除这些关键词，添加的越多识别准确率越高</small>
          </div>
          <div class="form-actions">
            <button class="btn btn-primary" @click="$root.saveSettings">保存</button>
          </div>
        </div>

        <div class="card" style="margin-top:16px">
          <div class="form-group">
            <label style="font-weight:600;font-size:15px">识别缓存自动清除</label>
            <select v-model.number="$root.cfg.cache_expiry_days" style="margin-top:8px;max-width:200px">
              <option :value="1">1 天</option>
              <option :value="3">3 天</option>
              <option :value="7">7 天（默认）</option>
              <option :value="14">14 天</option>
              <option :value="30">30 天</option>
              <option :value="0">永不过期</option>
            </select>
            <small style="color:#888;display:block;margin-top:6px">设置 API 识别缓存的自动过期时间，过期后下次识别将重新向 API 请求。选择「永不过期」则只能手动清除。</small>
          </div>
          <div class="form-actions">
            <button class="btn btn-primary" @click="$root.saveSettings">保存</button>
          </div>
        </div>

        <div class="card" style="margin-top:16px">
          <div class="form-group">
            <label style="font-weight:600;font-size:15px">元数据自动补齐</label>
            <small style="color:#1976d2;display:block;margin-top:6px">新番/新剧首次刮削时 TMDB 数据可能不完整（缺集名、简介、截图、演员等），开启后系统会定期巡检并自动从 TMDB/BGM 拉取最新数据补齐 NFO 和图片。</small>
          </div>
          <div class="form-group" style="margin-top:8px">
            <label style="display:flex;align-items:center;gap:8px;cursor:pointer">
              <input type="checkbox" v-model="$root.cfg.metadata_refresh_enabled">
              <span>启用定时巡检</span>
            </label>
          </div>
          <div class="form-row">
            <div class="form-group">
              <label>巡检间隔（小时）</label>
              <select v-model.number="$root.cfg.metadata_refresh_interval_hours" style="max-width:200px">
                <option :value="6">6 小时</option>
                <option :value="12">12 小时（默认）</option>
                <option :value="24">24 小时</option>
                <option :value="48">48 小时</option>
                <option :value="168">7 天</option>
              </select>
            </div>
            <div class="form-group">
              <label>回看天数</label>
              <select v-model.number="$root.cfg.metadata_refresh_lookback_days" style="max-width:200px">
                <option :value="7">最近 7 天</option>
                <option :value="14">最近 14 天（默认）</option>
                <option :value="30">最近 30 天</option>
                <option :value="90">最近 90 天</option>
                <option :value="0">不限制</option>
              </select>
              <small style="color:#888;display:block;margin-top:6px">只检查此范围内的已成功记录，超过该天数的记录不再自动巡检。</small>
            </div>
          </div>
          <div class="form-actions">
            <button class="btn btn-primary" @click="$root.saveSettings">保存</button>
          </div>
        </div>
      </section>
    `,
  },
  'proxy-page': {
    template: `
      <section>
        <div class="page-header"><h1>网络代理</h1></div>
        <div class="card proxy-card">
          <div class="proxy-title">
            <div class="proxy-icon">🌐</div>
            <div>
              <h2>网络代理</h2>
              <p>先不填代理直接测试；如果直连失败，再填写代理地址。Docker 版通常需要手动配置。</p>
            </div>
          </div>

          <label class="recognition-toggle proxy-enable-toggle">
            <input type="checkbox" v-model="$root.cfg.proxy_enabled">
            <span class="toggle-track"><span class="toggle-dot"></span></span>
            <span>
              <strong>启用手动代理</strong>
              <small>开启后，外网请求会显式使用下方代理；关闭时继续跟随系统/环境代理或直连。</small>
            </span>
          </label>

          <div class="form-group">
            <label>代理地址（单地址模式）</label>
            <input v-model="$root.cfg.proxy_url" placeholder="例如：http://127.0.0.1:7890 或 host.docker.internal:7890">
            <small style="color:#888;display:block;margin-top:6px">可直接填写 127.0.0.1:7890，保存时会自动补全为 http://127.0.0.1:7890。</small>
          </div>

          <div class="form-group">
            <label>NO_PROXY（不走代理）</label>
            <textarea v-model="$root.cfg.proxy_no_proxy" rows="3" style="font-family:Consolas,Monaco,monospace;resize:vertical"></textarea>
            <small style="color:#888;display:block;margin-top:6px">默认包含 localhost、127.0.0.1、host.docker.internal、192.168.*、10.*、172.16-31.*，避免 Ollama、Web 后端和本地服务被代理。</small>
          </div>

          <div class="proxy-help">
            <strong>怎么判断是否需要代理</strong>
            <ol>
              <li>先保持代理地址为空，点击“测试代理/直连”。</li>
              <li>如果测试成功，说明当前网络可用，不需要额外配置代理。</li>
              <li>如果测试失败，再填写你的代理地址并重新测试，直到结果正常。</li>
            </ol>
          </div>

          <div class="form-actions">
            <button class="btn btn-primary" @click="$root.saveSettings">保存</button>
            <button class="btn" @click="$root.testProxy" :disabled="$root.proxyTesting">{{$root.proxyTesting?'测试中...':'🧪 测试代理/直连'}}</button>
          </div>

          <div v-if="$root.proxyTestResult" class="proxy-result">
            <div :class="['msg', $root.proxyTestResult.ok?'msg-ok':'msg-err']">
              测试完成：{{$root.proxyModeText($root.proxyTestResult.proxy&&$root.proxyTestResult.proxy.mode)}}，成功 {{$root.proxyTestResult.summary&&$root.proxyTestResult.summary.success}}/{{$root.proxyTestResult.summary&&$root.proxyTestResult.summary.total}}
              <span v-if="$root.proxyTestResult.summary&&$root.proxyTestResult.summary.avg_latency_ms">，平均延迟 {{$root.proxyTestResult.summary.avg_latency_ms}}ms</span>
            </div>
            <div class="proxy-current" v-if="$root.proxyTestResult.proxy">
              当前代理：{{$root.proxyTestResult.proxy.proxy_url || '直连'}}（来源：{{$root.proxyTestResult.proxy.source || '-'}}）
            </div>
            <table class="table proxy-table" v-if="$root.proxyTestResult.results&&$root.proxyTestResult.results.length">
              <thead>
                <tr><th>服务</th><th>状态</th><th>延迟</th><th>备注</th></tr>
              </thead>
              <tbody>
                <tr v-for="row in $root.proxyTestResult.results" :key="row.name">
                  <td><span :class="['proxy-check', row.ok?'ok':'bad']">{{row.ok?'✓':'×'}}</span> {{row.name}}</td>
                  <td>{{row.status}}</td>
                  <td>{{row.latency_ms}}ms</td>
                  <td>{{row.message}}</td>
                </tr>
              </tbody>
            </table>
          </div>
        </div>
      </section>
    `,
  },
  'classify-page': {
    template: `
      <section>
        <div class="page-header"><h1>二级分类设置</h1></div>
        <div class="card">
          <div class="form-group">
            <label>TV 命名格式</label>
            <textarea v-model="$root.cfg.tv_format" rows="2" style="font-family:monospace;resize:vertical"></textarea>
            <div class="template-preview-trigger">
              <button class="btn btn-sm" @click="$root.previewFilenameTemplate(true)">预览</button>
            </div>
          </div>
          <div class="form-group">
            <label>电影命名格式</label>
            <textarea v-model="$root.cfg.movie_format" rows="2" style="font-family:monospace;resize:vertical"></textarea>
            <div class="template-preview-trigger">
              <button class="btn btn-sm" @click="$root.previewFilenameTemplate(false)">预览</button>
            </div>
          </div>
          <div class="form-group">
            <label>视频扩展名</label>
            <textarea v-model="$root.cfg.video_exts" rows="2" style="font-family:monospace;resize:vertical"></textarea>
          </div>
          <div class="form-group">
            <label>字幕/音频扩展名</label>
            <textarea v-model="$root.cfg.sub_audio_exts" rows="2" style="font-family:monospace;resize:vertical"></textarea>
          </div>
          <div class="form-group">
            <label>语言标签</label>
            <textarea v-model="$root.cfg.lang_tags" rows="2" style="font-family:monospace;resize:vertical"></textarea>
          </div>
          <div class="form-group">
            <label class="setting-check">
              <input type="checkbox" v-model="$root.cfg.preserve_media_suffix">
              <span>保留媒体信息后缀（如 2160p.TVING.WEB-DL.H.265.AAC-ColorTV）</span>
            </label>
            <small style="color:#888;display:block;margin-top:6px">开启后会从原文件名提取清晰度、片源、编码、音频、发布组等信息；如果命名模板未显式写入 {media_suffix}，会自动追加到扩展名前。</small>
          </div>
          <div class="form-row">
            <div class="form-group">
              <label>预览线程数</label>
              <input v-model.number="$root.cfg.preview_workers" type="number" min="1" max="10">
            </div>
            <div class="form-group">
              <label>软链接导出线程数</label>
              <input v-model.number="$root.cfg.symlink_export_workers" type="number" min="1" max="10">
            </div>
            <div class="form-group">
              <label>同步线程数</label>
              <input v-model.number="$root.cfg.sync_workers" type="number" min="1" max="10">
            </div>
            <div class="form-group">
              <label>执行线程数</label>
              <input v-model.number="$root.cfg.execution_workers" type="number" min="1" max="10">
            </div>
          </div>
          <div class="form-actions">
            <button class="btn btn-primary" @click="$root.saveSettings">保存</button>
          </div>
        </div>
      </section>
    `,
  },
  'tgnotify-page': {
    template: `
      <section>
        <div class="page-header"><h1>Telegram 通知设置</h1></div>
        <div class="card">
          <div class="form-group">
            <label><input type="checkbox" v-model="$root.cfg.tg_notify_enabled"> 启用 Telegram 入库通知</label>
          </div>
          <div class="form-group">
            <label>Bot Token</label>
            <div class="input-with-btn">
              <input v-model="$root.cfg.tg_bot_token" :type="$root.showTgToken ? 'text' : 'password'" placeholder="输入 Telegram Bot Token（从 @BotFather 获取）">
              <button class="btn btn-sm" @click="$root.showTgToken=!$root.showTgToken">{{$root.showTgToken?'隐藏':'显示'}}</button>
            </div>
          </div>
          <div class="form-group">
            <label>Chat ID</label>
            <input v-model="$root.cfg.tg_chat_id" type="text" placeholder="输入目标用户/群组/频道的 Chat ID">
          </div>
          <div class="form-group">
            <label>通知延迟（秒）</label>
            <input v-model.number="$root.cfg.tg_notify_delay" type="number" min="10" max="600" placeholder="60">
            <small style="color:#888;display:block;margin-top:4px">文件夹内最后一个文件处理完成后等待此秒数再发送通知，以便汇总同批次文件</small>
          </div>
          <div class="form-actions">
            <button class="btn btn-primary" @click="$root.saveSettings">保存</button>
            <button class="btn" @click="$root.testTelegram">测试发送</button>
          </div>
          <div v-if="$root.tgTestResult" :class="['msg', $root.tgTestResult.ok?'msg-ok':'msg-err']">{{$root.tgTestResult.message}}</div>
        </div>
      </section>
    `,
  },
  'embynotify-page': {
    template: `
      <section>
        <div class="page-header"><h1>Emby / Jellyfin 入库通知</h1></div>
        <div class="card">
          <div class="form-group">
            <label><input type="checkbox" v-model="$root.cfg.emby_notify_enabled"> 启用入库后自动搜媒体库</label>
            <small style="color:#888;display:block;margin-top:4px">刮削成功后自动触发 Emby / Jellyfin 扫描，无需手动刷新媒体库。支持 Emby 和 Jellyfin。</small>
          </div>
          <div class="form-group">
            <label>Emby / Jellyfin 地址</label>
            <input v-model="$root.cfg.emby_url" type="text" placeholder="例如： http://192.168.1.100:8096">
            <small style="color:#888;display:block;margin-top:4px">填入服务器地址，含协议与端口，末尾不加斜杠</small>
          </div>
          <div class="form-group">
            <label>API Key</label>
            <div class="input-with-btn">
              <input v-model="$root.cfg.emby_api_key" :type="$root.showEmbyKey ? 'text' : 'password'" placeholder="在 Emby 管理后台《 API 密鑰 》页面生成">
              <button class="btn btn-sm" @click="$root.showEmbyKey=!$root.showEmbyKey">{{$root.showEmbyKey?'隐藏':'显示'}}</button>
            </div>
            <small style="color:#888;display:block;margin-top:4px">Emby: 管理后台 → 高级 → API 密鑰；Jellyfin: 管理后台 → API 密鑰</small>
          </div>
          <div class="form-group">
            <label>通知延迟（秒）</label>
            <input v-model.number="$root.cfg.emby_notify_delay" type="number" min="5" max="300" placeholder="30">
            <small style="color:#888;display:block;margin-top:4px">最后一个文件刮削完成后等待此秒数再触发扫描，同批文件只触发一次</small>
          </div>
          <div class="form-actions">
            <button class="btn btn-primary" @click="$root.saveSettings">保存</button>
            <button class="btn" @click="$root.testEmby">测试连接</button>
          </div>
          <div v-if="$root.embyTestResult" :class="['msg', $root.embyTestResult.ok?'msg-ok':'msg-err']">{{$root.embyTestResult.message}}</div>
        </div>
      </section>
    `,
  },
  'symlink-folders-page': {
    template: `
      <section>
        <div class="page-header">
          <h1>导出软链接</h1>
          <button class="btn btn-primary" @click="$root.showAddSymlink=true">+ 添加目录</button>
        </div>
        <table class="table" v-if="$root.symlinkFolders.length">
          <thead>
            <tr>
              <th>监控路径</th><th>导出目标</th><th>状态</th><th>操作</th>
            </tr>
          </thead>
          <tbody>
            <tr v-for="f in $root.symlinkFolders" :key="f.id">
              <td>{{f.path}}</td>
              <td>{{f.target_root || '-'}}</td>
              <td><span :class="['badge', f.enabled?'badge-success':'badge-gray']">{{f.enabled?'监控中':'已停用'}}</span></td>
              <td class="actions">
                <button class="btn btn-sm" @click="$root.scanFolder(f.id)" title="立即扫描">🔍</button>
                <button class="btn btn-sm" @click="$root.toggleFolder(f)" title="启停">{{f.enabled?'⏸':'▶'}}</button>
                <button class="btn btn-sm btn-danger" @click="$root.deleteFolder(f.id)" title="删除">🗑</button>
              </td>
            </tr>
          </tbody>
        </table>
        <div class="empty" v-else>暂无软链接导出目录，点击右上角添加</div>

        <div class="modal-overlay" v-if="$root.showAddSymlink" @click.self="$root.showAddSymlink=false">
          <div class="modal" style="width:520px">
            <div style="display:flex;align-items:center;justify-content:space-between;margin-bottom:16px">
              <h2 style="margin:0">添加软链接导出目录</h2>
              <button @click="$root.showAddSymlink=false" style="background:none;border:none;font-size:22px;line-height:1;cursor:pointer;color:#999;padding:0 6px">&times;</button>
            </div>
            <div class="form-group">
              <label>监控路径（原始文件所在目录）</label>
              <div class="input-with-btn">
                <input v-model="$root.newSymlinkFolder.path" placeholder="例如 E:\\MPSTRM" readonly>
                <button class="btn btn-sm" @click="$root.openBrowse('symlink_path')">浏览...</button>
              </div>
            </div>
            <div class="form-group">
              <label>导出目标目录（软链接创建位置）</label>
              <div class="input-with-btn">
                <input v-model="$root.newSymlinkFolder.target_root" placeholder="例如 E:\\STRM" readonly>
                <button class="btn btn-sm" @click="$root.openBrowse('symlink_target')">浏览...</button>
              </div>
              <small style="color:#1976d2;display:block;margin-top:6px">监控路径中的文件将在目标目录创建同名软链接，保持相同的目录结构，不刮削不改名</small>
            </div>
            <div class="form-actions">
              <button class="btn" @click="$root.showAddSymlink=false">取消</button>
              <button class="btn btn-primary" @click="$root.addSymlinkFolder">确认添加</button>
            </div>
          </div>
        </div>
      </section>
    `,
  },
});
