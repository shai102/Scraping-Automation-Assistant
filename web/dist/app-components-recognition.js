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
});
