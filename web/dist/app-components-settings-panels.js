window.scraperAppPageComponents = Object.assign(window.scraperAppPageComponents || {}, {
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
        <div class="card">
          <div class="form-group">
            <label>Metadata Hub 本地目录</label>
            <input v-model="$root.cfg.metadata_hub_root" placeholder="/media/metadata hub">
            <small style="color:var(--text-muted);display:block;margin-top:6px">只读、手动调用。仅在记录页点击“从 Metadata Hub 更新”时读取，不会自动查询、覆盖或定期同步。</small>
          </div>
          <div class="form-actions">
            <button class="btn btn-primary" @click="$root.saveSettings">保存</button>
            <button class="btn" @click="$root.testMetadataHub">测试 Metadata Hub 目录</button>
          </div>
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
            <small style="color:var(--text-muted);display:block;margin-top:6px" v-if="$root.cfg.ai_mode==='disabled'">禁用：只使用 guessit 解析文件名并搜索资料库，不调用 AI。</small>
            <small style="color:var(--text-muted);display:block;margin-top:6px" v-else-if="!$root.cfg.ai_mode||$root.cfg.ai_mode==='assist'">辅助识别：标准命名优先走 guessit；遇到标题不可靠、番组命名或季集不清晰时自动调用 AI。记录来源只显示 guessit 或 AI，不显示混合。</small>
            <small style="color:var(--text-muted);display:block;margin-top:6px" v-else>强制使用：始终由 AI 解析标题与季集；AI 失败时会直接记为失败或待处理。</small>
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
              <small style="color:var(--text-muted);display:block;margin-top:6px">复用本区域的 API URL 和 API Key，通过 /embeddings 接口进行候选重排。</small>
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
              <span v-for="(kw, idx) in $root.cfg.strip_keywords" :key="idx" style="display:inline-flex;align-items:center;padding:4px 10px;background:var(--info-soft);color:var(--info-text);border-radius:14px;font-size:13px;border:1px solid var(--info-border)">
                {{kw}}
                <span @click="$root.removeStripKeyword(idx)" style="margin-left:6px;cursor:pointer;color:var(--text-muted);font-size:15px;line-height:1">&times;</span>
              </span>
            </div>
            <small style="color:var(--info-text);display:block;margin-top:8px">从视频文件名中提取影视剧标题时先删除这些关键词，添加的越多识别准确率越高</small>
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
            <small style="color:var(--text-muted);display:block;margin-top:6px">设置 API 识别缓存的自动过期时间，过期后下次识别将重新向 API 请求。选择「永不过期」则只能手动清除。</small>
          </div>
          <div class="form-actions">
            <button class="btn btn-primary" @click="$root.saveSettings">保存</button>
          </div>
        </div>

        <div class="card" style="margin-top:16px">
          <div class="form-group">
            <label style="font-weight:600;font-size:15px">元数据自动补齐</label>
            <small style="color:var(--info-text);display:block;margin-top:6px">新番/新剧首次刮削时 TMDB 数据可能不完整（缺集名、简介、截图、演员等），开启后系统会定期巡检并自动从 TMDB/BGM 拉取最新数据补齐 NFO 和图片。</small>
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
                <option :value="360">15 天（半个月）</option>
                <option :value="720">30 天（一个月）</option>
              </select>
            </div>
            <div class="form-group">
              <label>回看天数</label>
              <select v-model.number="$root.cfg.metadata_refresh_lookback_days" style="max-width:200px">
                <option :value="7">最近 7 天</option>
                <option :value="14">最近 14 天（默认）</option>
                <option :value="15">最近 15 天（半个月）</option>
                <option :value="30">最近 30 天</option>
                <option :value="90">最近 90 天</option>
                <option :value="0">不限制</option>
              </select>
              <small style="color:var(--text-muted);display:block;margin-top:6px">只检查此范围内的已成功记录，超过该天数的记录不再自动巡检。</small>
            </div>
          </div>
          <div class="form-group">
            <label>跳过集标题补齐的作品</label>
            <textarea v-model="$root.cfg.metadata_refresh_ignore_episode_title_rules" rows="3" placeholder="每行一个作品标题或 ID，例如：&#10;夺命许愿&#10;tmdb:285838" style="font-family:Consolas,Monaco,monospace;resize:vertical"></textarea>
            <small style="color:var(--text-muted);display:block;margin-top:6px">命中的作品不会因为“集标题”是第一集/第二集/第 1 集而反复巡检；其它字段如简介、剧照、演员仍会正常补齐。</small>
          </div>
          <div class="form-group">
            <label>跳过自动补齐的作品</label>
            <textarea v-model="$root.cfg.metadata_refresh_skip_rules" rows="3" placeholder="每行一个作品标题或 ID，例如：&#10;幽游白书&#10;tmdb:121659" style="font-family:Consolas,Monaco,monospace;resize:vertical"></textarea>
            <small style="color:var(--text-muted);display:block;margin-top:6px">命中的作品不会进入元数据自动补齐巡检；也可以在“元数据巡检日志”的分组视图里勾选作品后一键加入。</small>
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
            <small style="color:var(--text-muted);display:block;margin-top:6px">可直接填写 127.0.0.1:7890，保存时会自动补全为 http://127.0.0.1:7890。</small>
          </div>

          <div class="form-group">
            <label>NO_PROXY（不走代理）</label>
            <textarea v-model="$root.cfg.proxy_no_proxy" rows="3" style="font-family:Consolas,Monaco,monospace;resize:vertical"></textarea>
            <small style="color:var(--text-muted);display:block;margin-top:6px">默认包含 localhost、127.0.0.1、host.docker.internal、192.168.*、10.*、172.16-31.*，避免 Ollama、Web 后端和本地服务被代理。</small>
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
            <small style="color:var(--text-muted);display:block;margin-top:6px">开启后会从原文件名提取清晰度、片源、编码、音频、发布组等信息；如果命名模板未显式写入 {media_suffix}，会自动追加到扩展名前。</small>
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
});
