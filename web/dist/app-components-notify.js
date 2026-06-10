window.scraperAppPageComponents = Object.assign(window.scraperAppPageComponents || {}, {
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
            <small style="color:var(--text-muted);display:block;margin-top:4px">文件夹内最后一个文件处理完成后等待此秒数再发送通知，以便汇总同批次文件</small>
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
            <small style="color:var(--text-muted);display:block;margin-top:4px">刮削成功后自动触发 Emby / Jellyfin 扫描，无需手动刷新媒体库。支持 Emby 和 Jellyfin。</small>
          </div>
          <div class="form-group">
            <label>Emby / Jellyfin 地址</label>
            <input v-model="$root.cfg.emby_url" type="text" placeholder="例如： http://192.168.1.100:8096">
            <small style="color:var(--text-muted);display:block;margin-top:4px">填入服务器地址，含协议与端口，末尾不加斜杠</small>
          </div>
          <div class="form-group">
            <label>API Key</label>
            <div class="input-with-btn">
              <input v-model="$root.cfg.emby_api_key" :type="$root.showEmbyKey ? 'text' : 'password'" placeholder="在 Emby 管理后台《 API 密鑰 》页面生成">
              <button class="btn btn-sm" @click="$root.showEmbyKey=!$root.showEmbyKey">{{$root.showEmbyKey?'隐藏':'显示'}}</button>
            </div>
            <small style="color:var(--text-muted);display:block;margin-top:4px">Emby: 管理后台 → 高级 → API 密鑰；Jellyfin: 管理后台 → API 密鑰</small>
          </div>
          <div class="form-group">
            <label>通知延迟（秒）</label>
            <input v-model.number="$root.cfg.emby_notify_delay" type="number" min="5" max="300" placeholder="30">
            <small style="color:var(--text-muted);display:block;margin-top:4px">最后一个文件刮削完成后等待此秒数再触发扫描，同批文件只触发一次</small>
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
              <td>
                <label class="switch" :title="f.enabled?'监控中，点击停用':'已停用，点击启用'">
                  <input type="checkbox" :checked="f.enabled" @change="$root.toggleFolder(f)">
                  <span class="switch-track"><span class="switch-dot"></span></span>
                </label>
              </td>
              <td class="actions">
                <button class="btn btn-sm btn-icon" @click="$root.scanFolder(f.id)" title="立即扫描"><svg xmlns="http://www.w3.org/2000/svg" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2" stroke-linecap="round" stroke-linejoin="round"><circle cx="11" cy="11" r="8"/><path d="m21 21-4.34-4.34"/></svg></button>
                <button class="btn btn-sm btn-icon btn-danger" @click="$root.deleteFolder(f.id)" title="删除"><svg xmlns="http://www.w3.org/2000/svg" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2" stroke-linecap="round" stroke-linejoin="round"><path d="M3 6h18"/><path d="M19 6v14c0 1-1 2-2 2H7c-1 0-2-1-2-2V6"/><path d="M8 6V4c0-1 1-2 2-2h4c1 0 2 1 2 2v2"/><line x1="10" x2="10" y1="11" y2="17"/><line x1="14" x2="14" y1="11" y2="17"/></svg></button>
              </td>
            </tr>
          </tbody>
        </table>
        <div class="empty" v-else>暂无软链接导出目录，点击右上角添加</div>

        <div class="modal-overlay" v-if="$root.showAddSymlink" @click.self="$root.showAddSymlink=false">
          <div class="modal" style="width:520px">
            <div style="display:flex;align-items:center;justify-content:space-between;margin-bottom:16px">
              <h2 style="margin:0">添加软链接导出目录</h2>
              <button @click="$root.showAddSymlink=false" style="background:none;border:none;font-size:22px;line-height:1;cursor:pointer;color:var(--text-muted);padding:0 6px">&times;</button>
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
              <small style="color:var(--info-text);display:block;margin-top:6px">监控路径中的文件将在目标目录创建同名软链接，保持相同的目录结构，不刮削不改名</small>
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
