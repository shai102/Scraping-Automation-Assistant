window.scraperAppPageComponents = Object.assign(window.scraperAppPageComponents || {}, {
  'logs-page': {
    template: `
      <section>
        <div class="page-header"><h1>{{$root.currentLogTitle()}}</h1></div>
        <div class="toolbar">
          <div class="toolbar-left">
            <span class="toolbar-hint" v-if="$root.logPath">日志文件：{{$root.logPath}}</span>
            <span class="toolbar-hint" v-if="$root.logAutoRefreshEnabled" style="margin-left:12px">自动刷新中（3秒）</span>
            <template v-if="$root.currentLogKind()==='metadata' && $root.metadataLogGroupedView">
              <button class="btn-grad btn-grad-yellow btn-sm" @click="$root.skipSelectedMetadataLogGroups" :disabled="!$root.metadataLogSelectedSkipRules().length">跳过所选补齐</button>
              <span class="toolbar-hint">已选择 {{$root.metadataLogSelectedSkipRules().length}} 个作品</span>
            </template>
          </div>
          <div class="toolbar-right">
            <button v-if="$root.currentLogKind()==='metadata'" class="btn-grad btn-grad-blue btn-sm" @click="$root.toggleMetadataLogGroupedView">{{$root.metadataLogGroupedView?'列表视图':'分组视图'}}</button>
            <button class="btn-grad btn-grad-blue btn-sm" @click="$root.refreshLogs" :disabled="$root.logLoading">{{$root.logLoading?'加载中...':'刷新'}}</button>
            <button class="btn-grad btn-grad-red btn-sm" @click="$root.clearLogs">一键清除</button>
          </div>
        </div>
        <div class="filter-row">
          <select v-model="$root.logLevel" class="filter-select">
            <option value="">全部级别</option>
            <option value="INFO">INFO</option>
            <option value="WARNING">WARNING</option>
            <option value="ERROR">ERROR</option>
          </select>
          <select v-model.number="$root.logLimit" class="filter-select">
            <option :value="100">最近 100 条</option>
            <option :value="200">最近 200 条</option>
            <option :value="500">最近 500 条</option>
          </select>
          <select v-model="$root.logDate" class="filter-select" @change="$root.onLogDateChange">
            <option v-for="d in $root.logDates" :key="d" :value="d">{{d}}</option>
          </select>
          <input v-model="$root.logKeyword" placeholder="按关键字搜索日志内容" class="filter-input">
          <label style="display:flex;align-items:center;gap:6px;padding:0 4px;white-space:nowrap;color:#666">
            <input type="checkbox" v-model="$root.logShowAnnotations">
            中文注释
          </label>
          <button class="btn btn-primary btn-sm" @click="$root.loadLogs">筛选</button>
          <button class="btn btn-sm" @click="$root.resetLogFilter">重置</button>
        </div>
        <template v-if="!($root.currentLogKind()==='metadata' && $root.metadataLogGroupedView)">
          <table class="table" v-if="$root.logEntries.length">
            <thead>
              <tr>
                <th style="width:110px">级别</th>
                <th style="width:180px">时间</th>
                <th>日志内容</th>
              </tr>
            </thead>
            <tbody>
              <tr v-for="(log, idx) in $root.logEntries" :key="idx">
                <td><span :class="['badge', $root.logLevelClass(log.level)]">{{log.level || 'INFO'}}</span></td>
                <td style="white-space:nowrap;font-size:12px;color:#999">{{log.timestamp || '-'}}</td>
                <td style="font-family:Consolas,'Courier New',monospace;font-size:12px;line-height:1.5;word-break:break-all">
                  <div>{{log.message}}</div>
                  <div v-if="$root.logShowAnnotations && log.annotation" style="margin-top:10px;padding:12px 14px;border-radius:10px;background:#fafbff;border:1px solid #e8ecff;font-family:system-ui,-apple-system,sans-serif;color:#333">
                    <div style="font-weight:700;margin-bottom:6px">{{log.annotation.title}}</div>
                    <div v-if="log.annotation.summary" style="font-size:12px;color:#666;margin-bottom:8px">{{log.annotation.summary}}</div>
                    <ul v-if="log.annotation.items && log.annotation.items.length" style="margin:0;padding-left:18px">
                      <li v-for="(item, itemIdx) in log.annotation.items" :key="itemIdx" style="margin:0 0 8px 0">
                        <div style="font-family:Consolas,'Courier New',monospace;font-size:12px;font-weight:600">{{item.label}}{{item.value!=='' ? '=' + item.value : ''}}</div>
                        <div style="font-size:12px;color:#666;margin-top:2px">中文：{{item.note}}</div>
                      </li>
                    </ul>
                  </div>
                </td>
              </tr>
            </tbody>
          </table>
          <div class="empty" v-else>{{$root.logLoading?'日志加载中...':'暂无日志数据'}}</div>
        </template>
        <template v-else>
          <div class="empty" v-if="!$root.metadataLogGroupedRecords.length">{{$root.logLoading?'日志加载中...':'暂无日志数据'}}</div>
          <table class="table" v-else>
            <thead>
              <tr>
                <th style="width:52px"></th>
                <th>作品 / 分组</th>
                <th style="width:130px">跳过规则</th>
                <th style="width:80px">总数</th>
                <th style="width:80px">巡检</th>
                <th style="width:80px">成功</th>
                <th style="width:80px">失败</th>
                <th style="width:180px">最近时间</th>
              </tr>
            </thead>
            <template v-for="g in $root.metadataLogGroupedRecords" :key="g.key">
              <tbody>
                <tr class="group-row" @click="$root.toggleMetadataLogGroup(g)">
                  <td><input type="checkbox" :checked="$root.metadataLogGroupSelected(g)" :disabled="!$root.metadataLogSkipRule(g)" @click.stop="$root.toggleMetadataLogSelectGroup(g)"></td>
                  <td>
                    <span style="display:inline-block;width:18px;text-align:center;margin-right:4px">{{$root.metadataLogExpandedGroups[g.key]?'▼':'▶'}}</span>
                    <strong>{{g.dir_name}}</strong>
                    <span v-if="g.dir_path" style="color:#999;margin-left:8px;font-size:12px">{{g.dir_path}}</span>
                  </td>
                  <td><span class="badge badge-gray" v-if="$root.metadataLogSkipRule(g)">{{$root.metadataLogSkipRule(g)}}</span><span v-else>-</span></td>
                  <td><span class="badge badge-gray">{{g.total}}</span></td>
                  <td><span class="badge badge-warning" v-if="g.scan">{{g.scan}}</span><span v-else>-</span></td>
                  <td><span class="badge badge-success" v-if="g.success">{{g.success}}</span><span v-else>-</span></td>
                  <td><span class="badge badge-danger" v-if="g.failed">{{g.failed}}</span><span v-else>-</span></td>
                  <td style="white-space:nowrap;font-size:12px;color:#999">{{g.records[0] && g.records[0].timestamp || '-'}}</td>
                </tr>
              </tbody>
              <tbody v-if="$root.metadataLogExpandedGroups[g.key]">
                <tr class="group-record-row">
                  <td colspan="8" style="padding:0;background:#f8fafc">
                    <div style="display:grid;gap:10px;padding:12px 14px 16px 54px">
                      <article v-for="log in g.records" :key="g.key + '-' + log._idx" style="border:1px solid #e5e7eb;border-radius:12px;background:#fff;box-shadow:0 1px 2px rgba(15,23,42,.04);overflow:hidden">
                        <div style="display:flex;align-items:center;gap:10px;padding:10px 12px;border-bottom:1px solid #eef2f7;background:#fbfdff">
                          <span :class="['badge', $root.metadataLogKindClass(log.kind)]">{{$root.metadataLogKindText(log.kind)}}</span>
                          <span style="font-size:12px;color:#64748b">{{log.timestamp || '-'}}</span>
                          <span v-if="log.parsed && log.parsed.record_id" style="font-size:12px;color:#94a3b8">record_id={{log.parsed.record_id}}</span>
                        </div>
                        <div style="padding:12px 14px;font-family:Consolas,'Courier New',monospace;font-size:12px;line-height:1.65;word-break:break-all;color:#334155">
                          <div>{{log.message}}</div>
                          <div v-if="$root.logShowAnnotations && log.annotation" style="margin-top:10px;padding:12px 14px;border-radius:10px;background:#f8fafc;border:1px solid #e2e8f0;font-family:system-ui,-apple-system,sans-serif;color:#333">
                            <div style="font-weight:700;margin-bottom:6px">{{log.annotation.title}}</div>
                            <div v-if="log.annotation.summary" style="font-size:12px;color:#666;margin-bottom:8px">{{log.annotation.summary}}</div>
                            <ul v-if="log.annotation.items && log.annotation.items.length" style="margin:0;padding-left:18px">
                              <li v-for="(item, itemIdx) in log.annotation.items" :key="itemIdx" style="margin:0 0 8px 0">
                                <div style="font-family:Consolas,'Courier New',monospace;font-size:12px;font-weight:600">{{item.label}}{{item.value!=='' ? '=' + item.value : ''}}</div>
                                <div style="font-size:12px;color:#666;margin-top:2px">中文：{{item.note}}</div>
                              </li>
                            </ul>
                          </div>
                        </div>
                      </article>
                    </div>
                  </td>
                </tr>
              </tbody>
            </template>
          </table>
        </template>
      </section>
    `,
  },
});
