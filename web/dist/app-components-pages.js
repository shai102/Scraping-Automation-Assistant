window.scraperAppPageComponents = {
  'records-page': {
    template: `
      <section>
        <div class="page-header"><h1>刮削记录</h1></div>
        <div class="toolbar">
          <div class="toolbar-left">
            <button class="btn-grad btn-grad-orange btn-sm" @click="$root.exportErrors">导出识别错误</button>
            <button class="btn-grad btn-grad-red btn-sm" @click="$root.batchDeleteSelected" :disabled="!$root.selectedIds.length">删除所选记录</button>
            <button class="btn-grad btn-grad-yellow btn-sm" @click="$root.batchRetrySelected" :disabled="!$root.selectedIds.length">重新整理所有</button>
            <button class="btn-grad btn-grad-blue btn-sm" @click="$root.batchRefreshMetadata" :disabled="!$root.selectedIds.length">从 TMDB 刷新</button>
            <button class="btn-grad btn-grad-green btn-sm" @click="$root.batchUpdateFromMetadataHub" :disabled="!$root.selectedIds.length">从 Metadata Hub 更新</button>
            <span class="toolbar-hint">已选择 {{$root.selectedIds.length}} 条记录</span>
          </div>
          <div class="toolbar-right">
            <button class="btn-grad btn-grad-blue btn-sm" @click="$root.toggleGroupedView">{{$root.groupedView?'列表视图':'分组视图'}}</button>
            <button class="btn-grad btn-grad-blue btn-sm" @click="$root.refreshRecords">刷新</button>
            <button class="btn-grad btn-grad-orange btn-sm" @click="$root.clearFailed">清除失败记录</button>
            <button class="btn-grad btn-grad-red btn-sm" @click="$root.clearAll">清空记录</button>
          </div>
        </div>
        <div class="filter-row">
          <input v-model="$root.recordKeyword" placeholder="按文件名模糊搜索" class="filter-input">
          <select v-model="$root.recordFilter" class="filter-select">
            <option value="">筛选状态</option>
            <option value="success">成功</option>
            <option value="pending_manual">待手动</option>
            <option value="skipped">已跳过</option>
            <option value="processing">处理中</option>
            <option value="failed">失败</option>
          </select>
          <select v-model="$root.recordTypeFilter" class="filter-select">
            <option value="">筛选类型</option>
            <option value="movie">电影</option>
            <option value="tv">电视剧</option>
            <option value="auto">自动</option>
          </select>
          <select v-model="$root.recordParseFilter" class="filter-select">
            <option value="">识别来源</option>
            <option value="guessit">guessit</option>
            <option value="ai">AI</option>
          </select>
          <button class="btn btn-primary btn-sm" @click="$root.recordPage=1;$root.groupedView?$root.loadGroupedRecords():$root.loadRecords()">筛选</button>
          <button class="btn btn-sm" @click="$root.resetRecordFilter">重置</button>
        </div>
        <template v-if="!$root.groupedView">
          <table class="table">
            <thead>
              <tr>
                <th style="width:52px"><input type="checkbox" @change="$root.toggleSelectAll" :checked="$root.allSelected"></th>
                <th style="width:72px">运行状态</th>
                <th style="width:72px">文件状态</th>
                <th style="width:48px">类型</th>
                <th style="width:56px">来源</th>
                <th style="width:180px">识别信息</th>
                <th>文件路径 / 归档路径</th>
                <th style="width:148px">时间</th>
                <th style="width:260px">操作</th>
              </tr>
            </thead>
            <tbody>
              <tr v-for="r in $root.records" :key="r.id">
                <td><input type="checkbox" :value="r.id" v-model="$root.selectedIds"></td>
                <td><span :class="['badge', $root.statusClass(r.status)]">{{$root.statusText(r.status)}}</span></td>
                <td><span :class="['badge', $root.fileStatusClass(r)]">{{$root.fileStatusText(r)}}</span></td>
                <td>{{$root.recordType(r)}}</td>
                <td>{{r.matched_provider || '-'}}</td>
                <td class="cell-name">
                  <template v-if="r.matched_title">{{r.matched_title}}<span v-if="r.matched_id" style="color:#999;margin-left:4px">(ID:{{r.matched_id}})</span></template>
                  <span v-else-if="r.error_msg" style="color:#999;font-size:12px">{{r.error_msg}}</span>
                  <span v-else style="color:#999">-</span>
                  <span v-if="r.parse_source==='ai'" style="display:inline-block;margin-left:4px;padding:1px 5px;border-radius:3px;font-size:11px;background:#e8f4fd;color:#1a73e8;border:1px solid #90cdf4">AI</span>
                  <span v-else-if="r.parse_source==='guessit'" style="display:inline-block;margin-left:4px;padding:1px 5px;border-radius:3px;font-size:11px;background:#f0fdf4;color:#16a34a;border:1px solid #86efac">guessit</span>
                  <div v-if="r.status==='pending_manual' && r.error_msg && r.matched_title" style="font-size:11px;color:#e67e22;margin-top:2px">{{r.error_msg}}</div>
                </td>
                <td class="cell-path">
                  <div>{{r.original_path || '-'}}</div>
                  <div class="path-target" v-if="r.target_path">→ {{r.target_path}}</div>
                </td>
                <td style="white-space:nowrap;font-size:12px;color:#999">{{$root.formatTime(r.updated_at || r.created_at)}}</td>
                <td class="actions">
                  <button v-if="r.status!=='processing'" class="btn btn-sm btn-primary" @click="$root.openManualMatch(r)">手动识别</button>
                  <button v-if="r.status==='success'" class="btn btn-sm" @click="$root.refreshMetadata(r.id)" title="重新从 TMDB 拉取元数据">从 TMDB 刷新</button>
                  <button v-if="r.status==='success'&&r.matched_provider==='tmdb'" class="btn btn-sm btn-success" @click="$root.updateFromMetadataHub(r.id)" title="按 TMDB ID、季号、集号读取本地修正数据">从 Hub 更新</button>
                  <button v-if="r.status==='failed'||r.status==='pending_manual'" class="btn btn-sm" @click="$root.retryRecord(r.id)">重试</button>
                  <button class="btn btn-sm btn-danger" @click="$root.deleteRecord(r.id)">删除</button>
                </td>
              </tr>
            </tbody>
          </table>
          <div class="empty" v-if="!$root.records.length">No Data</div>
          <div class="pagination-bar">
            <span class="pagination-total">Total {{$root.recordTotal}}</span>
            <select v-model.number="$root.recordPageSize" @change="$root.recordPage=1;$root.loadRecords()" class="pagination-size">
              <option :value="20">20</option>
              <option :value="50">50</option>
              <option :value="100">100</option>
            </select>
            <div class="pagination-pages" v-if="$root.recordTotal > 0">
              <button class="btn btn-sm" :disabled="$root.recordPage<=1" @click="$root.recordPage--;$root.recordGoPage=$root.recordPage;$root.loadRecords()">&lt;</button>
              <span class="pagination-current">{{$root.recordPage}}</span>
              <span style="color:#aaa;margin:0 2px">/</span>
              <span class="pagination-total-pages">{{Math.ceil($root.recordTotal/$root.recordPageSize)||1}}</span>
              <button class="btn btn-sm" :disabled="$root.recordPage*$root.recordPageSize>=$root.recordTotal" @click="$root.recordPage++;$root.recordGoPage=$root.recordPage;$root.loadRecords()">&gt;</button>
              <span style="margin-left:8px">跳至</span>
              <input v-model.number="$root.recordGoPage" class="pagination-goto" @keyup.enter="$root.gotoPage">
              <button class="btn btn-sm" @click="$root.gotoPage">确定</button>
            </div>
          </div>
        </template>
        <template v-else>
          <div class="empty" v-if="!$root.groupedRecords.length">No Data</div>
          <table class="table" v-else>
            <thead>
              <tr>
                <th style="width:52px"></th>
                <th>目录</th>
                <th style="width:80px">总数</th>
                <th style="width:80px">成功</th>
                <th style="width:80px">失败</th>
                <th style="width:80px">待手动</th>
                <th style="width:120px">操作</th>
              </tr>
            </thead>
            <template v-for="g in $root.groupedRecords" :key="g.dir_path">
              <tbody>
                <tr class="group-row" @click="$root.toggleGroup(g)">
                  <td><input type="checkbox" :checked="$root.isGroupAllSelected(g)" @click.stop="$root.toggleSelectGroup(g)"></td>
                  <td>
                    <span style="display:inline-block;width:18px;text-align:center;margin-right:4px">{{$root.expandedGroups[g.dir_path]?'▼':'▶'}}</span>
                    <strong>{{g.dir_name}}</strong>
                    <span style="color:#999;margin-left:8px;font-size:12px">{{g.dir_path}}</span>
                  </td>
                  <td><span class="badge badge-gray">{{g.total}}</span></td>
                  <td><span class="badge badge-success" v-if="g.success">{{g.success}}</span><span v-else>-</span></td>
                  <td><span class="badge badge-danger" v-if="g.failed">{{g.failed}}</span><span v-else>-</span></td>
                  <td><span class="badge badge-warning" v-if="g.pending">{{g.pending}}</span><span v-else>-</span></td>
                  <td class="actions" @click.stop>
                    <button class="btn btn-sm btn-danger" @click="$root.deleteGroup(g)">删除全组</button>
                  </td>
                </tr>
              </tbody>
              <tbody v-if="$root.expandedGroups[g.dir_path]">
                <tr v-if="$root.expandedGroups[g.dir_path].loading">
                  <td colspan="7" style="text-align:center;color:#999;padding:12px">加载中...</td>
                </tr>
                <template v-else>
                  <tr class="group-record-row" v-for="r in $root.expandedGroups[g.dir_path].records" :key="r.id">
                    <td><input type="checkbox" :value="r.id" v-model="$root.selectedIds"></td>
                    <td colspan="2" class="cell-name" style="padding-left:40px">
                      <div style="font-size:13px">{{r.original_name}}</div>
                      <div class="path-target" v-if="r.target_path" style="font-size:12px">→ {{r.target_path}}</div>
                    </td>
                    <td>
                      <span :class="['badge', $root.statusClass(r.status)]">{{$root.statusText(r.status)}}</span>
                    </td>
                    <td class="cell-name">
                      <template v-if="r.matched_title">{{r.matched_title}}<span v-if="r.matched_id" style="color:#999;margin-left:4px;font-size:11px">({{r.matched_id}})</span></template>
                      <span v-else style="color:#999">-</span>
                      <span v-if="r.parse_source==='ai'" style="display:inline-block;margin-left:4px;padding:1px 5px;border-radius:3px;font-size:11px;background:#e8f4fd;color:#1a73e8;border:1px solid #90cdf4">AI</span>
                      <span v-else-if="r.parse_source==='guessit'" style="display:inline-block;margin-left:4px;padding:1px 5px;border-radius:3px;font-size:11px;background:#f0fdf4;color:#16a34a;border:1px solid #86efac">guessit</span>
                      <div v-if="r.status==='pending_manual' && r.error_msg && r.matched_title" style="font-size:11px;color:#e67e22;margin-top:2px">{{r.error_msg}}</div>
                    </td>
                    <td style="white-space:nowrap;font-size:12px;color:#999">{{$root.formatTime(r.updated_at || r.created_at)}}</td>
                    <td class="actions">
                      <button v-if="r.status!=='processing'" class="btn btn-sm btn-primary" @click="$root.openManualMatch(r)">手动识别</button>
                      <button v-if="r.status==='success'" class="btn btn-sm" @click="$root.refreshMetadata(r.id)">从 TMDB 刷新</button>
                      <button v-if="r.status==='success'&&r.matched_provider==='tmdb'" class="btn btn-sm btn-success" @click="$root.updateFromMetadataHub(r.id)">从 Hub 更新</button>
                      <button class="btn btn-sm btn-danger" @click="$root.deleteGroupRecord(g, r.id)">删除</button>
                    </td>
                  </tr>
                  <tr v-if="$root.expandedGroups[g.dir_path].total > 50" class="group-pagination-row">
                    <td colspan="7" style="text-align:center;padding:8px">
                      <button class="btn btn-sm" :disabled="$root.expandedGroups[g.dir_path].page<=1" @click.stop="$root.groupPagePrev(g)">&lt; 上一页</button>
                      <span style="margin:0 12px;font-size:13px;color:#666">{{$root.expandedGroups[g.dir_path].page}} / {{Math.ceil($root.expandedGroups[g.dir_path].total/50)}}</span>
                      <button class="btn btn-sm" :disabled="$root.expandedGroups[g.dir_path].page*50>=$root.expandedGroups[g.dir_path].total" @click.stop="$root.groupPageNext(g)">下一页 &gt;</button>
                    </td>
                  </tr>
                </template>
              </tbody>
            </template>
          </table>
        </template>
      </section>
    `,
  },
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
  'symlink-records-page': {
    template: `
      <section>
        <div class="page-header"><h1>软链接记录</h1></div>
        <div class="toolbar">
          <div class="toolbar-left">
            <button class="btn-grad btn-grad-red btn-sm" @click="$root.batchDeleteSymlinkSelected" :disabled="!$root.symlinkSelectedIds.length">删除所选记录</button>
            <button class="btn-grad btn-grad-yellow btn-sm" @click="$root.retrySymlinkFailed" :disabled="!$root.symlinkStats.failed">重试失败</button>
            <button class="btn-grad btn-grad-blue btn-sm" @click="$root.toggleSymlinkGroupedView">{{$root.symlinkGroupedView?'列表视图':'分组视图'}}</button>
            <span class="toolbar-hint">已选择 {{$root.symlinkSelectedIds.length}} 条记录</span>
            <span class="toolbar-hint" v-if="$root.symlinkStats.total" style="margin-left:12px">共 {{$root.symlinkStats.total}} 条，成功 {{$root.symlinkStats.success || 0}}，失败 {{$root.symlinkStats.failed || 0}}</span>
          </div>
        </div>
        <div class="filter-row">
          <input v-model="$root.symlinkKeyword" placeholder="按文件名模糊搜索" class="filter-input">
          <select v-model="$root.symlinkFilter" class="filter-select">
            <option value="">筛选状态</option>
            <option value="success">成功</option>
            <option value="failed">失败</option>
          </select>
          <button class="btn btn-primary btn-sm" @click="$root.symlinkPage=1;$root.symlinkGroupedView?$root.loadSymlinkGroupedRecords():$root.loadSymlinkRecords()">筛选</button>
          <button class="btn btn-sm" @click="$root.resetSymlinkFilter">重置</button>
        </div>
        <template v-if="!$root.symlinkGroupedView">
          <table class="table" v-if="$root.symlinkRecords.length">
            <thead>
              <tr>
                <th style="width:52px"><input type="checkbox" @change="$root.toggleSymlinkSelectAll" :checked="$root.symlinkAllSelected"></th>
                <th>状态</th>
                <th>原始文件</th>
                <th>软链接路径</th>
                <th style="width:148px">时间</th>
                <th style="width:90px">操作</th>
              </tr>
            </thead>
            <tbody>
              <tr v-for="r in $root.symlinkRecords" :key="r.id">
                <td><input type="checkbox" :value="r.id" v-model="$root.symlinkSelectedIds"></td>
                <td><span :class="['badge', r.status==='success'?'badge-success':'badge-danger']">{{r.status==='success'?'成功':'失败'}}</span></td>
                <td class="cell-path">
                  <div>{{r.original_path}}</div>
                  <div v-if="r.error_msg" style="color:#e53935;font-size:12px;margin-top:2px">⚠ {{r.error_msg}}</div>
                </td>
                <td class="cell-path">{{r.link_path || '-'}}</td>
                <td style="white-space:nowrap;font-size:12px;color:#999">{{$root.formatTime(r.created_at)}}</td>
                <td class="actions">
                  <button class="btn btn-sm btn-danger" @click="$root.deleteSymlinkRecord(r.id)">删除</button>
                </td>
              </tr>
            </tbody>
          </table>
          <div class="empty" v-else>暂无软链接记录</div>
          <div class="pagination-bar">
            <span class="pagination-total">Total {{$root.symlinkTotal}}</span>
            <select v-model.number="$root.symlinkPageSize" @change="$root.symlinkPage=1;$root.loadSymlinkRecords()" class="pagination-size">
              <option :value="20">20</option>
              <option :value="50">50</option>
              <option :value="100">100</option>
            </select>
            <div class="pagination-pages" v-if="$root.symlinkTotal > 0">
              <button class="btn btn-sm" :disabled="$root.symlinkPage<=1" @click="$root.symlinkPage--;$root.symlinkGoPage=$root.symlinkPage;$root.loadSymlinkRecords()">&lt;</button>
              <span class="pagination-current">{{$root.symlinkPage}}</span>
              <span style="color:#aaa;margin:0 2px">/</span>
              <span class="pagination-total-pages">{{Math.ceil($root.symlinkTotal/$root.symlinkPageSize)||1}}</span>
              <button class="btn btn-sm" :disabled="$root.symlinkPage*$root.symlinkPageSize>=$root.symlinkTotal" @click="$root.symlinkPage++;$root.symlinkGoPage=$root.symlinkPage;$root.loadSymlinkRecords()">&gt;</button>
              <span style="margin-left:8px">跳至</span>
              <input v-model.number="$root.symlinkGoPage" class="pagination-goto" @keyup.enter="$root.gotoSymlinkPage">
              <button class="btn btn-sm" @click="$root.gotoSymlinkPage">确定</button>
            </div>
          </div>
        </template>
        <template v-else>
          <div class="empty" v-if="!$root.symlinkGroupedRecords.length">No Data</div>
          <table class="table" v-else>
            <thead>
              <tr>
                <th style="width:52px"></th>
                <th>目录</th>
                <th style="width:80px">总数</th>
                <th style="width:80px">成功</th>
                <th style="width:80px">失败</th>
                <th style="width:120px">操作</th>
              </tr>
            </thead>
            <template v-for="g in $root.symlinkGroupedRecords" :key="g.dir_path">
              <tbody>
                <tr class="group-row" @click="$root.toggleSymlinkGroup(g)">
                  <td><input type="checkbox" :checked="$root.isSymlinkGroupAllSelected(g)" @click.stop="$root.toggleSelectSymlinkGroup(g)"></td>
                  <td>
                    <span style="display:inline-block;width:18px;text-align:center;margin-right:4px">{{$root.symlinkExpandedGroups[g.dir_path]?'▼':'▶'}}</span>
                    <strong>{{g.dir_name}}</strong>
                    <span style="color:#999;margin-left:8px;font-size:12px">{{g.dir_path}}</span>
                  </td>
                  <td><span class="badge badge-gray">{{g.total}}</span></td>
                  <td><span class="badge badge-success" v-if="g.success">{{g.success}}</span><span v-else>-</span></td>
                  <td><span class="badge badge-danger" v-if="g.failed">{{g.failed}}</span><span v-else>-</span></td>
                  <td class="actions" @click.stop>
                    <button class="btn btn-sm btn-danger" @click="$root.deleteSymlinkGroup(g)">删除全组</button>
                  </td>
                </tr>
              </tbody>
              <tbody v-if="$root.symlinkExpandedGroups[g.dir_path]">
                <tr v-if="$root.symlinkExpandedGroups[g.dir_path].loading">
                  <td colspan="6" style="text-align:center;color:#999;padding:12px">加载中...</td>
                </tr>
                <template v-else>
                  <tr class="group-record-row" v-for="r in $root.symlinkExpandedGroups[g.dir_path].records" :key="r.id">
                    <td><input type="checkbox" :value="r.id" v-model="$root.symlinkSelectedIds"></td>
                    <td colspan="2" class="cell-path" style="padding-left:40px">
                      <div>{{r.original_path}}</div>
                      <div v-if="r.error_msg" style="color:#e53935;font-size:12px;margin-top:2px">⚠ {{r.error_msg}}</div>
                    </td>
                    <td><span :class="['badge', r.status==='success'?'badge-success':'badge-danger']">{{r.status==='success'?'成功':'失败'}}</span></td>
                    <td class="cell-path">{{r.link_path || '-'}}</td>
                    <td class="actions">
                      <button class="btn btn-sm btn-danger" @click="$root.deleteSymlinkGroupRecord(g, r.id)">删除</button>
                    </td>
                  </tr>
                  <tr v-if="$root.symlinkExpandedGroups[g.dir_path].total > 50" class="group-pagination-row">
                    <td colspan="6" style="text-align:center;padding:8px">
                      <button class="btn btn-sm" :disabled="$root.symlinkExpandedGroups[g.dir_path].page<=1" @click.stop="$root.symlinkGroupPagePrev(g)">&lt; 上一页</button>
                      <span style="margin:0 12px;font-size:13px;color:#666">{{$root.symlinkExpandedGroups[g.dir_path].page}} / {{Math.ceil($root.symlinkExpandedGroups[g.dir_path].total/50)}}</span>
                      <button class="btn btn-sm" :disabled="$root.symlinkExpandedGroups[g.dir_path].page*50>=$root.symlinkExpandedGroups[g.dir_path].total" @click.stop="$root.symlinkGroupPageNext(g)">下一页 &gt;</button>
                    </td>
                  </tr>
                </template>
              </tbody>
            </template>
          </table>
        </template>
      </section>
    `,
  },
};
