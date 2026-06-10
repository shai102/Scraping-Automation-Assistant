window.scraperAppPageComponents = Object.assign(window.scraperAppPageComponents || {}, {
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
                  <template v-if="r.matched_title">{{r.matched_title}}<span v-if="r.matched_id" style="color:var(--text-muted);margin-left:4px">(ID:{{r.matched_id}})</span></template>
                  <span v-else-if="r.error_msg" style="color:var(--text-muted);font-size:12px">{{r.error_msg}}</span>
                  <span v-else style="color:var(--text-muted)">-</span>
                  <span v-if="r.parse_source==='ai'" style="display:inline-block;margin-left:4px;padding:1px 5px;border-radius:3px;font-size:11px;background:var(--info-soft);color:var(--info-text);border:1px solid var(--info-border)">AI</span>
                  <span v-else-if="r.parse_source==='guessit'" style="display:inline-block;margin-left:4px;padding:1px 5px;border-radius:3px;font-size:11px;background:var(--success-soft);color:var(--success-text);border:1px solid var(--success-border)">guessit</span>
                  <div v-if="r.status==='pending_manual' && r.error_msg && r.matched_title" style="font-size:11px;color:var(--warning-text);margin-top:2px">{{r.error_msg}}</div>
                </td>
                <td class="cell-path">
                  <div>{{r.original_path || '-'}}</div>
                  <div class="path-target" v-if="r.target_path">→ {{r.target_path}}</div>
                </td>
                <td style="white-space:nowrap;font-size:12px;color:var(--text-muted)">{{$root.formatTime(r.updated_at || r.created_at)}}</td>
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
              <span style="color:var(--text-faint);margin:0 2px">/</span>
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
                    <span style="color:var(--text-muted);margin-left:8px;font-size:12px">{{g.dir_path}}</span>
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
                  <td colspan="7" style="text-align:center;color:var(--text-muted);padding:12px">加载中...</td>
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
                      <template v-if="r.matched_title">{{r.matched_title}}<span v-if="r.matched_id" style="color:var(--text-muted);margin-left:4px;font-size:11px">({{r.matched_id}})</span></template>
                      <span v-else style="color:var(--text-muted)">-</span>
                      <span v-if="r.parse_source==='ai'" style="display:inline-block;margin-left:4px;padding:1px 5px;border-radius:3px;font-size:11px;background:var(--info-soft);color:var(--info-text);border:1px solid var(--info-border)">AI</span>
                      <span v-else-if="r.parse_source==='guessit'" style="display:inline-block;margin-left:4px;padding:1px 5px;border-radius:3px;font-size:11px;background:var(--success-soft);color:var(--success-text);border:1px solid var(--success-border)">guessit</span>
                      <div v-if="r.status==='pending_manual' && r.error_msg && r.matched_title" style="font-size:11px;color:var(--warning-text);margin-top:2px">{{r.error_msg}}</div>
                    </td>
                    <td style="white-space:nowrap;font-size:12px;color:var(--text-muted)">{{$root.formatTime(r.updated_at || r.created_at)}}</td>
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
                      <span style="margin:0 12px;font-size:13px;color:var(--text-muted)">{{$root.expandedGroups[g.dir_path].page}} / {{Math.ceil($root.expandedGroups[g.dir_path].total/50)}}</span>
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
});
