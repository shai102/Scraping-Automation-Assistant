window.scraperAppPageComponents = Object.assign(window.scraperAppPageComponents || {}, {
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
                  <div v-if="r.error_msg" style="color:var(--danger-text);font-size:12px;margin-top:2px">⚠ {{r.error_msg}}</div>
                </td>
                <td class="cell-path">{{r.link_path || '-'}}</td>
                <td style="white-space:nowrap;font-size:12px;color:var(--text-muted)">{{$root.formatTime(r.created_at)}}</td>
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
              <span style="color:var(--text-faint);margin:0 2px">/</span>
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
                    <span style="color:var(--text-muted);margin-left:8px;font-size:12px">{{g.dir_path}}</span>
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
                  <td colspan="6" style="text-align:center;color:var(--text-muted);padding:12px">加载中...</td>
                </tr>
                <template v-else>
                  <tr class="group-record-row" v-for="r in $root.symlinkExpandedGroups[g.dir_path].records" :key="r.id">
                    <td><input type="checkbox" :value="r.id" v-model="$root.symlinkSelectedIds"></td>
                    <td colspan="2" class="cell-path" style="padding-left:40px">
                      <div>{{r.original_path}}</div>
                      <div v-if="r.error_msg" style="color:var(--danger-text);font-size:12px;margin-top:2px">⚠ {{r.error_msg}}</div>
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
                      <span style="margin:0 12px;font-size:13px;color:var(--text-muted)">{{$root.symlinkExpandedGroups[g.dir_path].page}} / {{Math.ceil($root.symlinkExpandedGroups[g.dir_path].total/50)}}</span>
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
});
