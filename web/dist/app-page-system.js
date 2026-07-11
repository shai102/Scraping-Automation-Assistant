window.scraperAppPageComponents = Object.assign(window.scraperAppPageComponents || {}, {
  'system-page': {
    template: `
      <section>
        <div class="page-header"><h1>运行状态</h1><button class="btn btn-primary btn-sm" @click="$root.loadSystemStatus">刷新</button></div>
        <div class="card" v-if="$root.systemStatus">
          <div class="form-row">
            <div class="form-group"><label>服务</label><strong>{{$root.systemStatus.ok?'正常':'异常'}}</strong></div>
            <div class="form-group"><label>Watcher</label><strong>{{$root.systemStatus.watcher}}</strong></div>
            <div class="form-group"><label>监控目录</label><strong>{{$root.systemStatus.active_folders}}</strong></div>
            <div class="form-group"><label>WebSocket</label><strong>{{$root.systemStatus.websocket_clients}}</strong></div>
          </div>
        </div>
        <div class="card" v-if="$root.systemStatus">
          <h2>任务队列</h2>
          <table class="table"><tbody>
            <tr v-for="(value,key) in $root.systemStatus.tasks" :key="'task-'+key"><td>{{key}}</td><td>{{value}}</td></tr>
          </tbody></table>
        </div>
        <div class="card" v-if="$root.systemStatus">
          <h2>刮削与归档</h2>
          <div class="form-row">
            <div class="form-group"><label>处理中记录</label><strong>{{$root.systemStatus.records.processing}}</strong></div>
            <div class="form-group"><label>待手动</label><strong>{{$root.systemStatus.records.pending_manual}}</strong></div>
            <div class="form-group"><label>失败记录</label><strong>{{$root.systemStatus.records.failed}}</strong></div>
            <div class="form-group"><label>未完成归档</label><strong>{{$root.systemStatus.archive_operations.running}}</strong></div>
          </div>
          <p>最后轮询：{{$root.formatTime($root.systemStatus.last_poll_at)}}</p>
          <p>最后成功：{{$root.formatTime($root.systemStatus.last_success_at)}}</p>
          <p>最后维护：{{$root.formatTime($root.systemStatus.last_maintenance_at)}}</p>
        </div>
        <div class="empty" v-if="$root.systemStatusLoading">正在读取运行状态...</div>
      </section>
    `,
  },
});
