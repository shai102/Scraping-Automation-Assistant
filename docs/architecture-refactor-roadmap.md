# 模块拆分路线图

这份文档的目标不是“为了目录好看而重构”，而是为了让后续继续加功能时，代码边界更稳定、回归风险更低、定位问题更快。

## 当前状态

截至 `v4.8`：

- 第一阶段和大部分第二阶段拆分已经完成
- `watcher / delete_sync / metadata_refresh / logs / records / settings / symlinks / tmdb search / tg notify / cache / title parsing / frontend settings pages` 都已经拆成门面 + 子模块
- 当前文档以下内容仍保留其价值：
  - 解释为什么当时要拆
  - 记录拆分边界设计思路
  - 作为后续继续细化模块时的参考

现在这份文档更适合当作：

- `已完成重构的设计记录`
- `剩余可继续细拆模块的参考图`

当前结论：

- 现有项目已经有基本分层，不需要推倒重来
- 需要做的是按职责增量拆分
- 优先拆“大文件多职责”，而不是一次性全仓库重排

## 当前主要痛点

### 1. 单文件职责过多

以下文件已经明显承担了过多职责：

- `monitor/watcher.py`
  - 文件监控
  - 事件去抖
  - 删除同步
  - 元数据巡检
  - 自动修复
  - 批量扫描
- `utils/helpers.py`
  - 代理处理
  - 文本清洗
  - API 缓存
  - NFO/图片写入
  - 元数据完整性判断
  - 路径规则解析
- `core/workers/task_runner.py`
  - 识别流程
  - 目录级缓存
  - 候选匹配调度
  - 命名与目标路径生成
- `core/services/worker_context.py`
  - 配置读取
  - 运行时上下文
  - 部分业务辅助逻辑
- `api/routes/logs.py`
  - API
  - 日志读取
  - 日志解析
  - 中文注释规则
- `api/routes/records.py`
  - 记录查询
  - 手动匹配
  - 恢复/重跑
  - 批量操作

### 2. Web 前端仍是单文件应用思路

- `web/dist/app.js` 继续增长后，页面之间会越来越难维护
- 当前还能支撑，但后面再增加独立功能页时会更吃力

### 3. 业务边界不够清晰

目前不少逻辑是“能跑就放进去”，短期效率高，但长期会出现：

- 新功能不知道该放哪
- 改日志时容易碰到业务逻辑
- 改监控逻辑时容易顺带影响删除同步或元数据巡检
- 工具函数持续堆积到 `helpers.py`

## 重构原则

### 1. 保持功能不变，先拆边界

前几轮拆分只做职责迁移，不改用户可见行为。

### 2. 小步迁移，不做一次性重构

每次只拆一个主题，确保：

- 可以单独验证
- 可以单独回滚
- 不会把正在运行的版本拖进高风险状态

### 3. 先把“调用入口”保留住

例如：

- `FolderWatcher` 先保留
- `read_logs()` 路由先保留
- `process_task()` 入口先保留

内部再逐步转发到新模块。这样外部调用方不用同时大改。

### 4. 优先拆“高变更区域”

先拆最近一直在加功能、未来还会持续改的部分：

- 日志
- 元数据巡检
- 删除同步
- 记录操作

## 目标结构

这是建议目标，不要求一次到位。

```text
api/
  routes/
    logs.py
    monitor.py
    records.py
    settings.py
    symlinks.py
    ws.py

core/
  logging/
    reader.py
    annotation.py
    filters.py
    paths.py

  monitoring/
    watcher.py
    file_events.py
    full_scan.py
    delete_sync.py
    metadata_patrol.py
    repair.py

  recognition/
    task_runner.py
    query_builder.py
    candidate_resolution.py
    existing_library.py

  metadata/
    refresh_service.py
    completeness.py
    sidecar_service.py

  services/
    matcher_service.py
    naming_service.py
    worker_context.py

  records/
    query_service.py
    manual_match_service.py
    recovery_service.py
    batch_ops.py

utils/
  proxy.py
  cache.py
  text_normalize.py
  path_rules.py
```

说明：

- `api/routes/*.py` 尽量只保留接口层
- 业务逻辑逐步沉到 `core/*`
- `utils` 保持真正的通用工具，不再承接业务流程

## 第一阶段拆分清单

这是最值得先做的一轮，风险低，收益高。

### A. 先拆 `utils/helpers.py`

目标：把“通用工具”和“业务规则”分开。

建议拆成：

- `utils/proxy.py`
  - `normalize_proxy_url`
  - `apply_proxy_environment`
  - `request_proxy_kwargs`
  - `proxy_summary`
- `utils/cache.py`
  - `load_cache`
  - `save_cache`
  - `invalidate_cache_prefix`
  - `cached_request`
  - `flush_api_cache`
- `utils/text_normalize.py`
  - 标题清洗
  - 噪音 token 处理
  - 平台前缀剔除
  - 查询标题规范化
- `utils/path_rules.py`
  - `extract_db_id_from_path`
  - `build_existing_library_target`
  - 文件名/path 相关辅助
- `core/metadata/completeness.py`
  - `metadata_missing_fields`
  - `metadata_is_incomplete`
- `core/metadata/sidecar_service.py`
  - `save_image`
  - `write_nfo`

拆分标准：

- 通用能力放 `utils`
- 明显属于元数据业务的放 `core/metadata`

### B. 拆 `monitor/watcher.py`

目标：保留 `FolderWatcher` 作为入口，但把内部能力拆出去。

建议拆成：

- `core/monitoring/file_events.py`
  - watchdog 事件转统一任务
  - 去抖、排队、目录锁
- `core/monitoring/full_scan.py`
  - 启动时全盘扫描
  - 手动触发扫描
- `core/monitoring/delete_sync.py`
  - 删除同步
  - 两跳删除
  - 空目录清理
- `core/monitoring/metadata_patrol.py`
  - 定时巡检
  - 不完整记录查找
  - 刷新循环
- `core/monitoring/repair.py`
  - 缺失软链接产物自动修复
  - 缺失刮削产物自动修复

第一阶段不要求把 `watcher.py` 变很小，只要做到：

- 监控主类负责协调
- 具体动作移到子模块

### C. 拆 `api/routes/logs.py`

目标：让日志 API 和日志解析规则分开。

建议拆成：

- `api/routes/logs.py`
  - 参数接收
  - 返回 JSON
- `core/logging/reader.py`
  - 读文件
  - tail
  - 日期列表
  - kind 路径解析
- `core/logging/annotation.py`
  - `_analyze_log_message`
  - 各类日志注释规则
- `core/logging/filters.py`
  - `scrape/app/metadata` 过滤逻辑

这一步很适合先做，因为最近日志功能还在持续迭代。

## 第二阶段拆分清单

### A. 拆 `api/routes/records.py`

按职责分：

- 记录列表查询
- 分组视图
- 手动匹配
- 恢复/重跑
- 批量删除/批量操作

建议沉到：

- `core/records/query_service.py`
- `core/records/manual_match_service.py`
- `core/records/recovery_service.py`
- `core/records/batch_ops.py`

### B. 拆 `core/workers/task_runner.py`

建议拆成：

- 查询标题构建
- 候选解析与最终命中
- 目录缓存
- 已就位媒体库路径复用

这样以后你再加：

- 新的数据源
- 新的候选规则
- 新的目录保留策略

就不用一直堆在一个文件里。

## 第三阶段拆分清单

### 前端逐步模块化

当前不建议直接上大前端重构，但可以先做最小拆分：

- 把日志页逻辑单独抽出来
- 把记录页逻辑单独抽出来
- 把设置页逻辑单独抽出来

即使还保持原生 Vue + 本地 dist 方式，也可以先按功能拆 JS 文件。

例如：

```text
web/dist/
  app.js
  pages/
    logs.js
    records.js
    settings.js
  components/
    log-table.js
    folder-form.js
```

这样做的目的不是追框架，而是降低后续继续加页面时的复杂度。

## 不建议现在做的事

### 1. 不建议一次性全仓库改包结构

风险太高，收益不成比例。

### 2. 不建议现在就引入复杂 DI 框架

这个项目体量还没到需要那一步，先把职责拆清楚就够了。

### 3. 不建议为拆分而拆分

如果某个文件稳定、变更少、边界清楚，就先别动。

## 推荐执行顺序

推荐我们后面按这个顺序推进：

1. `helpers.py` 拆分
2. `logs.py` 拆分
3. `watcher.py` 拆分
4. `records.py` 拆分
5. `task_runner.py` 拆分
6. 前端按页面拆分

## 每一步的完成标准

每轮拆分都建议满足这几个标准：

- 对外接口不变
- 运行行为不变
- 日志输出不变或仅做增强
- 容器可正常启动
- 至少完成一次核心链路验证
  - 目录监控
  - 手动识别
  - 删除同步
  - 日志查看

## 建议的下一步

最适合先做的是：

### 方案 A：先拆 `helpers.py`

优点：

- 风险最低
- 收益明显
- 后续别的模块拆分都会更轻松

### 方案 B：先拆日志模块

优点：

- 近期变更最频繁
- 已经有 `app / scrape / metadata` 三类日志
- 当前正好是一个合适的边界点

如果没有特别偏好，默认建议先走：

`helpers.py` -> `日志模块` -> `watcher.py`
