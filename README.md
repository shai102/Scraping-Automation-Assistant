# 刮削助手 v2.0

媒体文件自动归档与刮削工具，Web 管理界面版本。

支持监控多个源文件夹，AI 自动识别文件名，通过 TMDB / Bangumi 匹配元数据，将媒体文件归档到目标目录并生成 NFO + 海报封面，供 Kodi / Jellyfin / Emby 直接读取。

## 功能概览

- **文件夹监控**：可添加多个监控目录，watchdog 实时检测新文件并自动入队。
- **AI 识别**：支持 OpenAI 兼容 API（SiliconFlow / DeepSeek / OpenAI 等）与本地 Ollama。
- **数据库匹配**：支持 TMDb 与 Bangumi（BGM），embedding 候选重排可选。
- **自动归档**：识别成功后自动将文件移动到目标目录，归档后清理空目录。
- **元数据刮削**：自动生成 NFO，下载 poster / fanart / still，写入演员、导演、类型等完整字段。
- **Web 管理界面**：Vue.js 3 单页应用，侧边栏导航，实时推送（WebSocket），无需刷新。
- **系统托盘**：后台运行，托盘图标右键可打开界面或退出。

## 项目结构

```text
main.py                         # 入口：启动 uvicorn + 托盘图标
server.py                       # FastAPI 应用、lifespan、静态文件挂载
api/
  routes/
    monitor.py                  # 监控文件夹 CRUD + 目录浏览 API
    records.py                  # 归档记录查询、批量删除/重试
    settings.py                 # 配置读写、TMDB/AI 连接测试、清除缓存
    ws.py                       # WebSocket 实时推送
monitor/
  watcher.py                    # watchdog 文件监控 + 自动处理流程
core/
  services/
    worker_context.py           # 无 GUI 版配置上下文（供 API 调用）
    matcher_service.py          # Ollama 解析、embedding 重排、候选判定
    naming_service.py           # 季集提取、标题复用、命名辅助
  workers/
    task_runner.py              # 预览/同步调度
    execution_runner.py         # 执行重命名/归档/刮削逻辑
  models/
    media_item.py               # MediaItem 数据模型（dataclass）
  mixins/
    config_mixin.py             # 配置加载/保存、并发参数
    list_mixin.py               # 文件列表增删与缓存清理
  ui/
    dialogs.py                  # 对话框辅助
    manual_match.py             # 手动匹配流程
ai/
  ollama_ai.py                  # OpenAI 兼容 API 解析与连通性测试
db/
  tmdb_api.py                   # TMDb/BGM 查询与元数据抓取
  database.py                   # SQLAlchemy 初始化、ORM 模型
utils/
  helpers.py                    # 通用工具（缓存、NFO/图片写入等）
web/
  dist/
    index.html                  # Vue.js SPA 主页面
    app.js                      # Vue 应用逻辑
    style.css                   # 样式
    vue.global.prod.js          # Vue 3 本地构建（离线可用）
tests/
  test_smoke.py                 # 冒烟测试
```

## 环境要求

- Python 3.10+

## 安装

```bash
pip install -r requirements.txt
```

或直接运行，自动安装后启动：

```bat
安装并启动.bat
```

## 运行

```bash
python main.py
```

启动后自动在浏览器打开 `http://127.0.0.1:8090`，同时在系统托盘显示图标。

也可以单独启动 Web 服务（不带托盘）：

```bash
python -m uvicorn server:app --host 0.0.0.0 --port 8090
```

## 打包为 EXE

```bat
一键打包.bat
```

生成 `dist/刮削助手.exe`，单文件可执行，包含所有静态资源。

## 配置说明

在 Web 界面的设置页中配置，所有设置保存在 `renamer_config.json`：

| 设置项 | 说明 |
|---|---|
| TMDb API Key | 从 themoviedb.org 获取 |
| BGM API Key | 可选，用于 Bangumi 查询 |
| Ollama API 地址 / 模型 | 本地大模型识别 |
| OpenAI 兼容 API Key / URL / 模型 | SiliconFlow / DeepSeek 等 |
| Temperature / Top-P | AI 推理参数，默认 0.20 / 0.85 |
| TV / 电影命名格式 | 支持 `{title}`, `{year}`, `{s:02d}`, `{e:02d}` 等占位符 |
| 预览/同步/执行线程数 | 各阶段并发数 |

## 日志

运行日志保存在程序目录下的 `media_renamer.log`。