# 媒体刮削助手发布流程记忆

适用范围：后续所有新会话，只要用户提到 GUI 版 / WEB 版发版、打 tag、上传发行版、替换 `.rar` 资产，都优先按本文件执行。

## 仓库映射

### GUI 版
- 本地路径：`C:\Users\Administrator\Desktop\项目源码\媒体刮削助手源码`
- GitHub 仓库：`https://github.com/shai102/media-renamer-ai`
- 打包入口：`一键打包.bat`
- 额外参考：`打包命令.txt`
- 发行资产命名：`vX.Y-windows-exe.rar`

### WEB 版
- 本地路径：`C:\Users\Administrator\Desktop\项目源码\自动监控刮削源码`
- GitHub 仓库：`https://github.com/shai102/Scraping-Automation-Assistant`
- 打包入口：无（WEB 版按源码 / Docker 流程发版）
- 发行资产命名：按实际发版内容决定；默认不再使用 `vX.Y-windows-exe.rar`

## 默认发布顺序

1. 进入对应仓库，先检查 `git status --short`，确认工作区状态。
2. 检查版本号是否已经同步到需要展示给用户的文件里。
3. 如有代码或版本修改，先提交 commit。
4. 推送当前分支到远端。
5. 创建对应版本 tag，tag 名直接用版本号，例如 `v3.3`。
6. 推送 tag 到远端。
7. 创建 GitHub Release。
8. 如需附带资产，上传对应源码包 / 说明文件到该 Release；不再执行 exe 打包或上传 exe 归档。
9. 最后再核对 Release 页面、tag、资产名、版本号是否一致。

## 用户偏好的执行方式

- 用户通常希望我直接把整套流程做完：`commit -> push -> tag -> push tag -> release -> upload asset`。
- 如果用户说“继续发布流程吧”，默认表示：
  - 不需要重新改代码
  - 不需要重新分析需求
  - 直接从发布链路剩余步骤继续
- 如果本地产物已经准备好，优先从 `git add` / `commit` / `push` 之后的断点续做，不重复打包。

## 发行说明风格

GitHub Release 正文默认写成简洁中文说明，结构优先如下：

1. 标题或开头点明版本号
2. `本次更新重点`
3. 2 到 5 条简洁 bullet，说明：
   - 识别逻辑修复 / 对齐
   - TMDb 匹配优化
   - 本地 OLLAMA / 在线 AI / Embedding 逻辑调整
   - 编码或乱码修复
   - 打包与稳定性修复

保持短、直白、可读，不写空话。

- WEB 版与 GUI 版即使修复点重合，Release 说明也不能直接复用同一份文案。
- WEB 版说明应更强调 Web 管理界面、自动监控、待手动确认、服务端识别策略调整。
- GUI 版说明应更强调桌面端批量整理、手动候选选择、窗口版本更新、README 维护记录。

## 版本与资产约定

- tag 名：直接使用 `vX.Y`
- Release 名：默认也使用 `vX.Y`
- 资产名：按实际发版内容命名；WEB 版默认不再使用 `vX.Y-windows-exe.rar`
- 用户若特别要求“替换仓库发行版的 -v2.8”，按用户指定版本覆盖对应 Release 资产。

## 关键安全注意事项

### WEB 版 `web/dist/index.html` 特别注意

- 不要用不安全的整文件文本替换去改版本号。
- 不要用可能破坏编码的粗暴重写方式处理这个文件。
- 如果只是改展示版本号，优先使用 `apply_patch` 做最小修改。
- 改完后必须检查 diff，确认只改了目标版本号或明确要改的几行。
- 如果页面出现白屏，先优先检查：
  - `web/dist/index.html` 是否被编码污染
  - HTML 标签是否被破坏
  - diff 是否出现大量无关字符变化

### 通用发布注意事项

- 不要回退用户已有改动，除非用户明确要求。
- 如果工作区里有无关改动，优先只提交本次发布相关文件。
- 打 tag 前确认当前 commit 就是要发出去的版本。
- 上传资产前确认压缩包来自最新构建，不要误传旧包。
- 新建 Release 后再次核对下载文件名是否准确。
- Release 资产上传并核对完成后，默认删除本地仓库根目录里的对应 `.rar` 文件。

## 已验证过的近期发布结果

- GUI：`v3.0`
  - Release：`https://github.com/shai102/media-renamer-ai/releases/tag/v3.0`
  - 资产：`v3.0-windows-exe.rar`

- WEB：`v3.3`
  - Release：`https://github.com/shai102/Scraping-Automation-Assistant/releases/tag/v3.3`
  - 资产：按实际发版内容命名

## 已知事故记录

### WEB 版版本号修改曾导致白屏

- 问题文件：`web/dist/index.html`
- 原因：使用了错误的整文件替换/编码写回方式，导致 HTML 乱码和结构损坏。
- 修复方式：
  - 先从上一个正常版本恢复该文件
  - 再只做最小版本号补丁
  - 检查 diff 确认只改目标行
- 后续所有会话发布 WEB 版时，都要规避这个坑。

## 新会话默认行为

如果用户提到以下任一需求，优先读取本记忆并按此执行：

- “推送到仓库”
- “打 tag”
- “创建 Release”
- “上传发行资产”
- “继续发布流程”
- “GUI 版 / WEB 版发版”

默认先核对仓库路径、版本号、资产名，再进入发布步骤。

