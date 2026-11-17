## 项目说明

- 本仓库是公开的 Go 库 `github.com/ZhcChen/cc-snowflake-id-go`：可复用的雪花 ID。
  - `generator`：进程内雪花 ID 生成、解码与时钟回退保护。
  - `lease`：PostgreSQL 租约、后台续约运行时、遥测与状态快照，供多实例安全发号。
  - 根包 `idgen` 只有包文档，不导出兼容 facade；调用方直接依赖子包。
- 库内不得出现任何具体公司、项目或业务名称，也不得依赖使用方的代码。

## 文档位置

- 设计、计划、规格、复核、运行手册全部放 Obsidian `我的/项目/cc-snowflake-id-go/`（`README.md` 为索引）。仓库只有一份面向 GitHub 访客的简短 `README.md`（定位、安装、包一览，不写设计与用法细节），不新增 docs；其余只保留本文件、`doc.go` 包文档、代码注释与 `examples/`（对外说明以 pkg.go.dev 显示的包文档为准）。
- 代码注释规范见 Obsidian《代码注释规范》；测试入口与 CI 口径见《测试与 CI》。
- 修改公共 API、序列化行为、租约或时钟语义前，先读相关文档并同步更新；设计与代码不一致时先讨论再改。

## 目录

- `generator/`、`lease/`：两个子包。
- `examples/`：可运行示例（`generator-basic`、`lease-runtime`、`production-runtime`），示例里的 `schema.sql` 是租约表参考结构。
- `scripts/test/`：`unit.ps1`、`race.ps1`、`integration.ps1`，CI 调用。
- `.gitattributes` 固定文本行尾，保证 Go module zip 跨平台可复现，不要删除。

## 代码规则

- 注释、错误说明使用简体中文；标识符使用英文。导出标识符必须有文档注释。
- 修改公共 API、序列化行为、PostgreSQL 租约、时钟或过期语义、generation fence 时，写明兼容性与边界，并按风险组合单元、race 与 PostgreSQL 集成测试。
- 公共 API 遵循语义化版本：v0 阶段允许不兼容变更，但须在提交说明中写明。
- 不为无关目的重构；不修改 Go module cache 或调用方项目来规避本仓库问题。

## Git

- 任务开始先执行 `git status --short --branch`，保留已有改动。
- 默认分支 `main`；较大改动用 `dev/<主题>` 或 `fix/<主题>` 分支，完成后合入 `main`。
- 任务完成并通过相应检查后，精确暂存相关路径并提交；提交说明首行为“类型: 中文说明”，类型取 feat、fix、docs、chore、refactor、test、perf、build、ci、style、revert。
- 对话结束前，将已完成的提交推送到当前分支对应远端。
- 不使用 `--no-verify`，不做 `reset --hard`、`clean`、强制推送。

## 开发与验证

- 本机只做编辑、格式化和纯文本检查；不在本机执行 `go build`、`go test`、`go mod tidy`。
- 测试在 GitHub Actions 运行（`.github/workflows/test.yml`：unit、race、integration（PostgreSQL 服务容器））；推送后用 `gh run watch` / `gh run view --log-failed` 查看结果。
- 汇报验证结果时区分通过、失败、跳过、未覆盖；没跑过的写“未验证”。

## 发布

- 用 Git 标签发布（`vX.Y.Z`），打标签前 CI 必须全绿；标签一经推送不得删除或移动，有问题发新版本。
- 发布的提交应在 `main` 上。
