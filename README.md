# Data Export Service

平台统一异步数据导出服务。它只通过共享的 `platform.export.v1.ExportProviderService` 读取业务服务的数据，不跨服务查询数据库；导出结果写入 S3 兼容对象存储，并通过短时效预签名 URL 交付。

## 能力

- CSV、JSONL、XLSX 流式编码，按批读取、常量级内存占用。
- 任务状态：`queued -> running -> succeeded|failed|canceled`，结果到期后清理对象并进入 `expired`；`expired` 元数据保留 365 天后按批删除，删除同时校验版本，避免清理并发更新后的任务。
- 租户隔离、幂等创建、版本号乐观锁取消和重试。
- 最大行数、最大字节数、任务超时、进度上报、SHA-256 校验。
- PostgreSQL、KingbaseES、MySQL 独立迁移；默认数据库 `platform`、Schema `data_export`。

生产环境应在元数据保留期结束前通过 CDC 或受控导出归档需要长期保存的审计资料；服务不会把历史记录复制到其他服务的 OLTP 数据库。
- 事务 Outbox + NATS JetStream durable consumer，失败重投和死信。
- JWT/JWKS、PSK、POST+JSON 前端接口和独立 gRPC 端口。
- MinIO/S3 流式 multipart 上传；失败或取消时删除不完整结果。
- 成功、失败、取消、重试和过期状态均通过事务 Outbox 发布；Cron 分批清理到期对象。
- 每个 Provider 批次检查数据库运行状态，跨副本取消无需等待整个导出结束；事件携带持久化后的准确版本号。

## 接口

前端接口统一返回 `{"code":0,"message":"success","body":...,"request_id":"..."}`：

- `POST /api/v1/exports/create`
- `POST /api/v1/exports/get`
- `POST /api/v1/exports/list`
- `POST /api/v1/exports/cancel`
- `POST /api/v1/exports/retry`
- `POST /api/v1/exports/download`
- `GET|POST /live`、`GET|POST /ready`

服务间接口来自中央模块 `github.com/lihongjie0209/platform-protos@v0.16.0`：

- `platform.export.v1.ExportService`：任务管理与文件交付。
- `platform.export.v1.ExportProviderService`：业务服务实现数据集描述与流式行读取。

新增业务数据集不需要 data-export-service 生成领域专用 client；业务服务实现稳定的通用 Provider 协议并将其 gRPC upstream 注册为 `provider_service` 即可。

## 数据源约束

Provider 必须：

- 验证租户和调用者权限，不能信任请求中的 `tenant_id`。
- 使用稳定快照或游标分页，禁止 offset 扫描大表。
- 每批最多返回请求的 `batch_size`，及时响应 Context 取消。
- 只返回声明的列；敏感列必须在源服务脱敏或拒绝导出。
- 不把全量结果加载到内存。

## 配置

配置优先级为命令行 profile > 环境变量 > `config-{profile}.yaml` > `config.yaml` > 默认值。环境变量使用 `APP_` 前缀，例如：

```bash
export APP_ENV=production
export APP_DATABASE_DSN='postgres://user:password@postgres:5432/platform?sslmode=require&search_path=data_export'
export APP_AUTH_JWKS_URL='https://identity.example.com/.well-known/jwks.json'
export APP_OBJECT_STORAGE_ENDPOINT='s3.example.com'
export APP_OBJECT_STORAGE_ACCESS_KEY='...'
export APP_OBJECT_STORAGE_SECRET_KEY='...'
export APP_EVENT_BUS_URLS='nats://nats:4222'
```

生产环境由密钥管理系统注入数据库、Redis、NATS、JWKS、S3 和 TLS 配置，配置文件不保存真实密钥。

## 开发与验证

```bash
make test          # 本机单元测试
make test-race
make lint
make swagger
make build         # 注入 version、Git commit、build time
```

根据项目策略，本机不运行 Testcontainers、Compose 集成测试或系统测试。CI 执行：

```bash
go test -tags=integration -count=1 -timeout=15m ./integration/...
```

需要完整依赖时，`make dev-up` 启动 PostgreSQL、MySQL、Redis、NATS、MinIO 和服务，并自动迁移；开发环境不包含 Prometheus、Grafana、Jaeger 或 OTel Collector。

## 迁移与审计

所有表包含 `version`、`created_at`、`updated_at`、`created_by`、`updated_by`。时间使用带时区类型，应用数据库会话默认 `Asia/Shanghai`。每个服务使用独立迁移记录表 `data_export_schema_migrations`，因此共享数据库不会互相污染迁移版本。

```bash
make migrate-up
make migrate-down
```

启动时设置 `APP_MIGRATION_AUTO_UP=true` 可在监听 HTTP/gRPC 前自动执行迁移。生产默认不授予建 Schema 权限，`data_export` Schema 应由平台预先创建。
