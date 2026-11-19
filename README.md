# 源到达SLA

> 遵守 `ops/底座生成前必读_效率与技术栈.md`。FastAPI + Vue 3 + SQLite，交付须 docker compose 一键起；库用 SQLite，不挂外置 Postgres。  
> **禁止同构换皮**：本仓主链路、表结构、接口与页面均独立，不得与其它仓共用同一套 `/api/items` 壳。

## 骨架一句话

多源到达时钟对照 SLA 分级，超限则冻结或降级输出，补齐后恢复并对账

## 基本信息

| 项 | 值 |
|----|-----|
| 产品名（首页） | 源到达SLA |
| 目录 | `06-lagboard` |
| 领域建议 | 数据采集、爬虫与 RPA |
| 接口 | http://127.0.0.1:10819 |
| 库 | `data/freeze/app.db`（禁写仓区） |
| 账号 | admin / 123456 |

## 冻结禁写（已封档）

冻结是写路径闸门：`违约` 源在冻结态下不得新增到达，`POST /api/arrivals` 同事务
锁行拒插并记审计，返回 `409`。全仓只走**甲路径**，不采用乙路径，两径不混用。
口径与验收顺序见 `ops/冻结禁写说明.md`（封档哈希见 `ops/冻结禁写说明.sha256`）。

五处读面同态：`/api/sources`、`/api/arrivals`、`/api/freeze/audit`、
`/api/freeze/watermarks`、`/api/freeze/tasks`；页面“到达登记（时限台）”为入库入口，
“冻结台账”为审计附页 / 水位页 / 任务列表，不另起禁写台。

## 为何能出多题

源时钟 vs 摄入时钟、分级处罚、降级可读不可写、补齐解冻

## 避开

别做成纯监控大盘；须有写路径与状态机

## 互斥切口候选

- 超SLA冻结 vs 降级只读
- 补齐整源解冻 vs 按业务日窗口解冻
- 只认源时钟 vs 双时钟取严

## 启动（一键）

`powershell
cd projects/06-lagboard
docker compose up -d --build
`

浏览器：http://127.0.0.1:10819/  
健康检查：GET http://127.0.0.1:10819/api/health

本机开发（可选）：

`powershell
python -m pip install -r requirements.txt
python -m uvicorn app:app --app-dir backend --host 127.0.0.1 --port 10603
python -m pytest -q
`
