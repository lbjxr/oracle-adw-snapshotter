# oracle-adw-snapshotter

一个偏框架化的 Python 项目，用于从 Oracle Autonomous Data Warehouse（ADW）按配置执行查询或读取源表，并把每次采集结果以统一快照结构写入归档表。

这个项目的目标不是写死某一条业务 SQL，而是先把一套可扩展的采集骨架搭好：
- 连接层
- 配置层
- 任务层
- 查询层
- 写入层
- 查看 / 导出层
- 调度层

## 当前能力

- 使用 `oracledb` 连接 Oracle ADW
- 支持 `.env` 管理连接参数
- 支持 YAML 管理多个采集任务
- 支持两类任务模式
  - `query`：直接执行 SQL
  - `table`：指定源表并拼接可选过滤条件
- 支持 `test-connection` 连接预检
- 支持 `run` 执行采集任务
- 支持 `view` 查看最新快照结果
- 支持 `export` 导出 JSON / CSV
- 提供 `scheduler` 随机调度入口，便于后续自动化扩展

## 项目结构

```text
oracle-adw-snapshotter/
├── .env.example
├── .gitignore
├── config/
│   └── tasks.example.yaml
├── pyproject.toml
├── README.md
├── sql/
│   └── init_snapshot_objects.sql
├── src/
│   └── oracle_adw_snapshotter/
│       ├── cli.py
│       ├── config/
│       ├── connectors/
│       ├── jobs/
│       ├── models/
│       ├── services/
│       └── storage/
└── tests/
```

## 安装

```bash
python3 -m venv .venv
. .venv/bin/activate
pip install -e .
```

开发测试：

```bash
pip install -e .[dev]
```

## 配置

先复制环境变量模板：

```bash
cp .env.example .env
```

再复制任务模板：

```bash
cp config/tasks.example.yaml config/tasks.yaml
```

### `.env` 示例

```dotenv
ORACLE_USER=ADMIN
ORACLE_PASSWORD=replace_with_real_password
ORACLE_DSN=myadw_high
ORACLE_CONNECTION_MODE=thin
ORACLE_WALLET_DIR=./secrets/oracle-wallet/Wallet_myadw
ORACLE_WALLET_PASSWORD=
ORACLE_CONFIG_DIR=
ORACLE_LIB_DIR=

SNAPSHOT_APP_NAME=oracle-adw-snapshotter
SNAPSHOT_CONFIG_PATH=config/tasks.yaml
SNAPSHOT_DEFAULT_OWNER=
SNAPSHOT_FETCH_SIZE=1000
```

### 任务配置说明

`config/tasks.example.yaml` 提供的是公开示例。你自己的真实任务请写到：

- `config/tasks.yaml`

这个文件默认不会提交到 git，避免把真实表名、真实业务 SQL、真实 schema 带出去。

如果要启用随机调度，可以在 `config/tasks.yaml` 里加入：

```yaml
scheduler:
  enabled: true
  schedule_name: daily-random-50
  timezone: Asia/Shanghai
  runs_per_day: 50
  parameter_min: 10
  parameter_max: 100
  read_source_table: SNAPSHOT_JOB_RUNS
  read_limit: 3
  poll_interval_seconds: 30
```

## 连接约定

常见用法：

- `thin + wallet + TNS alias`
- 用户通常是 `ADMIN`
- wallet 放在项目内 `secrets/` 目录下

例如：

```dotenv
ORACLE_CONNECTION_MODE=thin
ORACLE_WALLET_DIR=./secrets/oracle-wallet/Wallet_myadw
ORACLE_DSN=myadw_high
```

## 初始化数据库对象

先在 ADW 中执行：

```sql
@sql/init_snapshot_objects.sql
```

它会创建快照写入和调度日志所需的基础对象。

## 常用命令

### 1. 连接预检

```bash
python -m oracle_adw_snapshotter.cli test-connection --env-file .env --config config/tasks.yaml
```

### 2. 跑一次采集

```bash
python -m oracle_adw_snapshotter.cli run --env-file .env --config config/tasks.yaml
```

### 3. 查看结果

```bash
python -m oracle_adw_snapshotter.cli view --env-file .env --config config/tasks.yaml --table SNAP_DEMO_JOB --limit 5
```

按 job 查看最近几批：

```bash
python -m oracle_adw_snapshotter.cli view --env-file .env --config config/tasks.yaml --job demo_job --latest-runs 2 --rows-per-run 5
```

### 4. 导出结果

导出 JSON：

```bash
python -m oracle_adw_snapshotter.cli export --env-file .env --config config/tasks.yaml --table SNAP_DEMO_JOB --format json --output tmp/out.json
```

导出 CSV：

```bash
python -m oracle_adw_snapshotter.cli export --env-file .env --config config/tasks.yaml --table SNAP_DEMO_JOB --format csv --output tmp/out.csv
```

### 5. 启动 scheduler

```bash
python -m oracle_adw_snapshotter.cli scheduler --env-file .env --config config/tasks.yaml
```

只跑一次已到点任务：

```bash
python -m oracle_adw_snapshotter.cli scheduler --env-file .env --config config/tasks.yaml --once
```

### 6. 使用 systemd 持续运行 scheduler

```ini
[Unit]
Description=Oracle ADW Snapshotter Scheduler
After=network-online.target
Wants=network-online.target

[Service]
Type=simple
WorkingDirectory=/path/to/oracle-adw-snapshotter
ExecStart=/path/to/oracle-adw-snapshotter/.venv/bin/python -m oracle_adw_snapshotter.cli scheduler --env-file .env --config config/tasks.yaml
Restart=always
RestartSec=5
User=root

[Install]
WantedBy=multi-user.target
```

示例启用命令：

```bash
sudo systemctl daemon-reload
sudo systemctl enable --now oracle-adw-snapshotter-scheduler.service
sudo systemctl --no-pager -l status oracle-adw-snapshotter-scheduler.service
```

## 测试

```bash
pytest -q
```

## 敏感数据说明

这个仓库默认不应提交以下内容：

- `.env`
- `config/tasks.yaml`
- `secrets/`
- wallet 证书 / 密钥文件
- `tmp/` 导出结果
- 本地虚拟环境与缓存目录

如果你要公开发布，请务必继续保持：
- 示例配置只放 `.env.example` 和 `config/tasks.example.yaml`
- 真实库连接、真实任务 SQL、真实业务表名不要进仓

## 适合继续扩展的方向

- 结构化列映射而不只是 JSON payload
- 更完整的 scheduler 策略
- 失败重试 / 告警
- systemd / cron 部署脚本
- 面向多个 schema / 多库的配置组织
- 任务运行统计和 dashboard
