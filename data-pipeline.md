# data-pipeline.md — 通用数据工程模式

> 五条铁律：**增量回填、去重、自动备份、可续跑、密钥不入库。**
> 目标：任何时刻断电/报错/换机器，数据管线都能从断点继续，且不会产生重复或损坏数据。

---

## 1. 五条铁律

### 1.1 增量回填（Incremental Backfill）

- 每次运行只拉「上次成功之后」的新数据，**绝不每次全量重拉**。
- 用**水位线（watermark）**记录上次成功位置：时间戳或自增 ID，取「严格大于」避免漏行。
- 回填时按水位线从小到大、分块拉取（分页/分批），单块失败只重试该块。
- 重试上限：单块连续失败 3 次 → 跳过并记录，管线继续，事后人工补该块。

### 1.2 去重（Deduplication）

- 主键唯一：`(源, 实体, 时间)` 或业务天然主键；写入前 `DELETE 重叠区间 + INSERT` 或 `INSERT ... ON CONFLICT DO UPDATE`。
- 规则：**同键后到覆盖先到**（数据有修正时以最新为准）；同键不同源 → 不合并，分列存并记录差异。
- 每次写入后跑一次键计数校验：唯一键数量 == 行数，不一致即告警停管线。

### 1.3 自动备份（Auto Backup）

- 每次「破坏性操作」（批量删除、表重建、迁移）前，自动备份受影响数据。
- 备份命名带时间戳与版本；备份本身也定期异地/异盘存放。
- 备份要能**原样恢复**：恢复脚本和备份文件放一起；每次备份后立即做一次恢复演练（哪怕只验证行数与校验和）。

### 1.4 可续跑（Resumable）

- 状态持久化：水位线、已完成的批次号写进状态表/文件，**每批成功即落盘**，而不是全部完成后才写。
- 所有步骤幂等：同一批数据重复跑，结果一致且不重复写入。
- 断点恢复 = 读状态 → 从水位线继续；绝不「从头再来」。

### 1.5 密钥不入库（No Secrets in Repo）

- 密钥/口令只存环境变量或专用密钥管理服务，**绝不写进代码、配置、日志、数据文件、Git 仓库**。
- 提交前用预提交钩子扫描密钥特征；一旦误提交，视为已泄露：立即轮换，而不是「删掉提交」。
- 日志脱敏：打印连接串、请求头时先抹掉密钥字段。
- 一次性调试密钥用完即删；分享代码给别人前先过扫描。

---

## 2. 管线结构约定

```text
raw/            # 原始数据（只增不改，永远可回放）
staging/        # 清洗中数据（可重建，坏了就删掉重跑）
clean/          # 干净数据（主键唯一、口径统一，下游只用这里）
state/          # 水位线与批次状态（可续跑的关键）
backup/         # 自动备份
docs/data-notes.md   # 口径说明（字段含义、差异、坑）
```

- **raw 不可变**：任何清洗错误都可以从 raw 重放恢复。
- **下游只读 clean**：研究/验证环节一律从 clean 取数，禁止直接摸 raw 与 staging。
- 口径变了 → clean 表版本号 +1（如 `clean_v2`），并在 `data-notes.md` 记录差异；旧版本数据保留可回溯。

---

## 3. 日志与校验规范

- 每个批次记录一行结构化日志：批次号、起始/结束水位线、行数、耗时、状态（成功/跳过/失败）。
- 每天（或每 N 批）自动校验：
  - 键唯一性（`COUNT(*) == COUNT(DISTINCT key)`）；
  - 水位线单调递增；
  - 行数与前次快照对比，异常波动（如暴增/暴减 30%）即告警。
- 校验失败 → 管线自动停止，通知人（或父代理）介入，不带着脏数据继续跑。

---

## 4. 最小 Python 示例（SQLite，仅标准库）

```python
"""
最小可续跑增量管线：拉数据 → 去重入库 → 自动备份 → 断点续跑。
仅演示模式，生产环境请换 PostgreSQL 等，密钥走环境变量。
"""
import json
import os
import shutil
import sqlite3
import time
from datetime import datetime

DB = "clean.db"
STATE = "state/watermark.json"
BACKUP_DIR = "backup"

def fetch_batch(watermark, batch_size=1000):
    """占位：真实场景替换为你的数据源 API。密钥从环境变量读，绝不硬编码。"""
    # key = os.environ["API_KEY"]
    # ...调用上游，取 id > watermark 的 batch_size 行...
    return []  # 返回 [(id, entity, ts, value), ...]

def backup_db():
    os.makedirs(BACKUP_DIR, exist_ok=True)
    dst = os.path.join(BACKUP_DIR, f"clean-{int(time.time())}.db")
    if os.path.exists(DB):
        shutil.copy2(DB, dst)
    return dst

def load_watermark():
    if os.path.exists(STATE):
        with open(STATE) as f:
            return json.load(f).get("last_id", 0)
    return 0

def save_watermark(last_id):
    os.makedirs(os.path.dirname(STATE), exist_ok=True)
    with open(STATE, "w") as f:
        json.dump({"last_id": last_id, "updated_at": datetime.now().isoformat()}, f)

def upsert(conn, rows):
    """去重 + 可续跑：同键覆盖（后到为准），每批独立提交。"""
    for r in rows:
        conn.execute(
            "INSERT INTO clean(entity, ts, value) VALUES(?,?,?) "
            "ON CONFLICT(entity, ts) DO UPDATE SET value=excluded.value",
            r[1:],  # 跳过自增源 id，保留业务主键 (entity, ts)
        )
    conn.commit()  # 每批提交 = 断点粒度

def check_uniqueness(conn):
    """键唯一性校验：不一致说明去重规则被破坏，必须停机排查。"""
    n_rows, n_keys = conn.execute(
        "SELECT COUNT(*), COUNT(DISTINCT entity||'|'||ts) FROM clean").fetchone()
    assert n_rows == n_keys, f"重复键！行数 {n_rows} != 唯一键 {n_keys}"

def main():
    conn = sqlite3.connect(DB)
    conn.execute("""CREATE TABLE IF NOT EXISTS clean(
        entity TEXT, ts TEXT, value REAL,
        PRIMARY KEY(entity, ts))""")

    watermark = load_watermark()          # 1) 水位线：从上次成功处继续
    while True:
        rows = fetch_batch(watermark)     # 2) 增量：只拉新数据
        if not rows:
            break
        backup_db()                       # 3) 写入前自动备份（可节流为每日一次）
        upsert(conn, rows)                # 4) 去重入库，批批提交
        watermark = max(r[0] for r in rows)
        save_watermark(watermark)         # 5) 成功即落盘水位线 → 断点可续

    check_uniqueness(conn)                # 6) 键唯一性校验（去重纪律）
    conn.close()
    print(f"完成，水位线 = {watermark}")

if __name__ == "__main__":
    main()
```

要点对照：

| 代码位置 | 对应铁律 |
|---|---|
| `fetch_batch(watermark)` 取 `id > watermark` | 增量回填 + 分块 |
| `PRIMARY KEY(entity, ts)` + `ON CONFLICT DO UPDATE` | 去重（同键覆盖） |
| `backup_db()` 写入前自动复制 | 自动备份 |
| 每批 `commit()` + `save_watermark()` | 可续跑（批粒度断点） |
| `os.environ["API_KEY"]` 注释 + 日志脱敏要求 | 密钥不入库 |

---

## 5. 恢复演练脚本（备份的价值在于能恢复）

```python
"""备份恢复演练：把最新备份恢复到临时库，比对行数与校验和。"""
import glob
import sqlite3

DB = "clean.db"
latest = sorted(glob.glob("backup/clean-*.db"))[-1]

def row_stats(db):
    conn = sqlite3.connect(db)
    n = conn.execute("SELECT COUNT(*) FROM clean").fetchone()[0]
    conn.close()
    return n

src, dst = row_stats(DB), row_stats(latest)
print(f"当前库 {src} 行，最新备份 {dst} 行")
assert dst <= src, "备份行数多于当前库？备份异常！"
assert dst > 0, "备份为空，恢复演练失败！"
print("恢复演练通过：备份可用。")
```

---

## 6. 密钥扫描（预提交钩子示例，git pre-commit）

```python
"""pre-commit 钩子：扫描改动文件中的疑似密钥特征。git 侧调用即可。"""
import os
import re
import subprocess
import sys

PATTERNS = [
    r"AKIA[0-9A-Z]{16}",                 # AWS Access Key
    r"sk-[A-Za-z0-9]{20,}",              # 常见 API Key 前缀
    r"(password|secret|token|api_key)\s*[:=]\s*[\"'][^\"']{8,}[\"']",
]

def main():
    files = subprocess.check_output(["git", "diff", "--cached", "--name-only"])
    files = [f for f in files.decode().split() if os.path.isfile(f)]
    hits = []
    for f in files:
        with open(f, encoding="utf-8", errors="ignore") as fh:
            for i, line in enumerate(fh, 1):
                if any(re.search(p, line, re.I) for p in PATTERNS):
                    hits.append(f"{f}:{i}")
    if hits:
        print("疑似密钥，禁止提交：\n" + "\n".join(hits))
        print("正确做法：密钥放环境变量/密钥服务，误提交过则立即轮换。")
        sys.exit(1)

if __name__ == "__main__":
    main()
```

注意：钩子只能兜底。**一旦误提交过密钥，务必轮换密钥**，删除历史提交不能消除泄露。

---

## 7. 实操检查清单

```text
□ 水位线是「严格大于」吗？（等于会漏边界行）
□ 主键能保证唯一吗？重复行是覆盖还是丢弃，规则写了吗？
□ 破坏性操作前自动备份了吗？备份恢复演练跑过吗？
□ 单批失败后，从水位线重跑会不会产生重复数据？（幂等）
□ 日志会打印密钥吗？仓库历史里搜过密钥特征吗？
□ 口径变更在 data-notes.md 记录了吗？旧版本数据还能回溯吗？
□ 每日校验（键唯一/水位线单调/行数波动）在跑吗？
```

---

## 8. 事故响应顺序

```text
1. 先停管线（防止继续写入污染数据）
2. 定位断点（看 state/ 水位线与日志）
3. 从备份恢复受影响部分
4. 修因 → 从水位线续跑 → 校验键唯一性与行数
5. 写进 journal.md：什么时间、什么事故、什么影响、怎么防止再犯
```

---

## 9. 常见事故与对策

| 事故 | 根因 | 对策 |
|---|---|---|
| 重复数据翻倍 | 断点后重跑未做幂等/去重 | 主键约束 + ON CONFLICT；重跑前删重叠区间 |
| 漏数据（边界行） | 水位线用「>=」或时间精度不足 | 严格大于 + 自增 ID 水位线 |
| 备份也坏了 | 备份与主库同盘/同机 | 备份异地异盘 + 定期恢复演练 |
| 密钥进了仓库 | 调试时硬编码 | 预提交钩子 + 泄露即轮换 |
| 口径漂移无人知 | 清洗逻辑改了没记 | 口径变更必须改 data-notes.md 并升版本 |
| 下游用脏数据 | 下游直接摸 raw/staging | 只开放 clean 视图；权限上隔离 |
