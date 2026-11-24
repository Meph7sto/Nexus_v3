# Data 高可用

PostgreSQL 18 由 Patroni 管理。两台 Data 节点各运行 PostgreSQL 和一个 etcd 成员；首台 Entry 放第三个 etcd，所有 Entry 各运行自己的 HAProxy。HAProxy 只把新建连接交给通过 HTTPS `/primary` 检查的节点。旧主被摘下时关闭旧会话，Entry 的数据库连接池自行重连。不要把这些服务都放到一台主机来当作生产 HA。

## 私网与证书

在所有容器内解析相同的私网 DNS，例如 `data-1.internal.example`。把解析结果限定为实际 WireGuard/Tailscale 地址；`HA_PG1_HOST`、`HA_PG2_HOST`、`HA_ETCD1_HOST`、`HA_ETCD2_HOST` 和 `HA_ETCD3_HOST` 填对应 DNS 名，不填公网地址。各主机的 `HA_DATA1_BIND_IP`、`HA_DATA2_BIND_IP`、`HA_ENTRY1_BIND_IP` 填指定 WireGuard IP，不能用 `0.0.0.0`。同一集群的 `HA_SCOPE` 和 `HA_ETCD_CLUSTER_TOKEN` 必须一致，并与其它集群隔离。

`HA_SECRETS_DIR` 指向本机受限目录。不把文件内容写进 Compose、命令行或 Git。

| 子目录 | 文件与证书 |
| :-- | :-- |
| `etcd-1`、`etcd-2`、`etcd-3` | 各自的 `ca.crt`、`server.crt`、`server.key`。SAN 含对应 `HA_ETCD*_HOST`；用途含 serverAuth、clientAuth。etcd 客户端与 peer 通道都要求证书。 |
| `data-1`、`data-2` | 各自的 `ca.crt`、`server.crt`、`server.key`。SAN 同时包含该节点的 `HA_PG*_HOST` 和应用连接名 `pg-proxy`；用途含 serverAuth、clientAuth。 |
| `patroni-common` | `postgres-password`、`replicator-password`、`rewind-password`、`rest-password`，各用独立高强度口令。REST 管理操作另外需要 Basic 认证。 |
| `haproxy` | `ca.crt` 和仅供检查使用的 clientAuth 证书/私钥合并文件 `client.pem`。Entry 应用只挂载 PostgreSQL CA 公钥，不挂载这个私钥。 |

最小连通矩阵如下；读取/备份账号仍按 PostgreSQL 和 Redis 的应用权限单独授权。

| 来源 | 目标 | TCP |
| :-- | :-- | :-- |
| 授权 Entry | Worker | 9090 |
| Entry 本机 HAProxy、两台 Data | 两台 PostgreSQL / Patroni | 5432、8008 |
| 两台 Patroni | 3 个 etcd | 2379 |
| 3 个 etcd | 其它 etcd peer | 2380 |
| Entry 和 Sentinel | Sentinel | 26379 |
| Entry、Redis 副本和 Sentinel | Redis 数据节点 | 6379 |
| Worker | 本地配置的 SOCKS5 出口 | 配置的 SOCKS5 端口 |

Tailscale 示例（预先分配各角色标签；与已有策略合并，不保留能覆盖下列限制的全开放规则）：

```json
{
  "grants": [
    {"src":["tag:entry"], "dst":["tag:worker"], "ip":["tcp:9090"]},
    {"src":["tag:entry","tag:postgres"], "dst":["tag:postgres"], "ip":["tcp:5432","tcp:8008"]},
    {"src":["tag:postgres"], "dst":["tag:etcd"], "ip":["tcp:2379"]},
    {"src":["tag:etcd"], "dst":["tag:etcd"], "ip":["tcp:2380"]},
    {"src":["tag:entry","tag:sentinel"], "dst":["tag:sentinel"], "ip":["tcp:26379"]},
    {"src":["tag:entry","tag:redis","tag:sentinel"], "dst":["tag:redis"], "ip":["tcp:6379"]},
    {"src":["tag:worker"], "dst":["tag:egress"], "ip":["tcp:1080"]}
  ]
}
```

`1080` 替换为实际出站代理端口。WireGuard 可把同一矩阵落实到 WireGuard 接口的防火墙。承载多个服务的主机可同时有多个标签，不增加公网监听；h2c 只用于这一加密且设 ACL 的链路。

## 启动与 Entry 接线

先在对应主机启动三个 etcd，再启动两个 Data。以下指令从本目录执行，`compose.yaml` 单独使用时只适合单机演练；跨主机通过覆盖文件公布私网端口。每台主机只启动属于它的服务：

```sh
# Data-1（Data-2 对应换成 etcd-2、data-2）
docker compose -p nexus-ha -f compose.yaml -f private-hosts.compose.yaml up -d --no-deps etcd-1
docker compose -p nexus-ha -f compose.yaml -f private-hosts.compose.yaml up -d --no-deps data-1

# Entry-1 另启动 etcd-3；所有 Entry 各自行运行 pg-proxy
docker compose -p nexus-ha -f compose.yaml -f private-hosts.compose.yaml up -d --no-deps etcd-3
docker compose -p nexus-ha -f compose.yaml up -d --no-deps pg-proxy
```

为应用准备自己的数据库和最小权限角色。Entry 容器不连接 `127.0.0.1:15432`，因为容器回环地址不是宿主机。发布端口只用来从宿主机做连通检查。Entry 容器通过本机共享 Docker 网络访问 `pg-proxy:5432`。从仓库根目录运行（其余 Entry 环境按原部署设置）：

```sh
export HA_DOCKER_NETWORK=nexus-ha_ha
export HA_ENTRY_PG_CA_FILE=/run/private/nexus/postgres-ca.crt
docker compose -f deploy/entry/compose.yaml -f deploy/data/ha/entry-network.compose.yaml up -d entry
```

覆盖文件启用 `DATABASE_SSLMODE=verify-full` 并只读挂载 CA。两台 PostgreSQL 证书都须有 `pg-proxy` SAN。HAProxy 检查证书时用各节点的 `HA_PG*_HOST`；复制和 `pg_rewind` 也使用这些私网名称及验证模式。

## 同步策略与故障演练

| 模式 | 适用场景 | 故障时的写入边界 |
| :-- | :-- | :-- |
| `sync_strict`（默认） | 同区，必须等同步备库确认 | 备库不可用时提交等待。两台 Data 只剩一台时，切出了主节点也不等于写入已恢复。 |
| `sync` | 同区，可接受降级期间单主提交 | 处于同步复制且真正收到同步确认的交易受到保护；单主降级窗口不保证零丢失。 |
| `async` | 跨区、较高 RTT，优先控制提交延迟 | `maximum_lag_on_failover=1048576` 是候选筛选起点，不是数据丢失保证；按业务容忍度和实际 WAL 产生速度调整。 |

`HA_PG_MODE` 只参与首次引导。已有集群经 `patronictl edit-config` 修改 DCS 配置并执行受控变更。当前起点为 `ttl=30s`、`loop_wait=10s`、`retry_timeout=10s`、`primary_start_timeout=0`。同步模式演练前需同时确认 PostgreSQL 正在同步流复制、Patroni 的 DCS 已认可同步候选；仅看到 `pg_stat_replication.sync_state=sync` 不足以证明备库能接管。生产前须另行验收 watchdog 或外部 fencing，验证主机冻结时旧主仍会被隔离；容器存活检查和 Proxy 探活不能代替隔离。

本地准备 Docker、Go 1.27.4、PowerShell 7 和 OpenSSL 3。先让 Docker 能读取锁定的 etcd、HAProxy 镜像，并构建本地 Patroni 镜像；不推送：

```powershell
docker pull quay.io/coreos/etcd:v3.6.7
docker pull haproxy:3.2.6-alpine
pwsh -File deploy/data/ha/rehearse-pg.ps1 -Mode sync
pwsh -File deploy/data/ha/rehearse-pg.ps1 -Mode sync_strict -NoBuild
```

脚本使用随机 Compose 项目、回环端口、临时 CA 和数据卷。它从另一个容器验证 `pg-proxy:5432`，让同一数据库连接池在主库容器硬停后重连，再断开旧主的私网，检查拒写、复归以及 `pg_rewind` 移除分叉 WAL。分别计时新主经 HAProxy 可读、首次确认写入，以及旧主拒写；RPO 按缺失的合成已确认交易计数。严格同步场景观测 `SyncRep` 后保留同一笔写，待备库复归再确认提交；超时不等于事务没写入，不应据此自动重发非幂等写请求。`-Mode async` 可演练异步。结果不代表生产 SLA。

本机 Docker 演练结果：PostgreSQL 18.3、Patroni 4.1.1、etcd 3.6.7、HAProxy 3.2.6。两台 PostgreSQL 和三个 etcd 容器在同一台宿主机，无跨区网络延迟。

| 模式与故障 | 旧主停止接受写 | 新主经 HAProxy 可读 | 新主首次确认写入 | 缺失的已确认合成记录 |
| :-- | --: | --: | --: | --: |
| `sync`：硬停原主 | 容器硬停 | 28.13 s | 28.15 s | 0 |
| `sync`：隔离原主网络 | 27.06 s | 31.14 s | 31.46 s | 0 |
| `sync_strict`：硬停原主 | 容器硬停 | 29.20 s | 34.22 s | 0 |
| `sync_strict`：隔离原主网络 | 25.98 s | 29.53 s | 44.36 s | 0 |
| `async`：硬停原主 | 容器硬停 | 24.96 s | 24.96 s | 0 |
| `async`：隔离原主网络 | 24.55 s | 28.06 s | 28.29 s | 0 |

三种模式均验证了同一 `sql.DB` 池重连，并观察到隔离节点在 `pg_rewind` 清除分叉记录后以副本复归。严格同步两次均观察到单副本期间的 `SyncRep` 等待，保留的同一笔写在备库复归后完成。异步两次均未丢失这组同机低负载合成记录，不构成异步零丢失保证；未实测跨主机网络延迟或宿主机冻结。

备份和跨集群恢复另见 [备份恢复演练](rehearsal/backup-recovery.md)。
