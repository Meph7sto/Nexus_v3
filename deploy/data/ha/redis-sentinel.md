# Redis Sentinel 部署

Data-1 和 Data-2 各运行一个 Redis 数据节点和一个 Sentinel，Entry-1 运行第三个 Sentinel。三个 Sentinel 分别投票，法定人数为 2。模板使用 Redis 8，数据节点启用 AOF 和 `noeviction`。

## 三台主机

使用 [redis.node.compose.yaml](redis.node.compose.yaml)。在各主机设置：

- `HA_REDIS_BIND_IP`：该主机的私网 IPv4 地址。Redis 和 Sentinel 端口只绑定这个地址。
- `HA_REDIS_INITIAL_MASTER_HOST`：Data-1 的私网 IPv4 地址或固定 DNS 名。三个 Sentinel 和两个 Redis 节点用同一个值。节点停机时，该 DNS 名仍须能解析。
- `HA_REDIS_PASSWORD_FILE`、`HA_REDIS_SENTINEL_PASSWORD_FILE`：两个受限文件，分别保存 Redis 数据和 Sentinel 的不同口令。每个口令至少 32 位，使用 Base64URL 字符。运行时不要用命令行传口令。

在本目录启动对应服务：

```sh
# Data-1，首次创建主节点卷
HA_REDIS_INITIAL_ROLE=primary docker compose -f redis.node.compose.yaml --profile data up -d
# Data-2，首次创建副本卷
HA_REDIS_INITIAL_ROLE=replica docker compose -f redis.node.compose.yaml --profile data up -d
# Entry-1，只运行第三票
docker compose -f redis.node.compose.yaml up -d sentinel
```

只在加密私网上开放端口。三个 Sentinel 之间允许访问 TCP 26379；Sentinel 和授权 Entry 还要访问两个 Redis 节点的 TCP 6379，两个 Redis 节点之间也要允许复制。保留各节点自己的持久卷。启动模板只在首次创建卷时写入种子配置，之后保留 Redis 与 Sentinel 重写的角色和发现状态。换口令时同步修改服务端认证、复制及 Sentinel 监控认证，再更新 Entry；更换种子文件不会覆盖旧卷的运行配置。

## Entry 接线

首次安装向导选择「高可用 / Redis Sentinel」，PostgreSQL 填稳定的写入端点，Redis 填三个 Sentinel 地址及两个可成为主库的数据节点地址。向导检查发现的主是否可写，保存 Sentinel 配置，不固定当前主地址。已有实例可在 `config.yaml` 设置：

```yaml
redis:
  db: 0
  pool_size: 48
  min_idle_conns: 8
  sentinel:
    master_name: nexus
    addresses: ["data-1.internal:26379", "data-2.internal:26379", "entry-1.internal:26379"]
    allowed_primaries: ["data-1.internal:6379", "data-2.internal:6379"]
```

地址须与 Redis/Sentinel 实际通告的地址一致。由 Entry 的受限环境文件设置 `REDIS_PASSWORD` 和 `REDIS_SENTINEL_PASSWORD`，不要填入共享样例或导出文件。启用数据或 Sentinel TLS 时，分别设置 `redis.enable_tls` 或 `redis.sentinel.enable_tls`，并给 Entry 提供能验证相应节点名的受信 CA。Sentinel 配置未填写时继续使用 Redis 单点连接。Sentinel 模式的连接按需建立，`pool_size` 仍限制池大小，`min_idle_conns` 只影响单点模式的启动预热。

## 切换与恢复

两个数据节点都配置 `min-replicas-to-write 1`。主节点失联时，Sentinel 可以提升副本，Entry 的同一个客户端会转向新主。新主没有在线副本时返回 `NOREPLICAS`；恢复副本后才恢复写入。容量不足时 Redis 拒绝写入，不会为腾空间删除现有并发槽。复制仍是异步的，短暂断链期间未复制的写入和 Pub/Sub 消息可能丢失。

恢复旧主前确认当前主已切换，并从两侧确认旧主隔离。旧节点重新加入后核对它已成为新主的副本，再恢复业务流量。同步检查授权缓存失效、并发槽、限流计数、LeaderLock 和配额写回。不要仅凭 Redis 的 `PING` 认定写入已恢复。

在 Docker Desktop 上执行 `pwsh -File deploy/data/ha/rehearse-redis.ps1` 可复现隔离演练。脚本使用新建项目、随机测试口令和不发布端口的内部网络，只清理本次演练资源。

一次 Redis 8.6.3 隔离演练中，原主进程暂停且 DNS 保持可用。同一个客户端约 7.6 秒后从新主读到经一个副本确认的状态，恢复旧节点并完成降级后约 23.8 秒恢复写入。演练验证了原有并发槽和缓存仍可用、授权失效订阅恢复、限流和 LeaderLock 可继续执行、配额快照可写入仓储测试替身，以及触发内存拒绝写时不会驱逐活跃槽。这是实验室观测，不是线上恢复时限。
