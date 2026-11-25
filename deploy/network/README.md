# Nexus V3 private Worker network

Start with one Entry, one [Data host](../data/single/compose.yaml), and two [Workers](../worker/compose.yaml). Run each template from its role directory with `docker compose up -d`; add `--build` for Entry and Worker when building from source. Give each Worker a distinct node ID and a distinct Worker token.

## Network and secrets

- Bind Postgres, Redis and Worker ports to the host's encrypted private interface. Set `DATA_BIND_IP` and `WORKER_BIND_IP` to real private interface addresses, never wildcard or public addresses. Restrict Worker and Data ports to the Entry over the private network using ACLs and Docker-aware host firewall rules. Entry publishes only on loopback; put the existing TLS reverse proxy in front of it.
- Use mTLS for Worker by default. In the private Worker environment file, set `WORKER_TLS_CERT_FILE`, `WORKER_TLS_KEY_FILE`, and `WORKER_TLS_CLIENT_CA_FILE` to paths under `/run/nexus-worker-secrets`. The Entry's server-trust CA and client certificate/key belong in its own mounted secret directory. Use HTTPS URLs that match the Worker certificates.
- Use h2c only over an already encrypted network with the ACLs above. Set `WORKER_ALLOW_PRIVATE_H2C=true`, change node URLs to `http://`, and explicitly set `worker_pool.allow_private_h2c: true` on Entry. Do not publish h2c on a public interface.
- Keep environment files and secret source files out of Git and readable only by their owners and the processes that need them. Entry and Worker run as UID 1000; grant the containers read access to their own mounted files, not one shared directory of all credentials. Use separate random tokens (32 or more visible ASCII characters) for Workers and store them as files, not in command lines. In the Redis ACL file, set `user default off resetpass` and provision a separate authenticated application user. Make the file readable by the Redis container user and put the matching username/password in the Entry environment file.

## Entry setup and cutover

By default the Worker resolves and pins the destination, including for SOCKS5H and HTTP proxies. Private destinations are denied by default; explicitly allowlist private IP literals only for known self-hosted origins. Public-only requests cannot use that exception. In an environment without reliable public DNS, `WORKER_DNS_MODE=trusted_proxy` delegates DNS for dynamic SOCKS5H routes on an exact `WORKER_ALLOWED_TARGETS` list. Enable this only when every SOCKS5H proxy allowed to serve those routes enforces the exact destination allowlist and blocks non-public IPs after resolution. Public-only requests still resolve and pin on the Worker. Direct, SOCKS5, and HTTP routes never delegate target DNS.

Start [Entry](../entry/compose.yaml) with the existing standalone setup environment variables for the private Postgres and Redis endpoints. Use the existing first-run wizard or set `AUTO_SETUP=true` with explicit admin, JWT and TOTP settings. Keep the initialized `config.yaml` in the Entry data directory.

Manage account proxies and credentials only in Entry. Each relay carries the current account route, including direct egress. No Worker restart is needed. `WORKER_ALLOWED_TARGETS` optionally restricts destination authorities. `WORKER_ALLOW_PRIVATE_PROXY=false` limits the proxy itself to public addresses; it does not relax destination checks. Authorized administrator views and explicit backups include proxy credentials, as in V2. Protect exported files as secrets.

Configure private transport once on each Entry: install the Worker CA and client certificate/key for mTLS (or explicitly allow private h2c), enable cluster mode, and give every Entry the **same fixed** `TOTP_ENCRYPTION_KEY`. Keep a protected backup of this key: it decrypts panel-managed Worker tokens. The [Worker pool example](../entry/worker-pool.example.yaml) also shows the older file-based setup; static bindings are optional in cluster mode.

In **Admin → Cluster**, add a Worker, enter its unique node ID, private URL and protocol, and generate or enter a Bearer token (at least 32 visible ASCII characters, no commas). Copy it before saving: it cannot be read back after a successful save. Put the same token in `shared-token` under that Worker's `WORKER_SECRETS_DIR`, and start the Worker with the matching node ID and transport.

You can register a disabled Worker before its DNS record exists. Before enabling it, point the hostname to private addresses reachable from every routing Entry. Enabling a node or changing the target of an enabled node checks DNS; editing its note or disabling it does not require DNS. Every new connection, including a probe, still resolves the address and rejects the entire answer if it contains a non-private IP. Probe the Worker, enable it and enable the pool when ready.

Cluster changes reach the other Entries through Redis; a 30-second poll catches missed updates. Adding, editing, draining or deleting a managed Worker does not require editing an Entry config or restarting an Entry. Existing file-based bindings remain available as a panel shortcut.

Entry probes Workers using the authenticated HTTP/2 `/healthz` endpoint. Do not log credentials when checking a node. The pool serves HTTP/SSE and native OpenAI WebSocket handshakes. Disabling it restores local transport for new requests. To rotate a managed token, first configure the Worker to accept the new token and mount the old one through `WORKER_PREVIOUS_SHARED_TOKEN_FILE`. Reload the Worker process with both files, then save the new token in the panel. Wait for the routing Entries to apply the new version and verify health before removing the old token from the Worker. Leaving the panel token blank on edit preserves the existing credential.

Keep local fallback off until the same egress is verified. If an attempt's outcome is unknown, neither Entry nor Worker resends it. Clients should also avoid resubmitting that request automatically. The resource limits in these templates are starting points, not measured capacity.

## Multiple Entries

For planned maintenance, remove the Entry from the load balancer before sending SIGTERM. Standalone Entry reads `entry_node.shutdown_timeout` (default `2m`); the Entry Compose template sets it with `ENTRY_SHUTDOWN_TIMEOUT`. Keep `ENTRY_STOP_GRACE_PERIOD` (default `150s`) longer than that timeout plus cleanup. A stream lasting past the configured window may still be interrupted.

### Recover a blocked Worker drain

Normally request a drain in the admin panel, wait for all routing Entries to acknowledge it and the Worker to report zero streams, delete the registration, then stop the Worker. Missing heartbeats or failed probes do not prove a machine is off.

For an abnormal stop, use a human admin session with cluster delete permission and step-up verification. Both actions below are relative to `/api/v1/admin/cluster`. Do not use them for a mere network partition:

- Independently verify the old Entry process has exited or permanently isolate its Worker traffic. Use the **instance ID** from the drain's `uncertain_entries`, not the reusable node name. POST `/entries/:id/retire-offline` with `{"confirmed_fenced_instance":"<same instance ID>"}`. A currently heartbeating instance is refused. This does not release the shared-job claim.
- If the draining Worker itself has already stopped, confirm the host is fenced and no other process can receive traffic for that Worker. After every remaining Entry acknowledges the drain, POST `/workers/:id/forget-offline` with `{"confirmed_fenced_worker":"<same Worker ID>","expected_version":<current desired version>}`. A reachable Worker reporting valid health, outstanding Entry acknowledgments or a stale version blocks the operation. This removes the registration; it does not certify that interrupted streams completed.

Retired Entry records older than seven days are pruned on the next configuration save only when no drain still references them.

### Roles and shared tasks

Use the [Entry role example](../entry/entry-node.example.yaml) after initializing the shared Data. Give every Entry a stable unique ID. To prefer one control Entry for shared tasks, set `shared_task_owner_id` consistently across the cluster. `gateway_only` exposes inference and health, not panel routes. `control_only` exposes panel and health, not inference routes. `all` exposes both. Keep a single standalone `all` Entry when clustering is off. Initialize the data directory through a control Entry before starting a gateway-only process.

Only the current lease holder runs shared scans and timers, including scheduled account tests, backup, expiry, aggregation and periodic refresh. Each Entry still processes its own request-driven work. The cluster heartbeat reports the current holder through `shared_tasks_running`; send owner-only operations such as editing the backup schedule to that Entry.

If Redis ACLs restrict Pub/Sub channels, allow the Entry application user to publish and subscribe to `nexus:cluster:worker_pool:reload` and `nexus:cluster:settings:reload`. Saving general settings sends a notification, not the settings themselves. The gateway settings caches retain their existing TTL if Redis is unavailable.

**First upgrade from Phase 4:** stop the old shared-task owner before applying migration 256. The migration makes its old claim immediately eligible for takeover; an older running binary cannot observe lease loss. Upgrade every routing Entry and verify its running binary or image digest before adding a panel-managed Worker. Older Entries cannot read its credential.

**Rollback to Phase 4:** while still on Phase 5, drain and remove managed registrations and provision legacy bindings for Workers that must remain available. Gracefully stop all lease-aware control Entries before starting the older task owner. Phase 4 does not understand expiry; confirm its old ownership checks can pass before reopening scheduled work.

After this upgrade, do not manually delete `entry_shared_task_claim`. The holder renews a 30-second PostgreSQL lease every 10 seconds and releases its own lease on graceful shutdown. A replacement control-capable Entry can acquire a released lease without waiting for its expiry. After a crash it can acquire the lease once it expires. A gateway-only Entry never takes over, and standalone startup refuses an unexpired cluster lease. Review interrupted jobs after a crash: takeover restarts scheduling, not work already in flight. For example, a scheduled account-test occurrence is claimed before it starts and is not replayed.
