# Nexus V3 private Worker network

Start with one Entry, one [Data host](../data/single/compose.yaml), and two [Workers](../worker/compose.yaml). Run each template from its role directory with `docker compose up -d`; add `--build` for Entry and Worker when building from source. Give each Worker a distinct node ID and a distinct Worker token.

## Network and secrets

- Bind Postgres, Redis and Worker ports to the host's encrypted private interface. Set `DATA_BIND_IP` and `WORKER_BIND_IP` to real private interface addresses, never wildcard or public addresses. Restrict Worker and Data ports to the Entry over the private network using ACLs and Docker-aware host firewall rules. Entry publishes only on loopback; put the existing TLS reverse proxy in front of it.
- Use mTLS for Worker by default. In the private Worker environment file, set `WORKER_TLS_CERT_FILE`, `WORKER_TLS_KEY_FILE`, and `WORKER_TLS_CLIENT_CA_FILE` to paths under `/run/nexus-worker-secrets`. The Entry's server-trust CA and client certificate/key belong in its own mounted secret directory. Use HTTPS URLs that match the Worker certificates.
- Use h2c only over an already encrypted network with the ACLs above. Set `WORKER_ALLOW_PRIVATE_H2C=true`, change node URLs to `http://`, and explicitly set `worker_pool.allow_private_h2c: true` on Entry. Do not publish h2c on a public interface.
- Keep environment files and secret source files out of Git and readable only by their owners and the processes that need them. Entry and Worker run as UID 1000; grant the containers read access to their own mounted files, not one shared directory of all credentials. Use separate random tokens (32 or more visible ASCII characters) for Workers and store them as files, not in command lines. In the Redis ACL file, set `user default off resetpass` and provision a separate authenticated application user. Make the file readable by the Redis container user and put the matching username/password in the Entry environment file.

## Entry setup and cutover

Dynamic routes use the Worker resolver to check destination addresses, including through SOCKS5H or HTTP CONNECT. Private destinations are denied by default; explicitly allowlist private IP literals only for known self-hosted origins. Requests marked public-only cannot use that exception. `WORKER_DNS_MODE=trusted_proxy` applies only to legacy relay requests with a configured static SOCKS5H proxy and a target allowlist.

Start [Entry](../entry/compose.yaml) with the existing standalone setup environment variables for the private Postgres and Redis endpoints. Use the existing first-run wizard or set `AUTO_SETUP=true` with explicit admin, JWT and TOTP settings. Keep the initialized `config.yaml` in the Entry data directory.

Manage account proxies and credentials only in Entry. Each relay carries the current account route, including direct egress. No Worker restart is needed. `WORKER_ALLOWED_TARGETS` optionally restricts destination authorities. `WORKER_ALLOW_PRIVATE_PROXY=false` limits the proxy itself to public addresses; it does not relax destination checks. Proxy exports omit usernames and passwords; supply those separately when importing a proxy that needs authentication.

Merge the [Worker pool example](../entry/worker-pool.example.yaml) into the Entry configuration. Enable the pool after provisioning authenticated Workers. HTTP/SSE and native OpenAI WebSocket handshakes use the pool. Disabling the pool restores local transport for new requests.

Entry probes Workers using the authenticated HTTP/2 `/healthz` endpoint. Do not log or paste the credentials when checking a node. To drain a Worker, send SIGTERM (for example via `docker compose stop worker`); the process rejects new relay requests and lets in-flight streams finish until `WORKER_DRAIN_TIMEOUT`. Keep `WORKER_STOP_GRACE_PERIOD` longer than the drain timeout. For token rotation, mount the prior token and set `WORKER_PREVIOUS_SHARED_TOKEN_FILE` during the overlap; remove it once Entry has restarted with the new token.

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

Use the [Entry role example](../entry/entry-node.example.yaml) after initializing the shared Data. Each Entry has a stable unique ID and the same configured shared-task owner ID. `gateway_only` exposes inference and health, not panel routes. `control_only` exposes panel and health, not inference routes. `all` exposes both. Keep a single standalone `all` Entry when clustering is off. Initialize the data directory through a control Entry before starting a gateway-only process.

Send scheduled-job configuration changes to the designated owner. Only that process runs shared scans and timers, including scheduled account tests, backup, expiry, aggregation and periodic refresh. Each Entry still processes its own request-driven work and maintains its own local caches, subscriptions, logging, usage recording and security checks. Heartbeats show role, version, tracked HTTP connections and whether the instance holds the task claim; HTTP connections that were hijacked are not counted.

Task ownership does not fail over automatically. The same handoff is required for an upgrade or restart on the same Entry ID. Stop the old owner and confirm its process has exited. Review active backups, notifications and other interrupted jobs before releasing its single PostgreSQL `entry_shared_task_claim` row. Delete only the row matching the inspected `entry_id` and `process_token`, then restart the designated owner. If moving tasks, update and restart the other Entries with the same owner setting. Without release, a restarted clustered Entry still opens its assigned routes, but logs `entry.shared_task_claim_already_held` and reports that its shared tasks are not running. A scheduled account-test occurrence is consumed in PostgreSQL before it starts: a crash may skip that occurrence, but replacement processes do not replay it. A stopped heartbeat on its own is never proof that an owner has stopped.
