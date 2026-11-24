# All-in-one deployment

Use the existing [all-in-one Compose setup](../DOCKER.md) in the parent directory. It still runs the compatible server binary and keeps forwarding local. Set `worker_pool.enabled: false` when returning an Entry to local forwarding; no Worker or new Data service is needed.
