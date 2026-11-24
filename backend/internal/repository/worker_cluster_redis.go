package repository

import (
	"context"
	"encoding/json"
	"strconv"
	"strings"
	"time"

	"github.com/Wei-Shaw/sub2api/internal/service"
	"github.com/redis/go-redis/v9"
)

const (
	workerClusterReloadChannel = "nexus:cluster:worker_pool:reload"
	entryHeartbeatPrefix       = "nexus:cluster:entry:"
)

type workerClusterRedis struct{ redis redis.UniversalClient }

func NewWorkerClusterCoordination(client redis.UniversalClient) service.WorkerClusterCoordination {
	return &workerClusterRedis{redis: client}
}

func (c *workerClusterRedis) SubscribeWorkerChanges(ctx context.Context) <-chan int64 {
	out := make(chan int64, 1)
	go func() {
		defer close(out)
		sub := c.redis.Subscribe(ctx, workerClusterReloadChannel)
		defer sub.Close()
		messages := sub.Channel()
		for {
			select {
			case <-ctx.Done():
				return
			case message, open := <-messages:
				if !open {
					return
				}
				if version, err := strconv.ParseInt(message.Payload, 10, 64); err == nil && version > 0 {
					select {
					case out <- version:
					default:
					}
				}
			}
		}
	}()
	return out
}

func (c *workerClusterRedis) PublishWorkerChange(ctx context.Context, version int64) error {
	return c.redis.Publish(ctx, workerClusterReloadChannel, strconv.FormatInt(version, 10)).Err()
}

func (c *workerClusterRedis) PutEntryHeartbeat(ctx context.Context, entry service.EntryHeartbeat, ttl time.Duration) error {
	raw, err := json.Marshal(entry)
	if err != nil {
		return err
	}
	return c.redis.Set(ctx, entryHeartbeatPrefix+entry.InstanceID, raw, ttl).Err()
}

func (c *workerClusterRedis) DeleteEntryHeartbeat(ctx context.Context, instanceID string) error {
	return c.redis.Del(ctx, entryHeartbeatPrefix+instanceID).Err()
}

func (c *workerClusterRedis) ListEntryHeartbeats(ctx context.Context) ([]service.EntryHeartbeat, error) {
	var cursor uint64
	entries := []service.EntryHeartbeat{}
	seen := make(map[string]bool)
	for {
		keys, next, err := c.redis.Scan(ctx, cursor, entryHeartbeatPrefix+"*", 100).Result()
		if err != nil {
			return nil, err
		}
		if len(keys) != 0 {
			values, err := c.redis.MGet(ctx, keys...).Result()
			if err != nil {
				return nil, err
			}
			for i, value := range values {
				var entry service.EntryHeartbeat
				raw, ok := value.(string)
				if !ok || json.Unmarshal([]byte(raw), &entry) != nil ||
					entry.InstanceID == "" || entry.InstanceID != strings.TrimPrefix(keys[i], entryHeartbeatPrefix) || seen[entry.InstanceID] {
					continue
				}
				seen[entry.InstanceID] = true
				entries = append(entries, entry)
			}
		}
		if next == 0 {
			return entries, nil
		}
		cursor = next
	}
}
