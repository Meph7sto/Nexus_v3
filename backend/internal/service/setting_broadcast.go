package service

import (
	"context"
	"log/slog"
	"time"

	"github.com/redis/go-redis/v9"
)

const settingsChangeChannel = "nexus:cluster:settings:reload"

type settingsBroadcast struct {
	cancel context.CancelFunc
	done   chan struct{}
	dirty  chan struct{}
}

// StartSettingsBroadcast is called once, after the router has installed the local update callbacks.
func (s *SettingService) StartSettingsBroadcast(client *redis.Client) {
	ctx, cancel := context.WithCancel(context.Background())
	b := &settingsBroadcast{cancel: cancel, done: make(chan struct{}), dirty: make(chan struct{}, 1)}
	s.runtimeBroadcast = b
	go func() {
		defer close(b.done)
		sub := client.Subscribe(ctx, settingsChangeChannel)
		defer sub.Close()
		messages := sub.Channel()
		for {
			select {
			case <-ctx.Done():
				return
			case <-b.dirty:
				publishCtx, stop := context.WithTimeout(ctx, time.Second)
				err := client.Publish(publishCtx, settingsChangeChannel, "reload").Err()
				stop()
				if err != nil && ctx.Err() == nil {
					slog.Warn("publish cluster settings change failed", "error", err)
				}
			case _, ok := <-messages:
				if !ok {
					return
				}
				readCtx, stop := context.WithTimeout(ctx, 5*time.Second)
				stored, err := s.GetAllSettings(readCtx)
				stop()
				if err != nil {
					if ctx.Err() == nil {
						slog.Warn("reload cluster settings failed", "error", err)
					}
					continue
				}
				s.refreshCachedSettings(stored)
			}
		}
	}()
}

func (s *SettingService) StopSettingsBroadcast() {
	if b := s.runtimeBroadcast; b != nil {
		b.cancel()
		<-b.done
	}
}

func (s *SettingService) publishSettingsChange() {
	if b := s.runtimeBroadcast; b != nil {
		// Peers re-read the latest DB state. One pending notification covers any number of saves.
		select {
		case b.dirty <- struct{}{}:
		default:
		}
	}
}
