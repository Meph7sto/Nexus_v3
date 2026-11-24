package redisconn

import (
	"context"
	"crypto/tls"
	"errors"
	"net"
	"time"

	"github.com/Wei-Shaw/sub2api/internal/config"
	"github.com/redis/go-redis/v9"
)

func NewFailover(cfg config.RedisConfig) *redis.Client {
	options, dataDial := failoverOptions(cfg)
	client := redis.NewFailoverClient(options)
	client.AddHook(dataDialHook{open: dataDial})
	return client
}

func failoverOptions(cfg config.RedisConfig) (*redis.FailoverOptions, redis.DialHook) {
	seeds := make([]string, 0, len(cfg.Sentinel.Addresses))
	dataAddresses := make(map[string]struct{})
	sentinelAddresses := make(map[string]struct{})
	for _, addr := range cfg.Sentinel.AllowedPrimaries {
		if normalized, ok := config.NormalizeRedisEndpoint(addr); ok {
			dataAddresses[normalized] = struct{}{}
		}
	}
	for _, addr := range cfg.Sentinel.Addresses {
		if normalized, ok := config.NormalizeRedisEndpoint(addr); ok {
			seeds = append(seeds, normalized)
			sentinelAddresses[normalized] = struct{}{}
		}
	}
	timeout := time.Duration(cfg.DialTimeoutSeconds) * time.Second
	if timeout <= 0 {
		timeout = 5 * time.Second
	}
	dialer := &net.Dialer{Timeout: timeout}
	tlsDialer := &tls.Dialer{NetDialer: dialer, Config: &tls.Config{MinVersion: tls.VersionTLS12}}
	open := func(ctx context.Context, network, addr string, secure bool) (net.Conn, error) {
		if secure {
			return tlsDialer.DialContext(ctx, network, addr)
		}
		return dialer.DialContext(ctx, network, addr)
	}
	return &redis.FailoverOptions{
			MasterName: cfg.Sentinel.MasterName, SentinelAddrs: seeds,
			SentinelUsername: cfg.Sentinel.Username, SentinelPassword: cfg.Sentinel.Password,
			Username: cfg.Username, Password: cfg.Password, DB: cfg.DB,
			DialTimeout: timeout, ReadTimeout: time.Duration(cfg.ReadTimeoutSeconds) * time.Second,
			WriteTimeout: time.Duration(cfg.WriteTimeoutSeconds) * time.Second,
			// Leave the pool cold until the data-only dial hook is installed.
			PoolSize:              cfg.PoolSize,
			ContextTimeoutEnabled: true,
			Dialer: func(ctx context.Context, network, addr string) (net.Conn, error) {
				normalized, ok := config.NormalizeRedisEndpoint(addr)
				if !ok {
					return nil, errors.New("Redis Sentinel discovered an unconfigured endpoint")
				}
				if _, allowed := sentinelAddresses[normalized]; allowed {
					return open(ctx, network, normalized, cfg.Sentinel.EnableTLS)
				}
				if _, allowed := dataAddresses[normalized]; allowed {
					// go-redis uses this same dialer for the data client and the
					// internal Sentinel clients. Only the data hook opens this.
					return pendingDataAddress(normalized), nil
				}
				return nil, errors.New("Redis Sentinel discovered an unconfigured endpoint")
			},
		}, func(ctx context.Context, network, addr string) (net.Conn, error) {
			return open(ctx, network, addr, cfg.EnableTLS)
		}
}

type dataDialHook struct{ open redis.DialHook }

func (h dataDialHook) DialHook(next redis.DialHook) redis.DialHook {
	return func(ctx context.Context, network, addr string) (net.Conn, error) {
		conn, err := next(ctx, network, addr)
		if err != nil {
			return nil, err
		}
		data, allowed := conn.(pendingDataAddress)
		if !allowed {
			_ = conn.Close()
			return nil, errors.New("Redis Sentinel nominated a non-data endpoint")
		}
		return h.open(ctx, network, string(data))
	}
}

func (h dataDialHook) ProcessHook(next redis.ProcessHook) redis.ProcessHook { return next }
func (h dataDialHook) ProcessPipelineHook(next redis.ProcessPipelineHook) redis.ProcessPipelineHook {
	return next
}

// An internal Sentinel attempting to use a data address never gets a socket.
type pendingDataAddress string

var errDataCannotBeSentinel = errors.New("Redis data endpoint cannot serve as a Sentinel")

func (a pendingDataAddress) Network() string                  { return "tcp" }
func (a pendingDataAddress) String() string                   { return string(a) }
func (a pendingDataAddress) LocalAddr() net.Addr              { return a }
func (a pendingDataAddress) RemoteAddr() net.Addr             { return a }
func (a pendingDataAddress) Close() error                     { return nil }
func (a pendingDataAddress) Read([]byte) (int, error)         { return 0, errDataCannotBeSentinel }
func (a pendingDataAddress) Write([]byte) (int, error)        { return 0, errDataCannotBeSentinel }
func (a pendingDataAddress) SetDeadline(time.Time) error      { return errDataCannotBeSentinel }
func (a pendingDataAddress) SetReadDeadline(time.Time) error  { return errDataCannotBeSentinel }
func (a pendingDataAddress) SetWriteDeadline(time.Time) error { return errDataCannotBeSentinel }
