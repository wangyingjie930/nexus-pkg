package redis

import (
	"context"
	"fmt"
	"github.com/redis/go-redis/v9"
	"nexus/internal/pkg/logger"
	"strings"
	"sync"
	"time"
)

// Client 定义了一个通用的、解耦的 Redis 客户端
type Client struct {
	rdb redis.UniversalClient

	// ✨ [核心改造] 使用 sync.Map 来缓存已加载的 Lua 脚本，实现通用性
	scripts *sync.Map
}

// NewClient 创建一个新的 Redis 客户端实例
// 对于集群模式, redisAddrs 应该是逗号分隔的地址列表 "host1:port1,host2:port2"
func NewClient(redisAddrs string) (*Client, error) {
	addrs := strings.Split(redisAddrs, ",")
	logger.Logger.Printf("Connecting to Redis with addresses: %v", addrs)

	var rdb redis.UniversalClient
	if len(addrs) > 1 {
		rdb = redis.NewClusterClient(&redis.ClusterOptions{
			Addrs:        addrs,
			Password:     "",
			ReadTimeout:  3 * time.Second,
			WriteTimeout: 3 * time.Second,
		})
	} else {
		rdb = redis.NewClient(&redis.Options{
			Addr: addrs[0],
		})
	}

	if err := rdb.Ping(context.Background()).Err(); err != nil {
		return nil, fmt.Errorf("failed to connect to Redis: %w", err)
	}
	logger.Logger.Println("✅ Successfully connected to Redis.")

	return &Client{
		rdb:     rdb,
		scripts: new(sync.Map),
	}, nil
}

func (c *Client) LoadScriptFromContent(scriptName, content string) error {
	if _, loaded := c.scripts.Load(scriptName); loaded {
		return fmt.Errorf("script '%s' is already loaded", scriptName)
	}

	script := redis.NewScript(content)
	c.scripts.Store(scriptName, script)

	logger.Logger.Printf("✅ Lua script '%s' from %s loaded successfully.", scriptName, content)
	return nil
}

// ✨ [核心改造] RunScript 执行一个已加载的 Lua 脚本
// 这是完全通用的方法，它不关心脚本内容和返回值
func (c *Client) RunScript(ctx context.Context, scriptName string, keys []string, args ...interface{}) (interface{}, error) {
	val, ok := c.scripts.Load(scriptName)
	if !ok {
		return nil, fmt.Errorf("script '%s' not loaded", scriptName)
	}

	script, ok := val.(*redis.Script)
	if !ok {
		return nil, fmt.Errorf("invalid script object for '%s'", scriptName)
	}

	// Run 方法会返回一个 interface{}, 将其直接返回给业务层处理
	result, err := script.Run(ctx, c.rdb, keys, args...).Result()
	if err != nil {
		// go-redis 会自动处理 NOSCRIPT 错误并重新加载，所以这里通常只需要处理其他类型的错误
		return nil, fmt.Errorf("failed to run script '%s': %w", scriptName, err)
	}
	return result, nil
}

// GetClient 返回底层的 redis 客户端，以便执行其他通用命令
func (c *Client) GetClient() redis.UniversalClient {
	return c.rdb
}
