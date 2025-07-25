package session

import (
	"context"
	"github.com/pkg/errors"
	"github.com/redis/go-redis/v9"
	"time"
)

// Manager 定义了会话管理器的接口
type Manager struct {
	client *redis.Client
}

// NewManager 创建一个新的会话管理器实例
func NewManager(redisAddr string) *Manager {
	rdb := redis.NewClient(&redis.Options{
		Addr: redisAddr,
	})
	return &Manager{client: rdb}
}

// SetUserGateway 将用户ID与网关节点ID进行映射，并设置过期时间（心跳）
func (m *Manager) SetUserGateway(ctx context.Context, userID string, gatewayNodeID string) error {
	// key: "user_session:12345", value: "push-gateway-node-abc"
	key := "user_session:" + userID
	// 5分钟过期，实际应用中应由客户端心跳来续期
	return m.client.Set(ctx, key, gatewayNodeID, 5*time.Minute).Err()
}

// GetUserGateway 获取用户所在的网关节点ID
func (m *Manager) GetUserGateway(ctx context.Context, userID string) (string, error) {
	key := "user_session:" + userID
	val, err := m.client.Get(ctx, key).Result()
	if errors.Is(err, redis.Nil) {
		return "", nil // 用户不在线
	} else if err != nil {
		return "", err
	}
	return val, nil
}

// ClearUserGateway 清除用户的会话信息（用户下线时调用）
func (m *Manager) ClearUserGateway(ctx context.Context, userID string) error {
	key := "user_session:" + userID
	return m.client.Del(ctx, key).Err()
}
