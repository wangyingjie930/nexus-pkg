package transactional

import (
	"context"
	"gorm.io/gorm"
	"time"
)

// Store 定义了对事务消息表的操作接口
type Store interface {
	// CreateInTx 在一个给定的数据库事务中创建一条消息记录
	CreateInTx(ctx context.Context, msg *Message) error
	// FindPendingMessages 查找一定数量的待发送消息
	FindPendingMessages(ctx context.Context, limit int) ([]*Message, error)
	// UpdateStatus 更新消息的状态和重试次数
	UpdateStatus(ctx context.Context, id int64, status Status, newRetryCount int) error
}

// gormStore 是 Store 接口的 GORM 实现
type gormStore struct {
	db *gorm.DB
}

// NewGormStore 创建一个新的 GORM Store 实例
// 这个 *gorm.DB 实例应该是从您的业务代码中已经初始化好的数据库连接
func NewGormStore(db *gorm.DB) Store {
	// 建议在启动时执行一次 AutoMigrate，以确保表结构存在
	err := db.AutoMigrate(&Message{})
	if err != nil {
		// 在实际应用中，您可能需要更健壮的错误处理
		panic(err)
	}
	return &gormStore{db: db}
}

func (s *gormStore) CreateInTx(ctx context.Context, msg *Message) error {
	return s.db.WithContext(ctx).Create(msg).Error
}

func (s *gormStore) FindPendingMessages(ctx context.Context, limit int) ([]*Message, error) {
	var messages []*Message
	// 为了避免多个转发器实例处理同一批消息，可以增加一个 "locked_by" 和 "locked_until" 字段来实现悲观锁
	// 但为了简化，这里我们只查找 PENDING 状态的消息
	err := s.db.WithContext(ctx).
		Where("status = ?", StatusPending).
		Where("updated_at < ?", time.Now().Add(-1*time.Minute)). // 简单的失败重试间隔
		Order("id asc").
		Limit(limit).
		Find(&messages).Error
	return messages, err
}

func (s *gormStore) UpdateStatus(ctx context.Context, id int64, status Status, newRetryCount int) error {
	return s.db.WithContext(ctx).Model(&Message{}).Where("id = ?", id).Updates(map[string]interface{}{
		"status":      status,
		"retry_count": newRetryCount,
	}).Error
}
