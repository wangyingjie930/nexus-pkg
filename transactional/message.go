package transactional

import (
	"time"
)

// Status 定义了事务消息的状态
type Status string

const (
	// StatusPending 待发送状态，消息已在本地数据库，等待转发
	StatusPending Status = "PENDING"
	// StatusSent 已发送状态，消息已成功发送到消息队列
	StatusSent Status = "SENT"
	// StatusFailed 发送失败状态，所有重试都失败后标记为此状态
	StatusFailed Status = "FAILED"
)

// Message 对应数据库中的事务消息表 (transactional_messages)
// 建议表结构包含: id (BIGINT, PK), topic (VARCHAR), `key` (VARCHAR), payload (TEXT/BLOB),
// status (VARCHAR), retry_count (INT), created_at (DATETIME), updated_at (DATETIME)
type Message struct {
	ID         int64     `gorm:"primaryKey"`
	Topic      string    `gorm:"type:varchar(255);not null"`
	Key        string    `gorm:"type:varchar(255)"`
	Payload    []byte    `gorm:"type:blob;not null"`
	Status     Status    `gorm:"type:varchar(20);not null;index"`
	RetryCount int       `gorm:"not null;default:0"`
	CreatedAt  time.Time `gorm:"autoCreateTime"`
	UpdatedAt  time.Time `gorm:"autoUpdateTime"`
}

func (Message) TableName() string {
	return "transactional_messages"
}
