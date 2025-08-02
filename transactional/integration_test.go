//go:build integration
// +build integration

package transactional_test

import (
	"context"
	"fmt"
	"github.com/wangyingjie930/nexus-pkg/transactional"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/segmentio/kafka-go"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"gorm.io/driver/mysql"
	"gorm.io/gorm"
)

var (
	db     *gorm.DB
	store  transactional.Store
	writer *kafka.Writer
)

// TestMain 是一个特殊的测试函数，在包中所有测试运行前执行一次
// 非常适合用来做集成测试的初始化和清理工作
func TestMain(m *testing.M) {
	// 从环境变量中读取连接信息
	mysqlDSN := os.Getenv("MYSQL_DSN")
	kafkaBrokers := os.Getenv("KAFKA_BROKERS")

	// 如果没有设置环境变量，则跳过所有集成测试
	// 这使得开发者在本地不启动 docker 也能正常运行 go test ./... (它会跳过这些测试)
	if mysqlDSN == "" || kafkaBrokers == "" {
		fmt.Println("Skipping integration tests: MYSQL_DSN or KAFKA_BROKERS not set.")
		os.Exit(0)
	}

	// 初始化数据库连接
	var err error
	db, err = gorm.Open(mysql.Open(mysqlDSN), &gorm.Config{})
	if err != nil {
		panic(fmt.Sprintf("Failed to connect to MySQL: %v", err))
	}
	store = transactional.NewGormStore(db)

	// 初始化 Kafka Writer
	writer = kafka.NewWriter(kafka.WriterConfig{
		Brokers:   strings.Split(kafkaBrokers, ","),
		BatchSize: 1, // 测试时设为1，确保立即发送
	})

	// 运行包中的所有测试
	exitCode := m.Run()

	// 清理资源
	writer.Close()
	os.Exit(exitCode)
}

func TestTransactionalOutbox_EndToEnd(t *testing.T) {
	// --- 测试设置 ---
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	// 使用时间戳确保每次测试的 topic 唯一，避免测试间干扰
	testTopic := fmt.Sprintf("test.outbox.e2e.%d", time.Now().UnixNano())
	testKey := "order_123"
	testPayload := []byte(`{"message": "this is an end-to-end test"}`)

	kafkaBrokers := os.Getenv("KAFKA_BROKERS")

	// 创建 Kafka Reader 来消费消息进行验证
	reader := kafka.NewReader(kafka.ReaderConfig{
		Brokers:  strings.Split(kafkaBrokers, ","),
		Topic:    testTopic,
		GroupID:  "test-verifier",
		MinBytes: 1,
		MaxBytes: 10e6,
		// 从最早的消息开始读
		StartOffset: kafka.FirstOffset,
	})
	defer reader.Close()

	// 清理测试表，确保从干净的状态开始
	require.NoError(t, db.Exec("DELETE FROM transactional_messages").Error)

	// --- 核心测试逻辑 ---
	txService := transactional.NewService(store, writer)

	// 1. 在事务中保存消息
	var savedMsg transactional.Message
	err := db.Transaction(func(tx *gorm.DB) error {
		return txService.SendInTx(ctx, tx, testTopic, testKey, testPayload)
	})
	require.NoError(t, err)

	// 2. 验证消息已存入数据库，状态为 PENDING
	err = db.Where("topic = ?", testTopic).First(&savedMsg).Error
	require.NoError(t, err)
	assert.Equal(t, transactional.StatusPending, savedMsg.Status)
	assert.Equal(t, testPayload, savedMsg.Payload)

	// 3. 运行转发器逻辑
	err = txService.ForwardPendingMessages(ctx)
	require.NoError(t, err)

	// 4. 验证消息已发送到 Kafka
	// 从 Kafka 中读取消息，设置一个超时时间
	msg, err := reader.ReadMessage(ctx)
	require.NoError(t, err, "Failed to read message from Kafka")
	assert.Equal(t, testTopic, msg.Topic)
	assert.Equal(t, testKey, string(msg.Key))
	assert.Equal(t, testPayload, msg.Value)

	// 5. 验证数据库中的消息状态已更新为 SENT
	var finalMsg transactional.Message
	err = db.Where("id = ?", savedMsg.ID).First(&finalMsg).Error
	require.NoError(t, err)
	assert.Equal(t, transactional.StatusSent, finalMsg.Status)
}
