package transactional

import (
	"context"
	"github.com/segmentio/kafka-go"
	"github.com/wangyingjie930/nexus-pkg/logger"
	"github.com/wangyingjie930/nexus-pkg/mq"
	"go.opentelemetry.io/otel"
	"gorm.io/gorm"
)

// Service 封装了事务性消息的核心逻辑
type Service struct {
	store  Store
	writer *kafka.Writer // 复用 Kafka 生产者
}

// NewService 创建一个新的事务性消息服务
func NewService(store Store, writer *kafka.Writer) *Service {
	return &Service{
		store:  store,
		writer: writer,
	}
}

// SendInTx 在业务事务中保存待发送的消息。
// 这是给业务代码调用的核心方法。
func (s *Service) SendInTx(ctx context.Context, tx *gorm.DB, topic, key string, payload []byte) error {
	msg := &Message{
		Topic:   topic,
		Key:     key,
		Payload: payload,
		Status:  StatusPending,
	}

	// 将消息的创建操作包含在业务方的DB事务中
	return s.store.CreateInTx(ctx, tx, msg)
}

// ForwardPendingMessages 查找并转发待处理的消息
// 这个方法应该被一个后台任务周期性地调用
func (s *Service) ForwardPendingMessages(ctx context.Context) error {
	log := logger.Ctx(ctx)

	// 1. 查找待发送的消息
	messages, err := s.store.FindPendingMessages(ctx, 100) // 每次最多处理100条
	if err != nil {
		log.Error().Err(err).Msg("failed to find pending messages")
		return err
	}

	if len(messages) == 0 {
		return nil // 没有待处理消息
	}

	log.Info().Int("count", len(messages)).Msg("found pending transactional messages to forward")

	// 2. 遍历并发送
	for _, msg := range messages {
		// 构造 Kafka 消息
		kafkaMsg := kafka.Message{
			Topic: msg.Topic,
			Key:   []byte(msg.Key),
			Value: msg.Payload,
		}

		// 注入 OpenTelemetry trace context，实现全链路追踪
		// 注意这里我们从后台任务的context中创建新的追踪信息
		tracer := otel.Tracer("transactional-forwarder")
		spanCtx, span := tracer.Start(ctx, "forward_message")
		mq.InjectTraceContext(spanCtx, &kafkaMsg.Headers)

		// 3. 发送消息
		err := s.writer.WriteMessages(spanCtx, kafkaMsg)
		span.End()

		// 4. 更新消息状态
		if err != nil {
			log.Error().Err(err).Int64("msg_id", msg.ID).Msg("failed to write message to kafka")
			// 简单地增加重试次数，可以引入更复杂的重试策略（如指数退避）
			// 当重试次数超过阈值时，可以标记为 FAILED
			_ = s.store.UpdateStatus(ctx, msg.ID, StatusPending, msg.RetryCount+1)
		} else {
			log.Info().Int64("msg_id", msg.ID).Str("topic", msg.Topic).Msg("successfully forwarded message")
			_ = s.store.UpdateStatus(ctx, msg.ID, StatusSent, msg.RetryCount)
		}
	}

	return nil
}
