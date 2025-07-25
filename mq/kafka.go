// internal/mq/kafka.go
package mq

import (
	"context"
	"nexus/internal/pkg/logger"
	"time"

	"github.com/segmentio/kafka-go"
	"go.opentelemetry.io/otel"
)

// KafkaHeaderCarrier 实现了 opentelemetry.TextMapCarrier 接口
// 它允许我们将追踪上下文注入和提取到 Kafka 消息的 Header 中
type KafkaHeaderCarrier []kafka.Header

// Get 返回与给定键关联的值。
func (c KafkaHeaderCarrier) Get(key string) string {
	for _, h := range c {
		if h.Key == key {
			return string(h.Value)
		}
	}
	return ""
}

// Set 设置键值对。
func (c *KafkaHeaderCarrier) Set(key, value string) {
	// 检查键是否已存在，如果存在则更新
	for i := range *c {
		if (*c)[i].Key == key {
			(*c)[i].Value = []byte(value)
			return
		}
	}
	// 否则，添加新的 Header
	*c = append(*c, kafka.Header{Key: key, Value: []byte(value)})
}

// Keys 返回 carrier 中所有的键。
func (c KafkaHeaderCarrier) Keys() []string {
	keys := make([]string, len(c))
	for i, h := range c {
		keys[i] = h.Key
	}
	return keys
}

// NewKafkaWriter 创建一个新的 Kafka 生产者
func NewKafkaWriter(brokers []string, topic string) *kafka.Writer {
	return &kafka.Writer{
		Addr:     kafka.TCP(brokers...),
		Topic:    topic,
		Balancer: &kafka.LeastBytes{},
		// 关键改动：开启异步模式
		Async: true,
		// 可以配合异步模式调整批量参数，以提升吞吐量
		BatchSize:    100,
		BatchTimeout: 10 * time.Millisecond,
	}
}

// NewKafkaReader 创建一个新的 Kafka 消费者
func NewKafkaReader(brokers []string, topic, groupID string) *kafka.Reader {
	return kafka.NewReader(kafka.ReaderConfig{
		Brokers:        brokers,
		GroupID:        groupID,
		Topic:          topic,
		MinBytes:       10e3, // 10KB
		MaxBytes:       10e6, // 10MB
		CommitInterval: time.Second,
	})
}

// InjectTraceContext 将当前的 OpenTelemetry 追踪上下文注入到 Kafka 消息的 Headers 中
func InjectTraceContext(ctx context.Context, headers *[]kafka.Header) {
	propagator := otel.GetTextMapPropagator()
	carrier := KafkaHeaderCarrier(*headers)
	propagator.Inject(ctx, &carrier)
	*headers = carrier
}

// ExtractTraceContext 从 Kafka 消息的 Headers 中提取 OpenTelemetry 追踪上下文
func ExtractTraceContext(ctx context.Context, headers []kafka.Header) context.Context {
	propagator := otel.GetTextMapPropagator()
	carrier := KafkaHeaderCarrier(headers)
	return propagator.Extract(ctx, &carrier)
}

// ProduceMessage 向 Kafka 发送一条消息，并注入追踪上下文
func ProduceMessage(ctx context.Context, writer *kafka.Writer, key, value []byte) error {
	msg := kafka.Message{
		Key:   key,
		Value: value,
	}

	// 从当前上下文中注入追踪信息到消息头
	InjectTraceContext(ctx, &msg.Headers)

	logger.Ctx(ctx).Printf("Producing message to Kafka topic '%s', Trace context injected.", writer.Topic)

	return writer.WriteMessages(ctx, msg)
}
