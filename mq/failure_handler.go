// internal/pkg/mq/failure_handler.go
package mq

import (
	"context"
	"fmt"
	"github.com/segmentio/kafka-go"
	"github.com/wangyingjie930/nexus-pkg/logger"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/codes"
	"go.opentelemetry.io/otel/trace"
	"strconv"
	"strings"
	"sync"
)

const (
	HeaderOriginalTopic       = "dlt-original-topic"
	HeaderOriginalPartition   = "dlt-original-partition"
	HeaderOriginalOffset      = "dlt-original-offset"
	HeaderExceptionFqcn       = "dlt-exception-fqcn"
	HeaderExceptionMessage    = "dlt-exception-message"
	HeaderExceptionStacktrace = "dlt-exception-stacktrace"
	HeaderRetryCount          = "retry-count"
)

type ResilienceConfig struct {
	Enabled             bool
	RetryDelays         []int
	RetryTopicTemplate  string
	DltTopicTemplate    string
	retryableExceptions map[string]struct{}
	RetryableExceptions []string
}

type FailureHandler struct {
	brokers []string
	config  ResilienceConfig
	tracer  trace.Tracer
	writers map[string]*kafka.Writer
	mu      sync.Mutex
}

func NewFailureHandler(brokers []string, config ResilienceConfig, tracer trace.Tracer) *FailureHandler {
	retryableSet := make(map[string]struct{})
	for _, ex := range config.RetryableExceptions {
		retryableSet[ex] = struct{}{}
	}
	config.RetryableExceptions = nil
	config.retryableExceptions = retryableSet

	return &FailureHandler{
		brokers: brokers,
		config:  config,
		tracer:  tracer,
		writers: make(map[string]*kafka.Writer),
	}
}

func (h *FailureHandler) Handle(ctx context.Context, originalMsg kafka.Message, err error) {
	if !h.config.Enabled {
		return // Resilience is disabled
	}

	_, span := h.tracer.Start(ctx, "FailureHandler.Handle")
	defer span.End()

	retryCount, _ := strconv.Atoi(getHeaderValue(originalMsg.Headers, HeaderRetryCount))

	isRetryable := h.isRetryable(err)
	maxRetries := len(h.config.RetryDelays)

	var targetTopic string
	baseTopic := getHeaderValue(originalMsg.Headers, HeaderOriginalTopic)
	if baseTopic == "" {
		baseTopic = originalMsg.Topic
	}

	if isRetryable && retryCount < maxRetries {
		// --- Handle Retry ---
		delay := h.config.RetryDelays[retryCount]
		targetTopic = strings.NewReplacer(
			"{topic}", baseTopic,
			"{delaySec}", strconv.Itoa(delay),
		).Replace(h.config.RetryTopicTemplate)
		span.SetAttributes(
			attribute.String("originalMsg.Topic", baseTopic),
			attribute.String("failure.action", "RETRY"),
			attribute.String("failure.target_topic", targetTopic),
		)
		retryCount++
	} else {
		// --- Handle DLT ---
		targetTopic = strings.NewReplacer(
			"{topic}", baseTopic,
		).Replace(h.config.DltTopicTemplate)
		span.SetAttributes(attribute.String("failure.action", "DLT"), attribute.String("failure.target_topic", targetTopic))
	}

	// Enrich headers and publish
	newMsg := h.prepareMessage(originalMsg, err, retryCount, baseTopic)

	writer := h.getWriter(targetTopic)
	logger.Ctx(ctx).Info().Any("targetTopic", targetTopic).Msg("failure.Writer")

	if writeErr := writer.WriteMessages(ctx, newMsg); writeErr != nil {
		span.RecordError(writeErr)
		span.SetStatus(codes.Error, "Failed to publish to failure topic")
		// Log critical error
	}
}

func (h *FailureHandler) getWriter(topic string) *kafka.Writer {
	h.mu.Lock()
	defer h.mu.Unlock()
	if writer, ok := h.writers[topic]; ok {
		return writer
	}
	// Create writer on-demand
	writer := NewKafkaWriter(h.brokers, topic)
	h.writers[topic] = writer
	return writer
}

func (h *FailureHandler) prepareMessage(original kafka.Message, err error, retryCount int, baseTopic string) kafka.Message {
	newHeaders := make([]kafka.Header, 0, len(original.Headers)+5)

	for _, header := range original.Headers {
		if header.Key != HeaderRetryCount {
			newHeaders = append(newHeaders, header)
		}
	}

	// Add/Update mandatory headers
	newHeaders = append(newHeaders, kafka.Header{Key: HeaderRetryCount, Value: []byte(strconv.Itoa(retryCount))})
	newHeaders = append(newHeaders, kafka.Header{Key: HeaderOriginalTopic, Value: []byte(baseTopic)})
	newHeaders = append(newHeaders, kafka.Header{Key: HeaderOriginalPartition, Value: []byte(strconv.Itoa(original.Partition))})
	newHeaders = append(newHeaders, kafka.Header{Key: HeaderOriginalOffset, Value: []byte(strconv.FormatInt(original.Offset, 10))})

	if err != nil {
		newHeaders = append(newHeaders, kafka.Header{Key: HeaderExceptionFqcn, Value: []byte(fmt.Sprintf("%T", err))})
		newHeaders = append(newHeaders, kafka.Header{Key: HeaderExceptionMessage, Value: []byte(err.Error())})
		// In a real scenario, you'd get a proper stack trace.
		newHeaders = append(newHeaders, kafka.Header{Key: HeaderExceptionStacktrace, Value: []byte("stacktrace not implemented")})
	}

	return kafka.Message{
		Key:     original.Key,
		Value:   original.Value,
		Headers: newHeaders,
	}
}

func (h *FailureHandler) isRetryable(err error) bool {
	if err == nil {
		return false
	}
	errMsg := err.Error()
	_, ok := h.config.retryableExceptions[errMsg]
	return ok
}

func getHeaderValue(headers []kafka.Header, key string) string {
	for _, h := range headers {
		if h.Key == key {
			return string(h.Value)
		}
	}
	return ""
}
