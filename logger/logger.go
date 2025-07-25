package logger

import (
	"context"
	"github.com/rs/zerolog"
	"go.opentelemetry.io/otel/trace"
	"os"
)

// Logger 是一个全局的、配置好的 zerolog 实例
var Logger zerolog.Logger

func Init(serviceName string) {
	// zerolog 的一些默认配置，以实现更佳的性能和结构
	zerolog.TimeFieldFormat = zerolog.TimeFormatUnixMs // 使用毫秒级时间戳
	zerolog.LevelFieldName = "level"
	zerolog.MessageFieldName = "msg"
	zerolog.TimestampFieldName = "ts"

	// 创建一个带有一致性字段的 Logger 实例
	// 在真实的生产环境中，可以从配置中读取服务名
	Logger = zerolog.New(os.Stdout).With().
		Timestamp().
		Str("service_name", serviceName). // 从环境变量获取服务名
		Logger()
}

// Ctx 返回一个带有从 context 中提取的追踪信息的子 logger。
// 这是将日志与链路追踪关联起来的关键。
func Ctx(ctx context.Context) *zerolog.Logger {
	log := Logger // 从全局 logger 开始

	// 从 context 中获取 Span，并提取 TraceID 和 SpanID
	if span := trace.SpanFromContext(ctx); span.SpanContext().IsValid() {
		log = log.With().
			Str("trace_id", span.SpanContext().TraceID().String()).
			Str("span_id", span.SpanContext().SpanID().String()).
			Logger()
	}
	return &log
}
