// internal/tracing/tracer.go
package tracing

import (
	"context"
	"go.opentelemetry.io/otel/trace"
	"nexus/internal/pkg/logger"
	"time"

	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/exporters/jaeger"
	"go.opentelemetry.io/otel/propagation"
	"go.opentelemetry.io/otel/sdk/resource"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
	semconv "go.opentelemetry.io/otel/semconv/v1.26.0"
)

// InitTracerProvider initializes and registers a Jaeger TraceProvider.
func InitTracerProvider(serviceName, jaegerEndpoint string) (*sdktrace.TracerProvider, error) {
	// 创建 Jaeger Exporter，用于将 Span 数据发送到 Jaeger
	exporter, err := jaeger.New(jaeger.WithCollectorEndpoint(jaeger.WithEndpoint(jaegerEndpoint)))
	if err != nil {
		return nil, err
	}

	// 创建 TracerProvider，它是 OTel SDK 的核心组件
	tp := sdktrace.NewTracerProvider(
		// 始终对 Span 进行采样，在生产环境中应使用更复杂的采样策略
		sdktrace.WithSampler(sdktrace.AlwaysSample()),
		// 使用批处理 Span 处理器，提高性能
		sdktrace.WithBatcher(exporter,
			sdktrace.WithBatchTimeout(5*time.Second),
			sdktrace.WithMaxExportBatchSize(512),
		),
		// 设置服务名等资源属性，这对于在 Jaeger UI 中识别服务至关重要
		sdktrace.WithResource(resource.NewWithAttributes(
			semconv.SchemaURL,
			semconv.ServiceNameKey.String(serviceName),
		)),
	)

	// 将我们创建的 TracerProvider 设置为全局的
	otel.SetTracerProvider(tp)
	// 设置全局的 TextMapPropagator，用于在服务间传递上下文
	otel.SetTextMapPropagator(propagation.NewCompositeTextMapPropagator(propagation.TraceContext{}, propagation.Baggage{}))

	logger.Logger.Printf("Tracing initialized for service '%s' exporting to '%s'", serviceName, jaegerEndpoint)
	return tp, nil
}

// GetTraceIDFromContext 从 Context 中提取 Trace ID 字符串
func GetTraceIDFromContext(ctx context.Context) string {
	spanCtx := trace.SpanContextFromContext(ctx)
	if spanCtx.HasTraceID() {
		return spanCtx.TraceID().String()
	}
	return ""
}
