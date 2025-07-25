// internal/pkg/httpclient/client.go

package httpclient

import (
	"context"
	"fmt"
	"net/http"
	"net/url"
	"nexus/internal/pkg/nacos"
	"strings"

	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/codes"
	"go.opentelemetry.io/otel/propagation"
	"go.opentelemetry.io/otel/trace"
)

// Client 是一个可追踪的、可注入的HTTP客户端
type Client struct {
	Tracer      trace.Tracer
	HTTPClient  *http.Client  // ✨ [新增] 持有一个可复用的HTTP客户端实例
	NacosClient *nacos.Client // ✨ 2. 新增 Nacos 客户端实例
}

// NewClient 创建一个新的客户端实例
func NewClient(tracer trace.Tracer, ncClient *nacos.Client) *Client {
	// ✨ [改造] 在这里创建 http.Client，并且不设置 Timeout 字段
	// 让其完全受控于每次请求传入的 context
	httpClient := &http.Client{
		// 我们可以配置 Transport 来自定义连接池等行为
		Transport: &http.Transport{
			MaxIdleConns:        100,
			MaxIdleConnsPerHost: 100,
		},
	}
	return &Client{
		Tracer:      tracer,
		HTTPClient:  httpClient,
		NacosClient: ncClient,
	}
}

// Post 是 callService 的重构版本，作为 Client 的一个方法
func (c *Client) Post(ctx context.Context, serviceURL string, params url.Values) error {
	parsedURL, err := url.Parse(serviceURL)
	if err != nil {
		return err
	}
	// 从 URL 中解析出服务名用于 Span
	spanName := fmt.Sprintf("call-%s", strings.Split(parsedURL.Host, ":")[0])

	ctx, span := c.Tracer.Start(ctx, spanName, trace.WithSpanKind(trace.SpanKindClient))
	defer span.End()

	downstreamURL := *parsedURL
	q := downstreamURL.Query()
	for key, values := range params {
		for _, value := range values {
			q.Add(key, value)
		}
	}
	downstreamURL.RawQuery = q.Encode()

	req, err := http.NewRequestWithContext(ctx, "POST", downstreamURL.String(), nil)
	if err != nil {
		span.RecordError(err)
		return err
	}

	span.SetAttributes(
		attribute.String("http.url", downstreamURL.String()),
		attribute.String("http.method", "POST"),
	)
	otel.GetTextMapPropagator().Inject(ctx, propagation.HeaderCarrier(req.Header))

	resp, err := c.HTTPClient.Do(req)
	if err != nil {
		span.RecordError(err)
		span.SetStatus(codes.Error, err.Error())
		return err
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		err := fmt.Errorf("service %s returned status %s", serviceURL, resp.Status)
		span.RecordError(err)
		span.SetStatus(codes.Error, err.Error())
		return err
	}
	return nil
}

// CallService 方法现在通过服务名进行调用
// serviceName: 要调用的服务名, e.g., "inventory-service"
// requestPath: 具体的请求路径, e.g., "/reserve_stock"
func (c *Client) CallService(ctx context.Context, serviceName, requestPath string, params url.Values) error {
	// ✨ 5. 核心改造：通过 Nacos 发现服务实例
	instanceIP, instancePort, err := c.NacosClient.DiscoverServiceInstance(serviceName)
	if err != nil {
		// 服务发现失败是严重错误，直接返回
		return fmt.Errorf("failed to discover service '%s': %w", serviceName, err)
	}

	// 动态构建下游服务的 URL，将参数作为查询参数
	serviceURL := fmt.Sprintf("http://%s:%d%s", instanceIP, instancePort, requestPath)

	// 将参数添加到URL查询字符串中
	if len(params) > 0 {
		serviceURL += "?" + params.Encode()
	}

	// 从 serviceName 中解析出服务名用于 Span
	spanName := fmt.Sprintf("call-%s", serviceName)

	ctx, span := c.Tracer.Start(ctx, spanName, trace.WithSpanKind(trace.SpanKindClient))
	defer span.End()

	// 将发现的实例信息记录到 Span 中，便于追踪和调试
	span.SetAttributes(
		attribute.String("net.peer.name", instanceIP),
		attribute.Int("net.peer.port", instancePort),
		attribute.String("service.name.discovered", serviceName),
	)

	req, err := http.NewRequestWithContext(ctx, "POST", serviceURL, nil)
	if err != nil {
		span.RecordError(err)
		return err
	}

	span.SetAttributes(
		attribute.String("http.url", serviceURL),
		attribute.String("http.method", "POST"),
	)
	otel.GetTextMapPropagator().Inject(ctx, propagation.HeaderCarrier(req.Header))

	resp, err := c.HTTPClient.Do(req)
	if err != nil {
		span.RecordError(err)
		span.SetStatus(codes.Error, err.Error())
		return err
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		err := fmt.Errorf("service %s returned status %s", serviceURL, resp.Status)
		span.RecordError(err)
		span.SetStatus(codes.Error, err.Error())
		return err
	}
	return nil
}
