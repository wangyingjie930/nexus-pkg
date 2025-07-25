package bootstrap

import (
	"context"
	"errors"
	"fmt"
	"github.com/wangyingjie930/nexus-pkg/logger"
	"github.com/wangyingjie930/nexus-pkg/nacos"
	"github.com/wangyingjie930/nexus-pkg/tracing"
	"github.com/wangyingjie930/nexus-pkg/utils"
	"net/http"
	"os"
	"os/signal"
	"strconv"
	"syscall"
	"time"

	"golang.org/x/sync/errgroup"

	"github.com/nacos-group/nacos-sdk-go/v2/clients/config_client"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
)

// AppContext 包含了在组装阶段可以使用的核心依赖。
// 它由引导程序创建并传递给业务组装逻辑。
type AppContext struct {
	NamingClient   *nacos.Client
	TracerProvider *sdktrace.TracerProvider
}

// AppInfoV2 描述了如何构建和运行一个服务。
// 它是一个泛型结构，允许每个服务定义自己独特的依赖集合。
type AppInfoV2[T any] struct {
	ServiceName string
	// Assemble 负责使用 AppContext 创建并组装所有业务依赖。
	// 这是整个应用的“组装根”（Composition Root）。
	Assemble func(appCtx AppContext) (T, error)
	// Register 负责将组装好的业务依赖注册到应用生命周期中，
	// 例如启动HTTP服务器、启动Kafka消费者等。
	Register func(app *Application, deps T) error
}

// Application 是管理整个服务生命周期的核心结构体。
type Application struct {
	info        any
	serviceName string
	nacosConfig config_client.IConfigClient
	nacosNaming *nacos.Client

	tracer     *sdktrace.TracerProvider
	httpServer *http.Server

	g              *errgroup.Group
	shutdownCtx    context.Context
	shutdownCancel context.CancelFunc
}

// NewApplication 是应用的构造函数，负责完成所有组件的初始化、组装和注册。
func NewApplication[T any](info AppInfoV2[T]) (*Application, error) {
	// 1. 初始化最底层的配置，并获取 Nacos Config Client
	Init()

	// 1.1 初始化日志
	logger.Init(info.ServiceName)

	// 2. 初始化 Tracer Provider
	tp, err := tracing.InitTracerProvider(info.ServiceName, GetCurrentConfig().Infra.Jaeger.Endpoint)
	if err != nil {
		return nil, fmt.Errorf("failed to init tracer: %w", err)
	}

	serverConfigs, err := createNacosServerConfigs(nacosServerAddrs)
	if err != nil {
		logger.Logger.Fatal().Err(err).Msgf("FATAL: Invalid Nacos server address")
	}
	clientConfig := createNacosClientConfig(nacosNamespace)

	namingClient, err := nacos.NewNacosClientWithConfigs(serverConfigs, &clientConfig, nacosGroup)
	if err != nil {
		logger.Logger.Fatal().Err(err).Msgf("failed to initialize nacos client: %v", err)
	}

	// 4. 创建 Application 实例
	app := &Application{
		info:        info,
		serviceName: info.ServiceName,
		nacosConfig: nacosConfigClient,
		nacosNaming: namingClient,
		tracer:      tp,
	}
	app.shutdownCtx, app.shutdownCancel = context.WithCancel(context.Background())
	app.g, _ = errgroup.WithContext(app.shutdownCtx)

	// 5. 调用业务方的 Assemble 函数，组装所有业务依赖
	deps, err := info.Assemble(AppContext{
		NamingClient:   app.nacosNaming,
		TracerProvider: app.tracer,
	})
	if err != nil {
		return nil, fmt.Errorf("failed to assemble dependencies: %w", err)
	}

	// 6. 调用业务方的 Register 函数，注册所有需要运行的服务
	if err := info.Register(app, deps); err != nil {
		return nil, fmt.Errorf("failed to register services: %w", err)
	}

	// 7. 最后，注册核心组件自身的优雅关停逻辑
	app.addCoreShutdownTasks()

	return app, nil
}

// AddServer 注册一个需要优雅关停的 HTTP 服务器，并将其与 Nacos 服务发现集成。
func (app *Application) AddServer(mux *http.ServeMux, port int) error {
	serviceName := app.serviceName
	ip, err := utils.GetOutboundIP()
	if err != nil {
		return fmt.Errorf("failed to get outbound IP for service %s: %w", serviceName, err)
	}

	app.httpServer = &http.Server{
		Addr:    ":" + strconv.Itoa(port),
		Handler: mux,
	}

	// 启动 HTTP 服务器前，先向 Nacos 注册
	logger.Logger.Printf("Registering service '%s' to Nacos...", serviceName)
	if err := app.nacosNaming.RegisterServiceInstance(serviceName, ip, port); err != nil {
		return fmt.Errorf("failed to register '%s' with nacos: %w", serviceName, err)
	}
	logger.Logger.Printf("✅ Service '%s' registered to Nacos successfully (%s:%d)", serviceName, ip, port)

	// 将 HTTP 服务器的启动和关闭纳入 errgroup 的管理
	app.g.Go(func() error {
		logger.Logger.Printf("✅ HTTP server for '%s' listening on :%d", serviceName, port)
		if err := app.httpServer.ListenAndServe(); err != nil && !errors.Is(err, http.ErrServerClosed) {
			return fmt.Errorf("http server error for '%s': %w", serviceName, err)
		}
		return nil
	})

	app.g.Go(func() error {
		<-app.shutdownCtx.Done() // 等待关停信号
		logger.Logger.Printf("Shutting down HTTP server for '%s'...", serviceName)

		// 创建一个有超时的上下文用于关停
		shutdownTimeoutCtx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()

		// 先从 Nacos 注销
		if err := app.nacosNaming.DeregisterServiceInstance(serviceName, ip, port); err != nil {
			logger.Logger.Fatal().Msgf("❌ Error deregistering '%s' from Nacos: %v", serviceName, err)
			// 即使注销失败，也要继续关闭服务器，但记录错误
		} else {
			logger.Logger.Printf("✅ Service '%s' deregistered from Nacos.", serviceName)
		}

		// 再关闭 HTTP 服务器
		return app.httpServer.Shutdown(shutdownTimeoutCtx)
	})

	return nil
}

// AddTask 注册一个通用的后台任务，并管理其生命周期。
// start: 启动任务的函数。它接收一个上下文，当该上下文被取消时，任务应停止。
// stop:  （可选）关闭任务的函数，用于释放资源。
func (app *Application) AddTask(start func(ctx context.Context) error, stop func(ctx context.Context) error) {
	if start != nil {
		app.g.Go(func() error {
			return start(app.shutdownCtx)
		})
	}

	if stop != nil {
		app.g.Go(func() error {
			<-app.shutdownCtx.Done() // 等待关停信号
			logger.Logger.Println("Stopping background task...")
			// 为关停操作也设置一个超时
			timeoutCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
			defer cancel()
			return stop(timeoutCtx)
		})
	}
}

// addCoreShutdownTasks 注册核心基础设施组件的关停任务。
func (app *Application) addCoreShutdownTasks() {
	app.AddTask(nil, func(ctx context.Context) error {
		logger.Logger.Printf("Closing Nacos clients...")
		nacosConfigClient.CloseClient()
		app.nacosNaming.Close()
		logger.Logger.Printf("✅ Nacos clients closed.")
		return nil
	})
	app.AddTask(nil, func(ctx context.Context) error {
		logger.Logger.Printf("Shutting down tracer provider...")
		if err := app.tracer.Shutdown(ctx); err != nil {
			return err
		}
		logger.Logger.Printf("✅ Tracer provider shut down.")
		return nil
	})
}

// Run 启动整个应用，并阻塞等待关停信号。
func (app *Application) Run() error {
	// 启动一个 goroutine 来监听操作系统的中断信号
	quit := make(chan os.Signal, 1)
	signal.Notify(quit, syscall.SIGINT, syscall.SIGTERM)

	app.g.Go(func() error {
		select {
		case <-app.shutdownCtx.Done():
			return nil // 由其他任务触发的关停
		case sig := <-quit:
			logger.Logger.Printf("Received signal '%v', initiating graceful shutdown...", sig)
			app.shutdownCancel() // 触发所有任务的关停
		}
		return nil
	})

	serviceName := app.serviceName
	logger.Logger.Printf("🚀 Application '%s' started. Waiting for tasks to complete or shutdown signal...", serviceName)

	// 等待所有由 errgroup 管理的 goroutine 完成
	if err := app.g.Wait(); err != nil && !errors.Is(err, context.Canceled) {
		logger.Logger.Error().Msgf("❌ Application run failed with error: %v", err)
		return err
	}

	logger.Logger.Printf("✅ Application '%s' gracefully shut down.", app.serviceName)
	return nil
}
