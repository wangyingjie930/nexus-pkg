// internal/pkg/bootstrap/app.go
package bootstrap

import (
	"context"
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
)

type AppCtx struct {
	Mux   *http.ServeMux
	Nacos *nacos.Client
}

// AppInfo 包含了启动一个微服务所需的所有特定信息。
type AppInfo struct {
	ServiceName      string
	Port             int
	RegisterHandlers func(appCtx AppCtx) // 一个函数，允许每个服务注册自己独特的 HTTP 路由
}

// StartService 封装了所有微服务的通用启动和优雅关停逻辑。
func StartService(info AppInfo) {
	logger.Init(info.ServiceName)

	serverConfigs, err := createNacosServerConfigs(nacosServerAddrs)
	if err != nil {
		logger.Logger.Fatal().Msgf("FATAL: Invalid Nacos server address format: %v", err)
	}
	clientConfig := createNacosClientConfig(nacosNamespace)

	// 2. 初始化核心组件
	// a. Tracer
	tp, err := tracing.InitTracerProvider(info.ServiceName, GetCurrentConfig().Infra.Jaeger.Endpoint)
	if err != nil {
		logger.Logger.Fatal().Msgf("failed to initialize tracer provider: %v", err)
	}

	namingClient, err := nacos.NewNacosClientWithConfigs(serverConfigs, &clientConfig, nacosGroup)
	if err != nil {
		logger.Logger.Fatal().Msgf("failed to initialize nacos client: %v", err)
	}

	// 3. 获取本机 IP 用于注册
	ip, err := utils.GetOutboundIP()
	if err != nil {
		logger.Logger.Fatal().Msgf("failed to get outbound IP address: %v", err)
	}

	// 4. 执行服务注册
	err = namingClient.RegisterServiceInstance(info.ServiceName, ip, info.Port)
	if err != nil {
		logger.Logger.Fatal().Msgf("failed to register service with nacos: %v", err)
	}

	// 5. 创建并启动 HTTP Server
	mux := http.NewServeMux()
	if info.RegisterHandlers != nil {
		info.RegisterHandlers(AppCtx{Mux: mux, Nacos: namingClient})
	}
	server := &http.Server{Addr: ":" + strconv.Itoa(info.Port), Handler: mux}
	go func() {
		logger.Logger.Printf("%s listening on :%d", info.ServiceName, info.Port)
		if err := server.ListenAndServe(); err != nil && err != http.ErrServerClosed {
			logger.Logger.Fatal().Msgf("could not listen on %s: %v\n", server.Addr, err)
		}
	}()

	// 6. 优雅关停
	quit := make(chan os.Signal, 1)
	signal.Notify(quit, syscall.SIGINT, syscall.SIGTERM)

	// 阻塞主 goroutine，直到接收到退出信号
	<-quit
	logger.Logger.Printf("Shutting down service %s...", info.ServiceName)

	// 创建一个有超时的 context，用于关停流程
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	// 8. 在关停流程中，按顺序执行清理操作 (后进先出)
	// a. 从 Nacos 注销服务
	if err := namingClient.DeregisterServiceInstance(info.ServiceName, ip, info.Port); err != nil {
		logger.Logger.Printf("Error deregistering from Nacos: %v", err)
	} else {
		logger.Logger.Printf("Service %s deregistered from Nacos.", info.ServiceName)
	}

	if nacosConfigClient != nil {
		nacosConfigClient.CloseClient()
	}

	// b. 关闭 Tracer Provider，确保所有缓冲的 trace 都被发送出去
	if err := tp.Shutdown(ctx); err != nil {
		logger.Logger.Printf("Error shutting down tracer provider: %v", err)
	} else {
		logger.Logger.Printf("Tracer provider shut down.")
	}

	// c. 关闭 HTTP 服务器
	if err := server.Shutdown(ctx); err != nil {
		logger.Logger.Printf("Error shutting down http server: %v", err)
	} else {
		logger.Logger.Printf("HTTP server shut down.")
	}

	logger.Logger.Printf("Service %s gracefully shut down.", info.ServiceName)
}
