// internal/pkg/bootstrap/app.go
package bootstrap

import (
	"context"
	"github.com/nacos-group/nacos-sdk-go/v2/clients/config_client"
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
// 调用者现在必须先调用 Load() 来加载配置，然后将配置实例和 Nacos 客户端（如果存在）传入。
func StartService(info AppInfo, cfg Config, nacosConfigClient config_client.IConfigClient) {
	logger.Init(info.ServiceName)

	var namingClient *nacos.Client
	var err error

	// 检查是否处于 Nacos 模式 (通过 nacosConfigClient 是否为 nil 判断)
	isNacosMode := nacosConfigClient != nil

	if isNacosMode {
		logger.Logger.Info().Msg("Nacos integration is enabled.")
		// 从环境中读取 Nacos 连接信息来创建 Naming 客户端
		nacosServerAddrs := getEnv("NACOS_SERVER_ADDRS", "localhost:8848")
		nacosNamespace := getEnv("NACOS_NAMESPACE", "")
		nacosGroup := getEnv("NACOS_GROUP", "DEFAULT_GROUP")

		serverConfigs, err := createNacosServerConfigs(nacosServerAddrs)
		if err != nil {
			logger.Logger.Fatal().Msgf("FATAL: Invalid Nacos server address format: %v", err)
		}
		clientConfig := createNacosClientConfig(nacosNamespace)
		namingClient, err = nacos.NewNacosClientWithConfigs(serverConfigs, &clientConfig, nacosGroup)
		if err != nil {
			logger.Logger.Fatal().Msgf("failed to initialize nacos client: %v", err)
		}
	} else {
		logger.Logger.Info().Msg("Nacos integration is disabled (local mode).")
	}

	// 初始化 Tracer
	tp, err := tracing.InitTracerProvider(info.ServiceName, cfg.GetInfra().Jaeger.Endpoint)
	if err != nil {
		logger.Logger.Fatal().Msgf("failed to initialize tracer provider: %v", err)
	}

	// 只有在 Nacos 模式下才获取IP并注册服务
	var ip string
	if isNacosMode && namingClient != nil {
		ip, err = utils.GetOutboundIP()
		if err != nil {
			logger.Logger.Fatal().Msgf("failed to get outbound IP address: %v", err)
		}
		err = namingClient.RegisterServiceInstance(info.ServiceName, ip, info.Port)
		if err != nil {
			logger.Logger.Fatal().Msgf("failed to register service with nacos: %v", err)
		}
	}

	// 创建并启动 HTTP Server
	mux := http.NewServeMux()
	if info.RegisterHandlers != nil {
		// 即使Nacos为nil，也要将它传递下去，让业务代码决定如何处理
		info.RegisterHandlers(AppCtx{Mux: mux, Nacos: namingClient})
	}
	server := &http.Server{Addr: ":" + strconv.Itoa(info.Port), Handler: mux}
	go func() {
		logger.Logger.Printf("%s listening on :%d", info.ServiceName, info.Port)
		if err := server.ListenAndServe(); err != nil && err != http.ErrServerClosed {
			logger.Logger.Fatal().Msgf("could not listen on %s: %v\n", server.Addr, err)
		}
	}()

	// 优雅关停
	quit := make(chan os.Signal, 1)
	signal.Notify(quit, syscall.SIGINT, syscall.SIGTERM)

	<-quit
	logger.Logger.Printf("Shutting down service %s...", info.ServiceName)

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	// 只有在 Nacos 模式下才执行注销和关闭客户端
	if isNacosMode && namingClient != nil {
		if err := namingClient.DeregisterServiceInstance(info.ServiceName, ip, info.Port); err != nil {
			logger.Logger.Printf("Error deregistering from Nacos: %v", err)
		} else {
			logger.Logger.Printf("Service %s deregistered from Nacos.", info.ServiceName)
		}
		// 关闭由 Load() 函数创建并传入的 Nacos Config Client
		if nacosConfigClient != nil {
			nacosConfigClient.Close()
		}
	}

	// 关闭 Tracer Provider
	if err := tp.Shutdown(ctx); err != nil {
		logger.Logger.Printf("Error shutting down tracer provider: %v", err)
	} else {
		logger.Logger.Printf("Tracer provider shut down.")
	}

	// 关闭 HTTP Server
	if err := server.Shutdown(ctx); err != nil {
		logger.Logger.Printf("Error shutting down http server: %v", err)
	} else {
		logger.Logger.Printf("HTTP server shut down.")
	}

	logger.Logger.Printf("Service %s gracefully shut down.", info.ServiceName)
}
