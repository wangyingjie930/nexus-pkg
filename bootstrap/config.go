package bootstrap

import (
	"fmt"
	"github.com/nacos-group/nacos-sdk-go/v2/clients"
	"github.com/nacos-group/nacos-sdk-go/v2/clients/config_client"
	"github.com/nacos-group/nacos-sdk-go/v2/common/constant"
	"github.com/nacos-group/nacos-sdk-go/v2/vo"
	"github.com/wangyingjie930/nexus-pkg/logger"
	"gopkg.in/yaml.v3"
	"os"
	"strconv"
	"strings"
	"sync"
)

type InfraConfig struct {
	Kafka struct {
		Brokers string `yaml:"brokers"`
	} `yaml:"kafka"`
	Redis struct {
		Addrs string `yaml:"addrs"`
	} `yaml:"redis"`
	Jaeger struct {
		Endpoint string `yaml:"endpoint"`
	} `yaml:"jaeger"`
	Zookeeper struct {
		Addrs string `yaml:"addrs"`
	} `yaml:"zookeeper"`
	Mysql struct {
		Addrs string `yaml:"addrs"`
	}
}

// AppConfig 存放业务逻辑配置
type AppConfig struct {
	OrderService struct {
		ProcessingTimeoutSeconds int `yaml:"processingTimeoutSeconds"`
		PaymentTimeoutSeconds    int `yaml:"paymentTimeoutSeconds"`
	} `yaml:"orderService"`
	FeatureFlags struct {
		EnableVipPromotion bool `yaml:"enableVipPromotion"`
	} `yaml:"featureFlags"`

	Resilience ResilienceConfig `yaml:"resilience"`
}

// ResilienceConfig 结构体
type ResilienceConfig struct {
	Consumers map[string]ConsumerResilienceConfig `yaml:"consumers"`
}

// ConsumerResilienceConfig 结构体
type ConsumerResilienceConfig struct {
	Enabled             bool     `yaml:"enabled"`
	RetryDelays         []int    `yaml:"retryDelays"` // in seconds
	RetryTopicTemplate  string   `yaml:"retryTopicTemplate"`
	DltTopicTemplate    string   `yaml:"dltTopicTemplate"`
	RetryableExceptions []string `yaml:"retryableExceptions"`
}

// Config 是整个应用唯一的全局配置入口
type Config struct {
	Infra InfraConfig
	App   AppConfig
}

var (
	// 全局配置实例
	GlobalConfig = new(Config)
	// 用于保护全局配置的读写
	configLock = new(sync.RWMutex)
	// Nacos 配置客户端，在Init中创建，在StartService的优雅关停中关闭
	nacosConfigClient config_client.IConfigClient

	nacosServerAddrs string
	nacosNamespace   string
	nacosGroup       string
)

// Init 是应用启动的第一步，负责加载所有配置
func Init() {
	logger.Init("bootstrap")

	// 1. 获取最基础的引导配置 (Nacos地址)
	nacosServerAddrs = getEnv("NACOS_SERVER_ADDRS", "localhost:8848")
	nacosNamespace = getEnv("NACOS_NAMESPACE", "")
	nacosGroup = getEnv("NACOS_GROUP", "DEFAULT_GROUP")

	// 2. 创建 Nacos 客户端配置
	serverConfigs, err := createNacosServerConfigs(nacosServerAddrs)
	if err != nil {
		logger.Logger.Fatal().Msgf("FATAL: Invalid Nacos server address format: %v", err)
	}
	clientConfig := createNacosClientConfig(nacosNamespace)

	// 3. 创建 Nacos 配置客户端
	nacosConfigClient, err = clients.NewConfigClient(
		vo.NacosClientParam{
			ClientConfig:  &clientConfig,
			ServerConfigs: serverConfigs,
		},
	)
	if err != nil {
		logger.Logger.Fatal().Msgf("FATAL: Failed to create Nacos config client: %v", err)
	}

	// 4. 拉取并监听两个配置文件
	// a. 基础设施配置
	initAndWatchSingleConfig("nexus-infra.yaml", nacosGroup, &GlobalConfig.Infra)
	// b. 应用业务配置
	initAndWatchSingleConfig("nexus-app.yaml", nacosGroup, &GlobalConfig.App)

	logger.Logger.Info().Any("GlobalConfig", GlobalConfig).Msg("✅ Bootstrap Phase 1: All configurations loaded and watched successfully.")
}

// GetCurrentConfig 返回一个线程安全的配置副本
func GetCurrentConfig() Config {
	configLock.RLock()
	defer configLock.RUnlock()
	return *GlobalConfig
}

// initAndWatchSingleConfig 是一个通用函数，用于拉取、解析和监听单个配置文件
func initAndWatchSingleConfig(dataId, group string, configPtr interface{}) {
	content, err := nacosConfigClient.GetConfig(vo.ConfigParam{DataId: dataId, Group: group})
	if err != nil {
		logger.Logger.Fatal().Msgf("FATAL: Failed to get initial config for DataId '%s': %v", dataId, err)
	}

	updateConfig(content, configPtr) // 加载初始配置

	err = nacosConfigClient.ListenConfig(vo.ConfigParam{
		DataId: dataId,
		Group:  group,
		OnChange: func(_, _, _, data string) {
			logger.Logger.Printf("🔔 Nacos config changed for DataId: %s. Applying new config...", dataId)
			updateConfig(data, configPtr)
		},
	})
	if err != nil {
		logger.Logger.Fatal().Msgf("FATAL: Failed to listen config for DataId '%s': %v", dataId, err)
	}
}

// updateConfig 线程安全地更新配置
func updateConfig(content string, configPtr interface{}) {
	configLock.Lock()
	defer configLock.Unlock()
	if err := yaml.Unmarshal([]byte(content), configPtr); err != nil {
		logger.Logger.Printf("❌ ERROR: Failed to unmarshal Nacos config: %v", err)
	}
}

// ✨ 新增: Nacos ServerConfig 工厂函数
func createNacosServerConfigs(addrs string) ([]constant.ServerConfig, error) {
	var serverConfigs []constant.ServerConfig
	for _, addr := range strings.Split(addrs, ",") {
		parts := strings.Split(addr, ":")
		if len(parts) != 2 {
			return nil, fmt.Errorf("invalid address format: %s", addr)
		}
		port, err := strconv.ParseUint(parts[1], 10, 64)
		if err != nil {
			return nil, fmt.Errorf("invalid port: %s", parts[1])
		}
		serverConfigs = append(serverConfigs, *constant.NewServerConfig(parts[0], port))
	}
	return serverConfigs, nil
}

// ✨ 新增: Nacos ClientConfig 工厂函数
func createNacosClientConfig(namespaceId string) constant.ClientConfig {
	return *constant.NewClientConfig(
		constant.WithNamespaceId(namespaceId),
		constant.WithTimeoutMs(5000),
		constant.WithNotLoadCacheAtStart(true),
		constant.WithLogDir("/tmp/nacos/log"),
		constant.WithCacheDir("/tmp/nacos/cache"),
		constant.WithLogLevel("warn"),
	)
}

// getEnv 是一个内部辅助函数，从环境变量中读取配置。
func getEnv(key, fallback string) string {
	if value, ok := os.LookupEnv(key); ok {
		return value
	}
	return fallback
}
