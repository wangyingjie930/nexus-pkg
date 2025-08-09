package bootstrap

import (
	"fmt"
	"os"
	"strconv"
	"strings"
	"sync"

	"github.com/nacos-group/nacos-sdk-go/v2/clients"
	"github.com/nacos-group/nacos-sdk-go/v2/clients/config_client"
	"github.com/nacos-group/nacos-sdk-go/v2/common/constant"
	"github.com/nacos-group/nacos-sdk-go/v2/vo"
	"github.com/wangyingjie930/nexus-pkg/logger"
	"gopkg.in/yaml.v3"
)

// Config 是一个接口，定义了框架所需的最小配置集。
// 使用者可以通过在自己的配置结构体中嵌入 BaseConfig 来轻松实现此接口。
type Config interface {
	GetInfra() *InfraConfig
	GetApp() *AppConfig
}

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

// BaseConfig 是一个基础配置结构体，提供了框架所需的基本字段。
// 使用者应该将此结构体嵌入到他们自己的自定义配置结构体中。
type BaseConfig struct {
	Infra InfraConfig `yaml:"infra"`
	App   AppConfig   `yaml:"app"`
}

// GetInfra 实现了 Config 接口
func (c *BaseConfig) GetInfra() *InfraConfig {
	return &c.Infra
}

// GetApp 实现了 Config 接口
func (c *BaseConfig) GetApp() *AppConfig {
	return &c.App
}

// Load 是应用启动时加载配置的新入口。
// 它取代了旧的全局 Init() 函数。
// configHolder 必须是一个指针，指向一个嵌入了 BaseConfig 的自定义结构体。
func Load(configHolder interface{}) (config_client.IConfigClient, error) {
	logger.Init("bootstrap")

	// 优先尝试从本地文件加载
	configPath := getEnv("NEXUS_CONFIG_PATH", "")
	if configPath != "" {
		logger.Logger.Info().Msgf("Attempting to load configuration from file: %s", configPath)
		err := loadConfigFromFile(configPath, configHolder)
		if err == nil {
			logger.Logger.Info().Msg("✅ Configuration loaded successfully from file.")
			return nil, nil // 从文件加载时，不返回 Nacos 客户端
		} else {
			logger.Logger.Warn().Err(err).Msgf("⚠️ Failed to load configuration from file, falling back to Nacos...")
			return nil, err
		}
	}

	// 回退到 Nacos
	logger.Logger.Info().Msg("Loading configuration from Nacos...")
	return initFromNacos(configHolder)
}

// loadConfigFromFile 从单个 YAML 文件加载整个配置。
func loadConfigFromFile(filePath string, configHolder interface{}) error {
	content, err := os.ReadFile(filePath)
	if err != nil {
		return fmt.Errorf("failed to read config file %s: %w", filePath, err)
	}

	if err := yaml.Unmarshal(content, configHolder); err != nil {
		return fmt.Errorf("failed to unmarshal config file: %w", err)
	}

	logger.Logger.Info().Any("config", configHolder).Msg("✅ Bootstrap: Configuration loaded from file.")
	return nil
}

// initFromNacos 从 Nacos 初始化配置。
func initFromNacos(configHolder interface{}) (config_client.IConfigClient, error) {
	// 确保 configHolder 实现了 Config 接口，否则无法进行后续操作
	cfg, ok := configHolder.(Config)
	if !ok {
		return nil, fmt.Errorf("configHolder must implement the bootstrap.Config interface")
	}

	// 1. 获取 Nacos 连接配置
	nacosServerAddrs := getEnv("NACOS_SERVER_ADDRS", "localhost:8848")
	nacosNamespace := getEnv("NACOS_NAMESPACE", "")
	nacosGroup := getEnv("NACOS_GROUP", "DEFAULT_GROUP")

	// 2. 创建 Nacos 客户端配置
	serverConfigs, err := createNacosServerConfigs(nacosServerAddrs)
	if err != nil {
		return nil, fmt.Errorf("invalid Nacos server address format: %w", err)
	}
	clientConfig := createNacosClientConfig(nacosNamespace)

	// 3. 创建 Nacos 配置客户端
	nacosClient, err := clients.NewConfigClient(
		vo.NacosClientParam{
			ClientConfig:  &clientConfig,
			ServerConfigs: serverConfigs,
		},
	)
	if err != nil {
		return nil, fmt.Errorf("failed to create Nacos config client: %w", err)
	}

	// 使用一个锁来确保并发更新的安全性
	var lock sync.RWMutex

	// 4. 拉取并监听两个配置文件
	// a. 基础设施配置 (指向 BaseConfig.Infra)
	err = initAndWatchSingleConfig(nacosClient, "nexus-infra.yaml", nacosGroup, cfg.GetInfra(), &lock)
	if err != nil {
		return nacosClient, err
	}
	// b. 应用业务配置 (指向 BaseConfig.App)
	err = initAndWatchSingleConfig(nacosClient, "nexus-app.yaml", nacosGroup, cfg.GetApp(), &lock)
	if err != nil {
		return nacosClient, err
	}

	logger.Logger.Info().Any("config", configHolder).Msg("✅ Bootstrap: All configurations loaded and watched successfully from Nacos.")
	return nacosClient, nil
}

// initAndWatchSingleConfig 是一个通用函数，用于拉取、解析和监听单个配置文件
func initAndWatchSingleConfig(client config_client.IConfigClient, dataId, group string, configPtr interface{}, lock *sync.RWMutex) error {
	content, err := client.GetConfig(vo.ConfigParam{DataId: dataId, Group: group})
	if err != nil {
		return fmt.Errorf("failed to get initial config for DataId '%s': %w", dataId, err)
	}

	updateConfig(content, configPtr, lock) // 加载初始配置

	err = client.ListenConfig(vo.ConfigParam{
		DataId: dataId,
		Group:  group,
		OnChange: func(_, _, _, data string) {
			logger.Logger.Info().Msgf("🔔 Nacos config changed for DataId: %s. Applying new config...", dataId)
			updateConfig(data, configPtr, lock)
		},
	})
	if err != nil {
		return fmt.Errorf("failed to listen config for DataId '%s': %w", dataId, err)
	}
	return nil
}

// updateConfig 线程安全地更新配置
func updateConfig(content string, configPtr interface{}, lock *sync.RWMutex) {
	lock.Lock()
	defer lock.Unlock()
	if err := yaml.Unmarshal([]byte(content), configPtr); err != nil {
		logger.Logger.Error().Err(err).Msg("❌ ERROR: Failed to unmarshal Nacos config")
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
