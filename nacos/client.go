// internal/pkg/nacos/client.go
package nacos

import (
	"fmt"
	"github.com/nacos-group/nacos-sdk-go/v2/clients"
	"github.com/nacos-group/nacos-sdk-go/v2/clients/naming_client"
	"github.com/nacos-group/nacos-sdk-go/v2/common/constant"
	"github.com/nacos-group/nacos-sdk-go/v2/vo"
	"github.com/wangyingjie930/nexus-pkg/logger"
)

// Client 封装了 Nacos 命名客户端
type Client struct {
	namingClient naming_client.INamingClient

	namespaceId string // ✨ 新增: 存储命名空间ID
	groupName   string // ✨ 新增: 存储默认分组名
}

// ✨ 改造 NewNacosClient 函数，使其不再负责创建配置，只负责创建客户端
// 原来的 NewNacosClient 改名为 NewNacosClientWithConfigs
func NewNacosClientWithConfigs(serverConfigs []constant.ServerConfig, clientConfig *constant.ClientConfig, groupName string) (*Client, error) {
	if groupName == "" {
		groupName = "DEFAULT_GROUP"
		logger.Logger.Printf("⚠️ WARNING: NACOS_GROUP is not set. Using '%s'.", groupName)
	}

	namingClient, err := clients.NewNamingClient(
		vo.NacosClientParam{
			ClientConfig:  clientConfig,
			ServerConfigs: serverConfigs,
		},
	)
	if err != nil {
		return nil, fmt.Errorf("failed to create nacos naming client: %w", err)
	}

	namespaceId := clientConfig.NamespaceId
	logger.Logger.Printf("✅ Successfully connected to Nacos. Namespace: '%s', Group: '%s'", namespaceId, groupName)
	return &Client{
		namingClient: namingClient,
		namespaceId:  namespaceId,
		groupName:    groupName,
	}, nil
}

// RegisterServiceInstance 注册一个服务实例到 Nacos
func (c *Client) RegisterServiceInstance(serviceName, ip string, port int) error {
	success, err := c.namingClient.RegisterInstance(vo.RegisterInstanceParam{
		Ip:          ip,
		Port:        uint64(port),
		ServiceName: serviceName,
		Weight:      10,
		Enable:      true,
		Healthy:     true,
		Ephemeral:   true,        // 设置为临时节点，心跳断开后会自动摘除
		GroupName:   c.groupName, // ✨ 核心: 注册时使用客户端配置的分组
	})
	if err != nil {
		return fmt.Errorf("failed to register service with nacos: %w", err)
	}
	if !success {
		return fmt.Errorf("nacos registration was not successful for service: %s", serviceName)
	}
	logger.Logger.Printf("✅ Service '%s' registered to Nacos successfully (%s:%d)", serviceName, ip, port)
	return nil
}

// DeregisterServiceInstance 从 Nacos 注销一个服务实例
func (c *Client) DeregisterServiceInstance(serviceName, ip string, port int) error {
	_, err := c.namingClient.DeregisterInstance(vo.DeregisterInstanceParam{
		Ip:          ip,
		Port:        uint64(port),
		ServiceName: serviceName,
		Ephemeral:   true,
		GroupName:   c.groupName, // ✨ 核心: 注销时使用客户端配置的分组
	})
	if err != nil {
		return fmt.Errorf("failed to deregister service with nacos: %w", err)
	}
	logger.Logger.Printf("ℹ️ Service '%s' deregistered from Nacos (%s:%d)", serviceName, ip, port)
	return nil
}

// DiscoverServiceInstance 从 Nacos 发现一个健康的服务实例
// 使用 Nacos 内置的负载均衡算法
func (c *Client) DiscoverServiceInstance(serviceName string) (string, int, error) {
	instance, err := c.namingClient.SelectOneHealthyInstance(vo.SelectOneHealthInstanceParam{
		ServiceName: serviceName,
		GroupName:   c.groupName, // ✨ 核心: 服务发现时指定分组
	})
	if err != nil {
		return "", 0, fmt.Errorf("failed to discover healthy instance for service '%s': %w", serviceName, err)
	}
	if instance == nil {
		return "", 0, fmt.Errorf("no healthy instance available for service '%s'", serviceName)
	}
	return instance.Ip, int(instance.Port), nil
}

// Close 关闭 Nacos 客户端连接
func (c *Client) Close() {
	if c.namingClient != nil {
		// Nacos Go SDK v2.x.x 没有显式的 Close 方法
		// 临时节点会在心跳停止后自动过期
		logger.Logger.Println("ℹ️ Nacos client does not require explicit closing. Ephemeral nodes will expire.")
	}
}
