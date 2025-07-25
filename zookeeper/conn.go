// internal/zookeeper/conn.go
package zookeeper

import (
	"nexus/internal/pkg/logger"
	"time"

	"github.com/go-zookeeper/zk"
)

// Conn 是一个包装了官方zk.Conn的结构体，可以附加更多应用逻辑
type Conn struct {
	*zk.Conn
}

var (
	zkServers   = []string{"localhost:2181"} // 默认地址，后续可以从配置中读取
	connTimeout = 5 * time.Second
)

// InitZookeeper 初始化并返回一个ZooKeeper连接
// 在实际项目中，servers可以从配置（如ConfigMap）中传入
func InitZookeeper(servers []string) (*Conn, error) {
	if len(servers) > 0 && servers[0] != "" {
		zkServers = servers
	}

	// zk.Connect会返回一个连接实例和一个事件通道
	// 事件通道用于接收连接状态的变化通知
	c, eventChan, err := zk.Connect(zkServers, connTimeout)
	if err != nil {
		logger.Logger.Fatal().Err(err).Msg("ERROR: Failed to connect to ZooKeeper")
		return nil, err
	}

	// 启动一个goroutine来异步监听连接事件
	go func() {
		for event := range eventChan {
			// 只关心状态变化事件
			if event.Type == zk.EventSession {
				switch event.State {
				case zk.StateConnected:
					logger.Logger.Println("Successfully connected to ZooKeeper.")
				case zk.StateDisconnected:
					logger.Logger.Println("Disconnected from ZooKeeper.")
				case zk.StateExpired:
					// 会话过期通常意味着需要重新建立所有临时节点和Watcher
					logger.Logger.Println("ZooKeeper session expired.")
				}
			}
		}
	}()

	return &Conn{c}, nil
}
