// internal/zookeeper/lock.go
package zookeeper

import (
	"errors"
	"fmt"
	"sort"
	"strings"
	"time"

	"github.com/go-zookeeper/zk"
)

const (
	lockRoot = "/distributed_locks" // 所有分布式锁的根节点
)

// DistributedLock 定义了一个分布式锁对象
type DistributedLock struct {
	conn     *Conn  // ZooKeeper连接
	path     string // 锁的路径，例如 /distributed_locks/item-123
	lockNode string // 成功获取锁后，自己创建的节点路径
}

// NewDistributedLock 创建一个新的分布式锁实例
func NewDistributedLock(conn *Conn, resourceID string) *DistributedLock {
	lockPath := lockRoot + "/" + resourceID

	// <<<<<<< 修改点: 使用 ensurePath 替换原有的创建逻辑 >>>>>>>>>
	// 确保锁的根路径和资源路径都存在
	if err := ensurePath(conn, lockPath); err != nil {
		// 如果路径创建失败，这是一个严重问题，直接panic
		panic(fmt.Sprintf("Failed to ensure lock path %s exists: %v", lockPath, err))
	}
	// <<<<<<< 修改结束 >>>>>>>>>

	return &DistributedLock{
		conn: conn,
		path: lockPath,
	}
}

// Lock 尝试获取锁，如果获取不到则阻塞等待
func (l *DistributedLock) Lock() error {
	// 1. 在锁路径下创建一个临时顺序节点
	// 格式为: /distributed_locks/resourceID/lock-
	nodePath, err := l.conn.CreateProtectedEphemeralSequential(l.path+"/lock-", []byte(""), zk.WorldACL(zk.PermAll))
	if err != nil {
		return fmt.Errorf("failed to create sequential node: %w", err)
	}
	l.lockNode = nodePath

	for {
		// 2. 获取锁路径下的所有子节点
		children, _, err := l.conn.Children(l.path)
		if err != nil {
			return fmt.Errorf("failed to get children nodes: %w", err)
		}
		sort.Strings(children) // 排序，保证顺序

		// 3. 判断自己是否是最小的节点
		myNodeName := strings.TrimPrefix(l.lockNode, l.path+"/")
		if myNodeName == children[0] {
			// 是最小节点，成功获取锁
			return nil
		}

		// 4. 不是最小节点，监听前一个节点
		prevNodeIndex := -1
		for i, child := range children {
			if child == myNodeName {
				prevNodeIndex = i - 1
				break
			}
		}
		if prevNodeIndex < 0 {
			return errors.New("cannot find previous node, something is wrong")
		}
		prevNodePath := l.path + "/" + children[prevNodeIndex]

		// 使用 ExistsW 来设置一次性的Watcher
		_, _, eventChan, err := l.conn.ExistsW(prevNodePath)
		if err != nil {
			// 如果在前一个节点检查时它刚好被删除了，就重试循环
			if err == zk.ErrNoNode {
				continue
			}
			return fmt.Errorf("failed to watch previous node: %w", err)
		}

		// 阻塞等待事件
		select {
		case event := <-eventChan:
			// 如果前一个节点被删除，我们就收到通知，重新进入循环去竞争锁
			if event.Type == zk.EventNodeDeleted {
				continue
			}
		case <-time.After(30 * time.Second): // 设置超时，防止死等
			return errors.New("timeout waiting for lock")
		}
	}
}

// Unlock 释放锁
func (l *DistributedLock) Unlock() error {
	if l.lockNode == "" {
		return errors.New("no lock to unlock")
	}
	err := l.conn.Delete(l.lockNode, -1)
	if err != nil && err != zk.ErrNoNode {
		return fmt.Errorf("failed to delete lock node: %w", err)
	}
	l.lockNode = ""
	return nil
}

// 新增一个辅助函数，确保路径存在 (类似 mkdir -p)
func ensurePath(conn *Conn, path string) error {
	parts := strings.Split(path, "/")
	currentPath := ""
	for _, part := range parts {
		if part == "" {
			continue
		}
		currentPath += "/" + part
		exists, _, err := conn.Exists(currentPath)
		if err != nil {
			return fmt.Errorf("failed to check existence of path %s: %w", currentPath, err)
		}
		if !exists {
			_, err := conn.Create(currentPath, []byte{}, 0, zk.WorldACL(zk.PermAll))
			// 如果节点因为并发创建而已经存在，忽略这个错误
			if err != nil && err != zk.ErrNodeExists {
				return fmt.Errorf("failed to create path %s: %w", currentPath, err)
			}
		}
	}
	return nil
}
