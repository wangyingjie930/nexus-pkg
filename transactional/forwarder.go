package transactional

import (
	"context"
	"github.com/wangyingjie930/nexus-pkg/logger"
	"time"
)

// Forwarder 是一个后台任务，负责周期性地转发待发送的消息
type Forwarder struct {
	service  *Service
	ticker   *time.Ticker
	interval time.Duration
}

// NewForwarder 创建一个新的消息转发器
func NewForwarder(service *Service, interval time.Duration) *Forwarder {
	return &Forwarder{
		service:  service,
		interval: interval,
	}
}

// Start 启动转发器。它会阻塞直到上下文被取消。
func (f *Forwarder) Start(ctx context.Context) error {
	log := logger.Ctx(ctx)
	log.Info().Dur("interval", f.interval).Msg("starting transactional message forwarder")
	f.ticker = time.NewTicker(f.interval)
	defer f.ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			log.Info().Msg("stopping transactional message forwarder")
			return nil
		case <-f.ticker.C:
			log.Debug().Msg("forwarder tick: checking for pending messages")
			if err := f.service.ForwardPendingMessages(ctx); err != nil {
				log.Error().Err(err).Msg("error during message forwarding cycle")
			}
		}
	}
}
