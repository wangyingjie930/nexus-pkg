// internal/pkg/constants/servicenames.go
package constants

// 定义所有微服务的标准服务名
// 这些名称将用于服务注册、服务发现、日志记录和监控等场景
const (
	APIGatewayService     = "api-gateway"
	OrderService          = "order-service"
	InventoryService      = "inventory-service"
	NotificationService   = "notification-service"
	PricingService        = "pricing-service"
	FraudDetectionService = "fraud-detection-service"
	ShippingService       = "shipping-service"
	PromotionService      = "promotion-service"
	DelaySchedulerService = "delay-scheduler"
)

const (
	// FraudDetectionService Paths
	FraudCheckPath = "/check"

	// InventoryService Paths
	InventoryReservePath = "/reserve_stock"
	InventoryReleasePath = "/release_stock"

	// PromotionService Paths
	PromotionGetPromoPricePath = "/get_promo_price"

	// PricingService Paths
	PricingCalculatePricePath = "/calculate_price"

	// ShippingService Paths
	ShippingGetQuotePath = "/get_quote"
)
