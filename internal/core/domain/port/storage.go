package port

import (
	"context"

	"github.com/joseCarlosAndrade/notification-server/internal/core/domain/models"
)

type Storage interface {
	Runner
	IsHealthy(ctx context.Context) error 

	StoreNewNotification(ctx context.Context, notification *models.NotificationRecord, id string) error 
	MarkNotificationAsRead(ctx context.Context, notificationID string) error
	GetAllNotificationsByTime(ctx context.Context, serviceName string, filter models.LastTime) ([]*models.Notification, error)
	GetLatestNotifications(ctx context.Context, serviceName string, n int) ([]*models.Notification, error)
	GetNotReadNotifications(ctx context.Context, serviceName string) ([]*models.Notification, error)
	GetAllNotifications(ctx context.Context, serviceName string) ([]*models.Notification, error)
}