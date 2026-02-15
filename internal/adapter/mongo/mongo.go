package mongo

import (
	"context"
	"fmt"
	"time"

	"github.com/joseCarlosAndrade/notification-server/internal/core/domain"
	log "github.com/joseCarlosAndrade/notification-server/internal/core/domain/logger"
	"github.com/joseCarlosAndrade/notification-server/internal/core/domain/models"
	"github.com/joseCarlosAndrade/notification-server/internal/core/domain/port"
	"go.mongodb.org/mongo-driver/v2/bson"
	"go.mongodb.org/mongo-driver/v2/mongo"
	"go.mongodb.org/mongo-driver/v2/mongo/options"
	"go.mongodb.org/mongo-driver/v2/mongo/readpref"
	"go.uber.org/zap"
)

// Storage implements the port.Storage
type Storage struct {
	client         *mongo.Client
	dbName         string
	collectionName string

	notificationCollection *mongo.Collection
}

var _ port.Storage = (*Storage)(nil) // ensures Storage implements port.Storage

func NewStorage(ctx context.Context, connectionStr, mongoDB, mongoCollection string) (Storage, error) {
	client, err := mongo.Connect(options.Client().ApplyURI(connectionStr))
	if err != nil {
		log.L(ctx).Error("invalid mongo config", zap.Error(err))
		return Storage{}, err
	}

	ctx, cancel := context.WithTimeout(ctx, time.Second*5)
	defer cancel()

	err = client.Ping(ctx, readpref.Primary())
	if err != nil {
		log.L(ctx).Error("database not connected. ping failed", zap.Error(err))
		return Storage{}, fmt.Errorf("could not connect to database: %w", err)
	}

	log.L(ctx).Info("successfully connected to mongodb")

	return Storage{
		client:                 client,
		dbName:                 mongoDB,
		collectionName:         mongoCollection,
		notificationCollection: client.Database(mongoDB).Collection(mongoCollection),
	}, nil
}

// Run implements port.Runner interface
func (s *Storage) Run(ctx context.Context) error {
	return nil
}

// Close implements port.Runner interface
func (s *Storage) Close(ctx context.Context) error {
	// todo: close connection and clean up

	return s.client.Disconnect(ctx)
}

func (s *Storage) IsHealthy(ctx context.Context) error {
	return s.client.Ping(ctx, readpref.Primary())
}

func (s *Storage) StoreNewNotification(ctx context.Context, notification *models.NotificationRecord, id string) error {
	// start span here

	mongoNotification := transformNotificationToMongo(notification, id)

	filter := bson.M{"_id": id}

	update := bson.M{
		"$set": *mongoNotification,
	}

	opts := options.UpdateOne().SetUpsert(true)

	// using upsert to avoid duplicate if the service tries to save the same notification (idempotency)
	_, err := s.notificationCollection.UpdateOne(ctx, filter, update, opts)
	if err != nil {
		log.L(ctx).Error("could not insert new notification",
			zap.String("id", mongoNotification.ID),
			zap.String("service", mongoNotification.Service),
			zap.Error(err))

		return fmt.Errorf("failed to insert in mongodb: %w", err)
	}

	log.L(ctx).Debug("successfully stored new notification in mongo",
		zap.String("id", mongoNotification.ID),
		zap.String("service", mongoNotification.Service))

	return nil
}

func (s *Storage) MarkNotificationAsRead(ctx context.Context, notificationID string) error {
	if notificationID == "" {
		return fmt.Errorf("notificationID canont be empty")
	}

	filter := bson.M{
		"_id": notificationID,
	}

	now := domain.NewNowTimeString()

	update := bson.M{
		"$set": bson.M{
			"readAt": now,
			"isRead": true,
		},
	}

	res, err := s.notificationCollection.UpdateOne(ctx, filter, update)
	if err != nil {
		log.L(ctx).Error("could not update document in mongodb",
			zap.String("id", notificationID),
			zap.Error(err))

		return fmt.Errorf("could not update document in mongodb: %w", err)
	}

	if res.MatchedCount == 0 {
		log.L(ctx).Error("no documents matched this filter", zap.String("id", notificationID))
		return fmt.Errorf("no documents matched this filter")
	}

	log.L(ctx).Info("notification successfully marked as read", zap.String("id", notificationID))

	return nil
}

func transformNotificationToMongo(notification *models.NotificationRecord, id string) *Notification {
	return &Notification{
		ID:      id,
		Service: notification.Service,
		Message: notification.Message,
		IsRead:  false,
		SentAt:  *notification.SentAt,
	}
}

func (s *Storage) GetAllNotificationsByTime(ctx context.Context, serviceName string, filter models.LastTime) ([]*models.Notification, error) {
	// todo
	// start span

	finalMinutes := calculateFinalMinutes(filter)

	// calculates the base time to get all documents most recent up to that timestamp
	targetTimeAgo := time.Now().UTC().Add(-finalMinutes * time.Minute)

	log.L(ctx).Debug("getting documents from this time to now", zap.Time("targetTimeAgo", targetTimeAgo))

	pipeline := assembleGetNotificationsByTimeAggr(serviceName, targetTimeAgo)

	// timeout of 10 seconds for this pipeline
	ctxTimeout, cancel := context.WithTimeout(ctx, time.Second*10)
	defer cancel()

	cursor, err := s.notificationCollection.Aggregate(ctxTimeout, pipeline)
	if err != nil {
		log.L(ctx).Error("aggregate notifications by time failed", zap.Error(err))

		return nil, err
	}

	var results []Notification
	if err = cursor.All(ctx, &results); err != nil {
		log.L(ctx).Error("cursor iteration failed", zap.Error(err))
		
		return nil, err 
	}

	domainNotifications := transformNotificationsToDomain(results) 

	log.L(ctx).Debug("successfully got notifications", 
		zap.String("servcie", serviceName), 
		zap.Int("count", len(domainNotifications)))

	return domainNotifications, nil
}

func calculateFinalMinutes(filter models.LastTime) time.Duration {
	return time.Duration(filter.Minutes + filter.Hours*60 + filter.Days*1440)
}

// assemlbes the pipeline to get latest notifications using bson.D (document: is ordered. important for aggregations)
func assembleGetNotificationsByTimeAggr(serviceName string, targetTimeAgo time.Time) mongo.Pipeline {
	// match service name and from targetTime ago
	matchService := bson.D{{
		Key: "$match" , Value : bson.M{
			"service" : serviceName,
			"sentAt" : bson.M {
				"$gte" : targetTimeAgo,
			},
		},
	}}

	// sort for most recent
	sortBySentAt := bson.D{{
		Key: "$sort", Value: bson.M{
			"sentAt" : -1,
		},
	}}

	finalAggr := make([]bson.D, 0)
	finalAggr = append(finalAggr, matchService, sortBySentAt)
	

	return finalAggr
}

func transformNotificationsToDomain(notifications []Notification) []*models.Notification {
	final := make([]*models.Notification, 0)

	for _, n := range notifications {
		final = append(final, &models.Notification{
			ID: n.ID,
			Service: n.Service,
			Message: n.Message,
			IsRead: n.IsRead,
			SentAt: n.SentAt,
			ReadAt: n.ReadAt,
		})
	}

	return final
}

func (s *Storage) GetNotReadNotifications(ctx context.Context, serviceName string) ([]*models.Notification, error) {
	// todo
	// start span

	log.L(ctx).Debug("getting not read notifications for this service", zap.String("serviceName", serviceName))

	filter := bson.M{ "service" : serviceName, "isRead" : false}
	options := options.Find().SetSort(bson.D{{Key: "sentAt", Value: -1}})

	cursor, err := s.notificationCollection.Find(ctx, filter, options)
	if err != nil {
		log.L(ctx).Error("aggregate notifications by time failed", zap.Error(err))

		return nil, err
	}

	var results []Notification
	if err = cursor.All(ctx, &results); err != nil {
		log.L(ctx).Error("cursor iteration failed", zap.Error(err))
		
		return nil, err 
	}

	domainNotifications := transformNotificationsToDomain(results) 

	return domainNotifications, nil
}

func (s *Storage) GetLatestNotifications(ctx context.Context, serviceName string, n int) ([]*models.Notification, error) {
	// todo
	// start span

	log.L(ctx).Debug("getting latest notifications for this service", zap.String("serviceName", serviceName), zap.Int("n", n))

	filter := bson.M{"service" :serviceName}
	options := options.Find().
		SetSort(bson.D{{Key: "sentAt", Value: -1}}).
		SetLimit(int64(n))

	cursor, err := s.notificationCollection.Find(ctx, filter, options)
	if err != nil {
		log.L(ctx).Error("aggregate notifications by time failed", zap.Error(err))

		return nil, err
	}

	var results []Notification
	if err = cursor.All(ctx, &results); err != nil {
		log.L(ctx).Error("cursor iteration failed", zap.Error(err))
		
		return nil, err 
	}

	domainNotifications := transformNotificationsToDomain(results) 

	return domainNotifications, nil
}

func (s *Storage) GetAllNotifications(ctx context.Context, serviceName string) ([]*models.Notification, error) {
	// todo
	// start span

	log.L(ctx).Debug("getting all notifications for this service", zap.String("serviceName", serviceName))

	filter := bson.M{"service" :serviceName}
	options := options.Find().
		SetSort(bson.D{{Key: "sentAt", Value: -1}})

	cursor, err := s.notificationCollection.Find(ctx, filter, options)
	if err != nil {
		log.L(ctx).Error("aggregate notifications by time failed", zap.Error(err))

		return nil, err
	}

	var results []Notification
	if err = cursor.All(ctx, &results); err != nil {
		log.L(ctx).Error("cursor iteration failed", zap.Error(err))
		
		return nil, err 
	}

	domainNotifications := transformNotificationsToDomain(results) 

	return domainNotifications, nil
}