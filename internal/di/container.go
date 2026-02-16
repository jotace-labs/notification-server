package di

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/joseCarlosAndrade/notification-server/internal/adapter/http/server"
	"github.com/joseCarlosAndrade/notification-server/internal/adapter/mongo"
	"github.com/joseCarlosAndrade/notification-server/internal/adapter/redis"
	"github.com/joseCarlosAndrade/notification-server/internal/adapter/redpanda"
	"github.com/joseCarlosAndrade/notification-server/internal/core/config"
	log "github.com/joseCarlosAndrade/notification-server/internal/core/domain/logger"
	"github.com/joseCarlosAndrade/notification-server/internal/core/domain/port"
	"github.com/joseCarlosAndrade/notification-server/internal/core/service"
	"go.uber.org/zap"
)

type Shutdown func(context.Context) error
type HealthCheck func(context.Context) error

type Container struct {
	eventsHub            port.EventsHub
	controller          port.Controller
	storage port.Storage
	cache port.Cache
	// service?

	// TODO: BEFORE CONTINUING, CHECK OUT THE EMAIL DISPATCHER SERVICE TO SEE HOW THEY MANAGE KAFKA LISTENING

	cleanUpFuncs map[string]Shutdown
	healthProbeFuncs map[string]HealthCheck
}

func NewContainer(ctx context.Context) Container {
	log.L(ctx).Info("initializing container")

	// init dependencies

	cleanUps := make(map[string]Shutdown, 0)
	healthProbes := make(map[string]HealthCheck, 0)

	// storage
	storage := initStorage(ctx)
	cleanUps["storage"] = storage.Close
	healthProbes["storage"] = storage.IsHealthy
	

	// cache
	cache := initCache(ctx)
	cleanUps["cache"] = cache.Close
	healthProbes["cache"] = cache.IsHealthy

	// init service with dependencies
	service := initNotificationService(ctx, storage, cache)

	// init consumer
	consumer := initEventsHub(ctx, &service)

	// init controller
	controller := initAPIController(ctx, &service)

	// append every shutdown
	cleanUps["eventsHub"] = consumer.Close
	cleanUps["apiController"] = controller.Close

	// append health probe for the main services
	healthProbes["eventsHub"] = consumer.IsHealthy
	healthProbes["apiController"] = controller.IsHealthy

	// return container
	return Container{
		controller: controller,
		eventsHub: consumer,
		storage: storage,
		cache: cache,
		cleanUpFuncs: cleanUps,
		healthProbeFuncs: healthProbes,
	}	
}

// init dependencies. if anything crucial fails, panic

func initStorage(ctx context.Context) port.Storage {
	storage, err := mongo.NewStorage(ctx, config.App.MongoURI, 
			config.App.MongoNotificationsDB, 
			config.App.MongoNotificationsCollection)

	if err != nil {
		panic(err)
	}

	log.L(ctx).Debug("successfully initialized storage")

	return &storage
}

func initCache(ctx context.Context) port.Cache {
	cache := redis.NewCache(ctx)

	log.L(ctx).Debug("successfully initialized cache")
	return &cache
}

func initNotificationService(ctx context.Context, storage port.Storage, cache port.Cache) port.Service {
	service := service.NewService(ctx, storage, cache)

	return &service
}

func initEventsHub(ctx context.Context, service *port.Service) port.EventsHub {
	eventsHub, err := redpanda.NewEventsHub(ctx, service, config.App.RedpandaBrokers, config.App.NotificationTopic, config.App.KafkaConsumerGroup)
	if err != nil {
		panic(err)
	}

	log.L(ctx).Debug("successfully initialized eventshub")

	return &eventsHub
}

func initAPIController(ctx context.Context, service *port.Service) port.Controller {
	config := server.Config{
		ServiceName: config.AppTraceName,
		Port: config.DefaultAPIPort,
		Development: config.App.Development,
		Debug: config.App.Debug,
	}
	controller := server.NewController(ctx, service, &config)

	log.L(ctx).Debug("successfully initialized api controller")

	return controller
}

// implementing Run interface

// Run starts the app. Blocking
func (c *Container) Run(ctx context.Context) error {
	log.L(ctx).Info("starting application container")

	errCh := make(chan error, 3) // channel holds up errors

	// spawn go func to consume events. each rourine pipes the error returned to errCh unless its a context.Canceled, which is alredy handled
	go func() {
		log.L(ctx).Info("events hub is running")

		err := c.eventsHub.Run(ctx)
		if err != nil && !errors.Is(err, context.Canceled){
			errCh <- fmt.Errorf("Could not run eventsHub: %w", err)
		}
	}()

	go func() {
		log.L(ctx).Info("api controller is running")

		err := c.controller.Run(ctx)
		if err != nil && !errors.Is(err, context.Canceled) {
			errCh <- fmt.Errorf("Could not run controller: %w", err)
		}
	}()

	// start health probe
	go func () {
		time.Sleep(time.Second*5) // wait 5 seconds to start health check
		err := c.initHealthCheck(ctx)
		if err != nil && !errors.Is(err, context.Canceled) {
			errCh <- fmt.Errorf("healthcheck error: %w", err)
		}
	}()

	// wait for either an err is returned or context is canceled (main canceled the code)
	select {
	case err := <- errCh:  // error in the Run process
		log.L(ctx).Error("error running container. Exiting App", zap.Error(err))

		_ = c.Close(ctx)
		return err
	case <- ctx.Done(): // context canceled
		ctx := context.Background()
		log.L(ctx).Warn("context cancelled. Exiting App")

		return c.Close(ctx)
	}
}

func (c *Container) Close(ctx context.Context) error {
	log.L(ctx).Info("trying to close and clean up resources")

	errs := make([]error, 0)

	for name, f := range c.cleanUpFuncs {
		ctxWithTimeout, cancel := context.WithTimeout(ctx, time.Second*10)
		defer cancel()

		// tries to close the dependencies within 10 seconds. if its not successful, it cancels the context
		if err := f(ctxWithTimeout); err != nil {
			log.L(ctx).Error("could not gracefully shut down this dependency", zap.String(name, err.Error()))
			errs = append(errs, err)

			continue
		}

		log.L(ctx).Info("sucessfully claned up this dependency", zap.String("name", name))
	}

	// todo: somehow log or trace all errors here
	if len(errs) > 0 {
		log.L(ctx).Warn("could not properly clean up all resources")
	} else {
		log.L(ctx).Info("all resources cleaned up")
	}


	return nil
}


// initHealthCheck checks each 5 seconds for health status. TODO: maybe try for max attempts
func (c * Container) initHealthCheck(ctx context.Context) error {
	log.L(ctx).Info("starting healthcheck")
	
	for {
		// avoid checking with canceled context
		select {
		case <-ctx.Done():
			log.L(ctx).Warn("context canceled. exiting healthcheck")
			return nil
		default:
		}

		time.Sleep(time.Second*5)
		// log.L(ctx).Debug("checking services health")

		for name, checkHealth := range c.healthProbeFuncs {
			ctx, cancel := context.WithTimeout(ctx, time.Second*5)
			defer cancel()
			// log.L(ctx).Debug("checking", zap.String("name", name))
			if err := checkHealth(ctx); err != nil {
				return fmt.Errorf("healthcheck failed for %s. error: %w", name, err)
			}
		}
	}
}