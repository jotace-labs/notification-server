package server

import (
	"context"

	"github.com/gin-gonic/gin"
	"github.com/joseCarlosAndrade/notification-server/internal/core/domain/port"
)

const (
	healthProbeEndpoint string = "/"
)

type Config struct {
	ServiceName string
	Port string
	Development bool
	Debug bool
}

// Controller implements the port.Controller interface
type Controller struct {
	service *port.Service
	engine *gin.Engine
	config *Config
}

// makes sure Controller implements the interface
var _ port.Controller = (*Controller)(nil)

// makes sure Controller implements the runner
var _ port.Runner = (*Controller)(nil)

func NewController(ctx context.Context, serviceRepository *port.Service, config * Config) *Controller {
	gateway := gin.New()


	controller := &Controller{
		service:  serviceRepository,
		engine:  gateway,
		config: config,
	}

	controller.engine.Use(
		controller.loggingMiddleware(),
		recoverMiddleware(),
		tracingMiddleware(),
		CORSmiddleware(),
	)

	
	return controller
}

// Run implements port.Runner interface
func (s *Controller) Run(ctx context.Context) error {
	for {
	}
	return nil
}

// Close implements port.Runner interface
func (s *Controller) Close(ctx context.Context) error {
	// todo: close connection and clean up

	return nil
}

func (s *Controller) IsHealthy(ctx context.Context) error {
	return nil
}
