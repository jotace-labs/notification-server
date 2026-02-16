package server

import (
	"net/http"
	"time"

	"github.com/gin-gonic/gin"
	log "github.com/joseCarlosAndrade/notification-server/internal/core/domain/logger"
	"go.uber.org/zap"
)

// loggingMiddleware sets a custom logger
func (controller *Controller)loggingMiddleware() gin.HandlerFunc {
	return func(c *gin.Context) {
		path := c.Request.URL.Path

		// ignore health probe calls
		if path == "/" {
			c.Next()
			return
		}

		start := time.Now()
		
		c.Next()

		totalTime := time.Since(start)

		method := c.Request.Method
		responseStatus := c.Writer.Status()

		if controller.config.Debug {
			// log everything
			log.L(c.Request.Context()).Debug("Request finished", 
				zap.String("method", method),
				zap.String("path", path),
				zap.Int("status", responseStatus),
				zap.Duration("latency", totalTime),
				zap.Any("headers", c.Request.Header))
		} else {
			log.L(c.Request.Context()).Info("HTTP", 
				zap.String("method", method),
				zap.String("path", path),
				zap.Int("status", responseStatus))
		}

		if responseStatus >= http.StatusBadRequest {
			log.L(c.Request.Context()).Error("HTTP Errors", zap.Strings("errors", c.Errors.Errors()))
		}
	}
}

// tracingMiddleware puts a traceID in every context call to this api
func tracingMiddleware() gin.HandlerFunc {
	return func(c *gin.Context) {
		ctx :=c.Request.Context()

		c.Request = c.Request.WithContext(log.InitResources(ctx))

		c.Next()
	}
}

// recoverMiddleware recovers from panic calls
func recoverMiddleware() gin.HandlerFunc {
	return func(c *gin.Context) {
		// handle panic
		defer func() {
			if r := recover(); r != nil {
				// log.L(c.Request.Context()).Error("PANIC DETECTED. recovering", zap.Any("recover", r))
				log.LogPanic(c.Request.Context(), r)

				c.JSON(http.StatusInternalServerError, gin.H{"error" : "internal"})
			}
		}()
			
		c.Next()
	}
}

// CORSmiddleware cors policy
func CORSmiddleware() gin.HandlerFunc {
	return func(c *gin.Context) {
		c.Writer.Header().Set("Access-Control-Allow-Origin", "*")
		c.Writer.Header().Set("Access-Control-Allow-Credentials", "true")
		c.Writer.Header().Set("Access-Control-Allow-Headers", "Content-Type, Content-Length, Accept-Encoding, Authorization, accept, origin, Cache-Control, X-Requested-With")
		c.Writer.Header().Set("Access-Control-Allow-Methods", "POST, PUT, GET, DELETE")

		if c.Request.Method == http.MethodOptions {
			c.AbortWithStatus(http.StatusNoContent)
			return
		}

		c.Next()
	}
}