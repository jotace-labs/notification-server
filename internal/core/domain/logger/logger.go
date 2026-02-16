package logger

import (
	"context"
	"os"

	"github.com/google/uuid"
	"github.com/joseCarlosAndrade/notification-server/internal/core/config"
	"go.uber.org/zap"
	"go.uber.org/zap/zapcore"
)

var logger *zap.Logger

// type ctxKey string
const traceIDKey string = "trace_id"  

func InitLogger() *zap.Logger {
	var encoderConfig zapcore.EncoderConfig
	var encoder zapcore.Encoder
	var level zapcore.LevelEnabler
	
	if config.App.Development  {
		encoderConfig = zap.NewDevelopmentEncoderConfig() // plain text encoder
		encoderConfig.EncodeTime = zapcore.ISO8601TimeEncoder // time format
		encoderConfig.EncodeLevel = zapcore.CapitalColorLevelEncoder // display different colors for different levels
		// encoderConfig.EncodeCaller = zapcore.FullCallerEncoder // display full file path (/home/username/.../file.go)
	
		encoder = zapcore.NewConsoleEncoder(encoderConfig)
		level = zapcore.DebugLevel

	} else {
		encoderConfig = zap.NewProductionEncoderConfig() // JSON encoder 
		encoderConfig.EncodeLevel = zapcore.CapitalLevelEncoder // level is capital: INFO, DEBUG, ERROR
		encoderConfig.EncodeTime = zapcore.ISO8601TimeEncoder // time format // similar to rfc 3339

		encoder = zapcore.NewJSONEncoder(encoderConfig)
		level = zapcore.InfoLevel
	}
	

	core := zapcore.NewCore(
		encoder, 
		zapcore.AddSync(os.Stdout), 
		level,
	)

	logger = zap.New(
		core, 
		zap.AddCaller(), 
		// zap.AddCallerSkip(1), // when using wrappers around zap, this prevents the shown path to be the wrapper
		) // this addCaller also display the path to the file, but relative


	return logger
}

func InitResources(ctx context.Context) context.Context {
	traceId := uuid.NewString()
	ctx = context.WithValue(ctx, "trace_id", traceId)

	return ctx
}

// L gets a context and tries to put a field of trace_id from this context into the returned logger
func L(ctx context.Context) *zap.Logger {
	if ctx != nil {
		if id, ok := ctx.Value(traceIDKey).(string); ok {
			return logger.With(zap.String("trace_id", id))
		}
	} 

	return logger
}

func LogPanic(ctx context.Context, r any) {
	L(ctx).Error("\n\n\x1b[31m", zap.Any("PANIC", r))
}

// /*
// logs:
// development:
// 2025-01-28T00:00:00.000+0800    DEBUG    development/main.go:7    This is a DEBUG message
// 2025-01-28T00:00:00.000+0800    INFO    development/main.go:8    This is an INFO message

// production: (debug messages are ignored)
// {"level":"info","ts":1737907200.0000000,"caller":"production/main.go:8","msg":"This is an INFO message"}
// {"level":"info","ts":1737907200.0000000,"caller":"production/main.go:9","msg":"This is an INFO message with fields","region":["us-west"],"id":2}


// logger.Info("this is a log info", zap.Int("int", 10))
// 	logger.Debug("this is a log info", zap.Int("int", 10))
// 	logger.Warn("this is a log info", zap.Int("int", 10))
// 	logger.Error("this is a log info", zap.Int("int", 10))

// 	zap.L().Info("global logger", zap.String("eheheh", "string value"))

// 	// childLogger := logger.With(zap.String("requestID", "10000")) // every log with childLogger will include these params
// 	// childLogger.Info("info", zap.String("key", "value"))

// */