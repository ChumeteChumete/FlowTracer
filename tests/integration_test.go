package middleware

import (
	"context"
	"flowtracer/internal/logger"
	"flowtracer/internal/metrics"
	"time"

	"github.com/google/uuid"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

// RequestIDKey ключ для request ID в контексте
type RequestIDKey struct{}

// LoggingInterceptor логирует все gRPC запросы
func LoggingInterceptor() grpc.UnaryServerInterceptor {
	return func(ctx context.Context, req interface{}, info *grpc.UnaryServerInfo, handler grpc.UnaryHandler) (interface{}, error) {
		start := time.Now()
		requestID := uuid.New().String()
		
		// Добавляем request ID в контекст
		ctx = context.WithValue(ctx, RequestIDKey{}, requestID)
		
		logger.WithFields(map[string]interface{}{
			"request_id": requestID,
			"method":     info.FullMethod,
			"event":      "request_start",
		}).Info("gRPC request started")
		
		// Выполняем запрос
		resp, err := handler(ctx, req)
		
		duration := time.Since(start)
		
		// Логируем результат
		fields := map[string]interface{}{
			"request_id": requestID,
			"method":     info.FullMethod,
			"duration":   duration.String(),
			"event":      "request_end",
		}
		
		if err != nil {
			grpcStatus := status.Convert(err)
			fields["error"] = err.Error()
			fields["grpc_code"] = grpcStatus.Code().String()
			logger.WithFields(fields).Error("gRPC request failed")
		} else {
			logger.WithFields(fields).Info("gRPC request completed")
		}
		
		return resp, err
	}
}

// MetricsInterceptor собирает метрики для gRPC запросов
func MetricsInterceptor() grpc.UnaryServerInterceptor {
	return func(ctx context.Context, req interface{}, info *grpc.UnaryServerInfo, handler grpc.UnaryHandler) (interface{}, error) {
		start := time.Now()
		
		// Увеличиваем счетчик активных соединений
		metrics.ActiveConnections.Inc()
		defer metrics.ActiveConnections.Dec()
		
		// Выполняем запрос
		resp, err := handler(ctx, req)
		
		duration := time.Since(start).Seconds()
		
		// Записываем метрики
		method := info.FullMethod
		if err != nil {
			grpcStatus := status.Convert(err)
			code := grpcStatus.Code()
			
			logger.WithFields(map[string]interface{}{
				"method":   method,
				"duration": duration,
				"code":     code.String(),
			}).Debug("gRPC method metrics")
		} else {
			logger.WithFields(map[string]interface{}{
				"method":   method,
				"duration": duration,
				"code":     "OK",
			}).Debug("gRPC method metrics")
		}
		
		return resp, err
	}
}

// RecoveryInterceptor обрабатывает панику в gRPC handlers
func RecoveryInterceptor() grpc.UnaryServerInterceptor {
	return func(ctx context.Context, req interface{}, info *grpc.UnaryServerInfo, handler grpc.UnaryHandler) (resp interface{}, err error) {
		defer func() {
			if r := recover(); r != nil {
				logger.WithFields(map[string]interface{}{
					"method": info.FullMethod,
					"panic":  r,
					"event":  "panic_recovery",
				}).Error("Panic recovered in gRPC handler")
				
				err = status.Errorf(codes.Internal, "internal server error")
			}
		}()
		
		return handler(ctx, req)
	}
}

// GetRequestID извлекает request ID из контекста
func GetRequestID(ctx context.Context) string {
	if requestID, ok := ctx.Value(RequestIDKey{}).(string); ok {
		return requestID
	}
	return "unknown"
}