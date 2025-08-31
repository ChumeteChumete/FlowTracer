package logger

import (
	"os"
	"time"
	"context"

	"github.com/sirupsen/logrus"
)

var Logger *logrus.Logger

func init() {
	Logger = logrus.New()
	
	// Формат вывода
	Logger.SetFormatter(&logrus.JSONFormatter{
		TimestampFormat: time.RFC3339,
	})
	
	// Уровень логгирования из переменной среды
	level := os.Getenv("LOG_LEVEL")
	switch level {
	case "debug":
		Logger.SetLevel(logrus.DebugLevel)
	case "info":
		Logger.SetLevel(logrus.InfoLevel)
	case "warn":
		Logger.SetLevel(logrus.WarnLevel)
	case "error":
		Logger.SetLevel(logrus.ErrorLevel)
	default:
		Logger.SetLevel(logrus.InfoLevel)
	}
	
	Logger.SetOutput(os.Stdout)
}

// Удобные функции для использования
func Info(args ...interface{}) {
	Logger.Info(args...)
}

func Infof(format string, args ...interface{}) {
	Logger.Infof(format, args...)
}

func Error(args ...interface{}) {
	Logger.Error(args...)
}

func Errorf(format string, args ...interface{}) {
	Logger.Errorf(format, args...)
}

func Debug(args ...interface{}) {
	Logger.Debug(args...)
}

func Debugf(format string, args ...interface{}) {
	Logger.Debugf(format, args...)
}

func Warn(args ...interface{}) {
	Logger.Warn(args...)
}

func Warnf(format string, args ...interface{}) {
	Logger.Warnf(format, args...)
}

func Fatal(args ...interface{}) {
	Logger.Fatal(args...)
}

func Fatalf(format string, args ...interface{}) {
	Logger.Fatalf(format, args...)
}

// WithFields создает логгер с дополнительными полями
func WithFields(fields map[string]interface{}) *logrus.Entry {
	return Logger.WithFields(fields)
}

func FromContext(ctx context.Context) *logrus.Entry {
    type RequestIDKey struct{}
    
    if requestID, ok := ctx.Value(RequestIDKey{}).(string); ok {
        return Logger.WithField("request_id", requestID)
    }
    return Logger.WithField("request_id", "unknown")
}

// WithError добавляет ошибку в лог
func WithError(err error) *logrus.Entry {
    return Logger.WithError(err)
}

// WithField добавляет одно поле
func WithField(key string, value interface{}) *logrus.Entry {
    return Logger.WithField(key, value)
}