package healthcheck

import (
    "context"
    "time"
)

// CheckResult результат проверки одного сервиса
type CheckResult struct {
    ServiceName string        `json:"service_name"`
    Status      string        `json:"status"` // "up", "down"
    Error       string        `json:"error,omitempty"`
    Duration    time.Duration `json:"duration_ms"`
}

// Checker интерфейс для всех health checks
type Checker interface {
    Check(ctx context.Context) error
    Name() string
}

// HealthCheck основной объект для проверок
type HealthCheck struct {
    checkers []Checker
}

// New создает новый HealthCheck
func New() *HealthCheck {
    return &HealthCheck{}
}

// Register добавляет новую проверку
func (h *HealthCheck) Register(checker Checker) {
    h.checkers = append(h.checkers, checker)
}

// Run выполняет все проверки
func (h *HealthCheck) Run(ctx context.Context) map[string]CheckResult {
    results := make(map[string]CheckResult)
    
    for _, checker := range h.checkers {
        start := time.Now()
        
        err := checker.Check(ctx)
        status := "up"
        errorMsg := ""
        
        if err != nil {
            status = "down"
            errorMsg = err.Error()
        }
        
        results[checker.Name()] = CheckResult{
            ServiceName: checker.Name(),
            Status:      status,
            Error:       errorMsg,
            Duration:    time.Since(start),
        }
    }
    
    return results
}

// IsHealthy проверяет, все ли сервисы здоровы
func (h *HealthCheck) IsHealthy(results map[string]CheckResult) bool {
    for _, result := range results {
        if result.Status != "up" {
            return false
        }
    }
    return true
}
