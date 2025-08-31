package handlers

import (
	"context"
	"encoding/json"
	"flowtracer/internal/healthcheck"
	"net/http"
	"time"
)

type HealthHandler struct {
	healthCheck *healthcheck.HealthCheck
}

func NewHealthHandler(hc *healthcheck.HealthCheck) *HealthHandler {
	return &HealthHandler{healthCheck: hc}
}

type HealthResponse struct {
	Status   string                           `json:"status"`
	Services map[string]healthcheck.CheckResult `json:"services"`
	Version  string                           `json:"version"`
	Uptime   string                           `json:"uptime"`
}

var startTime = time.Now()

func (h *HealthHandler) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	ctx, cancel := context.WithTimeout(r.Context(), 10*time.Second)
	defer cancel()

	// Выполняем проверки
	results := h.healthCheck.Run(ctx)
	
	// Определяем общий статус
	status := "healthy"
	if !h.healthCheck.IsHealthy(results) {
		status = "unhealthy"
		w.WriteHeader(http.StatusServiceUnavailable)
	}

	response := HealthResponse{
		Status:   status,
		Services: results,
		Version:  "1.0.0", // Можно вынести в конфигурацию
		Uptime:   time.Since(startTime).String(),
	}

	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(response)
}

func (h *HealthHandler) LivenessCheck(w http.ResponseWriter, r *http.Request) {
	// Простая проверка "живости" - всегда возвращает OK
	w.WriteHeader(http.StatusOK)
	w.Write([]byte("OK"))
}

func (h *HealthHandler) ReadinessCheck(w http.ResponseWriter, r *http.Request) {
	ctx, cancel := context.WithTimeout(r.Context(), 5*time.Second)
	defer cancel()

	// Проверяем готовность критически важных сервисов
	results := h.healthCheck.Run(ctx)
	
	if h.healthCheck.IsHealthy(results) {
		w.WriteHeader(http.StatusOK)
		w.Write([]byte("READY"))
	} else {
		w.WriteHeader(http.StatusServiceUnavailable)
		w.Write([]byte("NOT_READY"))
	}
}