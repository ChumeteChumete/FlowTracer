package metrics

import (
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
)

var (
	// Основной счетчик транзакций  
	TransactionsTotal = promauto.NewCounterVec(
		prometheus.CounterOpts{
			Name: "flowtracer_transactions_total",
			Help: "Total number of transactions",
		},
		[]string{"status"},
	)

	// Активные gRPC соединения
	ActiveConnections = promauto.NewGauge(
		prometheus.GaugeOpts{
			Name: "flowtracer_active_connections", 
			Help: "Number of active gRPC connections",
		},
	)
)

// IncrementTransactionCounter увеличивает счетчик транзакций
func IncrementTransactionCounter(status string) {
	TransactionsTotal.WithLabelValues(status).Inc()
}