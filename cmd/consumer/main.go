package main

import (
	"context"
	"fmt"
	"log"
	"os"
	"os/signal"
	"sync"
	"syscall"
	"time"

	"flowtracer/internal/database"
	"flowtracer/proto/flowtracerpb"
	"flowtracer/internal/logger"
	"flowtracer/internal/metrics"

	"github.com/IBM/sarama"
	"google.golang.org/protobuf/proto"
)

const (
    StatusFailed    = "failed"
    StatusCompleted = "completed"
)

type Consumer struct {
	userRepo        *database.UserRepository
	transactionRepo *database.TransactionRepository
	ready           chan bool
}

func NewConsumer(userRepo *database.UserRepository, txRepo *database.TransactionRepository) *Consumer {
	return &Consumer{
		userRepo:        userRepo,
		transactionRepo: txRepo,
		ready:           make(chan bool),
	}
}

func (c *Consumer) Setup(sarama.ConsumerGroupSession) error {
	close(c.ready)
	return nil
}

func (c *Consumer) Cleanup(sarama.ConsumerGroupSession) error {
	return nil
}

func (c *Consumer) ConsumeClaim(session sarama.ConsumerGroupSession, claim sarama.ConsumerGroupClaim) error {
	for {
		select {
		case message := <-claim.Messages():
			if message == nil {
				return nil
			}

			// Парсим protobuf сообщение
			var req flowtracerpb.SendTransactionRequest
			if err := proto.Unmarshal(message.Value, &req); err != nil {
				logger.WithFields(map[string]interface{}{
					"topic": message.Topic,
					"partition": message.Partition,
					"offset": message.Offset,
				}).WithError(err).Error("Failed to unmarshal message")
				session.MarkMessage(message, "")
				continue
			}

			// Извлекаем transaction ID из ключа сообщения
			txID := string(message.Key)
			logger.WithFields(map[string]interface{}{
				"transaction_id": txID,
				"from": req.FromUsername,
				"to": req.ToUsername,
				"amount": req.Amount,
				"idempotency_key": req.IdempotencyKey,
			}).Info("Processing transaction")

			// Обрабатываем транзакцию
			if err := c.processTransaction(session.Context(), txID, &req); err != nil {
				logger.WithFields(map[string]interface{}{
					"transaction_id": txID,
				}).WithError(err).Error("Transaction failed")
			} else {
				logger.WithFields(map[string]interface{}{
					"transaction_id": txID,
				}).Info("Transaction completed successfully")
			}

			session.MarkMessage(message, "")

		case <-session.Context().Done():
			return nil
		}
	}
}

// processTransaction обрабатывает одну транзакцию
func (c *Consumer) processTransaction(ctx context.Context, txID string, req *flowtracerpb.SendTransactionRequest) error {

	txLogger := logger.WithFields(map[string]interface{}{
		"transaction_id": txID,
		"from": req.FromUsername,
		"to": req.ToUsername,
		"amount": req.Amount,
		"idempotency_key": req.IdempotencyKey,
	})

	// Контекст с таймаутом для обработки
	ctx, cancel := context.WithTimeout(ctx, 30*time.Second)
	defer cancel()

	// 1. Проверяем идемпотентность
	existingTx, err := c.transactionRepo.GetTransactionByIdempotencyKey(ctx, req.IdempotencyKey)
	if err != nil {
		txLogger.WithError(err).Error("Failed to check idempotency")
		return fmt.Errorf("failed to check idempotency: %w", err)
	}

	if existingTx != nil {
		txLogger.WithFields(map[string]interface{}{
			"existing_status": existingTx.Status,
		}).Warn("Duplicate transaction detected")
		metrics.IncrementTransactionCounter("duplicate")
		return nil // Не ошибка, просто дубликат
	}

	// 2. Создаем запись транзакции в БД
	_, err = c.transactionRepo.CreateTransaction(ctx, txID, req.FromUsername, req.ToUsername, req.Amount, req.IdempotencyKey)
	if err != nil {
		// Возможно конфликт по idempotency_key
		if isDuplicateKeyError(err) {
			txLogger.Warn("Idempotency key conflict")
			metrics.IncrementTransactionCounter("duplicate")
			return nil
		}
		return fmt.Errorf("failed to create transaction record: %w", err)
	}

	// 3. Выполняем перевод денег
	err = c.userRepo.TransferMoney(ctx, req.FromUsername, req.ToUsername, req.Amount)
	if err != nil {
		// Помечаем транзакцию как неудачную
		if updateErr := c.transactionRepo.UpdateTransactionStatus(ctx, txID, StatusFailed); updateErr != nil {
			txLogger.WithError(updateErr).Error("Failed to update transaction status to failed")
		}
		metrics.IncrementTransactionCounter("failed")
		return fmt.Errorf("money transfer failed: %w", err)
	}

	// 4. Помечаем транзакцию как завершенную
	if err := c.transactionRepo.UpdateTransactionStatus(ctx, txID, StatusCompleted); err != nil {
		txLogger.WithError(err).Warn("Transaction completed but failed to update status")
		// Не возвращаем ошибку, так как деньги уже переведены
	}

	txLogger.Info("Money transferred successfully")
	metrics.IncrementTransactionCounter("completed")
	return nil
}

// isDuplicateKeyError проверяет, является ли ошибка конфликтом уникального ключа
func isDuplicateKeyError(err error) bool {
	if err == nil {
		return false
	}
	// PostgreSQL error code для нарушения уникальности
	return containsAny(err.Error(), []string{
		"duplicate key value",
		"unique constraint",
		"already exists",
	})
}

// containsAny проверяет, содержит ли строка любую из подстрок
func containsAny(s string, substrings []string) bool {
	for _, substr := range substrings {
		if len(s) >= len(substr) {
			for i := 0; i <= len(s)-len(substr); i++ {
				if s[i:i+len(substr)] == substr {
					return true
				}
			}
		}
	}
	return false
}

func main() {
	// Инициализация базы данных
	log.Println("🔌 Connecting to database...")
	dbConfig := database.NewConfigFromEnv()
	db, err := database.Connect(dbConfig)
	if err != nil {
		log.Fatalf("❌ Database connection failed: %v", err)
	}
	defer db.Close()

	// Инициализация репозиториев
	userRepo := database.NewUserRepository(db)
	txRepo := database.NewTransactionRepository(db)

	// Kafka настройки
	brokers := []string{getEnv("KAFKA_BROKERS", "kafka:9092")}
	groupID := getEnv("KAFKA_GROUP_ID", "flowtracer-consumer")
	topics := []string{getEnv("KAFKA_TOPICS", "transactions")}

	config := sarama.NewConfig()
	config.Version = sarama.V2_8_0_0
	config.Consumer.Group.Rebalance.Strategy = sarama.NewBalanceStrategyRoundRobin()
	config.Consumer.Offsets.Initial = sarama.OffsetOldest
	config.Consumer.IsolationLevel = sarama.ReadCommitted
	config.Consumer.Return.Errors = true
	config.Consumer.Group.Session.Timeout = 10 * time.Second
	config.Consumer.Group.Heartbeat.Interval = 3 * time.Second

	// Создание consumer group
	consumerGroup, err := sarama.NewConsumerGroup(brokers, groupID, config)
	if err != nil {
		log.Fatalf("❌ Failed to create consumer group: %v", err)
	}

	// Создание handler
	consumer := NewConsumer(userRepo, txRepo)

	// Graceful shutdown
	ctx, cancel := context.WithCancel(context.Background())
	wg := &sync.WaitGroup{}

	// Горутина для обработки ошибок
	wg.Add(1)
	go func() {
		defer wg.Done()
		for err := range consumerGroup.Errors() {
			log.Printf("❌ Consumer error: %v", err)
		}
	}()

	// Горутина для потребления сообщений
	wg.Add(1)
	go func() {
		defer wg.Done()
		for {
			if err := consumerGroup.Consume(ctx, topics, consumer); err != nil {
				log.Printf("❌ Error from consumer: %v", err)
				return
			}
			// Проверяем, не отменен ли контекст
			if ctx.Err() != nil {
				return
			}
		}
	}()

	// Ждем готовности consumer
	<-consumer.ready
	log.Println("🚀 Consumer started and ready")

	// Ожидание сигнала завершения
	sigterm := make(chan os.Signal, 1)
	signal.Notify(sigterm, syscall.SIGINT, syscall.SIGTERM)

	select {
	case <-ctx.Done():
		log.Println("⏹️ Context cancelled")
	case <-sigterm:
		log.Println("⏹️ Termination signal received")
	}

	log.Println("🛑 Shutting down consumer...")
	cancel()

	// Graceful shutdown с таймаутом
	done := make(chan struct{})
	go func() {
		wg.Wait()
		close(done)
	}()

	select {
	case <-done:
		log.Println("✅ Consumer shut down gracefully")
	case <-time.After(30 * time.Second):
		log.Println("⚠️ Forced shutdown after timeout")
	}

	if err := consumerGroup.Close(); err != nil {
		log.Printf("❌ Error closing consumer group: %v", err)
	}
}

func getEnv(key, defaultValue string) string {
	if value := os.Getenv(key); value != "" {
		return value
	}
	return defaultValue
}