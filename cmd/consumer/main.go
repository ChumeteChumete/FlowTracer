package main

import (
	"context"
	"log"

	"flowtracer/proto/flowtracerpb"

	"github.com/IBM/sarama"
	"google.golang.org/protobuf/proto"
)

type Consumer struct{}

func (Consumer) Setup(_ sarama.ConsumerGroupSession) error   { return nil }
func (Consumer) Cleanup(_ sarama.ConsumerGroupSession) error { return nil }

func (Consumer) ConsumeClaim(session sarama.ConsumerGroupSession, claim sarama.ConsumerGroupClaim) error {
	for msg := range claim.Messages() {
		var req flowtracerpb.SendTransactionRequest
		if err := proto.Unmarshal(msg.Value, &req); err != nil {
			log.Printf("❌ Ошибка парсинга proto: %v", err)
			// Помечаем сообщение как обработанное даже при ошибке, чтобы не зависнуть
			session.MarkMessage(msg, "")
			continue
		}

		log.Printf("📥 Транзакция: %s -> %s, сумма=%d, ключ=%s",
			req.FromUsername, req.ToUsername, req.Amount, req.IdempotencyKey)

		// TODO: бизнес-логику обработки

		session.MarkMessage(msg, "")
	}
	return nil
}

func main() {
	brokers := []string{"kafka:9092"}
	groupID := "flowtracer-consumer"
	topics := []string{"transactions"}

	config := sarama.NewConfig()
	config.Version = sarama.V2_8_0_0
	config.Consumer.Group.Rebalance.Strategy = sarama.NewBalanceStrategyRoundRobin()
	config.Consumer.Offsets.Initial = sarama.OffsetOldest
	config.Consumer.IsolationLevel = sarama.ReadCommitted
	config.Consumer.Return.Errors = true

	consumerGroup, err := sarama.NewConsumerGroup(brokers, groupID, config)
	if err != nil {
		log.Fatalf("Ошибка создания consumer group: %v", err)
	}
	defer consumerGroup.Close()

	ctx := context.Background()
	handler := Consumer{}

	log.Println("🚀 Консьюмер запущен")
	
	// Горутина для логирования ошибок
	go func() {
		for err := range consumerGroup.Errors() {
			log.Printf("❌ Ошибка consumer: %v", err)
		}
	}()

	for {
		if err := consumerGroup.Consume(ctx, topics, handler); err != nil {
			log.Printf("Ошибка в consume: %v", err)
		}
	}
}