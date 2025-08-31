package kafka

import (
	"flowtracer/proto/flowtracerpb"
	"flowtracer/internal/logger"
	"fmt"
	"time"

	"github.com/IBM/sarama"
	"google.golang.org/protobuf/proto"
)

type Producer struct {
	saramaProducer sarama.SyncProducer
	brokers        []string
}

func NewProducer(brokers []string) (*Producer, error) {
	config := sarama.NewConfig()
	
	// Настройки для идемпотентности и надежности
	config.Version = sarama.V2_8_0_0
	config.Producer.Idempotent = true
	config.Net.MaxOpenRequests = 1
	config.Producer.RequiredAcks = sarama.WaitForAll
	config.Producer.Retry.Max = 5
	config.Producer.Retry.Backoff = 100 * time.Millisecond
	config.Producer.Return.Successes = true
	config.Producer.Return.Errors = true
	
	// Настройки сжатия и батчинга
	config.Producer.Compression = sarama.CompressionSnappy
	config.Producer.Flush.Frequency = 500 * time.Millisecond
	config.Producer.Flush.Messages = 100
	config.Producer.Flush.Bytes = 1024 * 1024 // 1MB
	
	// Таймауты
	config.Net.DialTimeout = 30 * time.Second
	config.Net.ReadTimeout = 30 * time.Second
	config.Net.WriteTimeout = 30 * time.Second

	saramaProducer, err := sarama.NewSyncProducer(brokers, config)
	if err != nil {
		return nil, fmt.Errorf("failed to create Kafka producer: %w", err)
	}

	logger.WithFields(map[string]interface{}{
		"brokers": brokers,
	}).Info("Kafka producer initialized")
	return &Producer{
		saramaProducer: saramaProducer,
		brokers:        brokers,
	}, nil
}

// SendTransactionEvent отправляет событие транзакции в Kafka
func (p *Producer) SendTransactionEvent(txID, from, to string, amount int64, idempotencyKey string) error {
	// Создаем протобаф-событие
	pbEvent := &flowtracerpb.SendTransactionRequest{
		FromUsername:   from,
		ToUsername:     to,
		Amount:         amount,
		IdempotencyKey: idempotencyKey,
	}

	// Сериализуем в bytes
	data, err := proto.Marshal(pbEvent)
	if err != nil {
		return fmt.Errorf("failed to marshal protobuf: %w", err)
	}

	// Создаем сообщение для Kafka
	message := &sarama.ProducerMessage{
		Topic: "transactions",
		Key:   sarama.StringEncoder(txID), // Ключ для партиционирования
		Value: sarama.ByteEncoder(data),
		Headers: []sarama.RecordHeader{
			{
				Key:   []byte("idempotency_key"),
				Value: []byte(idempotencyKey),
			},
			{
				Key:   []byte("event_type"),
				Value: []byte("transaction_request"),
			},
			{
				Key:   []byte("timestamp"),
				Value: []byte(fmt.Sprintf("%d", time.Now().Unix())),
			},
		},
		Timestamp: time.Now(),
	}

	// Отправляем сообщение
	partition, offset, err := p.saramaProducer.SendMessage(message)
	if err != nil {
		return fmt.Errorf("failed to send message to Kafka: %w", err)
	}

	logger.WithFields(map[string]interface{}{
		"topic": "transactions",
		"partition": partition,
		"offset": offset,
		"transaction_id": txID,
	}).Info("Message sent successfully")

	return nil
}

func (p *Producer) Close() error {
    if p.saramaProducer != nil {
        logger.Info("Closing Kafka producer")
        // Дожидаемся отправки всех буферизованных сообщений
        return p.saramaProducer.Close()
    }
    return nil
}
