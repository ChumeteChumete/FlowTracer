package healthcheck

import (
    "context"
    "fmt"
    "github.com/IBM/sarama"
    "time"
)

// KafkaChecker проверка подключения к Kafka
type KafkaChecker struct {
    brokers []string
    topic   string
    timeout time.Duration
}

// NewKafkaChecker создает проверку для Kafka
func NewKafkaChecker(brokers []string, topic string, timeout time.Duration) *KafkaChecker {
    return &KafkaChecker{
        brokers: brokers,
        topic:   topic,
        timeout: timeout,
    }
}

func (k *KafkaChecker) Name() string {
    return "kafka"
}

func (k *KafkaChecker) Check(ctx context.Context) error {
    ctx, cancel := context.WithTimeout(ctx, k.timeout)
    defer cancel() // Важно: defer после создания контекста
    
    config := sarama.NewConfig()
    config.Version = sarama.V2_8_0_0
    config.Producer.Return.Successes = true
    
    // 1. Проверяем подключение к брокерам (используем ctx с таймаутом)
    client, err := sarama.NewClient(k.brokers, config)
    if err != nil {
        return fmt.Errorf("failed to create client: %w", err)
    }
    defer client.Close()
    
    // 2. Проверяем, что топик существует
    topics, err := client.Topics()
    if err != nil {
        return fmt.Errorf("failed to get topics: %w", err)
    }
    
    topicExists := false
    for _, t := range topics {
        if t == k.topic {
            topicExists = true
            break
        }
    }
    
    if !topicExists {
        return fmt.Errorf("topic %s does not exist", k.topic)
    }
    
    // 3. Проверяем отправку сообщения (используем ctx с таймаутом)
    producer, err := sarama.NewSyncProducerFromClient(client)
    if err != nil {
        return fmt.Errorf("failed to create producer: %w", err)
    }
    defer producer.Close()
    
    testMsg := &sarama.ProducerMessage{
        Topic: k.topic,
        Value: sarama.StringEncoder(fmt.Sprintf("health-check-%d", time.Now().Unix())),
    }
    
    // Используем контекст для отправки
    select {
    case <-ctx.Done():
        return fmt.Errorf("kafka health check timeout: %w", ctx.Err())
    default:
        _, _, err = producer.SendMessage(testMsg)
        if err != nil {
            return fmt.Errorf("failed to send test message: %w", err)
        }
    }
    
    return nil
}