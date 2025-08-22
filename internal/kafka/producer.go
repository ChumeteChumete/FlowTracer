package kafka

import (
	"flowtracer/proto/flowtracerpb"
    "github.com/IBM/sarama"
    "google.golang.org/protobuf/proto"
)

type Producer struct {
	saramaProducer sarama.SyncProducer
}

func NewProducer(brokers []string) (*Producer, error) {
    config := sarama.NewConfig()
    config.Producer.Idempotent = true
	config.Net.MaxOpenRequests = 1
    config.Producer.RequiredAcks = sarama.WaitForAll
    config.Producer.Transaction.ID = "flowtracer-txn"
    config.Producer.Retry.Max = 5
	config.Producer.Return.Successes = true

    saramaProducer, err := sarama.NewSyncProducer(brokers, config)
    if err != nil {
        return nil, err
    }

    return &Producer{saramaProducer: saramaProducer}, nil
}

func (p *Producer) SendTransactionEvent(txID, from, to string, amount int64, key string) error {
	// Создаем протобаф-событие
	pbEvent := &flowtracerpb.SendTransactionRequest{
		FromUsername: 	from,
		ToUsername: 	to,
		Amount: 		amount,
		IdempotencyKey: key,
	}

	data, err := proto.Marshal(pbEvent)
	if err != nil {
		return err
	}

	message := &sarama.ProducerMessage{
			Topic: "transactions",
			Key: 	sarama.StringEncoder(txID),
			Value:  sarama.ByteEncoder(data),
		}
	
	// Отправить в Kafka
	_, _, err = p.saramaProducer.SendMessage(message)
	return err
}

func (p *Producer) Close() error {
    return p.saramaProducer.Close()
}