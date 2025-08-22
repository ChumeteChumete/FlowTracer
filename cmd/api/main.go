package main

import (
    "context"
    "flowtracer/internal/database"
    "flowtracer/internal/healthcheck"
    "flowtracer/internal/kafka"
    "flowtracer/proto/flowtracerpb"
    "log"
    "net"
    "os"
    "time"

    "github.com/google/uuid"
    "google.golang.org/grpc"
)

type Server struct {
    flowtracerpb.UnimplementedFlowTracerServiceServer
    kafkaProducer *kafka.Producer
}

func main() {
    // 1. Инициализация базы данных
    log.Println("Initializing database connection...")
    
    dbConfig := database.NewConfigFromEnv()
    db, err := database.Connect(dbConfig)
    if err != nil {
        log.Fatalf("FATAL: Database connection failed: %v", err)
    }
    defer db.Close()

    // 2. Запуск миграций
    log.Println("Running database migrations...")
    if err := database.RunMigrations(db); err != nil {
        log.Fatalf("FATAL: Database migrations failed: %v", err)
    }

    // 3. Инициализация Kafka
    kafkaBrokers := os.Getenv("KAFKA_BROKERS")
    if kafkaBrokers == "" {
        kafkaBrokers = "kafka:9092"
    }
    brokers := []string{kafkaBrokers}
    
    log.Printf("Connecting to Kafka brokers: %v", brokers)
    producer, err := kafka.NewProducer(brokers)
    if err != nil {
        log.Fatalf("FATAL: Kafka connection failed: %v", err)
    }
    defer producer.Close()

    // 4. Health check при старте
    log.Println("Performing health checks...")
    hc := healthcheck.New()
    kafkaChecker := healthcheck.NewKafkaChecker(
        brokers,
        "transactions",
        10*time.Second,
    )
    hc.Register(kafkaChecker)

    ctx := context.Background()
    results := hc.Run(ctx)
    
    if !hc.IsHealthy(results) {
        for service, result := range results {
            if result.Status != "up" {
                log.Printf("CRITICAL: %s is down: %v", service, result.Error)
            }
        }
        log.Fatal("Health check failed during startup")
    }

    log.Println("All systems ready!")

    // 5. Инициализация gRPC сервера
    server := &Server{
        kafkaProducer: producer,
    }

    lis, err := net.Listen("tcp", ":50051")
    if err != nil {
        log.Fatalf("Failed to listen on port 50051: %v", err)
    }

    s := grpc.NewServer()
    flowtracerpb.RegisterFlowTracerServiceServer(s, server)

    log.Printf("FlowTracer API server started on %v", lis.Addr())
    if err := s.Serve(lis); err != nil {
        log.Fatalf("Server failed: %v", err)
    }
}

func (s *Server) SendTransaction(ctx context.Context, req *flowtracerpb.SendTransactionRequest) (*flowtracerpb.SendTransactionResponse, error) {
    txID := uuid.New().String()

    err := s.kafkaProducer.SendTransactionEvent(txID, req.FromUsername, req.ToUsername, req.Amount, req.IdempotencyKey)

    if err != nil {
        log.Printf("Failed to send transaction to Kafka: %v", err)
        return &flowtracerpb.SendTransactionResponse{
            Accepted: false,
            Message:  "failed to enqueue transaction",
        }, nil
    }

    log.Printf("Transaction %s accepted for processing", txID)
    return &flowtracerpb.SendTransactionResponse{
        Accepted:      true,
        TransactionId: txID,
        Message:       "accepted for processing",
    }, nil
}