package main

import (
    "context"
    "flowtracer/internal/database"
    "flowtracer/internal/healthcheck"
    "flowtracer/internal/kafka"
    "flowtracer/proto/flowtracerpb"
    "flowtracer/internal/middleware"
    "flowtracer/internal/logger"
    "net"
    "os"
    "os/signal" 
    "syscall"
    "strings"
    "time"

    "github.com/google/uuid"
    "google.golang.org/grpc"
    "google.golang.org/grpc/codes"
    "google.golang.org/grpc/status"
    "google.golang.org/grpc/reflection"
)

type Server struct {
    flowtracerpb.UnimplementedFlowTracerServiceServer
    kafkaProducer *kafka.Producer
    userRepo      *database.UserRepository
}

func main() {
    // 1. Инициализация базы данных
    logger.Info("Initializing database connection")
    
    dbConfig := database.NewConfigFromEnv()
    db, err := database.Connect(dbConfig)
    if err != nil {
        logger.WithError(err).Fatal("Database connection failed")
    }
    defer db.Close()

    // 2. Запуск миграций
    logger.Info("Running database migrations")
    if err := database.RunMigrations(db); err != nil {
        logger.WithError(err).Fatal("Database migrations failed")
    }

    // 3. Инициализация Kafka
    kafkaBrokers := os.Getenv("KAFKA_BROKERS")
    if kafkaBrokers == "" {
        kafkaBrokers = "kafka:9092"
    }
    brokers := []string{kafkaBrokers}
    
    logger.WithField("brokers", brokers).Info("Connecting to Kafka brokers")
    producer, err := kafka.NewProducer(brokers)
    if err != nil {
        logger.WithError(err).Fatal("Kafka connection failed")
    }
    defer producer.Close()

    // 4. Health check при старте
    logger.Info("Performing health checks")
    hc := healthcheck.New()
    
    kafkaChecker := healthcheck.NewKafkaChecker(
        brokers,
        "transactions",
        10*time.Second,
    )
    postgresChecker := healthcheck.NewPostgresChecker(db, 5*time.Second)
    
    hc.Register(kafkaChecker)
    hc.Register(postgresChecker)

    ctx := context.Background()
    results := hc.Run(ctx)
    
    if !hc.IsHealthy(results) {
        for service, result := range results {
        if result.Status != "up" {
            logger.WithFields(map[string]interface{}{
                "service": service,
                "error": result.Error,
            }).Error("Service is down")
        }
    }
    logger.Fatal("Health check failed during startup")
    }

    logger.Info("All systems ready!")

    // 5. Инициализация репозиториев
    userRepo := database.NewUserRepository(db)

    // 6. Инициализация gRPC сервера
    server := &Server{
        kafkaProducer: producer,
        userRepo:      userRepo,
    }

    lis, err := net.Listen("tcp", ":50051")
    if err != nil {
        logger.WithError(err).Fatal("Failed to listen on port 50051")
    }

    s := grpc.NewServer(
        grpc.ChainUnaryInterceptor(
            middleware.LoggingInterceptor(),
            middleware.MetricsInterceptor(),
            middleware.RecoveryInterceptor(),
        ),
    )
    flowtracerpb.RegisterFlowTracerServiceServer(s, server)
    reflection.Register(s)

    logger.WithField("address", lis.Addr()).Info("FlowTracer API server starting")
    sigChan := make(chan os.Signal, 1)
    signal.Notify(sigChan, os.Interrupt, syscall.SIGTERM)

    go func() {
        if err := s.Serve(lis); err != nil {
            logger.WithError(err).Error("Server stopped")
        }
    }()

    <-sigChan
    logger.Info("Shutting down gracefully")
    s.GracefulStop()
    logger.Info("Server stopped")
}

// SendTransaction обрабатывает запрос на отправку транзакции
func (s *Server) SendTransaction(ctx context.Context, req *flowtracerpb.SendTransactionRequest) (*flowtracerpb.SendTransactionResponse, error) {
    // Валидация входных данных
    if err := validateSendTransactionRequest(req); err != nil {
        logger.FromContext(ctx).WithError(err).Warn("Validation failed")
        return &flowtracerpb.SendTransactionResponse{
            Accepted: false,
            Message:  err.Error(),
        }, nil
    }

    // Проверяем существование отправителя
    sender, err := s.userRepo.GetUserByUsername(ctx, req.FromUsername)
    if err != nil {
        logger.FromContext(ctx).WithError(err).Error("Failed to check sender")
        return &flowtracerpb.SendTransactionResponse{
            Accepted: false,
            Message:  "internal error",
        }, nil
    }
    if sender == nil {
        return &flowtracerpb.SendTransactionResponse{
            Accepted: false,
            Message:  "sender not found",
        }, nil
    }

    // Проверяем существование получателя
    recipient, err := s.userRepo.GetUserByUsername(ctx, req.ToUsername)
    if err != nil {
        logger.FromContext(ctx).WithError(err).Error("Failed to check recipient")
        return &flowtracerpb.SendTransactionResponse{
            Accepted: false,
            Message:  "internal error",
        }, nil
    }
    if recipient == nil {
        return &flowtracerpb.SendTransactionResponse{
            Accepted: false,
            Message:  "recipient not found",
        }, nil
    }

    // Проверяем достаточность средств
    if sender.Balance < req.Amount {
        return &flowtracerpb.SendTransactionResponse{
            Accepted: false,
            Message:  "insufficient balance",
        }, nil
    }

    // Генерируем ID транзакции
    txID := uuid.New().String()

    // Отправляем в Kafka для асинхронной обработки
    err = s.kafkaProducer.SendTransactionEvent(txID, req.FromUsername, req.ToUsername, req.Amount, req.IdempotencyKey)
    if err != nil {
        logger.FromContext(ctx).WithError(err).Error("Failed to send transaction to Kafka")
        return &flowtracerpb.SendTransactionResponse{
            Accepted: false,
            Message:  "failed to enqueue transaction",
        }, nil
    }

    logger.FromContext(ctx).WithFields(map[string]interface{}{
        "transaction_id": txID,
        "from": req.FromUsername,
        "to": req.ToUsername,
        "amount": req.Amount,
    }).Info("Transaction accepted for processing")
    
    return &flowtracerpb.SendTransactionResponse{
        Accepted:      true,
        TransactionId: txID,
        Message:       "accepted for processing",
    }, nil
}

// GetBalance возвращает баланс пользователя
func (s *Server) GetBalance(ctx context.Context, req *flowtracerpb.GetBalanceRequest) (*flowtracerpb.GetBalanceResponse, error) {
    // Валидация
    if req.Username == "" {
        return nil, status.Errorf(codes.InvalidArgument, "username cannot be empty")
    }
    
    if err := validateUsername(req.Username); err != nil {
        return nil, status.Errorf(codes.InvalidArgument, "invalid username: %v", err)
    }

    // Получаем баланс
    balance, found, err := s.userRepo.GetUserBalance(ctx, req.Username)
    if err != nil {
        logger.FromContext(ctx).WithFields(map[string]interface{}{
            "username": req.Username,
        }).WithError(err).Error("Failed to get balance")
        return nil, status.Errorf(codes.Internal, "failed to get balance")
    }

    logger.FromContext(ctx).WithFields(map[string]interface{}{
        "username": req.Username,
        "found": found,
        "balance": balance,
    }).Info("Balance request completed")

    return &flowtracerpb.GetBalanceResponse{
        Balance: balance,
        Found:   found,
    }, nil
}

// validateSendTransactionRequest проверяет корректность запроса на транзакцию
func validateSendTransactionRequest(req *flowtracerpb.SendTransactionRequest) error {
    if req.FromUsername == "" {
        return status.Error(codes.InvalidArgument, "from_username cannot be empty")
    }
    
    if req.ToUsername == "" {
        return status.Error(codes.InvalidArgument, "to_username cannot be empty")
    }
    
    if req.FromUsername == req.ToUsername {
        return status.Error(codes.InvalidArgument, "cannot send money to yourself")
    }
    
    if req.Amount <= 0 {
        return status.Error(codes.InvalidArgument, "amount must be positive")
    }
    
    if req.Amount > 1000000000 { // 10 млн рублей в копейках
        return status.Error(codes.InvalidArgument, "amount too large")
    }
    
    if req.IdempotencyKey == "" {
        return status.Error(codes.InvalidArgument, "idempotency_key cannot be empty")
    }
    
    if err := validateUsername(req.FromUsername); err != nil {
        return status.Errorf(codes.InvalidArgument, "invalid from_username: %v", err)
    }
    
    if err := validateUsername(req.ToUsername); err != nil {
        return status.Errorf(codes.InvalidArgument, "invalid to_username: %v", err)
    }
    
    return nil
}

// validateUsername проверяет корректность имени пользователя
func validateUsername(username string) error {
    if len(username) < 3 {
        return status.Error(codes.InvalidArgument, "username must be at least 3 characters")
    }
    
    if len(username) > 50 {
        return status.Error(codes.InvalidArgument, "username must be at most 50 characters")
    }
    
    for _, r := range username {
        if !((r >= 'a' && r <= 'z') || (r >= 'A' && r <= 'Z') || 
             (r >= '0' && r <= '9') || r == '_' || r == '-') {
            return status.Error(codes.InvalidArgument, "username contains invalid characters")
        }
    }
    
    if strings.HasPrefix(username, "_") || strings.HasPrefix(username, "-") ||
       strings.HasSuffix(username, "_") || strings.HasSuffix(username, "-") {
        return status.Error(codes.InvalidArgument, "username cannot start or end with _ or -")
    }
    
    return nil
}