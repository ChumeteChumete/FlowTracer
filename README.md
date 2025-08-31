# FlowTracer

Сервис для перевода денег между пользователями.

## Что это

Простое приложение на Go которое умеет:
- Переводить деньги между пользователями
- Проверять баланс
- Сохранять все в базу данных

## Как запустить

1. Склонируй репозиторий
2. Установи Docker
3. Запусти все сервисы:

```bash
cd docker
docker-compose up -d
```

Готово! API работает на порту 50051.

## Как использовать

### Отправить деньги

```bash
grpcurl -plaintext -d '{\"from_username\":\"alice\",\"to_username\":\"bob\",\"amount\":100,\"idempotency_key\":\"test123\"}' localhost:50051 flowtracer.FlowTracerService/SendTransaction
```

### Проверить баланс

```bash
grpcurl -plaintext -d '{\"username\":\"alice\"}' localhost:50051 flowtracer.FlowTracerService/GetBalance
```

## Что внутри

- **Go** - основной язык
- **gRPC** - API
- **PostgreSQL** - база данных
- **Kafka** - очередь сообщений
- **Docker** - контейнеры

## Структура проекта

```
flowtracer/
├── cmd/api/           - главный сервер
├── internal/kafka/    - работа с Kafka
├── internal/database/ - работа с базой
├── proto/            - описание API
└── docker-compose.yml - настройки контейнеров
```

## Тесты

Запуск тестов:

```bash
go test ./...
```

## Разработка

Для разработки нужно:
1. Go 1.24+
2. Docker
3. PostgreSQL (если локально)

Запуск без Docker:

```bash
# Запустить только базу и Kafka
cd docker
docker-compose up -d postgres kafka

# Вернуться в корень проекта
cd ..

# Запустить API локально
go run cmd/api/main.go
```

## Правила валидации

### Username:
- От 3 до 50 символов
- Только буквы, цифры, _ и -
- Не может начинаться или заканчиваться на _ или -

### Переводы:
- Сумма больше 0
- Максимум 10 млн рублей
- Нельзя переводить самому себе
- Обязательный idempotency_key

## Порты

- API: 50051
- PostgreSQL: 5433
- Kafka: 29092
- Kafka UI: 9000

## База данных

Таблицы:
- `users` - пользователи и их балансы
- `transactions` - история переводов

Миграции запускаются автоматически при старте.