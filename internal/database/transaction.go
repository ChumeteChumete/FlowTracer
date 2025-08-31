package database

import (
	"context"
	"database/sql"
	"fmt"
	"time"

	"github.com/google/uuid"
)

type Transaction struct {
	ID             uuid.UUID `db:"id"`
	FromUsername   string    `db:"from_username"`
	ToUsername     string    `db:"to_username"`
	Amount         int64     `db:"amount"`
	Status         string    `db:"status"`
	IdempotencyKey string    `db:"idempotency_key"`
	CreatedAt      time.Time `db:"created_at"`
	UpdatedAt      time.Time `db:"updated_at"`
}

const (
	StatusPending   = "pending"
	StatusCompleted = "completed"
	StatusFailed    = "failed"
	StatusCancelled = "cancelled"
)

type TransactionRepository struct {
	db *sql.DB
}

func NewTransactionRepository(db *sql.DB) *TransactionRepository {
	return &TransactionRepository{db: db}
}

// CreateTransaction создает новую транзакцию
func (r *TransactionRepository) CreateTransaction(ctx context.Context, txID, fromUsername, toUsername string, amount int64, idempotencyKey string) (*Transaction, error) {
	query := `
		INSERT INTO transactions (id, from_username, to_username, amount, status, idempotency_key) 
		VALUES ($1, $2, $3, $4, $5, $6) 
		RETURNING id, from_username, to_username, amount, status, idempotency_key, created_at, updated_at
	`
	
	var tx Transaction
	err := r.db.QueryRowContext(ctx, query, txID, fromUsername, toUsername, amount, StatusPending, idempotencyKey).Scan(
		&tx.ID,
		&tx.FromUsername,
		&tx.ToUsername,
		&tx.Amount,
		&tx.Status,
		&tx.IdempotencyKey,
		&tx.CreatedAt,
		&tx.UpdatedAt,
	)
	
	if err != nil {
		return nil, fmt.Errorf("failed to create transaction: %w", err)
	}
	
	return &tx, nil
}

// GetTransactionByIdempotencyKey находит транзакцию по ключу идемпотентности
func (r *TransactionRepository) GetTransactionByIdempotencyKey(ctx context.Context, key string) (*Transaction, error) {
	query := `
		SELECT id, from_username, to_username, amount, status, idempotency_key, created_at, updated_at
		FROM transactions 
		WHERE idempotency_key = $1
	`
	
	var tx Transaction
	err := r.db.QueryRowContext(ctx, query, key).Scan(
		&tx.ID,
		&tx.FromUsername,
		&tx.ToUsername,
		&tx.Amount,
		&tx.Status,
		&tx.IdempotencyKey,
		&tx.CreatedAt,
		&tx.UpdatedAt,
	)
	
	if err == sql.ErrNoRows {
		return nil, nil
	}
	
	if err != nil {
		return nil, fmt.Errorf("failed to get transaction by idempotency key: %w", err)
	}
	
	return &tx, nil
}

// UpdateTransactionStatus обновляет статус транзакции
func (r *TransactionRepository) UpdateTransactionStatus(ctx context.Context, txID string, status string) error {
	query := `
		UPDATE transactions 
		SET status = $1, updated_at = CURRENT_TIMESTAMP 
		WHERE id = $2
	`
	
	result, err := r.db.ExecContext(ctx, query, status, txID)
	if err != nil {
		return fmt.Errorf("failed to update transaction status: %w", err)
	}
	
	rowsAffected, err := result.RowsAffected()
	if err != nil {
		return fmt.Errorf("failed to get rows affected: %w", err)
	}
	
	if rowsAffected == 0 {
		return fmt.Errorf("transaction %s not found", txID)
	}
	
	return nil
}

// GetTransactionByID возвращает транзакцию по ID
func (r *TransactionRepository) GetTransactionByID(ctx context.Context, txID string) (*Transaction, error) {
	query := `
		SELECT id, from_username, to_username, amount, status, idempotency_key, created_at, updated_at
		FROM transactions 
		WHERE id = $1
	`
	
	var tx Transaction
	err := r.db.QueryRowContext(ctx, query, txID).Scan(
		&tx.ID,
		&tx.FromUsername,
		&tx.ToUsername,
		&tx.Amount,
		&tx.Status,
		&tx.IdempotencyKey,
		&tx.CreatedAt,
		&tx.UpdatedAt,
	)
	
	if err == sql.ErrNoRows {
		return nil, nil
	}
	
	if err != nil {
		return nil, fmt.Errorf("failed to get transaction: %w", err)
	}
	
	return &tx, nil
}

// GetUserTransactions возвращает транзакции пользователя с пагинацией
func (r *TransactionRepository) GetUserTransactions(ctx context.Context, username string, limit, offset int) ([]*Transaction, error) {
	query := `
		SELECT id, from_username, to_username, amount, status, idempotency_key, created_at, updated_at
		FROM transactions 
		WHERE from_username = $1 OR to_username = $1
		ORDER BY created_at DESC
		LIMIT $2 OFFSET $3
	`
	
	rows, err := r.db.QueryContext(ctx, query, username, limit, offset)
	if err != nil {
		return nil, fmt.Errorf("failed to get user transactions: %w", err)
	}
	defer rows.Close()
	
	var transactions []*Transaction
	for rows.Next() {
		var tx Transaction
		err := rows.Scan(
			&tx.ID,
			&tx.FromUsername,
			&tx.ToUsername,
			&tx.Amount,
			&tx.Status,
			&tx.IdempotencyKey,
			&tx.CreatedAt,
			&tx.UpdatedAt,
		)
		if err != nil {
			return nil, fmt.Errorf("failed to scan transaction: %w", err)
		}
		transactions = append(transactions, &tx)
	}
	
	if err = rows.Err(); err != nil {
		return nil, fmt.Errorf("rows iteration error: %w", err)
	}
	
	return transactions, nil
}
