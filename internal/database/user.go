package database

import (
	"context"
	"database/sql"
	"fmt"
	"time"

	"github.com/google/uuid"
)

type User struct {
	ID        uuid.UUID `db:"id"`
	Username  string    `db:"username"`
	Balance   int64     `db:"balance"` // в копейках
	CreatedAt time.Time `db:"created_at"`
	UpdatedAt time.Time `db:"updated_at"`	
}

type UserRepository struct {
	db *sql.DB
}

func NewUserRepository(db *sql.DB) *UserRepository {
	return &UserRepository{db: db}
}

// GetUserByUsername возвращает пользователя по имени
func (r *UserRepository) GetUserByUsername(ctx context.Context, username string) (*User, error) {
	query := `
		SELECT id, username, balance, created_at, updated_at 
		FROM users 
		WHERE username = $1
	`
	
	var user User
	err := r.db.QueryRowContext(ctx, query, username).Scan(
		&user.ID,
		&user.Username,
		&user.Balance,
		&user.CreatedAt,
		&user.UpdatedAt,
	)
	
	if err == sql.ErrNoRows {
		return nil, nil // пользователь не найден
	}
	
	if err != nil {
		return nil, fmt.Errorf("failed to get user %s: %w", username, err)
	}
	
	return &user, nil
}

// CreateUser создает нового пользователя
func (r *UserRepository) CreateUser(ctx context.Context, username string, initialBalance int64) (*User, error) {
	query := `
		INSERT INTO users (username, balance) 
		VALUES ($1, $2) 
		RETURNING id, username, balance, created_at, updated_at
	`
	
	var user User
	err := r.db.QueryRowContext(ctx, query, username, initialBalance).Scan(
		&user.ID,
		&user.Username,
		&user.Balance,
		&user.CreatedAt,
		&user.UpdatedAt,
	)
	
	if err != nil {
		return nil, fmt.Errorf("failed to create user %s: %w", username, err)
	}
	
	return &user, nil
}

// UpdateBalance обновляет баланс пользователя
func (r *UserRepository) UpdateBalance(ctx context.Context, username string, newBalance int64) error {
	query := `
		UPDATE users 
		SET balance = $1, updated_at = CURRENT_TIMESTAMP 
		WHERE username = $2
	`
	
	result, err := r.db.ExecContext(ctx, query, newBalance, username)
	if err != nil {
		return fmt.Errorf("failed to update balance for %s: %w", username, err)
	}
	
	rowsAffected, err := result.RowsAffected()
	if err != nil {
		return fmt.Errorf("failed to get rows affected: %w", err)
	}
	
	if rowsAffected == 0 {
		return fmt.Errorf("user %s not found", username)
	}
	
	return nil
}

// TransferMoney выполняет перевод между пользователями в транзакции
func (r *UserRepository) TransferMoney(ctx context.Context, fromUsername, toUsername string, amount int64) error {
	tx, err := r.db.BeginTx(ctx, &sql.TxOptions{
		Isolation: sql.LevelSerializable,
	})
	if err != nil {
		return fmt.Errorf("failed to begin transaction: %w", err)
	}
	defer tx.Rollback()

	// Проверяем баланс отправителя и блокируем запись
	var fromBalance int64
	err = tx.QueryRowContext(ctx, `
		SELECT balance FROM users WHERE username = $1 FOR UPDATE
	`, fromUsername).Scan(&fromBalance)
	
	if err == sql.ErrNoRows {
		return fmt.Errorf("sender %s not found", fromUsername)
	}
	if err != nil {
		return fmt.Errorf("failed to check sender balance: %w", err)
	}
	
	if fromBalance < amount {
		return fmt.Errorf("insufficient balance: have %d, need %d", fromBalance, amount)
	}

	// Проверяем существование получателя
	var toExists bool
	err = tx.QueryRowContext(ctx, `
		SELECT EXISTS(SELECT 1 FROM users WHERE username = $1)
	`, toUsername).Scan(&toExists)
	
	if err != nil {
		return fmt.Errorf("failed to check recipient: %w", err)
	}
	if !toExists {
		return fmt.Errorf("recipient %s not found", toUsername)
	}

	// Списываем с отправителя
	_, err = tx.ExecContext(ctx, `
		UPDATE users SET balance = balance - $1 WHERE username = $2
	`, amount, fromUsername)
	if err != nil {
		return fmt.Errorf("failed to debit sender: %w", err)
	}

	// Зачисляем получателю
	_, err = tx.ExecContext(ctx, `
		UPDATE users SET balance = balance + $1 WHERE username = $2
	`, amount, toUsername)
	if err != nil {
		return fmt.Errorf("failed to credit recipient: %w", err)
	}

	return tx.Commit()
}

// GetUserBalance возвращает только баланс пользователя
func (r *UserRepository) GetUserBalance(ctx context.Context, username string) (int64, bool, error) {
	var balance int64
	err := r.db.QueryRowContext(ctx, `
		SELECT balance FROM users WHERE username = $1
	`, username).Scan(&balance)
	
	if err == sql.ErrNoRows {
		return 0, false, nil // пользователь не найден
	}
	
	if err != nil {
		return 0, false, fmt.Errorf("failed to get balance for %s: %w", username, err)
	}
	
	return balance, true, nil
}
