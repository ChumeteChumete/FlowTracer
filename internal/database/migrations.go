package database

import (
	"database/sql"
	"fmt"
	"log"
)

// Представляет одну миграцию
type Migration struct {
	Version     int
	Description string
	SQL         string
}

// Все миграции в порядке применения
var migrations = []Migration{
	{
		Version:     1,
		Description: "Create users table",
		SQL: `
			CREATE TABLE IF NOT EXISTS users (
				id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
				username VARCHAR(100) UNIQUE NOT NULL,
				balance BIGINT DEFAULT 0 CHECK (balance >= 0),
				created_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
				updated_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP
			);
		`,
	},
	{
		Version:     2,
		Description: "Create transactions table",
		SQL: `
			CREATE TABLE IF NOT EXISTS transactions (
				id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
				from_username VARCHAR(100) NOT NULL,
				to_username VARCHAR(100) NOT NULL,
				amount BIGINT NOT NULL CHECK (amount > 0),
				status VARCHAR(50) DEFAULT 'pending' CHECK (status IN ('pending', 'completed', 'failed', 'cancelled')),
				idempotency_key VARCHAR(255) UNIQUE,
				created_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
				updated_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP
			);
		`,
	},
	{
		Version:     3,
		Description: "Create indexes and triggers",
		SQL: `
			-- Индексы для быстрого поиска
			CREATE INDEX IF NOT EXISTS idx_transactions_from_username ON transactions(from_username);
			CREATE INDEX IF NOT EXISTS idx_transactions_to_username ON transactions(to_username);
			CREATE INDEX IF NOT EXISTS idx_transactions_status ON transactions(status);
			CREATE INDEX IF NOT EXISTS idx_transactions_created_at ON transactions(created_at);
			CREATE INDEX IF NOT EXISTS idx_users_username ON users(username);
			
			-- Триггер для обновления updated_at
			CREATE OR REPLACE FUNCTION update_updated_at_column()
			RETURNS TRIGGER AS $$
			BEGIN
				NEW.updated_at = CURRENT_TIMESTAMP;
				RETURN NEW;
			END;
			$$ LANGUAGE plpgsql;
			
			DROP TRIGGER IF EXISTS update_users_updated_at ON users;
			CREATE TRIGGER update_users_updated_at
				BEFORE UPDATE ON users
				FOR EACH ROW
				EXECUTE FUNCTION update_updated_at_column();
				
			DROP TRIGGER IF EXISTS update_transactions_updated_at ON transactions;
			CREATE TRIGGER update_transactions_updated_at
				BEFORE UPDATE ON transactions
				FOR EACH ROW
				EXECUTE FUNCTION update_updated_at_column();
		`,
	},
	{
		Version:     4,
		Description: "Insert test data",
		SQL: `
			INSERT INTO users (username, balance) VALUES 
				('alice', 1000.00),
				('bob', 500.00),
				('charlie', 750.00)
			ON CONFLICT (username) DO NOTHING;
		`,
	},
}

// RunMigrations выполняет все pending миграции
func RunMigrations(db *sql.DB) error {
	log.Println("Starting database migrations...")

	// Создаем таблицу для отслеживания миграций
	if err := createMigrationsTable(db); err != nil {
		return fmt.Errorf("failed to create migrations table: %w", err)
	}

	// Получаем текущую версию схемы
	currentVersion, err := getCurrentVersion(db)
	if err != nil {
		return fmt.Errorf("failed to get current schema version: %w", err)
	}

	log.Printf("Current schema version: %d", currentVersion)

	// Применяем все миграции, которые новее текущей версии
	appliedCount := 0
	for _, migration := range migrations {
		if migration.Version <= currentVersion {
			continue
		}

		log.Printf("Applying migration %d: %s", migration.Version, migration.Description)

		if err := applyMigration(db, migration); err != nil {
			return fmt.Errorf("failed to apply migration %d: %w", migration.Version, err)
		}

		appliedCount++
	}

	if appliedCount == 0 {
		log.Println("Database schema is up to date")
	} else {
		log.Printf("Applied %d migrations successfully", appliedCount)
	}

	return nil
}

// Создаем таблицу для отслеживания миграций
func createMigrationsTable(db *sql.DB) error {
	query := `
		CREATE TABLE IF NOT EXISTS schema_migrations (
			version INTEGER PRIMARY KEY,
			description TEXT NOT NULL,
			applied_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP
		);
	`
	
	_, err := db.Exec(query)
	return err
}

// Возвращаем текущую версию схемы
func getCurrentVersion(db *sql.DB) (int, error) {
	var version int
	query := `SELECT COALESCE(MAX(version), 0) FROM schema_migrations`
	
	err := db.QueryRow(query).Scan(&version)
	if err != nil {
		return 0, err
	}
	
	return version, nil
}

// Применяет одну миграцию
func applyMigration(db *sql.DB, migration Migration) error {
	// Используем транзакцию для атомарности
	tx, err := db.Begin()
	if err != nil {
		return err
	}
	defer tx.Rollback()

	// Выполняем SQL миграции
	if _, err := tx.Exec(migration.SQL); err != nil {
		return fmt.Errorf("failed to execute migration SQL: %w", err)
	}

	// Записываем информацию о применении миграции
	insertQuery := `
		INSERT INTO schema_migrations (version, description) 
		VALUES ($1, $2)
	`
	if _, err := tx.Exec(insertQuery, migration.Version, migration.Description); err != nil {
		return fmt.Errorf("failed to record migration: %w", err)
	}

	return tx.Commit()
}
