// cmd/api/main_test.go
package main

import (
	"testing"
	"flowtracer/proto/flowtracerpb"

	"github.com/stretchr/testify/assert"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

func TestValidateUsername(t *testing.T) {
	// Валидные usernames
	validUsernames := []string{
		"user123",
		"test_user", 
		"alice-bob",
		"User_Name",
		"abc", // минимальная длина
	}
	
	for _, username := range validUsernames {
		t.Run("valid_"+username, func(t *testing.T) {
			err := validateUsername(username)
			assert.NoError(t, err, "Expected '%s' to be valid", username)
		})
	}
	
	// Невалидные usernames
	invalidCases := []struct {
		username string
		reason   string
	}{
		{"", "empty username"},
		{"ab", "too short"},
		{"_user", "starts with underscore"},
		{"-user", "starts with dash"},
		{"user_", "ends with underscore"},
		{"user-", "ends with dash"},
		{"user@name", "contains @ symbol"},
		{"user name", "contains space"},
		{"user.name", "contains dot"},
		{"verylongusernamethatexceedsfiftycharsandshouldfail123", "too long (over 50)"},
	}
	
	for _, tc := range invalidCases {
		t.Run("invalid_"+tc.reason, func(t *testing.T) {
			err := validateUsername(tc.username)
			assert.Error(t, err, "Expected '%s' to be invalid (%s)", tc.username, tc.reason)
			
			// Проверяем, что это gRPC status error
			statusErr, ok := status.FromError(err)
			assert.True(t, ok, "Error should be a gRPC status error")
			assert.Equal(t, codes.InvalidArgument, statusErr.Code())
		})
	}
}

func TestValidateSendTransactionRequest(t *testing.T) {
	// Валидный запрос для сравнения
	validRequest := &flowtracerpb.SendTransactionRequest{
		FromUsername:    "alice",
		ToUsername:      "bob", 
		Amount:          100,
		IdempotencyKey: "test-key-123",
	}
	
	t.Run("valid request", func(t *testing.T) {
		err := validateSendTransactionRequest(validRequest)
		assert.NoError(t, err)
	})
	
	// Тесты невалидных запросов
	testCases := []struct {
		name     string
		modifyFn func(*flowtracerpb.SendTransactionRequest)
		wantErr  string
	}{
		{
			name: "empty from_username",
			modifyFn: func(req *flowtracerpb.SendTransactionRequest) {
				req.FromUsername = ""
			},
			wantErr: "from_username cannot be empty",
		},
		{
			name: "empty to_username", 
			modifyFn: func(req *flowtracerpb.SendTransactionRequest) {
				req.ToUsername = ""
			},
			wantErr: "to_username cannot be empty",
		},
		{
			name: "same from and to username",
			modifyFn: func(req *flowtracerpb.SendTransactionRequest) {
				req.ToUsername = req.FromUsername
			},
			wantErr: "cannot send money to yourself",
		},
		{
			name: "zero amount",
			modifyFn: func(req *flowtracerpb.SendTransactionRequest) {
				req.Amount = 0
			},
			wantErr: "amount must be positive",
		},
		{
			name: "negative amount",
			modifyFn: func(req *flowtracerpb.SendTransactionRequest) {
				req.Amount = -100
			},
			wantErr: "amount must be positive",
		},
		{
			name: "amount too large",
			modifyFn: func(req *flowtracerpb.SendTransactionRequest) {
				req.Amount = 1000000001 // больше 10 млн рублей
			},
			wantErr: "amount too large",
		},
		{
			name: "empty idempotency key",
			modifyFn: func(req *flowtracerpb.SendTransactionRequest) {
				req.IdempotencyKey = ""
			},
			wantErr: "idempotency_key cannot be empty",
		},
		{
			name: "invalid from_username",
			modifyFn: func(req *flowtracerpb.SendTransactionRequest) {
				req.FromUsername = "ab" // слишком короткий
			},
			wantErr: "invalid from_username",
		},
		{
			name: "invalid to_username",
			modifyFn: func(req *flowtracerpb.SendTransactionRequest) {
				req.ToUsername = "user@name" // недопустимый символ
			},
			wantErr: "invalid to_username",
		},
	}
	
	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			// Копируем валидный запрос
			req := &flowtracerpb.SendTransactionRequest{
				FromUsername:    validRequest.FromUsername,
				ToUsername:      validRequest.ToUsername,
				Amount:          validRequest.Amount,
				IdempotencyKey: validRequest.IdempotencyKey,
			}
			
			// Применяем модификацию
			tc.modifyFn(req)
			
			err := validateSendTransactionRequest(req)
			assert.Error(t, err)
			
			// Проверяем, что это gRPC status error
			statusErr, ok := status.FromError(err)
			assert.True(t, ok, "Error should be a gRPC status error")
			assert.Equal(t, codes.InvalidArgument, statusErr.Code())
			assert.Contains(t, statusErr.Message(), tc.wantErr)
		})
	}
}