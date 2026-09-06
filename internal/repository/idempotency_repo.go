package repository

import (
	"context"
	"database/sql"
	"fmt"
	"log/slog"
	"time"

	"wallet-transfer-service/internal/apperror"
	"wallet-transfer-service/internal/model"
)

// var (
// 	ErrRequestInProgress = errors.New("request currently in progress, please retry shortly")
// 	ErrPayloadMismatch   = errors.New("idempotency key already used with different payload")
// )

type IdempotencyRepository interface {
	TryAcquire(ctx context.Context, key, requestHash string, lockTTL, retention time.Duration) (*model.IdempotencyRecord, bool, error)
	SaveResult(ctx context.Context, key string, status model.IdempotencyStatus, code int, body string) error
}

type idempotencyRepository struct {
	db *sql.DB
}

func NewIdempotencyRepository(db *sql.DB) IdempotencyRepository {
	return &idempotencyRepository{db: db}
}

func (r *idempotencyRepository) TryAcquire(
	ctx context.Context,
	key string,
	requestHash string,
	lockTTL time.Duration,
	retention time.Duration,
) (*model.IdempotencyRecord, bool, error) {
	slog.DebugContext(ctx, "idempotency repository acquire started", "idempotency_key", key)
	// Atomic insert for first-time callers
	insertQuery := `
		INSERT INTO idempotency_records (key, request_hash, status, created_at, updated_at)
		VALUES ($1, $2, $3, CURRENT_TIMESTAMP, CURRENT_TIMESTAMP)
		ON CONFLICT (key) DO NOTHING
	`
	res, err := r.db.ExecContext(ctx, insertQuery, key, requestHash, model.IdempotencyStatusStarted)
	if err != nil {
		slog.ErrorContext(ctx, "idempotency repository insert failed", "idempotency_key", key, "error", err)
		return nil, false, fmt.Errorf("failed to insert idempotency record: %w", err)
	}

	rowsAffected, err := res.RowsAffected()
	if err != nil {
		return nil, false, fmt.Errorf("failed checking rows affected: %w", err)
	}

	// First time caller got the key
	if rowsAffected == 1 {
		slog.DebugContext(ctx, "idempotency repository lease acquired", "idempotency_key", key)
		return nil, true, nil
	}

	// Row exists: Read existing record
	selectQuery := `
		SELECT key, request_hash, status, COALESCE(response_code, 0), COALESCE(response_body, ''), created_at, updated_at
		FROM idempotency_records
		WHERE key = $1
	`
	var rec model.IdempotencyRecord
	err = r.db.QueryRowContext(ctx, selectQuery, key).Scan(
		&rec.Key,
		&rec.RequestHash,
		&rec.Status,
		&rec.ResponseCode,
		&rec.ResponseBody,
		&rec.CreatedAt,
		&rec.UpdatedAt,
	)
	if err != nil {
		slog.ErrorContext(ctx, "idempotency repository lookup failed", "idempotency_key", key, "error", err)
		return nil, false, fmt.Errorf("failed fetching existing idempotency record: %w", err)
	}

	// Payload integrity check (prevent key reuse with different parameters)
	if rec.RequestHash != requestHash {
		slog.WarnContext(ctx, "idempotency repository payload mismatch", "idempotency_key", key)
		return nil, false, apperror.ErrPayloadMismatch
	}

	// Expiration check (Already marked EXPIRED, or exceeds 24-hour retention)
	if rec.Status == model.IdempotencyStatusExpired || time.Since(rec.CreatedAt) > retention {
		if rec.Status != model.IdempotencyStatusExpired {
			// Persist the status transition to EXPIRED in PostgreSQL
			expireQuery := `
				UPDATE idempotency_records
				SET status = $1, updated_at = CURRENT_TIMESTAMP
				WHERE key = $2
			`
			_, _ = r.db.ExecContext(ctx, expireQuery, model.IdempotencyStatusExpired, key)
		}
		return nil, false, apperror.ErrIdempotencyKeyExpired
	}

	// Active lease / crash recovery check
	if rec.Status == model.IdempotencyStatusStarted {
		if time.Since(rec.UpdatedAt) > lockTTL {
			// Crash recovery: reclaim lease using optimistic check on updated_at
			takeoverQuery := `
				UPDATE idempotency_records
				SET updated_at = CURRENT_TIMESTAMP
				WHERE key = $1 AND status = $2 AND updated_at = $3
			`
			takeoverRes, err := r.db.ExecContext(ctx, takeoverQuery, key, model.IdempotencyStatusStarted, rec.UpdatedAt)
			if err == nil {
				if count, _ := takeoverRes.RowsAffected(); count == 1 {
					return nil, true, nil // Successfully reclaimed expired lease
				}
			}
		}
		return nil, false, apperror.ErrRequestInProgress
	}

	// Return cached completed/failed record
	slog.DebugContext(ctx, "idempotency repository returning cached record", "idempotency_key", key, "status", rec.Status)
	return &rec, false, nil
}

func (r *idempotencyRepository) SaveResult(ctx context.Context, key string, status model.IdempotencyStatus, code int, body string) error {
	slog.DebugContext(ctx, "idempotency repository save result started", "idempotency_key", key, "status", status, "response_code", code)
	query := `
		UPDATE idempotency_records
		SET status = $1, response_code = $2, response_body = $3, updated_at = CURRENT_TIMESTAMP
		WHERE key = $4
	`
	_, err := r.db.ExecContext(ctx, query, status, code, body, key)
	if err != nil {
		slog.ErrorContext(ctx, "idempotency repository save result failed", "idempotency_key", key, "error", err)
		return fmt.Errorf("failed saving idempotency outcome: %w", err)
	}
	slog.DebugContext(ctx, "idempotency repository save result completed", "idempotency_key", key)
	return nil
}
