package repository

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"log/slog"
	"time"

	"github.com/jackc/pgx/v5/pgconn"

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

	// First time caller acquired the lease
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

	// Expiration check (Already marked EXPIRED, or exceeds retention window)
	if rec.Status == model.IdempotencyStatusExpired || time.Since(rec.CreatedAt) > retention {
		if rec.Status != model.IdempotencyStatusExpired {
			expireQuery := `
				UPDATE idempotency_records
				SET status = $1, updated_at = CURRENT_TIMESTAMP
				WHERE key = $2
			`
			if _, err := r.db.ExecContext(ctx, expireQuery, model.IdempotencyStatusExpired, key); err != nil {
				slog.WarnContext(ctx, "failed to transition expired idempotency record", "idempotency_key", key, "error", err)
			}
		}
		return nil, false, apperror.ErrIdempotencyKeyExpired
	}

	// Active lease / crash recovery check
	if rec.Status == model.IdempotencyStatusStarted {
		if time.Since(rec.UpdatedAt) > lockTTL {
			// Attempt safe lease takeover:
			// Non-blocking row-level lock check (FOR UPDATE NOWAIT) ensures we do not
			// steal a lease from a worker actively executing a long-running transaction.
			tx, err := r.db.BeginTx(ctx, &sql.TxOptions{Isolation: sql.LevelReadCommitted})
			if err != nil {
				slog.ErrorContext(ctx, "failed starting transaction for lease takeover", "idempotency_key", key, "error", err)
				return nil, false, fmt.Errorf("failed starting lease takeover tx: %w", err)
			}
			defer tx.Rollback()

			lockQuery := `
				SELECT status, COALESCE(response_code, 0), COALESCE(response_body, '')
				FROM idempotency_records 
				WHERE key = $1 
				FOR UPDATE NOWAIT
			`
			var currentStatus model.IdempotencyStatus
			var respCode int
			var respBody string
			err = tx.QueryRowContext(ctx, lockQuery, key).Scan(&currentStatus, &respCode, &respBody)
			if err != nil {
				type pqErrorCode interface {
					Get(byte) string
				}

				var pgErr *pgconn.PgError
				var pqErr pqErrorCode
				isLockNotAvailable := false

				// Check pgx (*pgconn.PgError.Code is a struct field)
				if errors.As(err, &pgErr) && pgErr.Code == "55P03" {
					isLockNotAvailable = true
					// Fallback check for lib/pq (*pq.Error.Get('C') is a method)
				} else if errors.As(err, &pqErr) && pqErr.Get('C') == "55P03" {
					isLockNotAvailable = true
				}

				if isLockNotAvailable {
					slog.DebugContext(ctx, "idempotency row lock held by active worker", "idempotency_key", key)
					return nil, false, apperror.ErrRequestInProgress
				}

				slog.ErrorContext(ctx, "failed probing idempotency lock", "idempotency_key", key, "error", err)
				return nil, false, fmt.Errorf("failed probing idempotency lock: %w", err)
			}

			// If the original transaction completed or failed right before we acquired the lock,
			// commit the transaction and return the newly finalized cached response.
			if currentStatus != model.IdempotencyStatusStarted {
				_ = tx.Commit()
				rec.Status = currentStatus
				rec.ResponseCode = respCode
				rec.ResponseBody = respBody
				slog.InfoContext(ctx, "idempotency transaction resolved during takeover lock", "idempotency_key", key, "status", currentStatus)
				return &rec, false, nil
			}

			takeoverQuery := `
				UPDATE idempotency_records
				SET updated_at = CURRENT_TIMESTAMP
				WHERE key = $1 AND status = $2 AND updated_at = $3
			`
			updateRes, updateErr := tx.ExecContext(ctx, takeoverQuery, key, model.IdempotencyStatusStarted, rec.UpdatedAt)
			if updateErr != nil {
				slog.ErrorContext(ctx, "failed updating record for lease takeover", "idempotency_key", key, "error", updateErr)
				return nil, false, fmt.Errorf("failed executing lease takeover: %w", updateErr)
			}

			count, rowsErr := updateRes.RowsAffected()
			if rowsErr != nil {
				return nil, false, fmt.Errorf("failed checking rows affected on lease takeover: %w", rowsErr)
			}

			if count == 1 {
				if commitErr := tx.Commit(); commitErr != nil {
					slog.ErrorContext(ctx, "failed committing lease takeover tx", "idempotency_key", key, "error", commitErr)
					return nil, false, fmt.Errorf("failed committing lease takeover: %w", commitErr)
				}
				slog.InfoContext(ctx, "idempotency repository reclaimed expired lease", "idempotency_key", key)
				return nil, true, nil
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

	// Status guard: ONLY transition rows that are currently in 'STARTED'.
	// This prevents a lagging or failing worker from overwriting a COMPLETED record.
	query := `
		UPDATE idempotency_records
		SET status = $1, response_code = $2, response_body = $3, updated_at = CURRENT_TIMESTAMP
		WHERE key = $4 AND status = $5
	`
	res, err := r.db.ExecContext(ctx, query, status, code, body, key, model.IdempotencyStatusStarted)
	if err != nil {
		slog.ErrorContext(ctx, "idempotency repository save result failed", "idempotency_key", key, "error", err)
		return fmt.Errorf("failed saving idempotency outcome: %w", err)
	}

	rows, err := res.RowsAffected()
	if err != nil {
		return fmt.Errorf("failed checking rows affected for key %s: %w", key, err)
	}

	if rows == 0 {
		// The record was not in STARTED state. Check if another worker already finalized it.
		var currentStatus model.IdempotencyStatus
		checkQuery := `SELECT status FROM idempotency_records WHERE key = $1`
		if scanErr := r.db.QueryRowContext(ctx, checkQuery, key).Scan(&currentStatus); scanErr == nil {
			if currentStatus == model.IdempotencyStatusCompleted {
				slog.WarnContext(ctx, "idempotency record already completed by another worker; ignoring overwrite attempt",
					"idempotency_key", key,
					"attempted_status", status,
				)
				return nil
			}
		}

		slog.WarnContext(ctx, "idempotency record status transition skipped; record not in STARTED state",
			"idempotency_key", key,
			"attempted_status", status,
		)
		return nil
	}

	slog.DebugContext(ctx, "idempotency repository save result completed", "idempotency_key", key)
	return nil
}
