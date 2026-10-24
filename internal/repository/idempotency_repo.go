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

type IdempotencyRepository interface {
	TryAcquire(ctx context.Context, key, requestHash string, lockTTL, retention time.Duration) (*model.IdempotencyRecord, bool, error)
	SaveResult(ctx context.Context, key string, status model.IdempotencyStatus, code int, body string) error
	ReleaseLease(ctx context.Context, key string, acquiredAt time.Time) error
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

	// Atomic insert returning updated_at so the caller receives the lease token
	insertQuery := `
		INSERT INTO idempotency_records (key, request_hash, status, created_at, updated_at)
		VALUES ($1, $2, $3, CURRENT_TIMESTAMP, CURRENT_TIMESTAMP)
		ON CONFLICT (key) DO NOTHING
		RETURNING updated_at
	`
	var acquiredAt time.Time
	err := r.db.QueryRowContext(ctx, insertQuery, key, requestHash, model.IdempotencyStatusStarted).Scan(&acquiredAt)
	if err == nil {
		slog.DebugContext(ctx, "idempotency repository lease acquired", "idempotency_key", key)
		return &model.IdempotencyRecord{
			Key:         key,
			RequestHash: requestHash,
			Status:      model.IdempotencyStatusStarted,
			UpdatedAt:   acquiredAt,
		}, true, nil
	}
	if !errors.Is(err, sql.ErrNoRows) {
		slog.ErrorContext(ctx, "idempotency repository insert failed", "idempotency_key", key, "error", err)
		return nil, false, fmt.Errorf("failed to insert idempotency record: %w", err)
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
			// commit and return the newly finalized cached response.
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
				RETURNING updated_at
			`
			var takeoverUpdatedAt time.Time
			updateErr := tx.QueryRowContext(ctx, takeoverQuery, key, model.IdempotencyStatusStarted, rec.UpdatedAt).Scan(&takeoverUpdatedAt)
			if updateErr != nil {
				if errors.Is(updateErr, sql.ErrNoRows) {
					return nil, false, apperror.ErrRequestInProgress
				}
				slog.ErrorContext(ctx, "failed updating record for lease takeover", "idempotency_key", key, "error", updateErr)
				return nil, false, fmt.Errorf("failed executing lease takeover: %w", updateErr)
			}

			if commitErr := tx.Commit(); commitErr != nil {
				slog.ErrorContext(ctx, "failed committing lease takeover tx", "idempotency_key", key, "error", commitErr)
				return nil, false, fmt.Errorf("failed committing lease takeover: %w", commitErr)
			}

			slog.InfoContext(ctx, "idempotency repository reclaimed expired lease", "idempotency_key", key)
			rec.UpdatedAt = takeoverUpdatedAt
			return &rec, true, nil
		}
		return nil, false, apperror.ErrRequestInProgress
	}

	// Return cached completed/failed record
	slog.DebugContext(ctx, "idempotency repository returning cached record", "idempotency_key", key, "status", rec.Status)
	return &rec, false, nil
}

func (r *idempotencyRepository) SaveResult(ctx context.Context, key string, status model.IdempotencyStatus, code int, body string) error {
	slog.DebugContext(ctx, "idempotency repository save result started", "idempotency_key", key, "status", status, "response_code", code)

	// Guarded transition: only update if the record is currently in STARTED
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
		var currentStatus model.IdempotencyStatus
		checkQuery := `SELECT status FROM idempotency_records WHERE key = $1`
		checkErr := r.db.QueryRowContext(ctx, checkQuery, key).Scan(&currentStatus)
		if checkErr != nil {
			if errors.Is(checkErr, sql.ErrNoRows) {
				return fmt.Errorf("idempotency record not found for key %s: %w", key, sql.ErrNoRows)
			}
			return fmt.Errorf("failed checking idempotency record status for key %s: %w", key, checkErr)
		}

		// Row exists and is already in a terminal/finalized state
		if currentStatus == model.IdempotencyStatusCompleted || currentStatus == model.IdempotencyStatusFailed {
			slog.WarnContext(ctx, "idempotency record already finalized by another worker",
				"idempotency_key", key,
				"current_status", currentStatus,
				"attempted_status", status,
			)
			return nil
		}

		// Row exists but is in an unfinalizable state (e.g. EXPIRED)
		return fmt.Errorf("idempotency record update affected 0 rows for key %s: expected status %s, current status is %s",
			key, model.IdempotencyStatusStarted, currentStatus)
	}

	slog.DebugContext(ctx, "idempotency repository save result completed", "idempotency_key", key)
	return nil
}

// ReleaseLease conditionally resets updated_at to the Unix epoch ONLY if status is still STARTED
// and updated_at matches the acquiredAt timestamp held by this specific worker.
func (r *idempotencyRepository) ReleaseLease(ctx context.Context, key string, acquiredAt time.Time) error {
	slog.DebugContext(ctx, "idempotency repository release lease started", "idempotency_key", key)

	query := `
		UPDATE idempotency_records
		SET updated_at = '1970-01-01 00:00:00Z'
		WHERE key = $1 AND status = $2 AND updated_at = $3
	`
	res, err := r.db.ExecContext(ctx, query, key, model.IdempotencyStatusStarted, acquiredAt)
	if err != nil {
		slog.ErrorContext(ctx, "failed releasing idempotency lease", "idempotency_key", key, "error", err)
		return fmt.Errorf("failed releasing idempotency lease: %w", err)
	}

	rows, err := res.RowsAffected()
	if err != nil {
		return fmt.Errorf("failed checking rows affected on lease release: %w", err)
	}

	if rows == 0 {
		slog.InfoContext(ctx, "lease release skipped; lease was already superseded or finalized", "idempotency_key", key)
	} else {
		slog.DebugContext(ctx, "idempotency repository lease released successfully", "idempotency_key", key)
	}

	return nil
}
