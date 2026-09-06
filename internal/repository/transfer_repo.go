package repository

import (
	"context"
	"database/sql"
	"fmt"
	"log/slog"
	"net/http"
	"time"

	"wallet-transfer-service/internal/apperror"
	"wallet-transfer-service/internal/model"
)

type TransferRepository interface {
	ExecuteTransfer(ctx context.Context, transfer *model.Transfer, responseBody string) error
	GetStatementByWalletID(ctx context.Context, walletID string) ([]StatementRecord, error)
}

type transferRepository struct {
	db *sql.DB
}

func NewTransferRepository(db *sql.DB) TransferRepository {
	return &transferRepository{db: db}
}

type walletState struct {
	balance int64
	status  model.WalletStatus
}

type StatementRecord struct {
	TransferID    string
	Type          string // "DEBIT" or "CREDIT"
	CounterpartID string
	Amount        int64
	BalanceBefore int64
	BalanceAfter  int64
	Status        model.TransferStatus
	CreatedAt     time.Time // Transaction Date
}

// ExecuteTransfer executes wallet debit/credit, ledger writes, and marks the idempotency
// record as COMPLETED inside a single atomic SQL transaction.
func (r *transferRepository) ExecuteTransfer(ctx context.Context, transfer *model.Transfer, responseBody string) error {
	slog.DebugContext(ctx, "transfer repository transaction started", "transfer_id", transfer.ID, "from_wallet_id", transfer.FromWalletID, "to_wallet_id", transfer.ToWalletID)
	if transfer.FromWalletID == transfer.ToWalletID {
		slog.WarnContext(ctx, "transfer repository rejected self transfer", "transfer_id", transfer.ID, "wallet_id", transfer.FromWalletID)
		return apperror.ErrSelfTransfer
	}

	// Begin atomic transaction
	tx, err := r.db.BeginTx(ctx, &sql.TxOptions{Isolation: sql.LevelReadCommitted})
	if err != nil {
		slog.ErrorContext(ctx, "transfer repository failed to begin transaction", "transfer_id", transfer.ID, "error", err)
		return fmt.Errorf("failed to begin tx: %w", err)
	}
	defer tx.Rollback()

	// Deterministic Lock Ordering:
	// Always sort wallet IDs lexicographically to prevent deadlocks across concurrent transfers.
	firstID, secondID := transfer.FromWalletID, transfer.ToWalletID
	if firstID > secondID {
		firstID, secondID = secondID, firstID
	}

	lockQuery := `
		SELECT id, balance, status 
		FROM wallets 
		WHERE id IN ($1, $2) 
		ORDER BY id 
		FOR UPDATE
	`
	rows, err := tx.QueryContext(ctx, lockQuery, firstID, secondID)
	if err != nil {
		slog.ErrorContext(ctx, "transfer repository failed to acquire wallet locks", "transfer_id", transfer.ID, "error", err)
		return fmt.Errorf("failed to acquire row locks: %w", err)
	}
	defer rows.Close()

	walletsMap := make(map[string]walletState)
	for rows.Next() {
		var id string
		var ws walletState
		if err := rows.Scan(&id, &ws.balance, &ws.status); err != nil {
			return fmt.Errorf("failed scanning wallet row: %w", err)
		}
		walletsMap[id] = ws
	}
	if err := rows.Err(); err != nil {
		return fmt.Errorf("error reading wallet rows: %w", err)
	}

	// Ensure both source and destination accounts exist
	if len(walletsMap) < 2 {
		slog.WarnContext(ctx, "transfer repository rejected transfer: wallet not found", "transfer_id", transfer.ID)
		return apperror.ErrWalletNotFound
	}

	fromWallet := walletsMap[transfer.FromWalletID]
	toWallet := walletsMap[transfer.ToWalletID]

	// Status checks (Account freezes)
	if !fromWallet.status.CanDebit() {
		slog.WarnContext(ctx, "transfer repository rejected transfer: source debit blocked", "transfer_id", transfer.ID)
		return apperror.ErrSourceDebitBlocked
	}
	if !toWallet.status.CanCredit() {
		slog.WarnContext(ctx, "transfer repository rejected transfer: destination credit blocked", "transfer_id", transfer.ID)
		return apperror.ErrDestCreditBlocked
	}

	// Balance verification
	if fromWallet.balance < transfer.Amount {
		slog.WarnContext(ctx, "transfer repository rejected transfer: insufficient balance", "transfer_id", transfer.ID)
		return apperror.ErrInsufficientBalance
	}

	// Calculate snapshot balances for ledger audit trail
	fromBalanceBefore := fromWallet.balance
	fromBalanceAfter := fromBalanceBefore - transfer.Amount

	toBalanceBefore := toWallet.balance
	toBalanceAfter := toBalanceBefore + transfer.Amount

	// Update wallet balances
	deductQuery := `
		UPDATE wallets 
		SET balance = balance - $1 
		WHERE id = $2
	`
	deductRes, err := tx.ExecContext(ctx, deductQuery, transfer.Amount, transfer.FromWalletID)
	if err != nil {
		return fmt.Errorf("failed to deduct balance: %w", err)
	}
	if rows, err := deductRes.RowsAffected(); err != nil {
		return fmt.Errorf("failed checking affected rows on debit: %w", err)
	} else if rows != 1 {
		return fmt.Errorf("deduct balance affected %d rows; expected 1 for wallet %s", rows, transfer.FromWalletID)
	}

	creditQuery := `
		UPDATE wallets 
		SET balance = balance + $1 
		WHERE id = $2
	`
	creditRes, err := tx.ExecContext(ctx, creditQuery, transfer.Amount, transfer.ToWalletID)
	if err != nil {
		return fmt.Errorf("failed to credit balance: %w", err)
	}
	if rows, err := creditRes.RowsAffected(); err != nil {
		return fmt.Errorf("failed checking affected rows on credit: %w", err)
	} else if rows != 1 {
		return fmt.Errorf("credit balance affected %d rows; expected 1 for wallet %s", rows, transfer.ToWalletID)
	}

	// Record transfer execution
	insertTransferQuery := `
		INSERT INTO transfers (id, idempotency_key, from_wallet_id, to_wallet_id, amount, status)
		VALUES ($1, $2, $3, $4, $5, $6)
	`
	if _, err := tx.ExecContext(ctx, insertTransferQuery,
		transfer.ID,
		transfer.IdempotencyKey,
		transfer.FromWalletID,
		transfer.ToWalletID,
		transfer.Amount,
		model.TransferStatusProcessed,
	); err != nil {
		return fmt.Errorf("failed to insert transfer record: %w", err)
	}

	// Insert immutable double-entry ledger records
	insertLedgerQuery := `
		INSERT INTO ledger_entries (
			wallet_id, transfer_id, type, amount, balance_before, balance_after, created_at
		)
		VALUES 
			($1, $2, $3, $4, $5, $6, CURRENT_TIMESTAMP),
			($7, $8, $9, $10, $11, $12, CURRENT_TIMESTAMP)
	`
	if _, err := tx.ExecContext(ctx, insertLedgerQuery,
		transfer.FromWalletID, transfer.ID, model.LedgerEntryDebit, transfer.Amount, fromBalanceBefore, fromBalanceAfter,
		transfer.ToWalletID, transfer.ID, model.LedgerEntryCredit, transfer.Amount, toBalanceBefore, toBalanceAfter,
	); err != nil {
		return fmt.Errorf("failed to write ledger entries: %w", err)
	}

	// Atomically finalize idempotency status to COMPLETED inside the same transaction
	updateIdempotencyQuery := `
		UPDATE idempotency_records 
		SET status = $1, response_code = $2, response_body = $3 
		WHERE key = $4
	`
	res, err := tx.ExecContext(ctx, updateIdempotencyQuery,
		model.IdempotencyStatusCompleted,
		http.StatusCreated,
		responseBody,
		transfer.IdempotencyKey,
	)
	if err != nil {
		return fmt.Errorf("failed to update idempotency record to completed: %w", err)
	}
	rowsAffected, err := res.RowsAffected()
	if err != nil {
		return fmt.Errorf("failed to retrieve affected rows for idempotency record: %w", err)
	}

	if rowsAffected != 1 {
		return fmt.Errorf("idempotency record update affected %d rows; expected exactly 1 for key %s", rowsAffected, transfer.IdempotencyKey)
	}

	// Commit all changes simultaneously
	if err := tx.Commit(); err != nil {
		slog.ErrorContext(ctx, "transfer repository transaction commit failed", "transfer_id", transfer.ID, "error", err)
		return fmt.Errorf("failed to commit transfer transaction: %w", err)
	}

	transfer.Status = model.TransferStatusProcessed
	slog.InfoContext(ctx, "transfer repository transaction committed", "transfer_id", transfer.ID)
	return nil
}

// GetStatementByWalletID reads historical ledger entries using idx_ledger_wallet_created_desc.
func (r *transferRepository) GetStatementByWalletID(ctx context.Context, walletID string) ([]StatementRecord, error) {
	slog.DebugContext(ctx, "statement repository query started", "wallet_id", walletID)
	query := `
		SELECT 
			l.transfer_id,
			l.type,
			CASE 
				WHEN t.from_wallet_id = $1 THEN t.to_wallet_id 
				ELSE t.from_wallet_id 
			END AS counterpart_id,
			l.amount,
			l.balance_before,
			l.balance_after,
			t.status,
			l.created_at
		FROM ledger_entries l
		INNER JOIN transfers t ON l.transfer_id = t.id
		WHERE l.wallet_id = $1
		ORDER BY l.created_at DESC
	`
	rows, err := r.db.QueryContext(ctx, query, walletID)
	if err != nil {
		slog.ErrorContext(ctx, "statement repository query failed", "wallet_id", walletID, "error", err)
		return nil, fmt.Errorf("failed to query statement: %w", err)
	}
	defer rows.Close()

	var records []StatementRecord
	for rows.Next() {
		var rec StatementRecord
		if err := rows.Scan(
			&rec.TransferID,
			&rec.Type,
			&rec.CounterpartID,
			&rec.Amount,
			&rec.BalanceBefore,
			&rec.BalanceAfter,
			&rec.Status,
			&rec.CreatedAt,
		); err != nil {
			slog.ErrorContext(ctx, "statement repository row scan failed", "wallet_id", walletID, "error", err)
			return nil, fmt.Errorf("failed to scan statement record: %w", err)
		}
		records = append(records, rec)
	}

	return records, rows.Err()
}
