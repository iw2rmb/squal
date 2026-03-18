package cdc

import (
	"context"
	"fmt"
	"strconv"
	"time"

	"github.com/jackc/pglogrepl"
	"github.com/jackc/pgx/v5/pgtype"
)

// decodeInsert converts an INSERT message to TxEvent.
func (c *Consumer) decodeInsert(msg *pglogrepl.InsertMessageV2, relations map[uint32]*pglogrepl.RelationMessageV2, typeMap *pgtype.Map, lsn pglogrepl.LSN) TxEvent {
	rel := relations[msg.RelationID]
	tableName := relationTableName(rel)
	newRow := c.decodeTupleData(msg.Tuple.Columns, rel, typeMap)

	return TxEvent{
		CommitLSN:  LSN(lsn.String()),
		Table:      tableName,
		Operation:  OpInsert,
		CommitTime: time.Now(), // overwritten on commit
		Keys:       c.extractKeys(newRow, rel),
		New:        newRow,
	}
}

// decodeUpdate converts an UPDATE message to TxEvent.
func (c *Consumer) decodeUpdate(msg *pglogrepl.UpdateMessageV2, relations map[uint32]*pglogrepl.RelationMessageV2, typeMap *pgtype.Map, lsn pglogrepl.LSN) TxEvent {
	rel := relations[msg.RelationID]
	tableName := relationTableName(rel)

	var oldRow map[string]any
	if msg.OldTuple != nil {
		oldRow = c.decodeTupleData(msg.OldTuple.Columns, rel, typeMap)
	}
	newRow := c.decodeTupleData(msg.NewTuple.Columns, rel, typeMap)

	return TxEvent{
		CommitLSN:  LSN(lsn.String()),
		Table:      tableName,
		Operation:  OpUpdate,
		CommitTime: time.Now(), // overwritten on commit
		Keys:       c.extractKeys(newRow, rel),
		Old:        oldRow,
		New:        newRow,
	}
}

// decodeDelete converts a DELETE message to TxEvent.
func (c *Consumer) decodeDelete(msg *pglogrepl.DeleteMessageV2, relations map[uint32]*pglogrepl.RelationMessageV2, typeMap *pgtype.Map, lsn pglogrepl.LSN) TxEvent {
	rel := relations[msg.RelationID]
	tableName := relationTableName(rel)

	var oldRow map[string]any
	if msg.OldTuple != nil {
		oldRow = c.decodeTupleData(msg.OldTuple.Columns, rel, typeMap)
	}

	return TxEvent{
		CommitLSN:  LSN(lsn.String()),
		Table:      tableName,
		Operation:  OpDelete,
		CommitTime: time.Now(), // overwritten on commit
		Keys:       c.extractKeys(oldRow, rel),
		Old:        oldRow,
	}
}

func relationTableName(rel *pglogrepl.RelationMessageV2) string {
	if rel == nil {
		return ""
	}
	return fmt.Sprintf("%s.%s", rel.Namespace, rel.RelationName)
}

// decodeTupleData decodes tuple columns into a row map.
func (c *Consumer) decodeTupleData(columns []*pglogrepl.TupleDataColumn, rel *pglogrepl.RelationMessageV2, typeMap *pgtype.Map) map[string]any {
	result := make(map[string]any)
	if rel == nil {
		return result
	}

	_ = typeMap // reserved for future OID-aware binary decoding expansion.

	for i, col := range columns {
		if i >= len(rel.Columns) {
			continue
		}

		colDef := rel.Columns[i]
		colName := colDef.Name

		switch col.DataType {
		case pglogrepl.TupleDataTypeNull:
			result[colName] = nil
		case pglogrepl.TupleDataTypeToast:
			continue
		case pglogrepl.TupleDataTypeText, pglogrepl.TupleDataTypeBinary:
			result[colName] = decodeColumnValueByOID(colDef.DataType, col.Data)
		default:
			result[colName] = string(col.Data)
		}
	}

	return result
}

// decodeColumnValueByOID converts WAL column values into stable Go types by PostgreSQL OID.
func decodeColumnValueByOID(oid uint32, data []byte) any {
	if data == nil {
		return nil
	}
	s := string(data)

	switch oid {
	case 20, 21, 23: // int8/int2/int4
		if i, err := strconv.ParseInt(s, 10, 64); err == nil {
			return int64(i)
		}
		return s
	case 1700, 700, 701: // numeric/float4/float8
		if f, err := strconv.ParseFloat(s, 64); err == nil {
			return f
		}
		return s
	case 16: // bool
		if s == "t" {
			return true
		}
		if s == "f" {
			return false
		}
		return s
	default:
		return s
	}
}

// extractKeys returns primary-key values for the row based on relation metadata.
func (c *Consumer) extractKeys(row map[string]any, rel *pglogrepl.RelationMessageV2) map[string]any {
	keys := make(map[string]any)
	if rel == nil || row == nil {
		return keys
	}

	for _, col := range rel.Columns {
		if col.Flags == 1 { // primary key
			if val, ok := row[col.Name]; ok {
				keys[col.Name] = val
			}
		}
	}
	return keys
}

func (c *Consumer) shouldApplyBackpressure(batchSize int) bool {
	if c.config.MaxBatchSize <= 0 {
		return false
	}
	return batchSize >= c.config.MaxBatchSize
}

// applyBackpressure handles behavior when MaxBatchSize is reached.
// It returns true when event should remain in the batch.
func (c *Consumer) applyBackpressure(ctx context.Context, batchSize int) bool {
	if !c.shouldApplyBackpressure(batchSize) {
		return true
	}

	_ = ctx

	policy := c.config.BackpressurePolicy
	if policy == "" {
		policy = BackpressurePolicyBlock
	}

	switch policy {
	case BackpressurePolicyDrop:
		c.logger.Warn().
			Int("batch_size", batchSize).
			Int("max_batch_size", c.config.MaxBatchSize).
			Msg("Dropping event due to backpressure")
		return false
	case BackpressurePolicyBlock:
		c.logger.Debug().
			Int("batch_size", batchSize).
			Int("max_batch_size", c.config.MaxBatchSize).
			Msg("Blocking due to backpressure")
		return true
	case BackpressurePolicyMerge:
		c.logger.Debug().
			Int("batch_size", batchSize).
			Msg("Merge policy degrades to block for streaming batches")
		return true
	default:
		c.logger.Warn().
			Str("policy", string(policy)).
			Msg("Unknown backpressure policy, defaulting to block")
		return true
	}
}
