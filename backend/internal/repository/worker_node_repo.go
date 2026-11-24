package repository

import (
	"context"
	"crypto/sha256"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"

	"github.com/Wei-Shaw/sub2api/internal/service"
	"github.com/lib/pq"
)

const workerClusterSetting = "nexus_worker_pool_configuration"
const workerEntryRegistry = "nexus:cluster:entry:"

type workerNodeStore struct{ db *sql.DB }

type workerClusterMetadata struct {
	Version int64                    `json:"version"`
	Policy  service.WorkerPoolPolicy `json:"policy"`
}

func NewWorkerNodeStore(db *sql.DB) service.WorkerClusterStore {
	return &workerNodeStore{db: db}
}

func (s *workerNodeStore) SetEntryMembership(ctx context.Context, member service.EntryMembership) error {
	if member.InstanceID == "" {
		return service.ErrWorkerClusterInvalid
	}
	key := fmt.Sprintf("%s%x", workerEntryRegistry, sha256.Sum256([]byte(member.InstanceID)))
	raw, err := json.Marshal(member)
	if err != nil {
		return err
	}
	if member.Retired {
		_, err = s.db.ExecContext(ctx, `UPDATE settings SET value=$2,updated_at=NOW() WHERE key=$1`, key, string(raw))
		return err
	}
	_, err = s.db.ExecContext(ctx, `
		INSERT INTO settings (key, value) VALUES ($1,$2)
		ON CONFLICT (key) DO UPDATE SET value=EXCLUDED.value,updated_at=NOW()`, key, string(raw))
	return err
}

func (s *workerNodeStore) ListEntryMemberships(ctx context.Context) ([]service.EntryMembership, error) {
	rows, err := s.db.QueryContext(ctx, `SELECT value FROM settings WHERE key LIKE $1`, workerEntryRegistry+"%")
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var members []service.EntryMembership
	for rows.Next() {
		var raw string
		var member service.EntryMembership
		if err := rows.Scan(&raw); err != nil {
			return nil, err
		}
		if json.Unmarshal([]byte(raw), &member) != nil || member.InstanceID == "" {
			return nil, errors.New("invalid stored Entry membership")
		}
		members = append(members, member)
	}
	return members, rows.Err()
}

func (s *workerNodeStore) Load(ctx context.Context) (service.WorkerClusterSnapshot, error) {
	tx, err := s.db.BeginTx(ctx, &sql.TxOptions{Isolation: sql.LevelRepeatableRead, ReadOnly: true})
	if err != nil {
		return service.WorkerClusterSnapshot{}, err
	}
	defer func() { _ = tx.Rollback() }()
	meta, err := readWorkerClusterMetadata(ctx, tx, false)
	if err != nil {
		return service.WorkerClusterSnapshot{}, err
	}
	rows, err := tx.QueryContext(ctx, `
		SELECT node_id, name, url, use_h2c, weight, max_inflight, tags, note,
		       enabled, secret_ref, drain_version, drain_entries
		FROM worker_nodes ORDER BY node_id`)
	if err != nil {
		return service.WorkerClusterSnapshot{}, err
	}
	defer rows.Close()
	draft := service.WorkerClusterSnapshot{Version: meta.Version, Policy: meta.Policy, Workers: []service.WorkerNode{}}
	for rows.Next() {
		var node service.WorkerNode
		var tags, entries []byte
		if err := rows.Scan(&node.NodeID, &node.Name, &node.URL, &node.UseH2C, &node.Weight, &node.MaxInFlight,
			&tags, &node.Note, &node.Enabled, &node.SecretRef, &node.DrainVersion, &entries); err != nil {
			return service.WorkerClusterSnapshot{}, err
		}
		if json.Unmarshal(tags, &node.Tags) != nil || json.Unmarshal(entries, &node.DrainEntries) != nil {
			return service.WorkerClusterSnapshot{}, errors.New("invalid stored worker node metadata")
		}
		draft.Workers = append(draft.Workers, node)
	}
	if err := rows.Err(); err != nil {
		return service.WorkerClusterSnapshot{}, err
	}
	if err := tx.Commit(); err != nil {
		return service.WorkerClusterSnapshot{}, err
	}
	return draft, nil
}

func (s *workerNodeStore) Save(ctx context.Context, expected int64, draft service.WorkerClusterSnapshot) error {
	if draft.Version < 1 || draft.Version-1 != expected {
		return service.ErrWorkerClusterConflict
	}
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return err
	}
	defer func() { _ = tx.Rollback() }()
	meta, err := json.Marshal(workerClusterMetadata{Version: draft.Version, Policy: draft.Policy})
	if err != nil {
		return err
	}
	if expected == 0 {
		result, err := tx.ExecContext(ctx, `
			INSERT INTO settings (key, value) VALUES ($1, $2) ON CONFLICT (key) DO NOTHING`,
			workerClusterSetting, string(meta))
		if err != nil {
			return err
		}
		if rows, err := result.RowsAffected(); err != nil {
			return err
		} else if rows != 1 {
			return service.ErrWorkerClusterConflict
		}
	} else {
		previous, err := readWorkerClusterMetadata(ctx, tx, true)
		if err != nil {
			return err
		}
		if previous.Version != expected {
			return service.ErrWorkerClusterConflict
		}
		if _, err := tx.ExecContext(ctx, `UPDATE settings SET value=$1, updated_at=NOW() WHERE key=$2`,
			string(meta), workerClusterSetting); err != nil {
			return err
		}
	}
	ids := make([]string, 0, len(draft.Workers))
	for _, node := range draft.Workers {
		ids = append(ids, node.NodeID)
		tags, err := json.Marshal(append([]string{}, node.Tags...))
		if err != nil {
			return err
		}
		entries, err := json.Marshal(append([]string{}, node.DrainEntries...))
		if err != nil {
			return err
		}
		if _, err := tx.ExecContext(ctx, `
			INSERT INTO worker_nodes (node_id,name,url,use_h2c,weight,max_inflight,tags,note,enabled,secret_ref,drain_version,drain_entries)
			VALUES ($1,$2,$3,$4,$5,$6,$7::jsonb,$8,$9,$10,$11,$12::jsonb)
			ON CONFLICT (node_id) DO UPDATE SET
				name=EXCLUDED.name,url=EXCLUDED.url,use_h2c=EXCLUDED.use_h2c,weight=EXCLUDED.weight,
				max_inflight=EXCLUDED.max_inflight,tags=EXCLUDED.tags,note=EXCLUDED.note,enabled=EXCLUDED.enabled,
				secret_ref=EXCLUDED.secret_ref,drain_version=EXCLUDED.drain_version,drain_entries=EXCLUDED.drain_entries,updated_at=NOW()`,
			node.NodeID, node.Name, node.URL, node.UseH2C, node.Weight, node.MaxInFlight,
			string(tags), node.Note, node.Enabled, node.SecretRef, node.DrainVersion, string(entries)); err != nil {
			return err
		}
	}
	if _, err := tx.ExecContext(ctx, `DELETE FROM worker_nodes WHERE NOT (node_id = ANY($1))`, pq.Array(ids)); err != nil {
		return err
	}
	// Keep tombstones while a drain refers to them. Serialize pruning with
	// configuration saves so an older drain snapshot cannot reintroduce a reference.
	if _, err := tx.ExecContext(ctx, `DELETE FROM settings s WHERE s.key LIKE $1
		AND s.updated_at < NOW() - INTERVAL '7 days'
		AND (CASE WHEN s.key LIKE $1 THEN s.value::jsonb END) @> '{"retired":true}'::jsonb
		AND NOT EXISTS (SELECT 1 FROM worker_nodes w WHERE w.drain_entries ?
			(CASE WHEN s.key LIKE $1 THEN s.value::jsonb END ->> 'instance_id'))`, workerEntryRegistry+"%"); err != nil {
		return err
	}
	return tx.Commit()
}

func readWorkerClusterMetadata(ctx context.Context, tx *sql.Tx, lock bool) (workerClusterMetadata, error) {
	query := `SELECT value FROM settings WHERE key=$1`
	if lock {
		query += " FOR UPDATE"
	}
	var raw string
	if err := tx.QueryRowContext(ctx, query, workerClusterSetting).Scan(&raw); err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return workerClusterMetadata{}, service.ErrWorkerClusterUninitialized
		}
		return workerClusterMetadata{}, err
	}
	var meta workerClusterMetadata
	if err := json.Unmarshal([]byte(raw), &meta); err != nil || meta.Version < 1 {
		return workerClusterMetadata{}, fmt.Errorf("invalid stored worker cluster version")
	}
	return meta, nil
}
