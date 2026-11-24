package repository

import (
	"context"
	"crypto/sha256"
	"database/sql"
	"errors"
	"fmt"
	"io/fs"
	"strings"

	"github.com/Wei-Shaw/sub2api/internal/service"
	"github.com/Wei-Shaw/sub2api/migrations"
)

func ProvideClusterMigrationCheck() service.ClusterMigrationCheck {
	return checkClusterMigrations
}

func checkClusterMigrations(ctx context.Context, conn *sql.Conn) (int, error) {
	rows, err := conn.QueryContext(ctx, "SELECT filename, checksum FROM schema_migrations")
	if err != nil {
		return 0, err
	}
	defer rows.Close()
	applied := make(map[string]string)
	for rows.Next() {
		var filename, checksum string
		if err := rows.Scan(&filename, &checksum); err != nil {
			return 0, err
		}
		applied[filename] = checksum
	}
	if err := rows.Err(); err != nil {
		return 0, err
	}
	count := len(applied)
	names, err := fs.Glob(migrations.FS, "*.sql")
	if err != nil {
		return count, err
	}
	for _, name := range names {
		sqlBytes, err := migrations.FS.ReadFile(name)
		if err != nil {
			return count, err
		}
		content := strings.TrimSpace(string(sqlBytes))
		if content == "" {
			continue
		}
		expected := fmt.Sprintf("%x", sha256.Sum256([]byte(content)))
		actual, ok := applied[name]
		if !ok || (actual != expected && !isMigrationChecksumCompatible(name, actual, expected)) {
			return count, errors.New("migration missing or checksum incompatible")
		}
		delete(applied, name)
	}
	if len(applied) != 0 {
		return count, errors.New("database contains unknown migrations")
	}
	return count, nil
}
