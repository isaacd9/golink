// Copyright 2022 Tailscale Inc & Contributors
// SPDX-License-Identifier: BSD-3-Clause

package golink

import (
	"context"
	_ "embed"
	"errors"
	"fmt"
	"io/fs"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
)

//go:embed postgres_schema.sql
var pgSchema string

// PostgresDB stores Links in a PostgreSQL database.
type PostgresDB struct {
	pool *pgxpool.Pool
}

// NewPostgresDB returns a new PostgresDB connected to connStr.
// The schema is applied automatically on startup.
func NewPostgresDB(ctx context.Context, connStr string) (*PostgresDB, error) {
	pool, err := pgxpool.New(ctx, connStr)
	if err != nil {
		return nil, err
	}
	if err := pool.Ping(ctx); err != nil {
		pool.Close()
		return nil, err
	}
	if _, err := pool.Exec(ctx, pgSchema); err != nil {
		pool.Close()
		return nil, err
	}
	return &PostgresDB{pool: pool}, nil
}

// Now returns the current time.
func (p *PostgresDB) Now() time.Time {
	return time.Now().UTC()
}

// Load returns a Link by its short name, or fs.ErrNotExist if not found.
func (p *PostgresDB) Load(short string) (*Link, error) {
	link := new(Link)
	var created, lastEdit int64
	row := p.pool.QueryRow(context.Background(),
		"SELECT short, long, created, last_edit, owner FROM links WHERE id = $1 LIMIT 1",
		linkID(short))
	err := row.Scan(&link.Short, &link.Long, &created, &lastEdit, &link.Owner)
	if err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return nil, fs.ErrNotExist
		}
		return nil, err
	}
	link.Created = time.Unix(created, 0).UTC()
	link.LastEdit = time.Unix(lastEdit, 0).UTC()
	return link, nil
}

// LoadAll returns all stored Links.
func (p *PostgresDB) LoadAll() ([]*Link, error) {
	rows, err := p.pool.Query(context.Background(),
		"SELECT short, long, created, last_edit, owner FROM links")
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var links []*Link
	for rows.Next() {
		link := new(Link)
		var created, lastEdit int64
		if err := rows.Scan(&link.Short, &link.Long, &created, &lastEdit, &link.Owner); err != nil {
			return nil, err
		}
		link.Created = time.Unix(created, 0).UTC()
		link.LastEdit = time.Unix(lastEdit, 0).UTC()
		links = append(links, link)
	}
	return links, rows.Err()
}

// Save stores a Link, inserting or replacing any existing entry.
func (p *PostgresDB) Save(link *Link) error {
	tag, err := p.pool.Exec(context.Background(),
		`INSERT INTO links (id, short, long, created, last_edit, owner)
		 VALUES ($1, $2, $3, $4, $5, $6)
		 ON CONFLICT (id) DO UPDATE SET
		     short     = EXCLUDED.short,
		     long      = EXCLUDED.long,
		     created   = EXCLUDED.created,
		     last_edit = EXCLUDED.last_edit,
		     owner     = EXCLUDED.owner`,
		linkID(link.Short), link.Short, link.Long, link.Created.Unix(), link.LastEdit.Unix(), link.Owner)
	if err != nil {
		return err
	}
	if tag.RowsAffected() != 1 {
		return fmt.Errorf("expected to affect 1 row, affected %d", tag.RowsAffected())
	}
	return nil
}

// Delete removes a Link by its short name.
func (p *PostgresDB) Delete(short string) error {
	tag, err := p.pool.Exec(context.Background(),
		"DELETE FROM links WHERE id = $1", linkID(short))
	if err != nil {
		return err
	}
	if tag.RowsAffected() != 1 {
		return fmt.Errorf("expected to affect 1 row, affected %d", tag.RowsAffected())
	}
	return nil
}

// GetLinksByOwner returns all Links owned by the given owner.
func (p *PostgresDB) GetLinksByOwner(owner string) ([]*Link, error) {
	rows, err := p.pool.Query(context.Background(),
		"SELECT short, long, created, last_edit, owner FROM links WHERE lower(owner) = lower($1)", owner)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var links []*Link
	for rows.Next() {
		link := new(Link)
		var created, lastEdit int64
		if err := rows.Scan(&link.Short, &link.Long, &created, &lastEdit, &link.Owner); err != nil {
			return nil, err
		}
		link.Created = time.Unix(created, 0).UTC()
		link.LastEdit = time.Unix(lastEdit, 0).UTC()
		links = append(links, link)
	}
	return links, rows.Err()
}

// LoadStats returns total click counts keyed by link short name.
func (p *PostgresDB) LoadStats() (ClickStats, error) {
	allLinks, err := p.LoadAll()
	if err != nil {
		return nil, err
	}
	linkmap := make(map[string]string, len(allLinks)) // map ID => Short
	for _, link := range allLinks {
		linkmap[linkID(link.Short)] = link.Short
	}

	rows, err := p.pool.Query(context.Background(),
		"SELECT id, sum(clicks) FROM stats GROUP BY id")
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	stats := make(ClickStats)
	for rows.Next() {
		var id string
		var clicks int
		if err := rows.Scan(&id, &clicks); err != nil {
			return nil, err
		}
		if short, ok := linkmap[id]; ok {
			stats[short] = clicks
		}
	}
	return stats, rows.Err()
}

// SaveStats records incremental click counts since the last call.
func (p *PostgresDB) SaveStats(stats ClickStats) error {
	ctx := context.Background()
	tx, err := p.pool.Begin(ctx)
	if err != nil {
		return err
	}
	defer tx.Rollback(ctx)

	now := p.Now().Unix()
	for short, clicks := range stats {
		_, err := tx.Exec(ctx,
			"INSERT INTO stats (id, created, clicks) VALUES ($1, $2, $3)",
			linkID(short), now, clicks)
		if err != nil {
			return err
		}
	}
	return tx.Commit(ctx)
}

// DeleteStats removes all click stats for a link.
func (p *PostgresDB) DeleteStats(short string) error {
	_, err := p.pool.Exec(context.Background(),
		"DELETE FROM stats WHERE id = $1", linkID(short))
	return err
}

// ExportStats returns all raw stat rows ordered by creation time and ID.
func (p *PostgresDB) ExportStats() ([]*StatEntry, error) {
	rows, err := p.pool.Query(context.Background(),
		"SELECT id, created, clicks FROM stats ORDER BY created, id")
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var entries []*StatEntry
	for rows.Next() {
		e := new(StatEntry)
		if err := rows.Scan(&e.ID, &e.Created, &e.Clicks); err != nil {
			return nil, err
		}
		entries = append(entries, e)
	}
	return entries, rows.Err()
}

// compile-time assertion that PostgresDB implements DB.
var _ DB = (*PostgresDB)(nil)
