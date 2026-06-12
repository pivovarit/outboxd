//go:build integration

package outboxd

import (
	"context"
	"strings"
	"testing"

	"github.com/jackc/pgx/v5"
)

func execSQL(t *testing.T, dsn, sql string) {
	t.Helper()
	ctx := context.Background()
	conn, err := pgx.Connect(ctx, dsn)
	if err != nil {
		t.Fatalf("connect: %v", err)
	}
	defer conn.Close(ctx)
	if _, err := conn.Exec(ctx, sql); err != nil {
		t.Fatalf("exec %q: %v", sql, err)
	}
}

func walSlotExists(t *testing.T, dsn, slot string) bool {
	t.Helper()
	ctx := context.Background()
	conn, err := pgx.Connect(ctx, dsn)
	if err != nil {
		t.Fatalf("slot check connect: %v", err)
	}
	defer conn.Close(ctx)
	var exists bool
	if err := conn.QueryRow(ctx,
		"SELECT EXISTS(SELECT 1 FROM pg_replication_slots WHERE slot_name = $1)",
		slot).Scan(&exists); err != nil {
		t.Fatalf("slot check query: %v", err)
	}
	return exists
}

func validationConfig(slot string, mutate func(*Config)) Config {
	cfg := Config{
		SlotName:     slot,
		Publications: []string{"outbox_pub"},
		Schema: SchemaConfig{
			Table:           "outbox",
			IDColumn:        "id",
			TopicColumn:     "topic",
			PayloadColumn:   "payload",
			CreatedAtColumn: "created_at",
		},
	}
	mutate(&cfg)
	cfg.setDefaults()
	return cfg
}

func TestIntegration_WAL_StartupValidation_FailsFast(t *testing.T) {
	dsn, cleanup := startPG(t)
	defer cleanup()
	setupOutboxTable(t, dsn)
	execSQL(t, dsn, "CREATE TABLE other (id BIGSERIAL PRIMARY KEY, payload BYTEA)")
	execSQL(t, dsn, "CREATE PUBLICATION other_pub FOR TABLE other")
	execSQL(t, dsn, "CREATE PUBLICATION update_only_pub FOR TABLE outbox WITH (publish = 'update')")

	cases := []struct {
		name    string
		mutate  func(*Config)
		wantErr []string
	}{
		{
			name:    "table typo",
			mutate:  func(c *Config) { c.Schema.Table = "outbocks" },
			wantErr: []string{"outbocks", "does not exist"},
		},
		{
			name:    "table case mismatch",
			mutate:  func(c *Config) { c.Schema.Table = "Outbox" },
			wantErr: []string{"Outbox", "does not exist"},
		},
		{
			name:    "schema-qualified table typo",
			mutate:  func(c *Config) { c.Schema.Table = "nosuchschema.outbox" },
			wantErr: []string{"nosuchschema", "does not exist"},
		},
		{
			name:    "empty schema config",
			mutate:  func(c *Config) { c.Schema = SchemaConfig{} },
			wantErr: []string{"Table"},
		},
		{
			name:    "id column typo",
			mutate:  func(c *Config) { c.Schema.IDColumn = "idz" },
			wantErr: []string{"idz", "missing"},
		},
		{
			name:    "payload column typo",
			mutate:  func(c *Config) { c.Schema.PayloadColumn = "payloda" },
			wantErr: []string{"payloda", "missing"},
		},
		{
			name:    "topic column typo",
			mutate:  func(c *Config) { c.Schema.TopicColumn = "topicz" },
			wantErr: []string{"topicz", "missing"},
		},
		{
			name:    "extra column typo",
			mutate:  func(c *Config) { c.Schema.ExtraColumns = []string{"aggregate_idz"} },
			wantErr: []string{"aggregate_idz", "missing"},
		},
		{
			name:    "publication does not exist",
			mutate:  func(c *Config) { c.Publications = []string{"outbox_pubz"} },
			wantErr: []string{"outbox_pubz", "does not exist"},
		},
		{
			name:    "publication does not cover table",
			mutate:  func(c *Config) { c.Publications = []string{"other_pub"} },
			wantErr: []string{"publication"},
		},
		{
			name:    "publication does not publish inserts",
			mutate:  func(c *Config) { c.Publications = []string{"update_only_pub"} },
			wantErr: []string{"update_only_pub", "insert"},
		},
		{
			name:    "no publications",
			mutate:  func(c *Config) { c.Publications = nil },
			wantErr: []string{"Publications"},
		},
	}

	for i, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			slot := "validation_slot_" + string(rune('a'+i))
			cfg := validationConfig(slot, tc.mutate)
			w, err := newWALListener(context.Background(), dsn, cfg)
			if err == nil {
				w.Close(context.Background())
				t.Fatal("expected startup validation error, got nil")
			}
			for _, want := range tc.wantErr {
				if !strings.Contains(err.Error(), want) {
					t.Errorf("error %q does not mention %q", err, want)
				}
			}
			if walSlotExists(t, dsn, slot) {
				t.Errorf("replication slot %q was created despite validation failure", slot)
			}
		})
	}
}

func TestIntegration_WAL_StartupValidation_AcceptsValidConfig(t *testing.T) {
	dsn, cleanup := startPG(t)
	defer cleanup()
	setupOutboxTable(t, dsn)
	execSQL(t, dsn, "CREATE TABLE other (id BIGSERIAL PRIMARY KEY, payload BYTEA)")
	execSQL(t, dsn, "CREATE PUBLICATION other_pub FOR TABLE other")

	cases := []struct {
		name   string
		mutate func(*Config)
	}{
		{
			name:   "conventional schema",
			mutate: func(c *Config) {},
		},
		{
			name:   "schema-qualified table",
			mutate: func(c *Config) { c.Schema.Table = "public.outbox" },
		},
		{
			name: "disabled optional columns",
			mutate: func(c *Config) {
				c.Schema.TopicColumn = "-"
				c.Schema.CreatedAtColumn = "-"
			},
		},
		{
			name:   "table covered by second publication",
			mutate: func(c *Config) { c.Publications = []string{"other_pub", "outbox_pub"} },
		},
	}

	for i, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			slot := "validation_ok_slot_" + string(rune('a'+i))
			cfg := validationConfig(slot, tc.mutate)
			w, err := newWALListener(context.Background(), dsn, cfg)
			if err != nil {
				t.Fatalf("expected listener to start, got: %v", err)
			}
			w.Close(context.Background())
		})
	}
}
