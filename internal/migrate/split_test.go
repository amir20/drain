package migrate

import "testing"

func TestSplit(t *testing.T) {
	tests := []struct {
		name string
		sql  string
		want int
	}{
		{"simple", "SELECT 1; SELECT 2;", 2},
		{"trailing statement without semicolon", "SELECT 1; SELECT 2", 2},
		{"semicolon in a string literal", "SELECT 'a;b'; SELECT 2;", 2},
		{"escaped quote inside a literal", "SELECT 'it''s; fine'; SELECT 2;", 2},
		{"semicolon in a line comment", "SELECT 1; -- a; comment\nSELECT 2;", 2},
		{"semicolon in a block comment", "SELECT 1; /* a; b */ SELECT 2;", 2},
		{"dollar quoted body", "CREATE FUNCTION f() RETURNS int AS $$ BEGIN; RETURN 1; END; $$ LANGUAGE plpgsql; SELECT 2;", 2},
		{"tagged dollar quote", "CREATE PROCEDURE p() AS $proc$ BEGIN; a; b; END; $proc$; SELECT 2;", 2},
		{"blank statements are dropped", "SELECT 1;;;\n\n; SELECT 2;", 2},
		{"comment only file", "-- nothing here\n", 1},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := len(Split(tt.sql)); got != tt.want {
				t.Fatalf("Split() = %d statements, want %d\n%q", got, tt.want, Split(tt.sql))
			}
		})
	}
}

func TestSplitKeepsDollarQuotedBodyIntact(t *testing.T) {
	sql := "CREATE PROCEDURE p() LANGUAGE plpgsql AS $proc$\nBEGIN\n  DELETE FROM t;\n  COMMIT;\nEND;\n$proc$;"
	got := Split(sql)
	if len(got) != 1 {
		t.Fatalf("expected the whole procedure as one statement, got %d: %q", len(got), got)
	}
	if got[0] != sql {
		t.Fatalf("statement was altered:\n got %q\nwant %q", got[0], sql)
	}
}

func TestSplitOnRealMigrationShape(t *testing.T) {
	sql := `
-- a comment with a ; in it
CREATE TABLE IF NOT EXISTS t (a text);

DO $$
BEGIN
  IF NOT EXISTS (SELECT 1 FROM x WHERE y = 'a;b') THEN
    CREATE MATERIALIZED VIEW v AS SELECT 1;
  END IF;
END $$;

SELECT create_hypertable('t', 'a', if_not_exists => TRUE);
`
	if got := Split(sql); len(got) != 3 {
		t.Fatalf("expected 3 statements, got %d: %q", len(got), got)
	}
}
