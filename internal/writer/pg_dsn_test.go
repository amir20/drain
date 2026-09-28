package writer

import (
	"net/url"
	"os"
	"path/filepath"
	"testing"
)

// password is the password a DSN carries, as lib/pq will read it.
func password(t *testing.T, dsn string) string {
	t.Helper()
	u, err := url.Parse(dsn)
	if err != nil {
		t.Fatalf("DSN %q does not parse: %v", dsn, err)
	}
	p, _ := u.User.Password()
	return p
}

func TestDSNPasswordFile(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "pw")
	// A trailing newline is what `echo` and every editor leave behind.
	if err := os.WriteFile(path, []byte("s3cr3t\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	t.Setenv("POSTGRES_PASSWORD_FILE", path)

	got, err := DSN("postgres", "fallback")
	if err != nil {
		t.Fatal(err)
	}
	if p := password(t, got); p != "s3cr3t" {
		t.Fatalf("secret not used, got %q", p)
	}
}

func TestDSNPasswordFileMissingIsAnError(t *testing.T) {
	t.Setenv("POSTGRES_PASSWORD_FILE", filepath.Join(t.TempDir(), "nope"))
	if _, err := DSN("postgres", "fallback"); err == nil {
		t.Fatal("want an error for an unreadable secret, got nil")
	}
}

func TestDSNPasswordFileEmptyIsAnError(t *testing.T) {
	path := filepath.Join(t.TempDir(), "pw")
	if err := os.WriteFile(path, []byte("\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	t.Setenv("POSTGRES_PASSWORD_FILE", path)
	if _, err := DSN("postgres", "fallback"); err == nil {
		t.Fatal("want an error for an empty secret, got nil")
	}
}

func TestDSNDatabaseURLWins(t *testing.T) {
	t.Setenv("DATABASE_URL", "postgres://u:p@h:5432/d")
	t.Setenv("POSTGRES_PASSWORD_FILE", "/nonexistent")
	got, err := DSN("postgres", "fallback")
	if err != nil {
		t.Fatal(err)
	}
	if got != "postgres://u:p@h:5432/d" {
		t.Fatalf("got %q", got)
	}
}

func TestDSNPasswordEnv(t *testing.T) {
	os.Unsetenv("DATABASE_URL")
	os.Unsetenv("POSTGRES_PASSWORD_FILE")
	t.Setenv("POSTGRES_PASSWORD", "from-env")
	got, err := DSN("postgres", "fallback")
	if err != nil {
		t.Fatal(err)
	}
	if password(t, got) != "from-env" {
		t.Fatalf("env not used, got %q", got)
	}
}

func TestDSNFileBeatsEnv(t *testing.T) {
	path := filepath.Join(t.TempDir(), "pw")
	if err := os.WriteFile(path, []byte("from-file"), 0o600); err != nil {
		t.Fatal(err)
	}
	t.Setenv("POSTGRES_PASSWORD_FILE", path)
	t.Setenv("POSTGRES_PASSWORD", "from-env")
	got, err := DSN("postgres", "fallback")
	if err != nil {
		t.Fatal(err)
	}
	if password(t, got) != "from-file" {
		t.Fatalf("file should win over env, got %q", got)
	}
}

func TestDSNFallsBackWithoutSecret(t *testing.T) {
	os.Unsetenv("DATABASE_URL")
	os.Unsetenv("POSTGRES_PASSWORD_FILE")
	os.Unsetenv("POSTGRES_PASSWORD")
	got, err := DSN("postgres", "password")
	if err != nil {
		t.Fatal(err)
	}
	if password(t, got) != "password" {
		t.Fatalf("dev fallback broken, got %q", got)
	}
}

// A password is opaque bytes. key=value DSNs end an unquoted value at whitespace, so
// every character a generated secret might hold has to survive the round trip.
func TestDSNEscapesPassword(t *testing.T) {
	os.Unsetenv("DATABASE_URL")
	os.Unsetenv("POSTGRES_PASSWORD_FILE")
	const tricky = `a b'c\\d@e/f:g?h#i%j+k=l`
	t.Setenv("POSTGRES_PASSWORD", tricky)
	got, err := DSN("postgres", "fallback")
	if err != nil {
		t.Fatal(err)
	}
	if p := password(t, got); p != tricky {
		t.Fatalf("password = %q, want %q", p, tricky)
	}
}
