package writer

import (
	"os"
	"path/filepath"
	"strings"
	"testing"
)

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
	if !strings.Contains(got, "password=s3cr3t ") {
		t.Fatalf("secret not used, got %q", got)
	}
	if strings.Contains(got, "fallback") {
		t.Fatalf("fell back to the literal, got %q", got)
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
	if !strings.Contains(got, "password=from-env ") {
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
	if !strings.Contains(got, "password=from-file ") {
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
	if !strings.Contains(got, "password=password") {
		t.Fatalf("dev fallback broken, got %q", got)
	}
}
