package web

import "testing"

func TestIPHasher(t *testing.T) {
	keyed := &IPHasher{key: []byte("k1")}
	other := &IPHasher{key: []byte("k2")}
	plain := &IPHasher{}

	if keyed.Hash("") != "" || keyed.Hash(" , 10.0.0.1") != "" {
		t.Error("no client address should hash to empty")
	}
	a := keyed.Hash("203.0.113.7")
	if len(a) != 64 {
		t.Errorf("hash = %q, want 64 hex chars", a)
	}
	if keyed.Hash(" 203.0.113.7 , 10.0.0.1, 10.0.0.2") != a {
		t.Error("only the first forwarded entry should count")
	}
	if keyed.Hash("203.0.113.8") == a {
		t.Error("different addresses hashed the same")
	}
	if other.Hash("203.0.113.7") == a || plain.Hash("203.0.113.7") == a {
		t.Error("key does not change the hash")
	}
}

func TestIPHasherFromEnvRejectsBadFile(t *testing.T) {
	t.Setenv("DRAIN_IP_HASH_KEY_FILE", t.TempDir()+"/missing")
	if _, err := NewIPHasherFromEnv(); err == nil {
		t.Error("missing key file should be an error")
	}
}
