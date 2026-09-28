package web

import (
	"crypto/hmac"
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"os"
	"strings"
)

// IPHasher turns a client address into a stable, opaque token so the raw IP is never
// written to the database. The same address always yields the same token, which is all
// the notebooks need to fall back on when a beacon has no serverID.
//
// With a key it is HMAC-SHA256. Without one it is plain SHA-256, which only hides the
// address from a casual reader: the whole IPv4 space hashes in seconds, so anyone with
// the database can reverse it. Production should always set a key.
type IPHasher struct {
	key []byte
}

// NewIPHasherFromEnv reads the key from DRAIN_IP_HASH_KEY_FILE (a Docker secret) or
// DRAIN_IP_HASH_KEY. Like POSTGRES_PASSWORD_FILE, a file that is set but unreadable or
// empty is an error rather than a silent fall back to the unkeyed hash.
func NewIPHasherFromEnv() (*IPHasher, error) {
	if path, ok := os.LookupEnv("DRAIN_IP_HASH_KEY_FILE"); ok && path != "" {
		b, err := os.ReadFile(path)
		if err != nil {
			return nil, fmt.Errorf("reading DRAIN_IP_HASH_KEY_FILE %s: %w", path, err)
		}
		key := strings.TrimRight(string(b), "\r\n")
		if key == "" {
			return nil, fmt.Errorf("DRAIN_IP_HASH_KEY_FILE %s is empty", path)
		}
		return &IPHasher{key: []byte(key)}, nil
	}
	return &IPHasher{key: []byte(os.Getenv("DRAIN_IP_HASH_KEY"))}, nil
}

// Keyed reports whether a key is set, i.e. whether the hash resists brute force.
func (h *IPHasher) Keyed() bool {
	return len(h.key) > 0
}

// Hash returns the hex token for the client address in an X-Forwarded-For value, or ""
// when there is none. Only the first entry is the client; the rest are proxies, and
// keeping them would give one client a different token per route.
func (h *IPHasher) Hash(forwardedFor string) string {
	ip, _, _ := strings.Cut(forwardedFor, ",")
	ip = strings.TrimSpace(ip)
	if ip == "" {
		return ""
	}
	if !h.Keyed() {
		sum := sha256.Sum256([]byte(ip))
		return hex.EncodeToString(sum[:])
	}
	mac := hmac.New(sha256.New, h.key)
	mac.Write([]byte(ip))
	return hex.EncodeToString(mac.Sum(nil))
}
