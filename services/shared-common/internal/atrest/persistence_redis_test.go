//go:build ahp_data

package atrest

import (
	"bytes"
	"context"
	"os"
	"testing"
	"time"

	"github.com/ahp-platform/ahp/internal/compute"
)

// Byte-check of a value persisted to the real Redis in the lane. Redis here is a
// lease/cache owner rather than a primary system of record; encrypting the value
// before it lands proves that even a memory/dump-exposed Redis entry never holds
// plaintext. Nothing prints the plaintext or key.

const redisSentinel = "REDIS-PLAINTEXT-MARKER-33aa"

func redis(t *testing.T) *compute.Redis {
	t.Helper()
	addr := os.Getenv("AHP_REDIS_ADDR")
	if addr == "" {
		t.Fatal("AHP_REDIS_ADDR must be set for at-rest Redis byte-check")
	}
	r := compute.NewRedis(addr)
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()
	if err := r.Ping(ctx); err != nil {
		t.Fatalf("redis ping: %v", err)
	}
	return r
}

func TestRedisSealedValueByteCheck(t *testing.T) {
	r := redis(t)
	auth := restartAuthority(t, testTenant)
	codec, _ := NewCodec(auth)
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	plaintext := "runtime-secret " + redisSentinel
	env, err := codec.Seal(ctx, testTenant, "secret.runtime", []byte(plaintext))
	if err != nil {
		t.Fatal(err)
	}
	key := "ahp:atrest:lab:" + testTenant
	if err := r.SetPX(ctx, key, string(env), 60_000); err != nil {
		t.Fatalf("set: %v", err)
	}
	t.Cleanup(func() { _ = r.Del(context.Background(), key) })

	stored, err := r.Get(ctx, key)
	if err != nil {
		t.Fatalf("get: %v", err)
	}
	if bytes.Contains([]byte(stored), []byte(redisSentinel)) {
		t.Fatal("Redis value contains the plaintext marker")
	}
	if stored == plaintext {
		t.Fatal("Redis value equals plaintext")
	}
	got, err := codec.Open(ctx, testTenant, "secret.runtime", []byte(stored))
	if err != nil || string(got) != plaintext {
		t.Fatalf("open stored redis value: got=%q err=%v", got, err)
	}
}
