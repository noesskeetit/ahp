package harness

import (
	"bufio"
	"fmt"
	"net"
	"strings"
	"testing"
	"time"
)

// redisInfo read the reply until a read ERROR, i.e. until EOF or the 3s
// deadline. Redis keeps an INFO connection open, so EVERY sample paid the
// full 3s deadline: at -interval-s 2 the stream could only deliver ~30 samples
// per 90s step instead of 45 (the density gate rejected the profile twice for
// exactly this). The reader must consume the declared bulk length and return.
func TestRedisInfoReturnsOnCompleteBulkReply(t *testing.T) {
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	defer ln.Close()
	payload := "# Server\r\nredis_version:7.4.0\r\nused_memory:1243896\r\nconnected_clients:1\r\ntotal_commands_processed:22013\r\ninstantaneous_ops_per_sec:5\r\ntotal_net_input_bytes:100\r\ntotal_net_output_bytes:200\r\nevicted_keys:0\r\nrejected_connections:0\r\n"
	go func() {
		for {
			c, err := ln.Accept()
			if err != nil {
				return
			}
			go func(c net.Conn) {
				defer c.Close()
				r := bufio.NewReader(c)
				for {
					if _, err := r.ReadString('\n'); err != nil {
						return
					}
					// the client sends INFO as a RESP array; after the last
					// element it waits for the reply: answer once, then hold
					// the connection OPEN like a real redis server does.
					fmt.Fprintf(c, "$%d\r\n%s\r\n", len(payload), payload)
					time.Sleep(30 * time.Second)
				}
			}(c)
		}
	}()
	start := time.Now()
	info, err := redisInfo(ln.Addr().String())
	elapsed := time.Since(start)
	if err != nil {
		t.Fatalf("redisInfo: %v", err)
	}
	if info["used_memory"] != "1243896" {
		t.Fatalf("payload not parsed: %v", info)
	}
	// The observation must cost milliseconds, not the 3s connection deadline:
	// a 2s tick needs room to keep its cadence.
	if elapsed > 500*time.Millisecond {
		t.Fatalf("redisInfo waited %v for the reply: it must stop at the declared bulk length, not at EOF/deadline (this is what halved sample density)", elapsed)
	}
	if strings.Contains(fmt.Sprint(info), "redis_version") == false {
		t.Fatal("version section missing")
	}
}
