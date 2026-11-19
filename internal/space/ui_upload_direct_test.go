package space

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
)

func TestBrowserDirectUploadOnlyForwardsMetadata(t *testing.T) {
	calls := 0
	machine := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		calls++
		if r.Header.Get("Authorization") != "Bearer scoped-task" {
			t.Error("missing exchanged authority")
		}
		if r.URL.Path != "/v1/artifacts/BeginUpload" {
			t.Error("wrong operation")
		}
		var in map[string]any
		if err := json.NewDecoder(r.Body).Decode(&in); err != nil {
			t.Fatal(err)
		}
		if in["size"] != float64(3) || in["name"] != "file.txt" {
			t.Error("metadata changed")
		}
		json.NewEncoder(w).Encode(map[string]any{"id": "art", "presigned_url": "https://objects.example/put?signature=bounded", "size": 3, "sha256": strings.Repeat("a", 64)})
	}))
	defer machine.Close()
	h, err := NewUploadProxy(UploadProxyConfig{ArtifactsURL: machine.URL, Client: machine.Client()})
	if err != nil {
		t.Fatal(err)
	}
	r := httptest.NewRequest("POST", UIUploadPath+"?operation=BeginUpload", strings.NewReader(`{"key":"direct","name":"file.txt","mime":"text/plain","sha256":"`+strings.Repeat("a", 64)+`","size":3}`))
	r.Header.Set("Content-Type", "application/json")
	w := httptest.NewRecorder()
	withTask(h, "scoped-task").ServeHTTP(w, r)
	if w.Code != 200 || calls != 1 {
		t.Fatalf("direct metadata status=%d calls=%d body=%s", w.Code, calls, w.Body.String())
	}
	if !strings.Contains(w.Body.String(), "presigned_url") {
		t.Fatal("browser needs the bounded PUT capability")
	}
	if strings.Contains(w.Body.String(), "scoped-task") {
		t.Fatal("task credential crossed browser boundary")
	}
}
