package space_test

import (
	"fmt"
	"net/url"
	"strings"
	"testing"
)

// These live in the default build so the native-Qwen gate's classification logic
// is regression-tested without spending a live LLM call. The live_llm journey
// (assertNativeQwen) reuses classifyNativeQwen; see live_llm_journey_test.go.

// nativeQwenHostSuffixes identifies the Cloud.ru Foundation Models / ModelRun
// inference plane that serves AHP's native Qwen models.
var nativeQwenHostSuffixes = []string{
	".modelrun.inference.cloud.ru",
}

// nativeQwenModels are the only model ids accepted as native-Qwen proof.
var nativeQwenModels = []string{
	"model-run-tqmbu-road",
	"qwen-38-27b-fp8",
}

// foreignProviderHostMarkers make a misconfiguration toward OpenAI/Astra a clear
// failure instead of a silent pass (no foreign-provider fallback is permitted).
var foreignProviderHostMarkers = []string{
	"openai.com", "openai.azure.com", "astra",
}

// classifyNativeQwen returns the normalized host when base/model constitute a
// native-Qwen proof target, else a non-nil error explaining the rejection. It is
// the pure core behind assertNativeQwen and is unit-tested in TestClassifyNativeQwen.
func classifyNativeQwen(base, model string) (string, error) {
	if base == "" || model == "" {
		return "", errf("AHP_LLM_BASE_URL/AHP_LLM_MODEL unset: live_llm must FAIL, not skip, when tagged")
	}
	u, err := url.Parse(base)
	if err != nil || (u.Scheme != "http" && u.Scheme != "https") || u.Host == "" {
		return "", errf("AHP_LLM_BASE_URL is not a valid http(s) URL: %q", base)
	}
	host := strings.ToLower(u.Hostname())
	for _, bad := range foreignProviderHostMarkers {
		if strings.Contains(host, bad) {
			return "", errf("host %q matches foreign provider marker %q (no OpenAI/Astra fallback)", host, bad)
		}
	}
	okHost := false
	for _, sfx := range nativeQwenHostSuffixes {
		if host == strings.TrimPrefix(sfx, ".") || strings.HasSuffix(host, sfx) {
			okHost = true
			break
		}
	}
	if !okHost {
		return "", errf("host %q is not a native-Qwen inference host; allowed suffixes %v", host, nativeQwenHostSuffixes)
	}
	okModel := false
	for _, m := range nativeQwenModels {
		if model == m {
			okModel = true
			break
		}
	}
	if !okModel {
		return "", errf("model %q is not an accepted native-Qwen model; allowed %v", model, nativeQwenModels)
	}
	return host, nil
}

func errf(format string, a ...any) error { return fmt.Errorf(format, a...) }

func TestClassifyNativeQwen(t *testing.T) {
	const nat = "https://0038df35-f6e5-4958-a4ec-0b289a0b0aef.modelrun.inference.cloud.ru/v1"
	cases := []struct {
		name     string
		base     string
		model    string
		wantErr  bool
		wantHost string
	}{
		{"native+road ok", nat, "model-run-tqmbu-road", false, "0038df35-f6e5-4958-a4ec-0b289a0b0aef.modelrun.inference.cloud.ru"},
		{"native+38b ok", nat, "qwen-38-27b-fp8", false, "0038df35-f6e5-4958-a4ec-0b289a0b0aef.modelrun.inference.cloud.ru"},
		{"foreign host openai", "https://api.openai.com/v1", "model-run-tqmbu-road", true, ""},
		{"foreign host astra", "https://astra.internal/v1", "model-run-tqmbu-road", true, ""},
		{"native host wrong model", nat, "gpt-4o", true, ""},
		{"non-native host", "https://api.example.com/v1", "model-run-tqmbu-road", true, ""},
		{"empty base", "", "model-run-tqmbu-road", true, ""},
		{"empty model", nat, "", true, ""},
		{"not http", "ftp://0038df35.modelrun.inference.cloud.ru", "model-run-tqmbu-road", true, ""},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			host, err := classifyNativeQwen(c.base, c.model)
			if c.wantErr && err == nil {
				t.Fatalf("expected error for %q/%q, got host=%q", c.base, c.model, host)
			}
			if !c.wantErr && err != nil {
				t.Fatalf("unexpected error for %q/%q: %v", c.base, c.model, err)
			}
			if !c.wantErr && host != c.wantHost {
				t.Fatalf("host=%q want %q", host, c.wantHost)
			}
		})
	}
}
