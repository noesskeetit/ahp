// Command digestvectors перегенерирует web/testdata/aifactory-digest-vectors.json.
//
// Назначение: векторы канонного digest'а должны исходить от платформы, а не от
// Fantasie клиента. Здесь формы манифестов прогоняются через registry.Manifest,
// registry.ValidateManifest и contracts.Digest — те же коды, что работают за
// POST /v1/registry/PutVersion. Тесты (Go и Node) читают уже готовый файл и
// ничего сами не вычисляют как эталон.
//
// Запуск: go run ./web/testdata/digestvectors > web/testdata/aifactory-digest-vectors.json
// (каталог testdata игнорируется сборочными шаблонами ./..., в продукт не попадает).
package main

import (
	"bytes"
	"encoding/json"
	"fmt"
	"os"

	"github.com/ahp-platform/ahp/internal/registry"
	"github.com/ahp-platform/ahp/pkg/contracts"
)

type manifestSource struct {
	Name     string          `json:"name"`
	Note     string          `json:"note,omitempty"`
	SameAs   string          `json:"same_digest_as,omitempty"`
	Manifest json.RawMessage `json:"manifest"`
}

type manifestVector struct {
	Name     string          `json:"name"`
	Note     string          `json:"note,omitempty"`
	Valid    bool            `json:"valid"`
	Digest   string          `json:"digest"`
	SameAs   string          `json:"same_digest_as,omitempty"`
	Manifest json.RawMessage `json:"manifest"`
}

type valueSource struct {
	Name  string          `json:"name"`
	Note  string          `json:"note,omitempty"`
	Value json.RawMessage `json:"value"`
}

type valueVector struct {
	Name   string          `json:"name"`
	Note   string          `json:"note,omitempty"`
	Digest string          `json:"digest"`
	Value  json.RawMessage `json:"value"`
}

type sources struct {
	Manifests []manifestSource `json:"manifests"`
	Values    []valueSource    `json:"canonical_values"`
}

type document struct {
	About   string           `json:"_about"`
	Vectors []manifestVector `json:"manifests"`
	Values  []valueVector    `json:"canonical_values"`
}

const about = "Векторы канонного digest'а. Значения digest порождена самой платформой: " +
	"contracts.Digest по разобранному registry.Manifest и по json.RawMessage " +
	"(go run ./web/testdata/digestvectors). Поэтому они — оракул для клиентской реализации " +
	"web/aifactory/ahp-canonical.js. Проверка двусторонняя: web/aifactory_digest_test.go сверяет " +
	"платформу, web/aifactory_canonical.test.mjs — браузерный код. Вектор ui-default — буквально " +
	"форма publishManifest(\"h24-demo-agent\"), то есть UI публикует манифест, прошедший ValidateManifest ядра."

func main() {
	if len(os.Args) > 1 {
		fmt.Fprintln(os.Stderr, "usage: go run ./web/testdata/digestvectors > web/testdata/aifactory-digest-vectors.json")
		os.Exit(2)
	}
	raw, err := os.ReadFile("web/testdata/digestvectors/sources.json")
	if err != nil {
		panic(err)
	}
	dec := json.NewDecoder(bytes.NewReader(raw))
	var src sources
	if err := dec.Decode(&src); err != nil {
		panic(err)
	}
	doc := document{About: about}
	for _, item := range src.Manifests {
		var m registry.Manifest
		if err := json.Unmarshal(item.Manifest, &m); err != nil {
			panic(fmt.Sprintf("%s: %v", item.Name, err))
		}
		// Валидность фиксируем сразу: ниже err перезаписывается digest'ом, и
		// «Valid» получилась бы правдой для любой разобрзированной формы.
		valid := registry.ValidateManifest(m) == nil
		if !valid {
			fmt.Fprintln(os.Stderr, "INVALID", item.Name, "->", registry.ValidateManifest(m))
		}
		digest, err := contracts.Digest(m)
		if err != nil {
			panic(err)
		}
		doc.Vectors = append(doc.Vectors, manifestVector{
			Name: item.Name, Note: item.Note, Valid: valid, Digest: digest,
			SameAs: item.SameAs, Manifest: item.Manifest,
		})
	}
	for _, item := range src.Values {
		digest, err := contracts.Digest(json.RawMessage(item.Value))
		if err != nil {
			panic(err)
		}
		doc.Values = append(doc.Values, valueVector{Name: item.Name, Note: item.Note, Digest: digest, Value: item.Value})
	}
	enc := json.NewEncoder(os.Stdout)
	enc.SetIndent("", "  ")
	enc.SetEscapeHTML(false)
	if err := enc.Encode(doc); err != nil {
		panic(err)
	}
}
