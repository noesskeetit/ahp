// Проверка браузерного канонизатора против векторов, порождённых платформой.
//
// Векторы (web/testdata/aifactory-digest-vectors.json) создали НЕ мы: значения
// digest считает contracts.Digest по разобранному registry.Manifest и по
// json.RawMessage (web/aifactory_digest_test.go сверяет ту же таблицу со
// стороны Go). Если кто-то поправит экранирование, порядок ключей или нулевые
// значения в JS — digest разойдётся с продуктовым, и нога «publish → гейт
// оценок» перестанет быть доказательством.
//
// Обе ветки хеширования обязаны совпасть: WebCrypto (браузер) и ручная
// реализация (окружения без crypto.subtle).
import test from "node:test";
import assert from "node:assert/strict";
import { readFileSync } from "node:fs";
import { webcrypto as crypto, createHash } from "node:crypto";
import vm from "node:vm";
import { fileURLToPath } from "node:url";
import path from "node:path";

const here = path.dirname(fileURLToPath(import.meta.url));
const source = readFileSync(path.join(here, "aifactory", "ahp-canonical.js"), "utf8");
const vectors = JSON.parse(readFileSync(path.join(here, "testdata", "aifactory-digest-vectors.json"), "utf8"));

// Загружаем файл так же, как браузер: обычным скриптом, где есть только window.
function load({ useWebCrypto }) {
  const sandbox = { window: {}, TextEncoder };
  if (useWebCrypto) sandbox.crypto = crypto;
  vm.runInNewContext(source, sandbox, { filename: "aifactory/ahp-canonical.js" });
  assert.ok(sandbox.window.AHP_CANONICAL, "файл не опубликовал window.AHP_CANONICAL");
  return sandbox.window.AHP_CANONICAL;
}

const inBrowser = load({ useWebCrypto: true });
const withoutWebCrypto = load({ useWebCrypto: false });

test("ручная SHA-256 совпадает с node:crypto на границах блока", () => {
  const cases = ["", "abc", "a".repeat(55), "a".repeat(56), "a".repeat(63), "a".repeat(64),
    "a".repeat(65), "a".repeat(112), "х".repeat(30), "привет мир".repeat(9)];
  for (const text of cases) {
    assert.equal(withoutWebCrypto.sha256FallbackHex(text),
      createHash("sha256").update(text, "utf8").digest("hex"),
      "длина " + Buffer.byteLength(text) + " байт");
  }
});

test("digest манифеста совпадает с платформенным в обеих ветках хеширования", async () => {
  for (const vector of vectors.manifests) {
    for (const [label, impl] of [["WebCrypto", inBrowser], ["fallback", withoutWebCrypto]]) {
      const got = await impl.manifestDigest(vector.manifest);
      assert.equal(got, vector.digest, vector.name + " (" + label + ")");
    }
  }
});

test("незнакомые поля выпадают до хеширования", async () => {
  const byName = new Map(vectors.manifests.map((m) => [m.name, m]));
  for (const vector of vectors.manifests) {
    if (!vector.same_digest_as) continue;
    const base = byName.get(vector.same_digest_as);
    assert.ok(base, "нет эталонного вектора " + vector.same_digest_as);
    assert.notDeepEqual(vector.manifest, base.manifest, "векторы не должны быть одинаковыми");
    assert.equal(await inBrowser.manifestDigest(vector.manifest),
      await inBrowser.manifestDigest(base.manifest),
      vector.name + " обязанdigest'ом совпадать с " + vector.same_digest_as);
  }
});

test("каноническая форма произвольных JSON-значений совпадает с платформенной", async () => {
  for (const vector of vectors.canonical_values) {
    const canonical = inBrowser.canonicalJSON(vector.value);
    assert.equal("sha256:" + (await inBrowser.sha256Hex(canonical)), vector.digest,
      vector.name + " (WebCrypto)");
    assert.equal("sha256:" + withoutWebCrypto.sha256FallbackHex(canonical), vector.digest,
      vector.name + " (fallback)");
  }
});

test("каноника сортирует ключи по байтам UTF-8, а не по UTF-16", () => {
  // '~' (0x7E) идёт до строчной латиницы только по байтовому порядку; ключи
  // нелатиницы сравниваются по UTF-8, где JS-ный sort() по кодовым единицам
  // разошёлся бы с Go на многобайтных последовательностях.
  const canonical = inBrowser.canonicalJSON({ "ключ": 1, "е": 2, "~t": 3, "A": 4 });
  assert.equal(canonical, '{"A":4,"~t":3,"е":2,"ключ":1}');
});

test("каноника экранирует <, >, & и U+2028 как encoding/json", () => {
  assert.equal(inBrowser.canonicalJSON({ s: "<a>&b" }), '{"s":"\\u003ca\\u003e\\u0026b"}');
  assert.equal(inBrowser.canonicalJSON({ s: "  " }), '{"s":"\\u2028\\u2029"}');
  assert.equal(inBrowser.canonicalJSON({ s: "a\nb\tc" }), '{"s":"a\\nb\\tc"}');
});

test("манифест неверного вида — честная ошибка клиента, а не молчаливый 400", () => {
  assert.throws(() => inBrowser.normaliseManifest({ kind: 1 }), /kind: ожидалась строка/);
  assert.throws(() => inBrowser.normaliseManifest({ limits: { depth: 1.5 } }), /depth: ожидалось целое число/);
  assert.throws(() => inBrowser.normaliseManifest({ components: "agent" }), /components: ожидался массив/);
  assert.throws(() => inBrowser.normaliseManifest({ state: [] }), /state: ожидался объект/);
});

test("нормализация повторяет нули encoding/json", () => {
  const sparse = inBrowser.normaliseManifest({ kind: "agent", system: "prototype" });
  // Объект создан в vm-контексте (другой realm), поэтому сравниваем через JSON.
  assert.deepEqual(JSON.parse(JSON.stringify(sparse)), {
    kind: "agent",
    system: "prototype",
    visibility: "",
    audience: "",
    class: "",
    components: null,
    state: { partition: "" },
    capabilities: { self_tasks: 0 },
    limits: { depth: 0, active_tasks: 0, task_timeout: "" },
  });
});
test("UI публикует ровно ту форму, которую принимает ядро", () => {
  // Вектор ui-default порождён платформой (contracts.Digest + ValidateManifest),
  // его манифест — эталон формы. Расхождение здесь означает, что прототип снова
  // отправляет макетные значения и PutVersion честит 400 до всякого гейта.
  const vector = vectors.manifests.find((m) => m.name === "ui-default");
  assert.ok(vector, "исчез вектор ui-default");
  assert.equal(vector.valid, true, "эталонная форма обязана проходить ValidateManifest ядра");
  assert.deepEqual(JSON.parse(JSON.stringify(inBrowser.publishManifest("h24-demo-agent"))),
    vector.manifest, "publishManifest разошёлся с проверенной ядром формой");
});
