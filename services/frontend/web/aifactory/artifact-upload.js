// The browser receives a bounded PUT capability. Human/task authorization is
// only sent to the same-origin metadata operations, never to object storage.
(() => {
  async function upload(file, call, fetchObject = fetch) {
    if (!file || !file.size || file.size > 64 * 1024 * 1024) throw new Error("Выберите непустой файл размером до 64 МиБ.");
    const sha256 = [...new Uint8Array(await crypto.subtle.digest("SHA-256", await file.arrayBuffer()))]
      .map(b => b.toString(16).padStart(2, "0")).join("");
    const begin = await call("BeginUpload", {key: crypto.randomUUID(), name: file.name,
      mime: file.type || "application/octet-stream", sha256, size: file.size});
    if (begin.status !== 200) return begin;
    const up = begin.json;
    const url = new URL(up.presigned_url);
    if (!["http:", "https:"].includes(url.protocol) || url.username || url.password || url.hash || !up.id || up.size !== file.size || up.sha256 !== sha256) {
      throw new Error("Сервис вернул неверные параметры загрузки.");
    }
    const headers = {};
    for (const [name, value] of Object.entries(up.headers || {})) {
      const lower = name.toLowerCase();
      if (lower === "content-length") {
        if (String(file.size) !== value) throw new Error("Размер загрузки не совпадает.");
        continue; // Browser sets Content-Length from the Blob body.
      }
      if (["authorization", "cookie", "host", "proxy-authorization"].includes(lower)) throw new Error("Неверный заголовок загрузки.");
      headers[name] = value;
    }
    const put = await fetchObject(url.href, {method: "PUT", body: file, headers,
      credentials: "omit", redirect: "error", referrerPolicy: "no-referrer"});
    if (!put.ok) throw new Error("Объектное хранилище отклонило загрузку: HTTP " + put.status);
    const sealed = await call("Seal", {id: up.id});
    if (sealed.status === 200 && (sealed.json.id !== up.id || sealed.json.size !== file.size || sealed.json.sha256 !== sha256 || !sealed.json.version)) {
      throw new Error("Контрольная сумма или версия загруженного файла не совпадает.");
    }
    return sealed;
  }
  window.AHP_DIRECT_UPLOAD = {upload};
})();
