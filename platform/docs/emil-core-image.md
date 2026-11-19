# Образ Core Agent Эмиля

Пользователь 29.10.2026 явно попросил получить образ из GitLab, ветка `mcp_auth`.

- Source: `https://git.sbercloud.tech/products/evo-ai-assistant/users-story/ai-agent/agents/langgraph-new-a2a` (project 16018).
- Ветка заново проверена GitLab API и новым checkout: `5ed238b4ac6d71b583304f0104edb37345ec432e`.
- Свежий checkout: `.local/emil-core-src/`, игнорируется Git. Старые локальные копии и Docker-стенды не использовались.
- В API GitLab нет pipeline для `mcp_auth` и нет встроенных registry repositories. Известный опубликованный `v0.0.3` собран другим commit `71248e0ad491b72a06b479038fc4078aca234205`; он не подменяет выбранную ветку.
- Выполнена локальная сборка неизменённого upstream Dockerfile с `--pull` и OCI revision label.
- Локальный тег: `ahp-handbook-emil-core:5ed238b4`.
- Image manifest: `sha256:c14e97f6c85d68f86cd75a3e83951d4bd41c7ca16784b47bb7380aa94aa06023`.
- Image config: `sha256:aa83f7b0616fba2cf2550582f01aafc9871c927ef76d87ee09152d45c12d6bca`.
- Локальный manifest list с provenance: `sha256:5df069c6c01bb5d37afab1c4659b011f42d51d97eeb98d2129c50bb8c979daa7`.

Это собственный Python runtime `core-agent` 0.1.0; в проверенном `pyproject.toml` нет зависимости LangGraph. Название репозитория не является доказательством используемого движка.

Сборка образа сама по себе не доказывает AHP-интеграцию, State API compatibility, model/tool E2E или deployment. Эти проверки входят в отдельные задачи плана.
