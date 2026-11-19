# syntax=docker/dockerfile:1

# AHP — сборка MinIO из исходников официального репозитория.
#
# Почему не публичный образ: prebuilt-образ minio/minio недоступен для выгрузки
# (Docker Hub отдаёт отсутствующие слои, Quay отвечает 401). Официальный README
# minio/minio рекомендует сборку из исходников — здесь ровно этот путь.
#
# Контекст сборки — игнорируемый каталог .local/minio-src (рабочее дерево
# официального тега RELEASE.2025-10-15T17-29-55Z, commit
# 9e49d5e7a648f00e26f2246f4dc28e6b07f8c84a). Исходники НЕ копируются в
# отслеживаемые каталоги репозитория; этот файл лежит вне контекста и
# передаётся через -f.
#
#   docker build -f infra/minio.Dockerfile -t ahp-handbook-minio:9e49d5e7 .local/minio-src
#
# Секретов здесь нет и быть не может: учётные данные MinIO передаются только
# через переменные окружения контейнера (MINIO_ROOT_USER / MINIO_ROOT_PASSWORD)
# в рантайме, в compose.yaml.

# ---------------------------------------------------------------------------
# Этап 1: сборка статического бинарника из исходников.
# ---------------------------------------------------------------------------
FROM golang:1.26.11-bookworm AS builder

WORKDIR /src

# Модульные файлы отдельно — слой зависимостей переиспользуется, пока они не
# менялись, а полная сборка исходников самая дорогая.
COPY go.mod go.sum ./
RUN go mod download

COPY . .

# CGO_ENABLED=0 — полностью статический бинарник, не зависит от libc runtime;
# -trimpath — пути сборки не попадают в бинарник (воспроизводимость).
RUN CGO_ENABLED=0 go build -trimpath -o /out/minio .

# ---------------------------------------------------------------------------
# Этап 2: минимальный рантайм. Только ca-certificates и curl.
# ---------------------------------------------------------------------------
FROM debian:bookworm-slim AS runtime

ENV DEBIAN_FRONTEND=noninteractive

# ca-certificates — TLS для внешнего хранилища/IDP; curl — healthcheck из
# compose.yaml. Ничего лишнего (ни shell-утилит сборки, ни пакета minio).
RUN apt-get update \
    && apt-get install -y --no-install-recommends ca-certificates curl \
    && rm -rf /var/lib/apt/lists/*

# Не root: фиксированный UID 10001, группа = пользователь.
RUN groupadd --gid 10001 minio \
    && useradd --uid 10001 --gid 10001 --home-dir /data --shell /usr/sbin/nologin --no-create-home minio

COPY --from=builder /out/minio /usr/local/bin/minio

# /data — точка монтирования тома go-minio-data; владелец — непривилегированный
# пользователь, секретов в образе нет.
RUN mkdir -p /data && chown 10001:10001 /data && chmod 0750 /data

USER 10001:10001
WORKDIR /data

EXPOSE 9000 9001

# Аргументы (server /data --address ...) приходят из compose.yaml.
ENTRYPOINT ["/usr/local/bin/minio"]

# Происхождение образа: официальный исходник и точная ревизия сборки.
LABEL org.opencontainers.image.title="ahp-handbook-minio" \
      org.opencontainers.image.description="MinIO built from official upstream source (prebuilt public image is not fetchable)" \
      org.opencontainers.image.source="https://github.com/minio/minio" \
      org.opencontainers.image.revision="9e49d5e7a648f00e26f2246f4dc28e6b07f8c84a" \
      org.opencontainers.image.version="RELEASE.2025-10-15T17-29-55Z"
