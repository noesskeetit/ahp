-- AHP: инициализация схемы PostgreSQL для ЛОКАЛЬНОЙ разработки.
--
-- Выполняется ТОЛЬКО образом postgres при первом старте на ПУСТОМ томе
-- (каталог /docker-entrypoint-initdb.d). Повторно на существующем томе не
-- запускается, поэтому существующие данные не затрагиваются.
--
-- Правила этого файла:
--   * идемпотентность (IF NOT EXISTS) — безопасно при переинициализации тома;
--   * никаких DROP / TRUNCATE / ALTER существующих объектов и схем;
--   * никаких предзаполненных данных, учётных записей и секретов.

-- Векторный поиск (pgvector) обязателен для сервисов, работающих с embeddings.
CREATE EXTENSION IF NOT EXISTS vector;

-- Раздельные схемы сервисов (см. docs/implementation.md: каждый сервис владеет
-- своей схемой). Создаются пустыми; таблицы создаёт код сервиса миграциями.
CREATE SCHEMA IF NOT EXISTS state;
CREATE SCHEMA IF NOT EXISTS identity;
CREATE SCHEMA IF NOT EXISTS operations;
CREATE SCHEMA IF NOT EXISTS registry;
CREATE SCHEMA IF NOT EXISTS gateway;
CREATE SCHEMA IF NOT EXISTS model;
CREATE SCHEMA IF NOT EXISTS tools;
CREATE SCHEMA IF NOT EXISTS artifacts;
CREATE SCHEMA IF NOT EXISTS workspace;
CREATE SCHEMA IF NOT EXISTS memory;

-- Совместимость с pgvector: операторы класса ivfflat/hnsw доступны из образа.
-- Проверка доступна: SELECT extname FROM pg_extension WHERE extname = 'vector';
