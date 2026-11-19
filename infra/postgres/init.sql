-- AHP: enable pgvector for embedding search (idempotent; safe on re-run).
CREATE EXTENSION IF NOT EXISTS vector;
