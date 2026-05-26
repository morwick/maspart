-- ═══════════════════════════════════════════════════════════════════════════════
-- 004_batch_harga_jobs.sql
-- Tabel: batch_harga_jobs
-- ───────────────────────────────────────────────────────────────────────────────
-- Menyimpan METADATA setiap batch (pn_list, label, input_mode).
-- Memungkinkan auto-restore batch yang belum selesai TANPA harus upload
-- file Excel ulang / re-type Part Number.
--
-- Relasi:
--   batch_harga_jobs (1)  ──→  batch_harga_progress (N)
--   join lewat kolom job_id.
--
-- Cara run:
--   1. Buka Supabase Dashboard → SQL Editor → New Query
--   2. Paste seluruh isi file ini → Run
--   3. Verifikasi: Table Editor → muncul tabel `batch_harga_jobs`
-- ═══════════════════════════════════════════════════════════════════════════════

create table if not exists batch_harga_jobs (
    job_id      text         primary key,
    pn_list     jsonb        not null,
    label       text,
    input_mode  text,
    total_pn    int          not null default 0,
    created_at  timestamptz  not null default now(),
    updated_at  timestamptz  not null default now()
);

create index if not exists idx_batch_harga_jobs_updated
    on batch_harga_jobs (updated_at desc);

-- ── RLS / Access Policy ─────────────────────────────────────────────────────
-- Konsisten dengan tabel batch_harga_progress dan opname_sessions.

alter table batch_harga_jobs disable row level security;

-- ── (Opsional) Auto-cleanup jobs lebih lama dari 30 hari ────────────────────
-- Trigger manual via SQL Editor kapan-kapan kalau perlu.
--
-- delete from batch_harga_jobs where updated_at < now() - interval '30 days';
