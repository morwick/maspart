-- ═══════════════════════════════════════════════════════════════════════════════
-- 003_batch_harga.sql
-- Tabel: batch_harga_progress
-- ───────────────────────────────────────────────────────────────────────────────
-- Menyimpan progress batch cari harga dari SIMS supaya tahan restart
-- container Streamlit Cloud (filesystem ephemeral).
--
-- Setiap (job_id, pn) = 1 row. job_id adalah hash MD5 dari sorted list PN
-- yang di-upload user, sehingga upload list yang sama akan ke-detect dan
-- resume otomatis.
--
-- Cara run:
--   1. Buka Supabase Dashboard → SQL Editor → New Query
--   2. Paste seluruh isi file ini → Run
--   3. Verifikasi: Table Editor → harus muncul tabel `batch_harga_progress`
-- ═══════════════════════════════════════════════════════════════════════════════

create table if not exists batch_harga_progress (
    id          bigserial primary key,
    job_id      text         not null,
    pn          text         not null,
    price       numeric,
    err         text,
    via_pn      text,
    ts          text,
    updated_at  timestamptz  not null default now(),
    constraint batch_harga_progress_job_pn_unique unique (job_id, pn)
);

create index if not exists idx_batch_harga_progress_job
    on batch_harga_progress (job_id);

create index if not exists idx_batch_harga_progress_updated
    on batch_harga_progress (updated_at);

-- ── RLS / Access Policy ─────────────────────────────────────────────────────
-- App pakai anon key (lihat .streamlit/secrets.toml), jadi tabel harus
-- bisa diakses publik untuk SELECT / INSERT / UPDATE / DELETE.
-- Disable RLS supaya konsisten dengan tabel opname_sessions / permissions.

alter table batch_harga_progress disable row level security;

-- ── (Opsional) Auto-cleanup data lebih lama dari 30 hari ────────────────────
-- Aktifkan kalau khawatir tabel membengkak. Bisa juga di-trigger manual
-- via SQL Editor kapan-kapan.
--
-- delete from batch_harga_progress where updated_at < now() - interval '30 days';
