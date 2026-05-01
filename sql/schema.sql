-- schema.sql
-- Mô hình dữ liệu chính: MongoDB (raw) -> Postgres (analytics + vận hành)

-- =============================================================================
-- 1. DIMENSION & OPERATION TABLES
-- =============================================================================

BEGIN;

CREATE EXTENSION IF NOT EXISTS pgcrypto;

-- 1.1. Bảng nhật ký ETL (Nguồn: Airflow Orchestration)
-- Mỗi lần chạy pipeline sẽ tạo một dòng để theo dõi trạng thái
CREATE TABLE IF NOT EXISTS etl_run_log (
    run_id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    dag_id VARCHAR(120) NOT NULL,
    run_type VARCHAR(20) NOT NULL CHECK (run_type IN ('scheduled', 'manual', 'backfill')),
    status VARCHAR(20) NOT NULL CHECK (status IN ('running', 'success', 'failed', 'partial')),
    started_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
    ended_at TIMESTAMP,
    records_read INT NOT NULL DEFAULT 0,
    records_loaded INT NOT NULL DEFAULT 0,
    records_quarantined INT NOT NULL DEFAULT 0,
    error_message TEXT,
    created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP
);

CREATE INDEX IF NOT EXISTS idx_etl_run_status_started_at ON etl_run_log(status, started_at DESC);

-- 1.2. Bảng nguồn dữ liệu (Nguồn: Cấu hình hệ thống)
CREATE TABLE IF NOT EXISTS dim_source (
    source_id SMALLSERIAL PRIMARY KEY,
    source_name VARCHAR(50) NOT NULL UNIQUE,
    source_url TEXT,
    is_active BOOLEAN NOT NULL DEFAULT TRUE,
    created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP
);

-- 1.3. Bảng quận/huyện (Nguồn: Danh mục hành chính Hà Nội)
CREATE TABLE IF NOT EXISTS dim_district (
    district_id SMALLSERIAL PRIMARY KEY,
    district_name VARCHAR(100) NOT NULL UNIQUE,
    city_name VARCHAR(100) NOT NULL DEFAULT 'Ha Noi',
    district_type VARCHAR(20) CHECK (district_type IN ('quan', 'huyen', 'thi_xa')),
    alias_names TEXT[] NOT NULL DEFAULT '{}'::text[],
    latitude NUMERIC(9,6),
    longitude NUMERIC(9,6),
    created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP
);

-- 1.4. Bảng phường/xã (Nguồn: Danh mục hành chính chi tiết)
CREATE TABLE IF NOT EXISTS dim_ward (
    ward_id SERIAL PRIMARY KEY,
    city_name VARCHAR(100) NOT NULL DEFAULT 'Hà Nội',
    district_id SMALLINT,
    ward_name VARCHAR(120) NOT NULL,
    canonical_name VARCHAR(120) NOT NULL,
    alias_names TEXT[] NOT NULL DEFAULT '{}'::text[],
    created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
    CONSTRAINT uq_dim_ward_canonical UNIQUE (district_id, canonical_name),
    CONSTRAINT fk_dim_ward_district FOREIGN KEY (district_id)
        REFERENCES dim_district(district_id)
        ON DELETE SET NULL
);

CREATE INDEX IF NOT EXISTS idx_dim_ward_district ON dim_ward(district_id);
CREATE INDEX IF NOT EXISTS idx_dim_ward_city ON dim_ward(city_name);

-- 1.5. Bảng thời gian (Date Dimension)
-- Dùng cho phân tích theo ngày/tháng/quý/năm
CREATE TABLE IF NOT EXISTS dim_date (
    date_key INT PRIMARY KEY,
    full_date DATE NOT NULL UNIQUE,
    year SMALLINT NOT NULL,
    quarter SMALLINT NOT NULL CHECK (quarter BETWEEN 1 AND 4),
    month SMALLINT NOT NULL CHECK (month BETWEEN 1 AND 12),
    day SMALLINT NOT NULL CHECK (day BETWEEN 1 AND 31),
    week_of_year SMALLINT NOT NULL CHECK (week_of_year BETWEEN 1 AND 53),
    day_of_week SMALLINT NOT NULL CHECK (day_of_week BETWEEN 1 AND 7),
    is_weekend BOOLEAN NOT NULL
);

-- 1.6. Bảng nhóm giá (Đơn vị: triệu VND)
CREATE TABLE IF NOT EXISTS dim_price_band (
    price_band_id SMALLSERIAL PRIMARY KEY,
    band_name VARCHAR(50) NOT NULL UNIQUE,
    min_price_million NUMERIC(12,2) NOT NULL,
    max_price_million NUMERIC(12,2),
    CHECK (max_price_million IS NULL OR min_price_million < max_price_million)
);

-- 1.7. Bảng nhóm diện tích (Đơn vị: m2)
CREATE TABLE IF NOT EXISTS dim_area_band (
    area_band_id SMALLSERIAL PRIMARY KEY,
    band_name VARCHAR(50) NOT NULL UNIQUE,
    min_area_sqm NUMERIC(10,2) NOT NULL,
    max_area_sqm NUMERIC(10,2),
    CHECK (max_area_sqm IS NULL OR min_area_sqm < max_area_sqm)
);

-- 1.8. Bảng loại hình bất động sản
CREATE TABLE IF NOT EXISTS dim_property_type (
    type_id SMALLSERIAL PRIMARY KEY,
    type_name VARCHAR(50) NOT NULL UNIQUE,
    created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP
);

-- =============================================================================
-- 2. FACT TABLES
-- =============================================================================

-- 2.1. Fact tin đăng bất động sản (Dữ liệu sạch để phân tích)
-- Mỗi dòng tương ứng một tin đăng đã chuẩn hóa từ pipeline
CREATE TABLE IF NOT EXISTS fact_property_listing (
    listing_id BIGSERIAL PRIMARY KEY,
    run_id UUID REFERENCES etl_run_log(run_id),
    source_id SMALLINT NOT NULL REFERENCES dim_source(source_id),
    external_id VARCHAR(50),
    district_id SMALLINT,
    ward_id INT, -- Thêm cột liên kết với dim_ward
    type_id SMALLINT REFERENCES dim_property_type(type_id),
    date_key INT NOT NULL REFERENCES dim_date(date_key),
    price_band_id SMALLINT REFERENCES dim_price_band(price_band_id),
    area_band_id SMALLINT REFERENCES dim_area_band(area_band_id),

    title TEXT,
    listing_url TEXT,
    address_text TEXT,
    ward_name VARCHAR(120), -- Lưu cả tên để đối chiếu nhanh

    price_million_vnd NUMERIC(12,2),
    area_sqm NUMERIC(10,2),
    price_per_sqm_million NUMERIC(12,4),

    first_seen_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
    last_seen_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
    is_active BOOLEAN NOT NULL DEFAULT TRUE,

    created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,

    CONSTRAINT fk_fact_property_district FOREIGN KEY (district_id)
        REFERENCES dim_district(district_id)
        ON DELETE SET NULL,
    CONSTRAINT uq_fact_source_listing UNIQUE (source_id, external_id)
);

CREATE INDEX IF NOT EXISTS idx_fact_date_district ON fact_property_listing(date_key, district_id);
CREATE INDEX IF NOT EXISTS idx_fact_district ON fact_property_listing(district_id);
CREATE INDEX IF NOT EXISTS idx_fact_source_date ON fact_property_listing(source_id, date_key);
CREATE INDEX IF NOT EXISTS idx_fact_price ON fact_property_listing(price_million_vnd);
CREATE INDEX IF NOT EXISTS idx_fact_area ON fact_property_listing(area_sqm);

-- 2.2. Bảng cách ly dữ liệu lỗi (Error Quarantine)
-- Lưu các bản ghi không đạt chuẩn để xử lý/reprocess sau
CREATE TABLE IF NOT EXISTS quarantine_listing (
    quarantine_id BIGSERIAL PRIMARY KEY,
    run_id UUID REFERENCES etl_run_log(run_id),
    source_name VARCHAR(50),
    listing_url TEXT,
    error_stage VARCHAR(30) NOT NULL CHECK (error_stage IN ('ingest', 'transform', 'load')),
    error_code VARCHAR(50),
    error_message TEXT NOT NULL,
    raw_payload JSONB NOT NULL,
    quarantined_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
    is_resolved BOOLEAN NOT NULL DEFAULT FALSE,
    resolved_at TIMESTAMP
);

CREATE INDEX IF NOT EXISTS idx_quarantine_stage_time ON quarantine_listing(error_stage, quarantined_at DESC);
CREATE INDEX IF NOT EXISTS idx_quarantine_resolved ON quarantine_listing(is_resolved);

COMMIT;