-- 03_seed.sql
-- Chứa dữ liệu danh mục mặc định

BEGIN;

-- 3.1. Seed nguồn dữ liệu
INSERT INTO dim_source (source_name, source_url)
VALUES
    ('chotot', 'https://www.nhatot.com/ban-nha-ha-noi'),
    ('batdongsan', 'https://batdongsan.com/ban-nha-ha-noi')
ON CONFLICT (source_name) DO NOTHING;

-- 3.2. Seed nhóm giá
INSERT INTO dim_price_band (band_name, min_price_million, max_price_million)
VALUES
    ('duoi_1_ty', 0, 1000),
    ('1_3_ty', 1000, 3000),
    ('3_5_ty', 3000, 5000),
    ('5_10_ty', 5000, 10000),
    ('tren_10_ty', 10000, NULL)
ON CONFLICT (band_name) DO NOTHING;

-- 3.3. Seed nhóm diện tích
INSERT INTO dim_area_band (band_name, min_area_sqm, max_area_sqm)
VALUES
    ('duoi_30m2', 0, 30),
    ('30_50m2', 30, 50),
    ('50_80m2', 50, 80),
    ('80_120m2', 80, 120),
    ('tren_120m2', 120, NULL)
ON CONFLICT (band_name) DO NOTHING;

-- 3.4. Seed loại hình bất động sản
INSERT INTO dim_property_type (type_name)
VALUES
    ('chung_cu'),
    ('nha_rieng'),
    ('nha_mat_pho'),
    ('dat_nen'),
    ('biet_thu')
ON CONFLICT (type_name) DO NOTHING;

-- 3.5. Seed quận/huyện Hà Nội
INSERT INTO dim_district (district_name, city_name, district_type, latitude, longitude)
VALUES
    ('Ba Dinh', 'Ha Noi', 'quan', 21.0340, 105.8142),
    ('Hoan Kiem', 'Ha Noi', 'quan', 21.0287, 105.8526),
    ('Dong Da', 'Ha Noi', 'quan', 21.0181, 105.8292),
    ('Cau Giay', 'Ha Noi', 'quan', 21.0362, 105.7907),
    ('Thanh Xuan', 'Ha Noi', 'quan', 20.9966, 105.8107),
    ('Hai Ba Trung', 'Ha Noi', 'quan', 21.0058, 105.8574),
    ('Hoang Mai', 'Ha Noi', 'quan', 20.9744, 105.8644),
    ('Long Bien', 'Ha Noi', 'quan', 21.0469, 105.8944),
    ('Nam Tu Liem', 'Ha Noi', 'quan', 21.0125, 105.7693),
    ('Bac Tu Liem', 'Ha Noi', 'quan', 21.0716, 105.7610)
ON CONFLICT (district_name) DO NOTHING;

-- 3.6. Seed bảng ngày cho khoảng +/- 365 ngày
INSERT INTO dim_date (
    date_key, full_date, year, quarter, month, day, week_of_year, day_of_week, is_weekend
)
SELECT
    TO_CHAR(d::date, 'YYYYMMDD')::INT AS date_key,
    d::date AS full_date,
    EXTRACT(YEAR FROM d)::SMALLINT AS year,
    EXTRACT(QUARTER FROM d)::SMALLINT AS quarter,
    EXTRACT(MONTH FROM d)::SMALLINT AS month,
    EXTRACT(DAY FROM d)::SMALLINT AS day,
    EXTRACT(WEEK FROM d)::SMALLINT AS week_of_year,
    EXTRACT(ISODOW FROM d)::SMALLINT AS day_of_week,
    (EXTRACT(ISODOW FROM d) IN (6, 7)) AS is_weekend
FROM generate_series(CURRENT_DATE - INTERVAL '365 days', CURRENT_DATE + INTERVAL '365 days', INTERVAL '1 day') d
ON CONFLICT (date_key) DO NOTHING;

COMMIT;
