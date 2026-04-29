-- 03_seed.sql
-- Chứa dữ liệu danh mục mặc định

BEGIN;

-- 3.1. Seed nguồn dữ liệu
INSERT INTO dim_source (source_name, source_url)
VALUES
    ('chotot', 'https://www.nhatot.com/mua-ban-bat-dong-san-ha-noi'),
    ('batdongsan', 'https://batdongsan.com.vn/ban-nha-dat-ha-noi')
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
    ('biet_thu'),
    ('phong_tro'),
    ('mat_bang'),
    ('kho_xuong')
ON CONFLICT (type_name) DO NOTHING;

-- 3.5. Seed quận/huyện Hà Nội (22 districts with coordinates)
INSERT INTO dim_district (district_name, city_name, district_type, latitude, longitude, alias_names)
VALUES
    -- Central urban districts (10)
    ('Ba Dinh', 'Ha Noi', 'quan', 21.0285, 105.8581, ARRAY['Ba Dinh', 'Quan Ba Dinh', 'Q. Ba Dinh', 'Q.Ba Dinh']::text[]),
    ('Hoan Kiem', 'Ha Noi', 'quan', 21.0269, 105.8541, ARRAY['Hoan Kiem', 'Quan Hoan Kiem', 'Q. Hoan Kiem', 'Q.Hoan Kiem']::text[]),
    ('Dong Da', 'Ha Noi', 'quan', 21.0216, 105.8656, ARRAY['Dong Da', 'Quan Dong Da', 'Q. Dong Da', 'Q.Dong Da']::text[]),
    ('Cau Giay', 'Ha Noi', 'quan', 21.0087, 105.7836, ARRAY['Cau Giay', 'Quan Cau Giay', 'Q. Cau Giay', 'Q.Cau Giay']::text[]),
    ('Thanh Xuan', 'Ha Noi', 'quan', 21.0033, 105.8426, ARRAY['Thanh Xuan', 'Quan Thanh Xuan', 'Q. Thanh Xuan', 'Q.Thanh Xuan']::text[]),
    ('Hai Ba Trung', 'Ha Noi', 'quan', 21.0093, 105.8801, ARRAY['Hai Ba Trung', 'Quan Hai Ba Trung', 'Q. Hai Ba Trung', 'Q.HBT']::text[]),
    ('Hoang Mai', 'Ha Noi', 'quan', 20.9927, 105.8920, ARRAY['Hoang Mai', 'Quan Hoang Mai', 'Q. Hoang Mai', 'Q.Hoang Mai']::text[]),
    ('Long Bien', 'Ha Noi', 'quan', 21.0616, 105.8865, ARRAY['Long Bien', 'Quan Long Bien', 'Q. Long Bien', 'Q.Long Bien']::text[]),
    ('Nam Tu Liem', 'Ha Noi', 'quan', 21.0488, 105.7584, ARRAY['Nam Tu Liem', 'Quan Nam Tu Liem', 'Q. Nam Tu Liem', 'Q.Nam Tu Liem']::text[]),
    ('Bac Tu Liem', 'Ha Noi', 'quan', 21.0932, 105.7737, ARRAY['Bac Tu Liem', 'Quan Bac Tu Liem', 'Q. Bac Tu Liem', 'Q.Bac Tu Liem']::text[]),
    ('Tay Ho', 'Ha Noi', 'quan', 21.0808, 105.8142, ARRAY['Tay Ho', 'Quan Tay Ho', 'Q. Tay Ho', 'Q.Tay Ho']::text[]),
    ('Dong Anh', 'Ha Noi', 'huyen', 21.1789, 105.9330, ARRAY['Dong Anh', 'Huyen Dong Anh', 'H. Dong Anh', 'H.Dong Anh']::text[]),
    ('Gia Lam', 'Ha Noi', 'huyen', 21.1493, 105.9757, ARRAY['Gia Lam', 'Huyen Gia Lam', 'H. Gia Lam', 'H.Gia Lam']::text[]),
    ('Thanh Tri', 'Ha Noi', 'huyen', 20.8860, 105.9140, ARRAY['Thanh Tri', 'Huyen Thanh Tri', 'H. Thanh Tri', 'H.Thanh Tri']::text[]),
    ('Son Tay', 'Ha Noi', 'thi_xa', 21.1050, 105.5380, ARRAY['Son Tay', 'Thi xa Son Tay', 'TX Son Tay', 'TX. Son Tay']::text[]),
    ('Quoc Oai', 'Ha Noi', 'huyen', 21.2120, 105.5450, ARRAY['Quoc Oai', 'Huyen Quoc Oai', 'H. Quoc Oai', 'H.Quoc Oai']::text[]),
    ('Hoai Duc', 'Ha Noi', 'huyen', 21.2260, 105.6870, ARRAY['Hoai Duc', 'Huyen Hoai Duc', 'H. Hoai Duc', 'H.Hoai Duc']::text[]),
    ('Ha Dong', 'Ha Noi', 'huyen', 20.9620, 105.7660, ARRAY['Ha Dong', 'Huyen Ha Dong', 'H. Ha Dong', 'H.Ha Dong']::text[]),
    ('Chuong My', 'Ha Noi', 'huyen', 20.8580, 105.6660, ARRAY['Chuong My', 'Huyen Chuong My', 'H. Chuong My', 'H.Chuong My']::text[]),
    ('Thach That', 'Ha Noi', 'huyen', 21.3690, 105.7460, ARRAY['Thach That', 'Huyen Thach That', 'H. Thach That', 'H.Thach That']::text[]),
    ('Dan Phuong', 'Ha Noi', 'huyen', 21.3250, 105.6950, ARRAY['Dan Phuong', 'Huyen Dan Phuong', 'H. Dan Phuong', 'H.Dan Phuong']::text[]),
    ('Soc Son', 'Ha Noi', 'huyen', 21.5000, 105.9500, ARRAY['Soc Son', 'Huyen Soc Son', 'H. Soc Son', 'H.Soc Son']::text[])
ON CONFLICT (district_name) DO NOTHING;

-- 3.6. Seed bảng ngày cho khoảng +/- 5 năm
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
FROM generate_series(CURRENT_DATE - INTERVAL '5 years', CURRENT_DATE + INTERVAL '5 years', INTERVAL '1 day') d
ON CONFLICT (date_key) DO NOTHING;

-- 3.7. Seed phường/xã tiêu biểu (Dan Phuong)
INSERT INTO dim_ward (district_id, ward_name, canonical_name, alias_names)
VALUES
    (21, 'Xa Tan Hoi', 'Tân Hội', ARRAY['Tan Hoi', 'Xa Tan Hoi', 'Tân Hội', 'Xã Tân Hội']::text[])
ON CONFLICT (district_id, canonical_name) DO NOTHING;

COMMIT;