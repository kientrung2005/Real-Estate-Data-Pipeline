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
    -- Urban Districts (12)
    ('Ba Đình', 'Hà Nội', 'quan', 21.0285, 105.8581, ARRAY['Ba Đình', 'Quận Ba Đình', 'Q. Ba Đình', 'Q.Ba Đình', 'Ba Dinh']::text[]),
    ('Hoàn Kiếm', 'Hà Nội', 'quan', 21.0269, 105.8541, ARRAY['Hoàn Kiếm', 'Quận Hoàn Kiếm', 'Q. Hoàn Kiếm', 'Q.Hoàn Kiếm', 'Hoan Kiem']::text[]),
    ('Đống Đa', 'Hà Nội', 'quan', 21.0216, 105.8656, ARRAY['Đống Đa', 'Quận Đống Đa', 'Q. Đống Đa', 'Q.Đống Đa', 'Dong Da']::text[]),
    ('Cầu Giấy', 'Hà Nội', 'quan', 21.0087, 105.7836, ARRAY['Cầu Giấy', 'Quận Cầu Giấy', 'Q. Cầu Giấy', 'Q.Cầu Giấy', 'Cau Giay']::text[]),
    ('Thanh Xuân', 'Hà Nội', 'quan', 21.0033, 105.8426, ARRAY['Thanh Xuân', 'Quận Thanh Xuân', 'Q. Thanh Xuân', 'Q.Thanh Xuân', 'Thanh Xuan']::text[]),
    ('Hai Bà Trưng', 'Hà Nội', 'quan', 21.0093, 105.8801, ARRAY['Hai Bà Trưng', 'Quận Hai Bà Trưng', 'Q. Hai Bà Trưng', 'Q.HBT', 'Hai Ba Trung']::text[]),
    ('Hoàng Mai', 'Hà Nội', 'quan', 20.9927, 105.8920, ARRAY['Hoàng Mai', 'Quận Hoàng Mai', 'Q. Hoàng Mai', 'Q.Hoàng Mai', 'Hoang Mai']::text[]),
    ('Long Biên', 'Hà Nội', 'quan', 21.0616, 105.8865, ARRAY['Long Biên', 'Quận Long Biên', 'Q. Long Biên', 'Q.Long Biên', 'Long Bien']::text[]),
    ('Nam Từ Liêm', 'Hà Nội', 'quan', 21.0488, 105.7584, ARRAY['Nam Từ Liêm', 'Quận Nam Từ Liêm', 'Q. Nam Từ Liêm', 'Q.Nam Từ Liêm', 'Nam Tu Liem']::text[]),
    ('Bắc Từ Liêm', 'Hà Nội', 'quan', 21.0932, 105.7737, ARRAY['Bắc Từ Liêm', 'Quận Bắc Từ Liêm', 'Q. Bắc Từ Liêm', 'Q.Bắc Từ Liêm', 'Bac Tu Liem']::text[]),
    ('Tây Hồ', 'Hà Nội', 'quan', 21.0808, 105.8142, ARRAY['Tây Hồ', 'Quận Tây Hồ', 'Q. Tây Hồ', 'Q.Tây Hồ', 'Tay Ho']::text[]),
    ('Hà Đông', 'Hà Nội', 'quan', 20.9620, 105.7660, ARRAY['Hà Đông', 'Huyện Hà Đông', 'H. Hà Đông', 'H.Hà Đông', 'Ha Dong']::text[]),
    ('Sơn Tây', 'Hà Nội', 'thi_xa', 21.1050, 105.5380, ARRAY['Sơn Tây', 'Thị xã Sơn Tây', 'TX Sơn Tây', 'TX. Sơn Tây', 'Son Tay']::text[]),
    ('Đông Anh', 'Hà Nội', 'huyen', 21.1789, 105.9330, ARRAY['Đông Anh', 'Huyện Đông Anh', 'H. Đông Anh', 'H.Đông Anh', 'Dong Anh']::text[]),
    ('Gia Lâm', 'Hà Nội', 'huyen', 21.1493, 105.9757, ARRAY['Gia Lâm', 'Huyện Gia Lâm', 'H. Gia Lâm', 'H.Gia Lâm', 'Gia Lam']::text[]),
    ('Thanh Trì', 'Hà Nội', 'huyen', 20.8860, 105.9140, ARRAY['Thanh Trì', 'Huyện Thanh Trì', 'H. Thanh Trì', 'H.Thanh Trì', 'Thanh Tri']::text[]),
    ('Sóc Sơn', 'Hà Nội', 'huyen', 21.5000, 105.9500, ARRAY['Sóc Sơn', 'Huyện Sóc Sơn', 'H. Sóc Sơn', 'H.Sóc Sơn', 'Soc Son']::text[]),
    ('Mê Linh', 'Hà Nội', 'huyen', 21.2160, 105.7190, ARRAY['Mê Linh', 'Huyện Mê Linh', 'H. Mê Linh', 'H.Mê Linh', 'Me Linh']::text[]),
    ('Quốc Oai', 'Hà Nội', 'huyen', 21.2120, 105.5450, ARRAY['Quốc Oai', 'Huyện Quốc Oai', 'H. Quốc Oai', 'H.Quốc Oai', 'Quoc Oai']::text[]),
    ('Hoài Đức', 'Hà Nội', 'huyen', 21.2260, 105.6870, ARRAY['Hoài Đức', 'Huyện Hoài Đức', 'H. Hoài Đức', 'H.Hoài Đức', 'Hoai Duc']::text[]),
    ('Chương Mỹ', 'Hà Nội', 'huyen', 20.8580, 105.6660, ARRAY['Chương Mỹ', 'Huyện Chương Mỹ', 'H. Chương Mỹ', 'H.Chương Mỹ', 'Chuong My']::text[]),
    ('Thạch Thất', 'Hà Nội', 'huyen', 21.3690, 105.7460, ARRAY['Thạch Thất', 'Huyện Thạch Thất', 'H. Thạch Thất', 'H.Thạch Thất', 'Thach That']::text[]),
    ('Đan Phượng', 'Hà Nội', 'huyen', 21.3250, 105.6950, ARRAY['Đan Phượng', 'Huyện Đan Phượng', 'H. Đan Phượng', 'H.Đan Phượng', 'Dan Phuong']::text[]),
    ('Thanh Oai', 'Hà Nội', 'huyen', 20.8870, 105.7680, ARRAY['Thanh Oai', 'Huyện Thanh Oai', 'H. Thanh Oai', 'H.Thanh Oai', 'Thanh Oai']::text[]),
    ('Thường Tín', 'Hà Nội', 'huyen', 20.8500, 105.8670, ARRAY['Thường Tín', 'Huyện Thường Tín', 'H. Thường Tín', 'H.Thường Tín', 'Thuong Tin']::text[]),
    ('Phú Xuyên', 'Hà Nội', 'huyen', 20.7300, 105.9000, ARRAY['Phú Xuyên', 'Huyện Phú Xuyên', 'H. Phú Xuyên', 'H.Phú Xuyên']::text[]),
    ('Ứng Hòa', 'Hà Nội', 'huyen', 20.7500, 105.7800, ARRAY['Ứng Hòa', 'Huyện Ứng Hòa', 'H. Ứng Hòa', 'H.Ứng Hòa']::text[]),
    ('Mỹ Đức', 'Hà Nội', 'huyen', 20.6700, 105.7500, ARRAY['Mỹ Đức', 'Huyện Mỹ Đức', 'H. Mỹ Đức', 'H.Mỹ Đức']::text[]),
    ('Ba Vì', 'Hà Nội', 'huyen', 21.2300, 105.4000, ARRAY['Ba Vì', 'Huyện Ba Vì', 'H. Ba Vì', 'H.Ba Vì']::text[]),
    ('Phúc Thọ', 'Hà Nội', 'huyen', 21.1000, 105.5800, ARRAY['Phúc Thọ', 'Huyện Phúc Thọ', 'H. Phúc Thọ', 'H.Phúc Thọ']::text[])
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

COMMIT;