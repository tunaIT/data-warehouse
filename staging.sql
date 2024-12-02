-- Xóa database DB_Staging nếu tồn tại
DROP DATABASE IF EXISTS db_staging;

-- Tạo database DB_Staging
CREATE DATABASE IF NOT EXISTS db_staging;

-- Sử dụng database DB_Staging
USE db_staging;

-- Tạo bảng date_dim_without_quarter để lưu giá trị các mốc thời gian
CREATE TABLE IF NOT EXISTS date_dim (
    id INT PRIMARY KEY,
    dates DATE,
    index_id INT,
    month_num INT,
    days_of_the_week VARCHAR(255),
    month_text VARCHAR(255),
    years INT,
    year_and_month VARCHAR(255),
    day_of_the_month INT,
    day_of_the_year INT,
    week_of_the_year_num_last_weekend INT,
    week_code_last_weekend VARCHAR(255),
    previous_week_date DATE,
    week_of_the_year_num_earlier_this_week INT,
    week_code_last_weekend_earlier_this_week VARCHAR(255),
    first_day_of_this_week DATE,
    holiday_status VARCHAR(255),
    weekday_or_weekend VARCHAR(255)
);
-- Xóa bảng top100_zing_daily nếu đã tồn tại
DROP TABLE IF EXISTS top100_zing_daily;

-- Tạo lại bảng top100_zing_daily
CREATE TABLE top100_zing_daily (
    ID INT PRIMARY KEY AUTO_INCREMENT,  -- ID duy nhất cho mỗi hàng dữ liệu
    Top INT,                            -- Thứ hạng của bài hát trong top 100
	Artist TEXT NOT NULL ,                    -- Tên nghệ sĩ hoặc ban nhạc
	SongName TEXT NOT NULL,                  -- Tên bài hát
    TimeGet DATE,                       -- Thời gian lấy dữ liệu (năm-tháng-ngày)
    date_dim_id INT,                    -- Tham chiếu đến bảng date_dim
    FOREIGN KEY (date_dim_id) REFERENCES date_dim (id) ON DELETE SET NULL
);

-- Xóa bảng top100_sportify_daily nếu đã tồn tại
DROP TABLE IF EXISTS top100_sportify_daily;

-- Tạo lại bảng top100_sportify_daily
CREATE TABLE top100_sportify_daily (
    ID INT PRIMARY KEY AUTO_INCREMENT,  -- ID duy nhất cho mỗi hàng dữ liệu
    Top INT,                            -- Thứ hạng của bài hát trong top 100
	Artist TEXT NOT NULL ,                    -- Tên nghệ sĩ hoặc ban nhạc
	SongName TEXT NOT NULL,                  -- Tên bài hát
    TimeGet DATE,                       -- Thời gian lấy dữ liệu (năm-tháng-ngày)
    date_dim_id INT,                    -- Tham chiếu đến bảng date_dim
    FOREIGN KEY (date_dim_id) REFERENCES date_dim (id) ON DELETE SET NULL
);
