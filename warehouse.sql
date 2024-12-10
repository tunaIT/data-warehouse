-- Xóa database db_control nếu tồn tại
DROP DATABASE IF EXISTS db_dw;

CREATE DATABASE IF NOT EXISTS db_dw; 
USE db_dw;

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
-- Xóa bảng dim_song nếu đã tồn tại
DROP TABLE IF EXISTS dim_song;
-- tạo bảng dim_song (thông tin bài hát)
CREATE TABLE IF NOT EXISTS dim_song (
    song_id INT PRIMARY KEY AUTO_INCREMENT,  -- Khóa chính
    song_name VARCHAR(255) NULL,         -- Tên bài hát
    Artist VARCHAR(255) NULL       -- Tên nghệ sĩ hoặc ban nhạc
);

-- Xóa bảng top_song_fact nếu đã tồn tại
DROP TABLE IF EXISTS top_song_fact;
-- Tạo bảng top_song_fact (thứ hạng bài hát)
CREATE TABLE top_song_fact (
    id INT PRIMARY KEY AUTO_INCREMENT,
    song_key INT NULL,                     -- Tham chiếu đến dim_song
    top INT,                                   -- Thứ hạng của bài hát
    time_get DATE NULL,                    -- Ngày lấy dữ liệu
    created_at DATETIME DEFAULT CURRENT_TIMESTAMP,
    date_dim_id INT NULL,                  -- Tham chiếu đến dim_date
    FOREIGN KEY (song_key) REFERENCES dim_song(song_id) ON DELETE SET NULL,
    FOREIGN KEY (date_dim_id) REFERENCES date_dim(id) ON DELETE SET NULL
);

-- Xóa bảng aggregate_top_song nếu đã tồn tại
DROP TABLE IF EXISTS aggregate_top_song;

-- Tạo bảng aggregate_top_song (thống kê tổng hợp bài hát)
CREATE TABLE aggregate_top_song (
    id INT PRIMARY KEY AUTO_INCREMENT,        -- Khóa chính
    song_name VARCHAR(255) NULL,              -- tên bài hát
    Artist VARCHAR(255) NULL, 				  -- tên ca sĩ
    first_appearance DATE NOT NULL,           -- Ngày xuất hiện đầu tiên
    last_appearance DATE NOT NULL,            -- Ngày xuất hiện gần nhất
    total_weeks_on_chart INT DEFAULT 0,       -- Tổng số tuần xuất hiện trong bảng xếp hạng
    avg_rank FLOAT,                           -- Thứ hạng trung bình
    highest_rank INT,                         -- Thứ hạng cao nhất
    sourceName VARCHAR(255) NULL              -- nguồn
);

