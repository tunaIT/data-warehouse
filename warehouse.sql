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
-- Xóa bảng top_song nếu đã tồn tại
DROP TABLE IF EXISTS top_song;
-- Tạo bảng top_song
CREATE TABLE top_song_fact (
    ID INT PRIMARY KEY AUTO_INCREMENT,  -- ID duy nhất cho mỗi hàng dữ liệu (SK)
    SongKey INT,                        -- Tham chiếu đến khóa chính của dim_song (NK)
    Top INT,                            -- Thứ hạng của bài hát trong top 100
    TimeGet DATE,                       -- Thời gian lấy dữ liệu (năm-tháng-ngày)
    date_dim_id INT,                    -- Tham chiếu đến bảng date_dim
    date_expired DATE,                  -- Ngày bài hát hết hạn trong bảng xếp hạng
    FOREIGN KEY (date_dim_id) REFERENCES date_dim (id) ON DELETE SET NULL
);

