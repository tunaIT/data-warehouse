-- Xóa database data_mart nếu tồn tại
DROP DATABASE IF EXISTS db_dataMart;

CREATE DATABASE IF NOT EXISTS db_dataMart;
USE db_dataMart;

-- Bảng aggregate_top_song: Lưu dữ liệu tổng hợp từ aggregate_top_song của db data warehouse
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

-- Bảng top_song: Lưu dữ liệu sẵn sàng truy vấn
CREATE TABLE top_song (
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
