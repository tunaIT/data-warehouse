-- Xóa database db_control nếu tồn tại
DROP DATABASE IF EXISTS db_control;

CREATE DATABASE IF NOT EXISTS db_control; 
USE db_control;

CREATE TABLE IF NOT EXISTS config_file (
    index_id INT AUTO_INCREMENT PRIMARY KEY COMMENT 'ID duy nhất cho mỗi bản ghi cấu hình tệp',
    name_source VARCHAR(255) NOT NULL UNIQUE COMMENT 'Tên mô tả của cửa hàng laptop',
    source_web VARCHAR(255) NOT NULL UNIQUE COMMENT 'URL nguồn hoặc đường dẫn đến trang web của cửa hàng',
    source_folder_location VARCHAR(255) NOT NULL COMMENT 'Đường dẫn thư mục chứa tệp được crawl từ cửa hàng',
    dest_table_staging VARCHAR(255) NOT NULL COMMENT 'Tên bảng sẽ chứa dữ liệu được đổ vào của Database STAGING_LAPTOP',
    dest_table_dw VARCHAR(255) NOT NULL COMMENT 'Tên bảng đích trong kho dữ liệu (Data Warehouse)'
);

/*
Tạo bảng date_dim_without_quarter để lưu giá trị các mốc thời gian
*/
CREATE TABLE IF NOT EXISTS log_file (
    index_id INT AUTO_INCREMENT PRIMARY KEY COMMENT 'ID duy nhất cho mỗi bản ghi log của tệp',
    config_file_id INT NOT NULL COMMENT 'ID tham chiếu tới tệp cấu hình trong bảng config_file',
    file_name VARCHAR(255) COMMENT 'Tên của tệp được tải lên',
    file_size VARCHAR(255) COMMENT 'Kích thước của tệp tải lên (dạng chuỗi để dễ đọc, ví dụ: "10 MB")',
    status_log ENUM(
        'Extract_Start',      -- Khi bắt đầu thu thập dữ liệu
        'Extract_Complete',   -- Khi thu thập dữ liệu hoàn thành
        'Extract_Failed',     -- Khi thu thập dữ liệu bị lỗi
        'Transform_Start',    -- Khi bắt đầu chuyển đổi dữ liệu
        'Transform_Complete', -- Khi chuyển đổi dữ liệu hoàn thành
        'Transform_Failed',   -- Khi chuyển đổi dữ liệu bị lỗi
        'Load_Start',         -- Khi bắt đầu tải dữ liệu vào DW
        'Load_Complete',      -- Khi tải dữ liệu vào DW hoàn thành
        'Load_Failed'  		  -- Khi tải dữ liệu vào DW hoàn thành
    ) NOT NULL,
    total_row_infile BIGINT COMMENT 'Tổng số dòng dữ liệu đang có trong file',
    created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP COMMENT 'Thời gian tạo bản ghi log',
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP COMMENT 'Thời gian cập nhật gần nhất của bản ghi log',
    FOREIGN KEY (config_file_id) REFERENCES config_file (index_id) ON DELETE CASCADE
);

INSERT INTO config_file (
    index_id, 
    name_source, 
    source_web, 
    source_folder_location, 
    dest_table_staging, 
    dest_table_dw
) 
VALUES (
    1, 
    'zingmp3', 
    'https://zingmp3.vn/album/Top-100-Nhac-Rap-Viet-Nam-Hay-Nhat-HIEUTHUHAI-Rhyder-Bray-Double2T/ZWZB96AI.html', 
    'C:\\\\ProgramData\\\\MySQL\\\\MySQL Server 8.0\\\\Uploads\\\\', 
    'top100_zing_daily', 
    'top_song_fact'
);
INSERT INTO config_file (
    index_id, 
    name_source, 
    source_web, 
    source_folder_location, 
    dest_table_staging, 
    dest_table_dw
) 
VALUES (
    2, 
    'spotify', 
    'https://open.spotify.com/playlist/5ABHKGoOzxkaa28ttQV9sE', 
    'C:\\\\ProgramData\\\\MySQL\\\\MySQL Server 8.0\\\\Uploads\\\\', 
    'top100_sportify_daily', 
    'top_song_fact'
);

