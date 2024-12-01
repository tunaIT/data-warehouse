use db_dw;
-- Xóa toàn bộ dữ liệu trong bảng date_dim_without_quarter
-- Tắt kiểm tra khóa ngoại
SET foreign_key_checks = 0;
-- Truncate bảng
TRUNCATE TABLE date_dim;
-- Bật lại kiểm tra khóa ngoại
SET foreign_key_checks = 1;
-- Tải dữ liệu từ file CSV vào bảng
LOAD DATA INFILE 'C:\\ProgramData\\MySQL\\MySQL Server 8.0\\Uploads\\date_dim_without_quarter.csv'
INTO TABLE date_dim
FIELDS TERMINATED BY ',' 
ENCLOSED BY '"'
LINES TERMINATED BY '\n'
(
	id, 
	dates, 
	index_id, 
	month_num,
	days_of_the_week,
	month_text,
	years, 
	year_and_month, 
	day_of_the_month, 
	day_of_the_year,
	week_of_the_year_num_last_weekend,
	week_code_last_weekend,
	previous_week_date,
	week_of_the_year_num_earlier_this_week,
	week_code_last_weekend_earlier_this_week,
	first_day_of_this_week,
	holiday_status,
	weekday_or_weekend
);