DELIMITER //

CREATE PROCEDURE `LoadDataFromStagingToDW`()
BEGIN
    DECLARE done INT DEFAULT 0;
    DECLARE song_id INT;
    DECLARE song_name VARCHAR(255);
    DECLARE artist_name VARCHAR(255);
    DECLARE top INT;
    DECLARE time_get DATE;
    DECLARE date_dim_id INT;
    DECLARE source VARCHAR(255);
    DECLARE config_id INT;
    DECLARE log_file_id INT;
    DECLARE row_count INT DEFAULT 0;

    -- Lấy config_id từ config_file trong DB_Control
    SELECT index_id INTO config_id 
    FROM db_control.config_file 
    WHERE name_source IN ('zingmp3', 'spotify');

    -- Bắt đầu một giao dịch
    START TRANSACTION;

    -- Log Start ETL Process
    INSERT INTO db_control.log_file (config_file_id, status_log, total_row_infile)
    VALUES (config_id, 'Extract_Start', 0);  -- Số dòng sẽ được tính sau

    -- Lấy dữ liệu từ top100_zing_daily trong db_staging và xử lý
    INSERT INTO db_dw.top_song_fact (song_key, top, artist_name, song_name, time_get, date_dim_id, date_expired)
    SELECT A.SongKey, A.Top, A.Artist, A.SongName, A.TimeGet, A.date_dim_id, '9999-12-31'
    FROM db_staging.top100_zing_daily A
    JOIN db_dw.top_song_fact B ON A.SongKey = B.SongKey
    WHERE A.Top != B.Top AND B.date_expired = '9999-12-31';

    -- Cập nhật cột date_expired trong top_song_fact
    UPDATE db_dw.top_song_fact B
    JOIN db_staging.top100_zing_daily A ON A.SongKey = B.SongKey
    SET B.date_expired = NOW()
    WHERE A.Top != B.Top AND B.date_expired = '9999-12-31';

    -- Xác định số dòng đã thay đổi
    SELECT ROW_COUNT() INTO row_count;

    -- Log Extract Complete
    INSERT INTO db_control.log_file (config_file_id, status_log, total_row_infile)
    VALUES (config_id, 'Extract_Complete', (SELECT COUNT(*) FROM db_staging.top100_zing_daily));

    -- Log Transform Start
    INSERT INTO db_control.log_file (config_file_id, status_log, total_row_infile)
    VALUES (config_id, 'Transform_Start', row_count);

    -- Cập nhật trạng thái transform
    -- (Ở đây bạn có thể thêm các bước biến đổi dữ liệu nếu cần thêm)

    -- Log Transform Complete
    INSERT INTO db_control.log_file (config_file_id, status_log, total_row_infile)
    VALUES (config_id, 'Transform_Complete', row_count);

    -- Log Load Start
    INSERT INTO db_control.log_file (config_file_id, status_log, total_row_infile)
    VALUES (config_id, 'Load_Start', row_count);

    -- Xử lý việc chuyển dữ liệu vào Data Warehouse hoàn tất
    COMMIT;

    -- Log Load Complete
    INSERT INTO db_control.log_file (config_file_id, status_log, total_row_infile)
    VALUES (config_id, 'Load_Complete', row_count);

END $$

DELIMITER ;


