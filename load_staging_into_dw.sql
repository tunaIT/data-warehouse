INSERT INTO db_dw.top_song_fact (SongKey, Top, Artist, SongName, TimeGet, date_dim_id, date_expired)
SELECT A.SongKey, A.Top, A.Artist, A.SongName, A.TimeGet, A.date_dim_id, "9999-12-31" AS date_expired
FROM db_staging.top100_zing_daily A
LEFT JOIN db_dw.top_song_fact B ON A.SongKey = B.SongKey
WHERE B.SongKey IS NULL;
-- SET SQL_SAFE_UPDATES = 0;
DELETE FROM db_staging.top100_zing_daily;
-- DELETE FROM db_dw.top_song_fact;

-- Bắt đầu một giao dịch
START TRANSACTION;

-- Bước 1: Insert các dòng từ top100_zing_daily vào top_song_fact với điều kiện songkey đã tồn tại và Top khác
INSERT INTO db_dw.top_song_fact (SongKey, Top, Artist, SongName, TimeGet, date_dim_id, date_expired)
SELECT A.SongKey, A.Top, A.Artist, A.SongName, A.TimeGet, A.date_dim_id, '9999-12-31'
FROM db_staging.top100_zing_daily A
JOIN db_dw.top_song_fact B ON A.SongKey = B.SongKey
WHERE A.Top != B.Top and B.date_expired = '9999-12-31';
-- Bước 2: Cập nhật cột date_expired trong top_song_fact cho các dòng có songkey trùng và date_expired = "9999-12-31"
UPDATE db_dw.top_song_fact B
JOIN db_staging.top100_zing_daily A ON A.SongKey = B.SongKey
SET B.date_expired = NOW()
WHERE A.Top != B.Top and B.date_expired = '9999-12-31';
-- Kết thúc giao dịch
COMMIT;

