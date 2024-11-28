USE DB_Staging;
LOAD DATA INFILE 'C:\\ProgramData\\MySQL\\MySQL Server 8.0\\Uploads\\top100_2024-10-24.csv'
INTO TABLE top100_zing_daily 
FIELDS TERMINATED BY ','              
ENCLOSED BY '"'                       
LINES TERMINATED BY '\n'              
IGNORE 1 LINES                        
(Top, Artist, @SongName, @TimeGet)    
SET 
    SongName = TRIM(BOTH '''' FROM SUBSTRING_INDEX(SUBSTRING_INDEX(@SongName, ',', 1), '[', -1)),
    TimeGet = STR_TO_DATE(@TimeGet, '%Y/%m/%d');
