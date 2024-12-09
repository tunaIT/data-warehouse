USE DB_Staging;
LOAD DATA INFILE 'C:\\ProgramData\\MySQL\\MySQL Server 8.0\\Uploads\\top100Sportify_2024-10-24.csv'
INTO TABLE Staging_Top100InSportify
FIELDS TERMINATED BY ','              
ENCLOSED BY '"'   -- giá trị cột có thể được bao quanh bởi dấu nháy kép ", thường dùng cho các giá trị dạng chuỗi.                    
LINES TERMINATED BY '\n'  -- chỉ định rằng mỗi dòng dữ liệu trong file CSV kết thúc bằng ký tự xuống dòng \n            
IGNORE 1 LINES      -- Bỏ qua dòng đầu tiên trong file CSV, vì đây thường là dòng tiêu đề chứa tên các cột.                  
(Top, Artist, SongName, @TimeGet)   -- Xác định thứ tự của các cột trong file CSV và gán chúng cho các cột tương ứng trong bảng
SET 
 -- @TimeGet là một biến tạm thời để xử lý dữ liệu thời gian. 
    TimeGet = STR_TO_DATE(@TimeGet, '%d/%m/%Y');
