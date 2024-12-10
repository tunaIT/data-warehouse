import xml.etree.ElementTree as ET  # Thư viện xử lý file XML.
import mysql.connector  # Thư viện để kết nối và thao tác với MySQL.
from mysql.connector import Error  # Class xử lý lỗi của mysql.connector.
from datetime import datetime  # Sửa đổi import để dùng trực tiếp datetime.now().
import argparse

# Hàm đọc cấu hình từ file XML.
def ReadDatabaseConfig(filePath):
    try:
        tree = ET.parse(filePath)
        root = tree.getroot()
        databaseConfig = root.find('database')
        if databaseConfig is None:
            raise ValueError("Không tìm thấy phần tử <database> trong file XML.")
        
        return {  # Trích xuất thông tin từ thẻ <database>.
            "host": databaseConfig.find('ip').text,
            "port": int(databaseConfig.find('port').text),
            "user": databaseConfig.find('username').text,
            "password": databaseConfig.find('password').text,
            "database": databaseConfig.find('dbname').text,
        }
    except Exception as e:
        print(f"Lỗi khi đọc file XML: {e}")
        return None

# Hàm kết nối đến cơ sở dữ liệu.
def ConnectToDatabase(configDb):
    try:
        connection = mysql.connector.connect(**configDb)
        if connection.is_connected():
            print("Kết nối thành công tới database!")
            return connection
    except Error as e:
        print(f"Lỗi khi kết nối tới database: {e}")
        return None

# Hàm thực thi câu truy vấn SQL.
def ExecuteQuery(connection, query, params=None, fetchOne=False):
    try:
        cursor = connection.cursor(dictionary=True)
        cursor.execute(query, params)
        return cursor.fetchone() if fetchOne else cursor.fetchall()
    except Error as e:
        print(f"Lỗi khi thực thi truy vấn: {e}")
        return None
    finally:
        cursor.close()

# Hàm cập nhật trạng thái và thời gian của một dòng.
def UpdateStatus(connection, table, indexId, status):
    try:
        query = f"""
        UPDATE {table}
        SET status_log = %s, updated_at = %s
        WHERE index_id = %s;
        """
        updatedAt = datetime.now()
        ExecuteQuery(connection, query, (status, updatedAt, indexId))
        connection.commit()
        print(f"Cập nhật trạng thái '{status}' cho index_id = {indexId}.")
    except Error as e:
        print(f"Lỗi khi cập nhật trạng thái: {e}")

# Hàm load dữ liệu từ file CSV.
def LoadDataIntoDM(connection, row):
    try:
        query = """
        SELECT dest_table_dw
        FROM db_control.config_file
        WHERE index_id = %s;
        """
        configRow = ExecuteQuery(connection, query, (row['config_file_id'],), fetchOne=True)
        if not configRow:
            print(f"Không tìm thấy cấu hình cho config_file_id = {row['config_file_id']}")
            return
        
        sourceTable = configRow['dest_table_dw']
        connection.database = 'db_dataMart'
        connection.database = 'db_dw'

        #load dữ liệu từ data warehouse qua data mart
        # Load dữ liệu từ bảng (aggregate_top_song trong DW) vào bảng tạm aggregate_top_song trong DM
        loadDataATSQuery = f"""
        INSERT INTO db_dataMart.aggregate_top_song(
            song_name,
            Artist,
            first_appearance,
            last_appearance,
            total_weeks_on_chart,
            avg_rank,
            highest_rank,
            sourceName
        )  
        SELECT 
            ats.song_name,
            ats.Artist,
            ats.first_appearance,
            ats.last_appearance,
            ats.total_weeks_on_chart,
            ats.avg_rank,
            ats.highest_rank,
            ats.sourceName
        FROM db_dw.{sourceTable} ats
        """
        ExecuteQuery(connection, loadDataATSQuery)
        connection.commit()
        print(f"Dữ liệu đã được load vào bảng aggregate_top_song từ bảng {sourceTable}.")
        
        # Kiểm tra bảng top_song có tồn tại không, nếu có thì đổi tên
        checkTableQuery = """
        SELECT COUNT(*)
        FROM information_schema.tables 
        WHERE table_schema = 'db_dataMart' AND table_name = 'top_song';
        """
        result = ExecuteQuery(connection, checkTableQuery, fetchOne=True)
        if result[0] > 0:
            renameOldBQuery = """
            RENAME TABLE top_song TO top_song_b1;
            """
            ExecuteQuery(connection, renameOldBQuery)
            connection.commit()
            print("Bảng top_song đã được đổi tên thành top_song_b1.")
        
        # Đổi tên bảng aggregate_top_song thành top_song
        renameTempBQuery = """
        RENAME TABLE aggregate_top_song TO top_song;
        """
        ExecuteQuery(connection, renameTempBQuery)
        connection.commit()
        print("Bảng aggregate_top_song đã được đổi tên thành top_song.")
        
    except Error as e:
        print(f"Lỗi khi load dữ liệu: {e}")

# Ghi lỗi vào file
def WriteErrorLog(errorMessage, filePath):
    try:
        with open(filePath, "a", encoding="utf-8") as errorFile:
            currentTime = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
            errorFile.write(f"[{currentTime}] {errorMessage}\n")
        print(f"Lỗi đã được ghi vào file: {filePath}")
    except Exception as e:
        print(f"Lỗi khi ghi lỗi vào file: {e}")


def main(filePath, configId):
    retryCount = 0  # Đếm số lần thử
    while retryCount < 5:
        try:
            # 1. Đọc file config.xml ( db control info ) để lấy cấu hình
            config = ReadDatabaseConfig(filePath)
            if not config:
                raise ValueError("Cấu hình database không hợp lệ hoặc bị lỗi.")
            # 2. Kết nối db_control
            # 3. Kiểm tra kết nối cơ sở dữ liệu?
            try:
                connection = mysql.connector.connect(
                    host=config['host'],
                    port=config['port'],
                    user=config['user'],
                    password=config['password'],
                    database=config['database']
                )
                if connection.is_connected():
                     # 4. Query db_control.log_file dòng với status = Extract_Complete , config_id
                    row = ExecuteQuery(connection, """
                    SELECT *
                    FROM log_file
                    WHERE (status_log = 'Load_Complete' OR status_log = 'Load_Failed') AND config_file_id = %s
                    LIMIT 1;
                    """, (configId,), fetchOne=True)
                    # 5. Kiểm tra kết quả truy vấn thành công ?
                    if row:
                        # 6. cập nhật status = Load_Start
                        UpdateStatus(connection, "db_control.log_file", row['index_id'], "Extract_DataMart_Start")
                        # 7. Đọc dữ liệu từ file vào db_dataMart
                        # 8. Kiểm tra load dữ liệu thành công ?
                        try:
                            LoadDataIntoDM(connection, row)
                            # 9. cập nhật status = Load_Complete khi load vào dataMart thành công
                            UpdateStatus(connection, "db_control.log_file", row['index_id'], "Extract_DataMart_Success")
                            break
                        except Exception as e:
                            # Ghi lỗi vào file D:\\error_LOAD_DM.txt
                            WriteErrorLog(str(e),"D:\\error_LOAD_DM.txt")
                            # 8.1. cập nhật status = Load_Failed khi load vào dataMart thất bại
                            UpdateStatus(connection, "db_control.log_file", row['index_id'], "Extract_DataMart_Failed")
                            connection.close()
                            break
                    else:
                        connection.close()
                        break
            except Exception as e:
                # Ghi lỗi vào file D:\\error_CONNECT_DB
                WriteErrorLog(str(e), "D:\\error_CONNECT_DB.txt")
                # 3.1. Kiểm tra số lần lặp có hơn 5 lần không ? 
                retryCount += 1
                if retryCount > 5:
                    break
                print(f"Thử lại lần thứ {retryCount}...")
        finally:
            if 'connection' in locals() and connection.is_connected():
                connection.close()
                print("Đã đóng kết nối database.")

if __name__ == "__main__":
    # Khởi tạo trình phân tích tham số
    parser = argparse.ArgumentParser(description="Script xử lý dữ liệu với tham số từ file config và configId.")
    parser.add_argument("filePath", type=str, help="Đường dẫn tới file cấu hình (config.xml).")
    parser.add_argument("configId", type=int, help="ID cấu hình trong database.")
    
    # Phân tích tham số
    args = parser.parse_args()
    
    # Gọi hàm main với các tham số
    main(args.filePath, args.configId)
