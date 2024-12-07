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
def LoadDataIntoDW(connection, row):
    try:
        query = """
        SELECT dest_table_staging
        FROM db_control.config_file
        WHERE index_id = %s;
        """
        configRow = ExecuteQuery(connection, query, (row['config_file_id'],), fetchOne=True)
        if not configRow:
            print(f"Không tìm thấy cấu hình cho config_file_id = {row['config_file_id']}")
            return
        
        sourceTable = configRow['dest_table_staging']
        connection.database = 'db_staging'
        connection.database = 'db_dw'

        #load dữ liệu từ staging qua data warehouse
        loadDataDimSongQuery = f"""
        INSERT INTO dim_song (song_name, Artist)
        SELECT DISTINCT t.SongName, t.Artist
        FROM db_staging.{sourceTable} t
        WHERE NOT EXISTS (
            SELECT 1 FROM dim_song d
            WHERE d.song_name = t.SongName AND d.Artist = t.Artist
        );
        """
        ExecuteQuery(connection, loadDataDimSongQuery)
        print(f"Dữ liệu đã được chèn vào bảng dim_song từ bảng {sourceTable}.")
        loadDataTopSongFactQuery = f"""
        INSERT INTO top_song_fact (song_key, top, time_get, date_dim_id)
        SELECT
            d.song_id,
            t.Top,
            t.TimeGet,
            t.date_dim_id
        FROM db_staging.{sourceTable} t
        INNER JOIN dim_song d
            ON d.song_name = t.SongName AND d.Artist = t.Artist
        WHERE NOT EXISTS (
            SELECT 1 FROM top_song_fact dw
            WHERE dw.time_get = t.TimeGet AND dw.song_key = d.song_id
        );
        """
        ExecuteQuery(connection, loadDataTopSongFactQuery)
        connection.commit()
        print(f"Dữ liệu đã được load vào bảng top_song_fact từ bảng {sourceTable}.")
        
        # Xóa dữ liệu trong bảng staging 
        deleteQuery = f"DELETE FROM db_staging.{sourceTable};"
        ExecuteQuery(connection, deleteQuery)
        connection.commit()
        print(f"Dữ liệu đã được xóa khỏi bảng {sourceTable}.")
    except Error as e:
        print(f"Lỗi khi load dữ liệu: {e}")

# # Hàm thêm dữ liệu vào `date_dim`.
# def AddValueDateDim(connection, row):
#     try:
#         query = """
#         SELECT dest_table_dw
#         FROM db_control.config_file
#         WHERE index_id = %s;
#         """
#         configRow = ExecuteQuery(connection, query, (row['config_file_id'],), fetchOne=True)
#         if not configRow:
#             print(f"Không tìm thấy cấu hình cho config_file_id = {row['config_file_id']}")
#             return

#         connection.database = 'db_dw'
#         updateQuery = f"""
#         UPDATE {configRow['dest_table_dw']} t
#         INNER JOIN date_dim d ON t.TimeGet = d.dates
#         SET t.date_dim_id = d.id;
#         """
#         ExecuteQuery(connection, updateQuery)
#         connection.commit()
#         print(f"Transform thành công cho bảng: {configRow['dest_table_dw']}")
#     except Error as e:
#         print(f"Lỗi khi transform date: {e}")

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
                    WHERE (status_log = 'Transform_Complete' OR status_log = 'Transform_Failed') AND config_file_id = %s
                    LIMIT 1;
                    """, (configId,), fetchOne=True)
                    # 5. Kiểm tra kết quả truy vấn thành công ?
                    if row:
                        # 6. cập nhật status = Load_Start
                        UpdateStatus(connection, "db_control.log_file", row['index_id'], "Load_Start")
                        # 7. Đọc dữ liệu từ file vào db_dw
                        # 8. Kiểm tra load dữ liệu thành công ?
                        try:
                            LoadDataIntoDW(connection, row)
                            # 9. cập nhật status = Load_Complete khi load vào dw thành công
                            UpdateStatus(connection, "db_control.log_file", row['index_id'], "Load_Complete")
                            break
                        except Exception as e:
                            # Ghi lỗi vào file D:\\error_LOAD_STAGING.txt
                            WriteErrorLog(str(e),"D:\\error_LOAD_STAGING.txt")
                            # 8.1. cập nhật status = Load_Failed khi load vào dw thất bại
                            UpdateStatus(connection, "db_control.log_file", row['index_id'], "Load_Failed")
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
