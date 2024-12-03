import xml.etree.ElementTree as ET  # Thư viện xử lý file XML.
import mysql.connector  # Thư viện để kết nối và thao tác với MySQL.
from mysql.connector import Error  # Class xử lý lỗi của mysql.connector.
import datetime  # Thư viện hỗ trợ làm việc với ngày giờ.


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
        updatedAt = datetime.datetime.now()
        ExecuteQuery(connection, query, (status, updatedAt, indexId))
        connection.commit()
        print(f"Cập nhật trạng thái '{status}' cho index_id = {indexId}.")
    except Error as e:
        print(f"Lỗi khi cập nhật trạng thái: {e}")


# Hàm load dữ liệu từ Staging vào Data Warehouse.
def LoadDataIntoDW(connection, row):
    try:
        query = """
        SELECT source_folder_location, dest_table_staging
        FROM db_control.config_file
        WHERE index_id = %s;
        """
        configRow = ExecuteQuery(connection, query, (row['config_file_id'],), fetchOne=True)
        if not configRow:
            print(f"Không tìm thấy cấu hình cho config_file_id = {row['config_file_id']}")
            return
        
        # Dữ liệu load từ Staging vào Data Warehouse
        sourceTable = configRow['dest_table_staging']
        connection.database = 'db_dw'

        loadDataQuery = f"""
        INSERT INTO db_dw.top_song_fact (song_key, top, artist_name, song_name, time_get, date_dim_id, date_expired)
        SELECT A.SongKey, A.Top, A.Artist, A.SongName, A.TimeGet, A.date_dim_id, '9999-12-31'
        FROM db_staging.{sourceTable} A
        LEFT JOIN db_dw.top_song_fact B ON A.SongKey = B.SongKey
        WHERE A.Top != B.Top AND B.date_expired = '9999-12-31';
        """
        ExecuteQuery(connection, loadDataQuery)
        connection.commit()
        print(f"Dữ liệu đã được load từ Staging vào Data Warehouse.")
    except Error as e:
        print(f"Lỗi khi load dữ liệu từ Staging vào DW: {e}")


# Ghi lỗi vào file
def WriteErrorLog(errorMessage, filePath):
    try:
        with open(filePath, "a", encoding="utf-8") as errorFile:
            currentTime = datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')
            errorFile.write(f"[{currentTime}] {errorMessage}\n")
        print(f"Lỗi đã được ghi vào file: {filePath}")
    except Exception as e:
        print(f"Lỗi khi ghi lỗi vào file: {e}")


import argparse

def main(filePath, configId):
    retryCount = 0  # Đếm số lần thử
    while retryCount < 5:
        try:
            # 1. Đọc file config.xml ( db control info ) để lấy cấu hình
            config = ReadDatabaseConfig(filePath)
            if not config:
                raise ValueError("Cấu hình database không hợp lệ hoặc bị lỗi.")
            
            # 2. Kết nối db_control
            connection = mysql.connector.connect(
                host=config['host'],
                port=config['port'],
                user=config['user'],
                password=config['password'],
                database=config['database']
            )

            if connection.is_connected():
                # 3. Query db_control.log_file dòng với status = Extract_Complete , config_id
                row = ExecuteQuery(connection, """
                SELECT *
                FROM db_control.log_file
                WHERE status_log = 'Extract_Staging_Success' AND config_file_id = %s
                LIMIT 1;
                """, (configId,), fetchOne=True)

                # 4. Kiểm tra kết quả truy vấn thành công ?
                if row:
                    # 5. cập nhật status = Load_Start
                    UpdateStatus(connection, "db_control.log_file", row['index_id'], "Load_Start")

                    # 6. Load dữ liệu từ Staging vào Data Warehouse
                    try:
                        LoadDataIntoDW(connection, row)

                        # 7. cập nhật status = Load_Complete
                        UpdateStatus(connection, "db_control.log_file", row['index_id'], "Load_Complete")
                        break  # Thoát khỏi vòng lặp khi load thành công
                    except Exception as e:
                        # Ghi lỗi vào file D:\\error_LOAD_DW.txt
                        WriteErrorLog(str(e), "D:\\error_LOAD_DW.txt")
                        # 8. cập nhật status = Load_Failed
                        UpdateStatus(connection, "db_control.log_file", row['index_id'], "Load_Failed")
                        connection.close()
                        break
                else:
                    connection.close()
                    break

        except Exception as e:
            # Ghi lỗi vào file D:\\error_CONNECT_DB.txt
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
