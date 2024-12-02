import xml.etree.ElementTree as ET  # Thư viện xử lý file XML.
import mysql.connector  # type: ignore # Thư viện để kết nối và thao tác với MySQL.
from mysql.connector import Error  # type: ignore # Class xử lý lỗi của mysql.connector.
import datetime  # Thư viện hỗ trợ làm việc với ngày giờ.
import sys  # Để nhận tham số từ dòng lệnh.

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
        connection = mysql.connector.connect(
            host=configDb['host'],
            port=configDb['port'],
            user=configDb['user'],
            password=configDb['password'],
            database=configDb['database']
        )
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


# Hàm gọi stored procedure LoadDataFromStagingToDW.
# sửa lại 
def CallLoadDataProcedure(connection):
    try:
        print(connection.database)
        cursor = connection.cursor()
        cursor.callproc('LoadDataFromStagingToDW')  # Gọi stored procedure
        connection.commit()
        print("Stored procedure 'LoadDataFromStagingToDW' đã được thực thi thành công.")
        return True
    except Error as e:
        print(f"Lỗi khi gọi stored procedure: {e}")
        return e
    finally:
        cursor.close()


# Work flow chính.
def Main(filePath, configId):
    # Đọc cấu hình từ file XML.
    config = ReadDatabaseConfig(filePath)
    if config:
        connection = ConnectToDatabase(config)
        if connection:
            # Lấy thông tin log_file nơi trạng thái là 'Extract_Start'.
            row = ExecuteQuery(connection, """
            SELECT *
            FROM db_control.log_file
            WHERE ( status_log = 'Transform_Complete' OR status_log = 'Load_Failed')  AND config_file_id = %s
            LIMIT 1;
            """, (configId,), fetchOne=True)
            print("hihi")

            # Nếu tìm thấy dòng có trạng thái 'Transform_Complete', gọi stored procedure.
            if row:
                
                # Cập nhật trạng thái 'Load_Start' trước khi bắt đầu ETL.
                UpdateStatus(connection, "db_control.log_file", row['index_id'], "Load_Start")
                
                # Gọi stored procedure để thực hiện ETL từ Staging vào DW.
                rs = CallLoadDataProcedure(connection)
                if rs == True:
                # Cập nhật trạng thái sau khi thực hiện xong ETL
                    UpdateStatus(connection, "db_control.log_file", row['index_id'], "Load_Complete")
                else:
                    UpdateStatus(connection, "db_control.log_file", row['index_id'], "Load_Failed")

            connection.close()


if __name__ == "__main__":
    # Nhận tham số từ dòng lệnh.
    if len(sys.argv) != 3:
        print("Cách sử dụng: python script.py <filePath> <configId>")
    else:
        filePath = sys.argv[1]  # Đường dẫn đến file config.xml
        configId = int(sys.argv[2])  # Config ID từ tham số dòng lệnh
        Main(filePath, configId)
