import xml.etree.ElementTree as ET
import mysql.connector
from mysql.connector import Error
import os
import subprocess
from datetime import datetime
import sys  # Thêm import sys để xử lý đối số dòng lệnh

# Đọc cấu hình từ file XML
def ReadDatabaseConfig(filePath):
    try:
        tree = ET.parse(filePath)
        root = tree.getroot()
        databaseConfig = root.find('database')
        if databaseConfig is None:
            raise ValueError("Không tìm thấy phần tử <database> trong file XML.")
        
        config = {
            "host": databaseConfig.find('ip').text,
            "port": int(databaseConfig.find('port').text),
            "user": databaseConfig.find('username').text,
            "password": databaseConfig.find('password').text,
            "database": databaseConfig.find('dbname').text,
        }
        return config
    except Exception as e:
        print(f"Lỗi khi đọc file XML: {e}")
        WriteErrorLog(str(e))
        return None

# Chạy script Python khác và lấy kết quả
def ExecutePythonScript(scriptPath):
    try:
        result = subprocess.run(
            ["python", scriptPath],
            check=True,
            capture_output=True,
            text=True,
            encoding='utf-8'  # Use utf-8 encoding to avoid unicode errors
        )
        print("Script executed successfully!")
        return result
    except subprocess.CalledProcessError as error:
        print(f"Error while executing script: {error.stderr}")
        WriteErrorLog(error.stderr)
        return None

# Ghi log vào database
def LogStatus(connection, configFileId, status):
    try:
        current_time = datetime.now().strftime('%Y-%m-%d')
        filePath = os.path.join(r"C:\ProgramData\MySQL\MySQL Server 8.0\Uploads", f'top100_{current_time}.csv')
        print(f"Đường dẫn file CSV: {filePath}")
        
        fileName = f'top100_{current_time}.csv'
        fileSize = round(os.path.getsize(filePath) / 1024, 2) if os.path.exists(filePath) else 0
        totalRows = 100  # Số dòng cố định
        currentTime = datetime.now().strftime('%Y-%m-%d %H:%M:%S')

        cursor = connection.cursor()
        query = """
            INSERT INTO log_file (
                config_file_id, 
                file_name, 
                file_size, 
                status_log, 
                total_row_infile, 
                created_at, 
                updated_at
            )
            VALUES (%s, %s, %s, %s, %s, %s, %s)
        """
        values = (configFileId, fileName, fileSize, status, totalRows, currentTime, currentTime)
        cursor.execute(query, values)
        connection.commit()
        print("Log đã được ghi vào bảng log_file!")
        return True
    except Error as err:
        print(f"Lỗi khi ghi log: {err}")
        WriteErrorLog(str(err))
        return False
    finally:
        if 'cursor' in locals():
            cursor.close()

# Ghi lỗi vào file
def WriteErrorLog(errorMessage, filePath="D:\\error_CONNECT_DB.txt"):
    try:
        with open(filePath, "a", encoding="utf-8") as errorFile:
            currentTime = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
            errorFile.write(f"[{currentTime}] {errorMessage}\n")
        print(f"Lỗi đã được ghi vào file: {filePath}")
    except Exception as e:
        print(f"Lỗi khi ghi lỗi vào file: {e}")
        
def SetStatus(connection, configFileId, status):
    try:
        cursor = connection.cursor()
        query = """
            UPDATE log_file
            SET status_log = %s, updated_at = NOW()
            WHERE config_file_id = %s
            AND status_log = 'Extract_Start'
        """
        values = (status, configFileId)
        cursor.execute(query, values)
        connection.commit()
        print(f"Trạng thái đã được cập nhật thành {status}!")
        return True
    except Error as err:
        print(f"Lỗi khi cập nhật trạng thái: {err}")
        WriteErrorLog(str(err))
        return False
    finally:
        if 'cursor' in locals():
            cursor.close()

# ====================== Quy trình chính =======================
if len(sys.argv) < 3:
    print("Vui lòng nhập đầy đủ đối số: file config.xml và configId.")
    sys.exit(1)

# Lấy đường dẫn file config và configId từ dòng lệnh
filePath = sys.argv[1]
configId = int(sys.argv[2])  # Chuyển configId từ chuỗi sang int

scriptPath = r"C:\ProgramData\MySQL\MySQL Server 8.0\Uploads\crawDataInZingMp3.py"
retryLimit = 5  # Giới hạn số lần thử lại
retryCount = 0  # Đếm số lần thử

while retryCount < retryLimit:
    try:
        # 1. Đọc file config.xml ( db control info ) để lấy cấu hình
        config = ReadDatabaseConfig(filePath)
        if not config:
            raise ValueError("Cấu hình database không hợp lệ hoặc bị lỗi.")
        # 2. Kết nối db_control
        try:
            connection = mysql.connector.connect(
                host=config['host'],
                port=config['port'],
                user=config['user'],
                password=config['password'],
                database=config['database']
            )
            # 3. Kiểm tra kết nối cơ sở dữ liệu?
            if connection.is_connected():
                # 4. Ghi log vào db_control.log_file 
                LogStatus(connection, configId, "Extract_Start")
                # 5. Lấy dữ liệu từ source zingmp3 
                result = ExecutePythonScript(scriptPath)
                # 6. Kiểm tra kết quả lấy dữ liệu thành công?
                if  result.stdout:  # dữ liệu đầu ra có dl
                    # 7. Cập nhật log trong db_control.log_file
                    SetStatus(connection, configId, "Extract_Complete")
                    break  # Thành công -> thoát vòng lặp
                else:
                    # 6.1. Cập nhật log trong db_control.log_file
                    SetStatus(connection, configId, "Extract_Failed")
                    retryCount += 1
                    # 6.2. Kiểm tra số lần lặp có hơn 5 lần không? 
                    if retryCount > 5:
                        break
                    print(f"Thử lại lần thứ {retryCount}...")
        except Exception as e:
            # Ghi lỗi vào file D:\\error_CONNECT_DB
            WriteErrorLog(str(e))
            # 3.1. Kiểm tra số lần lặp có hơn 5 lần không ? 
            retryCount += 1
            if retryCount > 5:
                break
            print(f"Thử lại lần thứ {retryCount}...")
    finally:
        if 'connection' in locals() and connection.is_connected():
            connection.close()
            print("Đã đóng kết nối database.")
