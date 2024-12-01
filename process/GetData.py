import xml.etree.ElementTree as ET
import mysql.connector
from mysql.connector import Error
import datetime
import os
import subprocess
import csv

import time
from datetime import datetime

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
        return None

# Kết nối đến database
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
        if result.stdout:  # Check if there is any output
            return result.stdout.strip()
        else:
            print("No output returned from the script.")
            return None
    except subprocess.CalledProcessError as error:
        print(f"Error while executing script: {error.stderr}")
        return None

# Ghi log vào database
def LogStatus(connection, configFileId):
    try:
        current_time = datetime.now().strftime('%Y-%m-%d')
        filePath = os.path.join(r"C:\ProgramData\MySQL\MySQL Server 8.0\Uploads",f'top100_{current_time}.csv')
        print(f"Đường dẫn file CSV: {filePath}")
        
        if not os.path.exists(filePath):
            print(f"File không tồn tại: {filePath}")
            return
        
        fileName = f'top100_{current_time}.csv'
        fileSize = round(os.path.getsize(filePath) / 1024, 2)  # KB
        totalRows = 100  # Số dòng cố định

        cursor = connection.cursor()
        currentTime = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
        query = """
            SET SQL_SAFE_UPDATES = 0;
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
        values = (
            configFileId, 
            fileName, 
            fileSize, 
            "Extract_Start",  # Trạng thái mặc định
            totalRows, 
            currentTime, 
            currentTime
        )
        cursor.execute(query, values)
        connection.commit()  # Đảm bảo dữ liệu được commit
        print("Log đã được ghi vào bảng log_file!")
        return True
    except Error as err:
        print(f"Lỗi khi ghi log: {err}")
    finally:
        if 'cursor' in locals():
            cursor.close()


# ====================== Quy trình chính =======================
filePath = r"D:\myStudySpace\hk7\datawarehouse\scriptETL\config.xml"
scriptPath = r"C:\ProgramData\MySQL\MySQL Server 8.0\Uploads\crawDataInZingMp3.py"
configId = 1

# 1. Đọc cấu hình từ XML
config = ReadDatabaseConfig(filePath)
if config:
    # 2. Kết nối đến database
    connection = ConnectToDatabase(config)
    if connection:
        # 3. Chạy script Python để crawl dữ liệu
        ExecutePythonScript(scriptPath)
        # 4. Ghi log kết quả
        LogStatus(connection, configId)
        connection.close()
        print("Đã đóng kết nối database.")
