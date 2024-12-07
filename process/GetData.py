import xml.etree.ElementTree as ET
import mysql.connector
from mysql.connector import Error
import os
import subprocess
from datetime import datetime
import sys  # Thêm import sys để xử lý đối số dòng lệnh
from selenium import webdriver
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.common.by import By
import pandas as pd
import os
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
import time
from datetime import datetime
import sys

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
    sys.stdout.reconfigure(encoding='utf-8')

    # Cài đặt trình điều khiển Coc Coc
    options = webdriver.ChromeOptions()
    options.binary_location = 'C:\\Program Files\\Google\\Chrome\\Application\\chrome.exe'
    options.add_argument('--no-sandbox')
    options.add_argument('--disable-dev-shm-usage')
    options.add_experimental_option("excludeSwitches", ["enable-automation"])
    options.add_experimental_option('useAutomationExtension', False)

    # Đường dẫn đến ChromeDriver
    driver = webdriver.Chrome(service=Service("D:\\Download\\chromedriver-win64\\chromedriver.exe"), options=options)

    # URL cần truy cập
    url = 'https://zingmp3.vn/album/Top-100-Nhac-Rap-Viet-Nam-Hay-Nhat-HIEUTHUHAI-Rhyder-Bray-Double2T/ZWZB96AI.html'
    driver.get(url)
    time.sleep(15)  # Chờ trang load ban đầu

    # Lấy thông tin bài hát
    products = driver.find_elements(By.CSS_SELECTOR, 'div.card-info')
    data = []
    count = 1

    for product in products:
        try:
            # Lấy tên bài hát
            title_element = product.find_element(By.CSS_SELECTOR, 'h3.is-one-line.is-truncate.subtitle')
            # Lấy nghệ sĩ
            links = product.find_elements(By.CSS_SELECTOR, 'div.title-wrapper span.item-title span')
            link_texts = [link.text for link in links]
            name = title_element.text
            current_time = datetime.now().strftime('%Y/%m/%d')

            data.append([count, name, link_texts, current_time])
            count += 1
            
        except Exception as e:
            print(f"Error retrieving product information: {e}")
            print(product.get_attribute('outerHTML'))  # Print HTML nếu có lỗi

    # Tạo DataFrame
    df = pd.DataFrame(data, columns=['Top', 'Song Name', 'Artist', 'Time Get'])
    
    # Đặt tên file
    current_time = datetime.now().strftime('%Y-%m-%d')
    filename = os.path.join(r'C:\ProgramData\MySQL\MySQL Server 8.0\Uploads', f'top100_{current_time}.csv')  # Đường dẫn đầy đủ
    
    # Lưu file CSV
    df.to_csv(filename, index=False, encoding='utf-8-sig')
    driver.quit()
    
    return True  # Trả về đường dẫn file CSV

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
                if result:  # dữ liệu đầu ra có dl
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
