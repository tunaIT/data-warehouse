import xml.etree.ElementTree as ET
import mysql.connector
from mysql.connector import Error
import datetime

# Hàm đọc cấu hình từ file XML
def ReadDatabaseConfig(filePath):
    try:
        # Parse file XML
        tree = ET.parse(filePath)
        root = tree.getroot()
        
        # Tìm phần tử <database>
        databaseConfig = root.find('database')
        if databaseConfig is None:
            raise ValueError("Không tìm thấy phần tử <database> trong file XML.")
        
        # Lấy thông tin từ các thẻ con
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

# Hàm kết nối cơ sở dữ liệu
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

def QueryRowER(connection, configId):
    try:
        # Tạo con trỏ truy vấn
        cursor = connection.cursor(dictionary=True)

        # Câu lệnh truy vấn
        query = """
        SELECT *
        FROM log_file
        WHERE status_log = 'ER' AND config_file_id = %s
        LIMIT 1;
        """
        cursor.execute(query, (configId,))  # Truyền giá trị config_file_id
        # Lấy dòng kết quả
        row = cursor.fetchone()
        
        return row
    except Error as e:
        print(f"Lỗi khi truy vấn database: {e}")
        return None
    finally:
        cursor.close()

# Cập nhật trạng thái của dòng trong log_file
def SetStatus(connection, row):
    try:
        cursor = connection.cursor()

        # Câu lệnh cập nhật trạng thái (bảng này nằm trong db_control)
        query = """
        UPDATE db_control.log_file
        SET status_log = 'PS', updated_at = %s
        WHERE index_id = %s;
        """
        # Tham số truyền vào (cập nhật thời gian hiện tại)
        updated_at = datetime.datetime.now()
        cursor.execute(query, (updated_at, row['index_id']))
        connection.commit()

        print(f"Cập nhật trạng thái thành 'PS' cho index_id = {row['index_id']}.")
        cursor.close()
    except Error as e:
        print(f"Lỗi khi cập nhật trạng thái: {e}")

# Load dữ liệu vào bảng staging từ file CSV
def LoadDataIntoStaging(connection, row):
    try:
        # Truy vấn thông tin từ bảng config_file (bảng này nằm trong db_control)
        cursor = connection.cursor(dictionary=True)
        query = """
        SELECT source_folder_location, dest_table_staging
        FROM db_control.config_file
        WHERE index_id = %s;
        """
        cursor.execute(query, (row['config_file_id'],))
        config_row = cursor.fetchone()

        if not config_row:
            print(f"Không tìm thấy thông tin config_file cho config_file_id = {row['config_file_id']}")
            return
        
        # Tạo đường dẫn đầy đủ cho file CSV
        file_path = config_row['source_folder_location'] + row['file_name']
        print(file_path)
        dest_table = config_row['dest_table_staging']
        print(dest_table)
        # Đảm bảo kết nối đúng với db_staging khi load dữ liệu vào bảng top100_zing_daily
        connection.database = 'db_staging'  # Chỉ định đúng database cho bảng top100_zing_daily

        # Tạo câu lệnh SQL động
        load_data_query = f"""
        LOAD DATA INFILE '{file_path}'
        INTO TABLE {dest_table}
        FIELDS TERMINATED BY ',' 
        ENCLOSED BY '"' 
        LINES TERMINATED BY '\\n' 
        IGNORE 1 LINES
        (Top, Artist, @SongName, @TimeGet)
        SET 
            SongName = TRIM(BOTH '''' FROM SUBSTRING_INDEX(SUBSTRING_INDEX(@SongName, ',', 1), '[', -1)),
            TimeGet = STR_TO_DATE(@TimeGet, '%Y/%m/%d');
        """
        
        # Thực thi câu lệnh SQL
        cursor.execute(load_data_query)
        connection.commit()
        print(f"Dữ liệu đã được load vào bảng {dest_table} từ file {file_path}")

        cursor.close()
    except Error as e:
        print(f"Lỗi khi thực thi LOAD DATA INFILE: {e}")

# Code - Work flow
# 1. Load init config.xml, configId = 1 (source: zingmp3)
filePath = r"D:\myStudySpace\hk7\datawarehouse\scriptETL\config.xml"
configId = 2

config = ReadDatabaseConfig(filePath)
if config:
    # 2. Kết nối db_control
    connection = ConnectToDatabase(config)
    if connection:
        # 3. Query db_control.file_log 1 dòng với status = ER , config_id = 1
        row = QueryRowER(connection, configId)
        if row:
            # 4. Cập nhật status = PS
            SetStatus(connection, row)
            # # 6. Transform date to date_id
            # GetDateId(connection, row)
            # 5. Đọc dữ liệu từ file vào db_staging.top100_zing_daily
            LoadDataIntoStaging(connection, row)

        # Đảm bảo đóng kết nối sau khi hoàn thành công việc
        connection.close()
