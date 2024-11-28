import xml.etree.ElementTree as ET  # Thư viện xử lý file XML.
import mysql.connector  # Thư viện để kết nối và thao tác với MySQL.
from mysql.connector import Error  # Class xử lý lỗi của mysql.connector.
import datetime  # Thư viện hỗ trợ làm việc với ngày giờ.

# Hàm đọc cấu hình từ file XML.
def ReadDatabaseConfig(filePath):
    try:
        # Parse file XML để lấy dữ liệu.
        tree = ET.parse(filePath)
        root = tree.getroot()
        
        # Tìm phần tử <database> trong file XML.
        databaseConfig = root.find('database')
        if databaseConfig is None:
            # Nếu không tìm thấy thẻ <database>, báo lỗi.
            raise ValueError("Không tìm thấy phần tử <database> trong file XML.")
        
        # Trích xuất thông tin từ các thẻ con bên trong <database>.
        config = {
            "host": databaseConfig.find('ip').text,  # Địa chỉ IP database.
            "port": int(databaseConfig.find('port').text),  # Cổng kết nối.
            "user": databaseConfig.find('username').text,  # Tên người dùng.
            "password": databaseConfig.find('password').text,  # Mật khẩu.
            "database": databaseConfig.find('dbname').text,  # Tên database.
        }
        
        return config  # Trả về dictionary chứa cấu hình.
    except Exception as e:
        # In ra lỗi nếu xảy ra lỗi trong quá trình đọc file.
        print(f"Lỗi khi đọc file XML: {e}")
        return None

# Hàm kết nối cơ sở dữ liệu.
def ConnectToDatabase(configDb):
    try:
        # Sử dụng thông tin cấu hình để kết nối đến MySQL.
        connection = mysql.connector.connect(
            host=configDb['host'],
            port=configDb['port'],
            user=configDb['user'],
            password=configDb['password'],
            database=configDb['database']
        )
        if connection.is_connected():
            # Nếu kết nối thành công, in thông báo.
            print("Kết nối thành công tới database!")
            return connection
    except Error as e:
        # Nếu có lỗi trong kết nối, in lỗi.
        print(f"Lỗi khi kết nối tới database: {e}")
        return None

# Hàm truy vấn một dòng có trạng thái 'ER' trong bảng log_file.
def QueryRowER(connection, configId):
    try:
        # Tạo con trỏ truy vấn với dictionary=True để dữ liệu trả về dạng từ điển.
        cursor = connection.cursor(dictionary=True)

        # Câu truy vấn SQL để lấy dòng đầu tiên với trạng thái 'ER' và config_id tương ứng.
        query = """
        SELECT *
        FROM log_file
        WHERE status_log = 'ER' AND config_file_id = %s
        LIMIT 1;
        """
        cursor.execute(query, (configId,))  # Truyền giá trị config_file_id vào câu truy vấn.
        # Lấy kết quả của dòng đầu tiên.
        row = cursor.fetchone()
        
        return row  # Trả về dòng dữ liệu (hoặc None nếu không tìm thấy).
    except Error as e:
        # In lỗi nếu xảy ra lỗi truy vấn.
        print(f"Lỗi khi truy vấn database: {e}")
        return None
    finally:
        cursor.close()  # Đảm bảo đóng con trỏ truy vấn.

# Hàm cập nhật trạng thái của một dòng trong log_file.
def SetStatus(connection, row, status):
    try:
        cursor = connection.cursor()  # Tạo con trỏ truy vấn.

        # Câu lệnh SQL cập nhật trạng thái và thời gian cập nhật.
        query = """
        UPDATE db_control.log_file
        SET status_log = %s, updated_at = %s
        WHERE index_id = %s;
        """
        # Lấy thời gian hiện tại để cập nhật.
        updated_at = datetime.datetime.now()
        cursor.execute(query, (status, updated_at, row['index_id']))
        connection.commit()  # Lưu thay đổi vào database.

        print(f"Cập nhật trạng thái thành '{status}' cho index_id = {row['index_id']}.")
        cursor.close()
    except Error as e:
        # In lỗi nếu xảy ra lỗi cập nhật.
        print(f"Lỗi khi cập nhật trạng thái: {e}")

# Hàm load dữ liệu từ file CSV vào bảng staging.
def LoadDataIntoStaging(connection, row):
    try:
        print(f"connection = {connection.database}")
        # Truy vấn thông tin cấu hình cho file cần load dữ liệu.
        cursor = connection.cursor(dictionary=True)
        query = """
        SELECT source_folder_location, dest_table_staging
        FROM db_control.config_file
        WHERE index_id = %s;
        """
        cursor.execute(query, (row['config_file_id'],))
        config_row = cursor.fetchone()  # Lấy thông tin cấu hình.

        if not config_row:
            # Nếu không tìm thấy cấu hình, báo lỗi.
            print(f"Không tìm thấy thông tin config_file cho config_file_id = {row['config_file_id']}")
            return
        
        # Tạo đường dẫn đầy đủ của file CSV.
        file_path = config_row['source_folder_location'] + row['file_name']
        print(file_path)
        dest_table = config_row['dest_table_staging']
        print(dest_table)
        # Chuyển database hiện tại thành db_staging để load dữ liệu.
        connection.database = 'db_staging'

        # Tạo câu lệnh LOAD DATA INFILE để nạp dữ liệu từ file CSV.
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
        
        cursor.execute(load_data_query)  # Thực thi lệnh LOAD DATA INFILE.
        connection.commit()  # Lưu thay đổi vào database.
        print(f"Dữ liệu đã được load vào bảng {dest_table} từ file {file_path}")

        cursor.close()
    except Error as e:
        # In lỗi nếu quá trình load dữ liệu thất bại.
        print(f"Lỗi khi thực thi LOAD DATA INFILE: {e}")
def AddValueDatedim(connection, row):
    try:
        # Truy vấn thông tin cấu hình cho file cần load dữ liệu.
        connection.database = 'db_control'
        cursor = connection.cursor(dictionary=True)
        query = """
        SELECT dest_table_staging
        FROM db_control.config_file
        WHERE index_id = %s;
        """
        cursor.execute(query, (row['config_file_id'],))
        config_row = cursor.fetchone()  # Lấy thông tin cấu hình.
        
        if not config_row:
            # Nếu không tìm thấy cấu hình, báo lỗi.
            print(f"Không tìm thấy thông tin config_file cho config_file_id = {row['config_file_id']}")
            cursor.close()
            return
        
        # Đảm bảo đóng con trỏ sau khi lấy thông tin config.
        cursor.close()

        # Thực hiện truy vấn trên database db_staging.
        connection.database = 'db_staging'
        cursor = connection.cursor()

        # Xây dựng câu lệnh SQL động cho UPDATE
        update_query = f"""
        UPDATE {config_row['dest_table_staging']} t
        INNER JOIN date_dim d ON t.TimeGet = d.dates
        SET t.date_dim_id = d.id;
        """
        
        cursor.execute(update_query)  # Thực thi câu lệnh UPDATE.
        connection.commit()  # Lưu thay đổi vào database.
        print(f"Transform thành công cho bảng: {config_row['dest_table_staging']}")
        
        cursor.close()  # Đảm bảo đóng con trỏ.
    
    except Error as e:
        # In lỗi nếu quá trình load dữ liệu thất bại.
        print(f"Lỗi khi thực thi transform date: {e}")


# Code - Work flow
# 1. Load init config.xml, configId = 1 (source: zingmp3)
filePath = r"D:\myStudySpace\hk7\datawarehouse\scriptETL\config.xml"
configId = 1

config = ReadDatabaseConfig(filePath)
if config:
    # 2. Kết nối db_control
    connection = ConnectToDatabase(config)
    if connection:
        # 3. Query db_control.file_log 1 dòng với status = ER , config_id = 1
        row = QueryRowER(connection, configId)
        if row:
            # 4. Cập nhật status = PS
            SetStatus(connection, row, "PS")
            # 5. Đọc dữ liệu từ file vào db_staging.top100_zing_daily
            LoadDataIntoStaging(connection, row)
            # 6. cập nhật status = ES
            SetStatus(connection, row, "ES")
            # 7. Transform date to datedim 
            AddValueDatedim(connection, row)
            # 8. cập nhật status = TS
            SetStatus(connection, row, "TS")


        # Đảm bảo đóng kết nối sau khi hoàn thành công việc
        connection.close()
