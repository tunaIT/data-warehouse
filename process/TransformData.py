import xml.etree.ElementTree as ET  # Thư viện xử lý file XML.
import mysql.connector  # Thư viện để kết nối và thao tác với MySQL.
from mysql.connector import Error  # Class xử lý lỗi của mysql.connector.
from datetime import datetime  # Thư viện hỗ trợ làm việc với ngày giờ.

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
def QueryRowEC(connection, configId):
    try:
        # Tạo con trỏ truy vấn với dictionary=True để dữ liệu trả về dạng từ điển.
        cursor = connection.cursor(dictionary=True)

        # Câu truy vấn SQL để lấy dòng đầu tiên với trạng thái 'ER' và config_id tương ứng.
        query = """
        SELECT *
        FROM log_file
        WHERE (status_log = 'Extract_Staging_Success' OR status_log = 'Transform_Start' OR status_log = 'Transform_Failed') AND config_file_id = %s
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
        updated_at = datetime.now()
        cursor.execute(query, (status, updated_at, row['index_id']))
        connection.commit()  # Lưu thay đổi vào database.

        print(f"Cập nhật trạng thái thành '{status}' cho index_id = {row['index_id']}.")
        cursor.close()
    except Error as e:
        # In lỗi nếu xảy ra lỗi cập nhật.
        print(f"Lỗi khi cập nhật trạng thái: {e}")
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
        print(f"Transform date thành công cho bảng: {config_row['dest_table_staging']}")
        
        cursor.close()  # Đảm bảo đóng con trỏ.
        return True
    except Error as e:
        # In lỗi nếu quá trình load dữ liệu thất bại.
        return e
def AddValueSongKey(connection, row):
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
        dest_table_staging = cursor.fetchone()  # Lấy thông tin cấu hình.

        if not dest_table_staging:
            # Nếu không tìm thấy cấu hình, báo lỗi.
            print(f"Không tìm thấy thông tin config_file cho config_file_id = {row['config_file_id']}")
            cursor.close()
            return

        # Đảm bảo đóng con trỏ sau khi lấy thông tin config.
        cursor.close()

        # Bước 2: Truy vấn dữ liệu từ bảng dest_table_staging
        connection.database = 'db_staging'
        cursor = connection.cursor(dictionary=True)

        # Truy vấn dữ liệu từ bảng staging lấy được từ config
        select_query = f"""
        SELECT * 
        FROM {dest_table_staging['dest_table_staging']};
        """
        cursor.execute(select_query)
        staging_row = cursor.fetchall()  # Lấy toàn bộ dữ liệu từ bảng staging.

        # Bước 3: Truy vấn dữ liệu từ bảng song_dim
        song_dim_query = """
        SELECT SongKey, SongName, Artist
        FROM song_dim;
        """
        cursor.execute(song_dim_query)
        song_dim_row = cursor.fetchall()  # Lấy toàn bộ dữ liệu từ bảng song_dim.

        # Lặp qua từng dòng trong staging_row
        for staging in staging_row:
            song_name = staging['SongName']
            artist = staging['Artist']
            song_key = None  # Biến để lưu SongKey

            # Kiểm tra sự tồn tại của SongName và Artist trong song_dim_row
            song_dim_match = next((song for song in song_dim_row if song['SongName'] == song_name and song['Artist'] == artist), None)

            if song_dim_match:
                # Nếu có, lấy SongKey
                song_key = song_dim_match['SongKey']
                # Cập nhật top100_zing_daily với SongKey
                update_query = """
                UPDATE top100_zing_daily
                SET SongKey = %s
                WHERE SongName = %s AND Artist = %s;
                """
                cursor.execute(update_query, (song_key, song_name, artist))
            else:
                # Nếu không có, insert vào song_dim và lấy SongKey mới
                insert_query = """
                INSERT INTO song_dim (SongName, Artist)
                VALUES (%s, %s);
                """
                cursor.execute(insert_query, (song_name, artist))
                connection.commit()  # commit để có SongKey mới

                # Lấy SongKey mới từ song_dim
                cursor.execute("""
                SELECT SongKey
                FROM song_dim
                WHERE SongName = %s AND Artist = %s;
                """, (song_name, artist))
                new_song_dim_row = cursor.fetchone()
                song_key = new_song_dim_row['SongKey']

                # Cập nhật top100_zing_daily với SongKey mới
                update_query = """
                UPDATE top100_zing_daily
                SET SongKey = %s
                WHERE SongName = %s AND Artist = %s;
                """
                cursor.execute(update_query, (song_key, song_name, artist))

        # Commit tất cả các thay đổi sau khi xử lý xong
        connection.commit()
        cursor.close()
        return True

    except Error as e:
        return e
def WriteErrorLog(errorMessage, filePath):
    try:
        with open(filePath, "a", encoding="utf-8") as errorFile:
            currentTime = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
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
                     # 4. Query db_control.file_log 1 dòng với status = Extract_Staging_Successe , config_id = 1
                    row = QueryRowEC(connection, configId)
                    # 5. Kiểm tra kết quả truy vấn thành công ?
                    if row:
                      #  6. cập nhật status = Transform_Start
                        SetStatus(connection, row, "Transform_Start")
                       # 7. chuyển đổi date to date_id
                        errAddDimId = AddValueDatedim(connection, row)
                        # 8. Kiểm tra việc chuyển đổi thành công không ?
                        if errAddDimId == True:
                            # 9. chuyển đổi song thành song_key
                            # errAddSongKey = AddValueSongKey(connection, row)
                            # 10. Kiểm tra việc chuyển đổi thành công không ?
                            # if errAddSongKey == True:
                            # 11. cập nhật status = Transform_Complete
                                SetStatus(connection, row,"Transform_Complete")
                                break
                            # else:
                            #     # 10.1. cập nhật status = Transform_Failed
                            #     SetStatus(connection, row,"Transform_Failed")
                            #     break
                        else:
                            # 8.1. cập nhật status = Transform_Failed
                            SetStatus(connection, row,"Transform_Failed")
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
                    SetStatus(connection, row,"Transform_Failed")
                    print(f"Thử lại lần thứ {retryCount}...")
                    break
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