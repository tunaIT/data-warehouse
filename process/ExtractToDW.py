# import xml.etree.ElementTree as ET  # Thư viện xử lý file XML.
# import mysql.connector  # Thư viện để kết nối và thao tác với MySQL.
# from mysql.connector import Error  # Class xử lý lỗi của mysql.connector.
# import datetime  # Thư viện hỗ trợ làm việc với ngày giờ.


# # Hàm đọc cấu hình từ file XML.
# def ReadDatabaseConfig(filePath):
#     try:
#         tree = ET.parse(filePath)
#         root = tree.getroot()
#         databaseConfig = root.find('database')
#         if databaseConfig is None:
#             raise ValueError("Không tìm thấy phần tử <database> trong file XML.")
        
#         return {  # Trích xuất thông tin từ thẻ <database>.
#             "host": databaseConfig.find('ip').text,
#             "port": int(databaseConfig.find('port').text),
#             "user": databaseConfig.find('username').text,
#             "password": databaseConfig.find('password').text,
#             "database": databaseConfig.find('dbname').text,
#         }
#     except Exception as e:
#         print(f"Lỗi khi đọc file XML: {e}")
#         return None


# # Hàm kết nối đến cơ sở dữ liệu.
# def ConnectToDatabase(configDb):
#     try:
#         connection = mysql.connector.connect(**configDb)
#         if connection.is_connected():
#             print("Kết nối thành công tới database!")
#             return connection
#     except Error as e:
#         print(f"Lỗi khi kết nối tới database: {e}")
#         return None


# # Hàm thực thi câu truy vấn SQL.
# def ExecuteQuery(connection, query, params=None, fetchOne=False):
#     try:
#         cursor = connection.cursor(dictionary=True)
#         cursor.execute(query, params)
#         return cursor.fetchone() if fetchOne else cursor.fetchall()
#     except Error as e:
#         print(f"Lỗi khi thực thi truy vấn: {e}")
#         return None
#     finally:
#         cursor.close()


# # Hàm cập nhật trạng thái và thời gian của một dòng.
# def UpdateStatus(connection, table, indexId, status):
#     try:
#         query = f"""
#         UPDATE {table}
#         SET status_log = %s, updated_at = %s
#         WHERE index_id = %s;
#         """
#         updatedAt = datetime.datetime.now()
#         ExecuteQuery(connection, query, (status, updatedAt, indexId))
#         connection.commit()
#         print(f"Cập nhật trạng thái '{status}' cho index_id = {indexId}.")
#     except Error as e:
#         print(f"Lỗi khi cập nhật trạng thái: {e}")


# # Hàm load dữ liệu từ file CSV.
# def LoadDataIntoStaging(connection, row):
#     try:
#         query = """
#         SELECT source_folder_location, dest_table_staging
#         FROM db_control.config_file
#         WHERE index_id = %s;
#         """
#         configRow = ExecuteQuery(connection, query, (row['config_file_id'],), fetchOne=True)
#         if not configRow:
#             print(f"Không tìm thấy cấu hình cho config_file_id = {row['config_file_id']}")
#             return
        
#         filePath = configRow['source_folder_location'] + row['file_name']
#         destTable = configRow['dest_table_staging']
#         connection.database = 'db_staging'

#         loadDataQuery = f"""
#         LOAD DATA INFILE '{filePath}'
#         INTO TABLE {destTable}
#         FIELDS TERMINATED BY ',' 
#         ENCLOSED BY '"' 
#         LINES TERMINATED BY '\\n' 
#         IGNORE 1 LINES
#         (Top, Artist, @SongName, @TimeGet)
#         SET 
#             SongName = TRIM(BOTH '''' FROM SUBSTRING_INDEX(SUBSTRING_INDEX(@SongName, ',', 1), '[', -1)),
#             TimeGet = STR_TO_DATE(@TimeGet, '%Y/%m/%d');
#         """
#         ExecuteQuery(connection, loadDataQuery)
#         connection.commit()
#         print(f"Dữ liệu đã được load vào bảng {destTable} từ file {filePath}")
#     except Error as e:
#         print(f"Lỗi khi load dữ liệu: {e}")


# # Hàm thêm dữ liệu vào `date_dim`.
# def AddValueDateDim(connection, row):
#     try:
#         query = """
#         SELECT dest_table_staging
#         FROM db_control.config_file
#         WHERE index_id = %s;
#         """
#         configRow = ExecuteQuery(connection, query, (row['config_file_id'],), fetchOne=True)
#         if not configRow:
#             print(f"Không tìm thấy cấu hình cho config_file_id = {row['config_file_id']}")
#             return

#         connection.database = 'db_staging'
#         updateQuery = f"""
#         UPDATE {configRow['dest_table_staging']} t
#         INNER JOIN date_dim d ON t.TimeGet = d.dates
#         SET t.date_dim_id = d.id;
#         """
#         ExecuteQuery(connection, updateQuery)
#         connection.commit()
#         print(f"Transform thành công cho bảng: {configRow['dest_table_staging']}")
#     except Error as e:
#         print(f"Lỗi khi transform date: {e}")


# # Work flow chính.
# def Main():
#     filePath = r"D:\myStudySpace\hk7\datawarehouse\scriptETL\config.xml"
#     configId = 1

#     config = ReadDatabaseConfig(filePath)
#     if config:
#         connection = ConnectToDatabase(config)
#         if connection:
#             row = ExecuteQuery(connection, """
#             SELECT *
#             FROM log_file
#             WHERE status_log = 'Extract_Start' AND config_file_id = %s
#             LIMIT 1;
#             """, (configId,), fetchOne=True)

#             if row:
#                 LoadDataIntoStaging(connection, row)
#                 UpdateStatus(connection, "db_control.log_file", row['index_id'], "Extract_Complete")
#             connection.close()


# if __name__ == "__main__":
#     Main()
# import xml.etree.ElementTree as ET  # Thư viện xử lý file XML.
# import mysql.connector  # Thư viện để kết nối và thao tác với MySQL.
# from mysql.connector import Error  # Class xử lý lỗi của mysql.connector.
# import datetime  # Thư viện hỗ trợ làm việc với ngày giờ.


# # Hàm đọc cấu hình từ file XML.
# def ReadDatabaseConfig(filePath):
#     try:
#         tree = ET.parse(filePath)
#         root = tree.getroot()
#         databaseConfig = root.find('database')
#         if databaseConfig is None:
#             raise ValueError("Không tìm thấy phần tử <database> trong file XML.")
        
#         return {  # Trích xuất thông tin từ thẻ <database>.
#             "host": databaseConfig.find('ip').text,
#             "port": int(databaseConfig.find('port').text),
#             "user": databaseConfig.find('username').text,
#             "password": databaseConfig.find('password').text,
#             "database": databaseConfig.find('dbname').text,
#         }
#     except Exception as e:
#         print(f"Lỗi khi đọc file XML: {e}")
#         return None


# # Hàm kết nối đến cơ sở dữ liệu.
# def ConnectToDatabase(configDb):
#     try:
#         connection = mysql.connector.connect(**configDb)
#         if connection.is_connected():
#             print("Kết nối thành công tới database!")
#             return connection
#     except Error as e:
#         print(f"Lỗi khi kết nối tới database: {e}")
#         return None


# # Hàm thực thi câu truy vấn SQL.
# def ExecuteQuery(connection, query, params=None, fetchOne=False):
#     try:
#         cursor = connection.cursor(dictionary=True)
#         cursor.execute(query, params)
#         return cursor.fetchone() if fetchOne else cursor.fetchall()
#     except Error as e:
#         print(f"Lỗi khi thực thi truy vấn: {e}")
#         return None
#     finally:
#         cursor.close()


# # Hàm cập nhật trạng thái và thời gian của một dòng.
# def UpdateStatus(connection, table, indexId, status):
#     try:
#         query = f"""
#         UPDATE {table}
#         SET status_log = %s, updated_at = %s
#         WHERE index_id = %s;
#         """
#         updatedAt = datetime.datetime.now()
#         ExecuteQuery(connection, query, (status, updatedAt, indexId))
#         connection.commit()
#         print(f"Cập nhật trạng thái '{status}' cho index_id = {indexId}.")
#     except Error as e:
#         print(f"Lỗi khi cập nhật trạng thái: {e}")


# # Hàm load dữ liệu từ file CSV.
# def LoadDataIntoDW(connection, row):
#     try:
#         query = """
#         SELECT dest_table_staging, dest_table_dw
#         FROM db_control.config_file
#         WHERE index_id = %s;
#         """
#         configRow = ExecuteQuery(connection, query, (row['config_file_id'],), fetchOne=True)
#         if not configRow:
#             print(f"Không tìm thấy cấu hình cho config_file_id = {row['config_file_id']}")
#             return
#         # xử lí thêm dl 
#         connection.database = 'db_staging'
#         dwTable = configRow['dest_table_dw']
#         stagingTable = configRow['dest_table_staging']

#         loadDataQuery = f"""
#         LOAD DATA INFILE '{filePath}'
#         INTO TABLE {destTable}
#         FIELDS TERMINATED BY ',' 
#         ENCLOSED BY '"' 
#         LINES TERMINATED BY '\\n' 
#         IGNORE 1 LINES
#         (Top, Artist, @SongName, @TimeGet)
#         SET 
#             SongName = TRIM(BOTH '''' FROM SUBSTRING_INDEX(SUBSTRING_INDEX(@SongName, ',', 1), '[', -1)),
#             TimeGet = STR_TO_DATE(@TimeGet, '%Y/%m/%d');
#         """
#         ExecuteQuery(connection, loadDataQuery)
#         connection.commit()
#         print(f"Dữ liệu đã được load vào bảng {destTable} từ file {filePath}")
#     except Error as e:
#         print(f"Lỗi khi load dữ liệu: {e}")


# # Hàm thêm dữ liệu vào `date_dim`.
# def AddValueDateDim(connection, row):
#     try:
#         query = """
#         SELECT dest_table_staging
#         FROM db_control.config_file
#         WHERE index_id = %s;
#         """
#         configRow = ExecuteQuery(connection, query, (row['config_file_id'],), fetchOne=True)
#         if not configRow:
#             print(f"Không tìm thấy cấu hình cho config_file_id = {row['config_file_id']}")
#             return

#         connection.database = 'db_staging'
#         updateQuery = f"""
#         UPDATE {configRow['dest_table_staging']} t
#         INNER JOIN date_dim d ON t.TimeGet = d.dates
#         SET t.date_dim_id = d.id;
#         """
#         ExecuteQuery(connection, updateQuery)
#         connection.commit()
#         print(f"Transform thành công cho bảng: {configRow['dest_table_staging']}")
#     except Error as e:
#         print(f"Lỗi khi transform date: {e}")


# # Work flow chính.
# def Main():
#     filePath = r"D:\myStudySpace\hk7\datawarehouse\scriptETL\config.xml"

#     config = ReadDatabaseConfig(filePath)
#     if config:
#         connection = ConnectToDatabase(config)
#         if connection:
#             row = ExecuteQuery(connection, """
#             SELECT *
#             FROM log_file
#             WHERE status_log = 'Transform_Complete'
#             LIMIT 1;
#             """, fetchOne=True)

#             if row:
#                 UpdateStatus(connection, "db_control.log_file", row['index_id'], "Load_Start")
#                 LoadDataIntoDW(connection, row)
#                 UpdateStatus(connection, "db_control.log_file", row['index_id'], "Extract_Complete")
#             connection.close()


# if __name__ == "__main__":
#     Main()
