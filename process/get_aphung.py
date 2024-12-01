# 1. đọc file dbControl.ini
db_config = configparser.ConfigParser()
db_config.read(filename)
host = db_config.get('mysql', 'host')
user = db_config.get('mysql', 'user')
password = db_config.get('mysql', 'password')
database = db_config.get('mysql', 'database')

try:
    # 2. kết nối cơ sở dữ liệu
    cnx = mysql.connector.connect(
        user=user, password=password, host=host, database=database)
    if cnx.is_connected():
        print('Connected to MySQL database')

    # 3. Kiểm tra kết nối cơ sở dữ liệu
    if cnx is not None:
        try:
            # 4. Ghi log kết nối thành công và ghi vào 1 dòng với status= START_EXTRACT ở bảng control_data_file
            cursor = cnx.cursor()
            query = "INSERT INTO log (status, note, log_date) VALUES (%s, %s, %s)"
            insertData = ('CONNECT_DB_SUCCESS', 'connect db control success', datetime.now())
            cursor.execute(query, insertData)

            insert_query = """INSERT INTO control_data_file 
            (id_config, name, row_count, status, data_range_from, data_range_to, note, create_at, update_at, create_by, update_by) 
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)"""
            currentdate = datetime.now()
            cursor.execute(insert_query, (1, 'null', 'null', 'START_EXTRACT', 'null', 'null', 'crawl', currentdate, currentdate, 'Nhơn', 'Nhơn'))
            cnx.commit()

            # 5. Lấy dữ liệu từ src_path tại control_data_file_configs mà nguồn bằng 1
            cursor.execute('SELECT src_path FROM control_data_file_configs WHERE id = 1')
            result = cursor.fetchone()
            print(result[0])

            count = 0  # Biến đếm số lần lặp
            while True:
                # 6. Gửi request tới source
                response = requests.get(result[0])

                # 7. Kiểm tra kết nối
                if response.status_code == 200:
                    # 8. Ghi dữ liệu vào file
                    getDataFromAPI()

                    # 9. Ghi log thành công
                    insert_query = "INSERT INTO log (id_config, status, note, log_date) VALUES (%s, %s, %s, %s)"
                    cursor.execute(insert_query, (1, 'EXTRACT_SUCCESS', 'crawl success', currentdate))

                    with open(file_name, 'r') as file:
                        dataforecast = json.load(file)

                    # 10. Cập nhật dữ liệu Extract thành công vào control_data_file [status= EXTRACT_SUCCESS]
                    update_query = """UPDATE control_data_file 
                    SET name = %s, row_count = %s, status = %s, data_range_from = %s, data_range_to = %s, note = %s, update_at = %s, update_by = %s 
                    WHERE id_config = 1 AND status = 'START_EXTRACT' AND create_at = CURRENT_DATE"""
                    cursor.execute(update_query, (
                        f'{currentdate}_weather_forecast.json', 6, 'EXTRACT_SUCCESS',
                        dataforecast['timelines']['daily'][0]['time'], 
                        dataforecast['timelines']['daily'][5]['time'], 'crawl', currentdate, 'Nhơn'))
                    
                    cnx.commit()
                    print("Dữ liệu đã được thêm thành công!")
                    break
                else:
                    # 11. Ghi log thất bại
                    insert_query = "INSERT INTO log (id_config, status, note, log_date) VALUES (%s, %s, %s, %s)"
                    cursor.execute(insert_query, (1, 'CONNECT_SOURCE_FAIL', 'connect source unsuccess', currentdate))
                    print("Kết nối nguồn thất bại! Thử lại sau 10 phút.")
                    countdown(600)
                    count += 1

                    # 12. Kiểm tra lần lặp có lớn hơn 10 không
                    if count >= 10:
                        # 13. Cập nhật dữ liệu Extract thất bại vào control_data_file [status= EXTRACT_FAIL]
                        update_query = """UPDATE control_data_file 
                        SET name = %s, row_count = %s, status = %s, data_range_from = %s, data_range_to = %s, note = %s, update_at = %s, update_by = %s 
                        WHERE id_config = 1 AND status = 'START_EXTRACT' AND create_at = CURRENT_DATE"""
                        cursor.execute(update_query, ('null', 'null', 'EXTRACT_FAIL', 'null', 'null', 'crawl', currentdate, 'Nhơn'))
                        cnx.commit()
                        break
                    else:
                        continue
                break
        except mysql.connector.Error as err:
            print(f"Lỗi thêm dữ liệu: {err}")
            with open(file_err, 'a') as file:
                file.write(f'Error: {str(err)}\n')
            continue
        finally:
            # Đóng cursor và kết nối
            cursor.close()
            cnx.close()
    else:
        count += 1
        # 3.1 Kiểm tra lần lặp có lớn hơn 10 không?
        if count >= 10:
            err = "connect unsuccess"
            print(f"Lỗi kết nối CSDL: {err}")
            with open(file_err, 'a') as file:
                file.write(f'Error: {str(err)}\n')
            break
        print("Kết nối lại sau 10 phút.")
        countdown(600)
        print("Bắt đầu kết nối lại.")
except Exception as e:
    print(f"Lỗi: {e}")
