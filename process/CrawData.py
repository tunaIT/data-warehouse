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

# Đảm bảo stdout hỗ trợ mã hóa UTF-8
sys.stdout.reconfigure(encoding='utf-8')

# Cài đặt trình điều khiển Coc Coc
def setup_driver():
    options = webdriver.ChromeOptions()
    options.binary_location = 'C:\\Program Files\\Google\\Chrome\\Application\\chrome.exe'
    options.add_argument('--no-sandbox')
    options.add_argument('--disable-dev-shm-usage')
    options.add_experimental_option("excludeSwitches", ["enable-automation"])
    options.add_experimental_option('useAutomationExtension', False)

    # Đường dẫn đến ChromeDriver
    driver = webdriver.Chrome(service=Service("D:\\Download\\chromedriver-win64\\chromedriver.exe"), options=options)
    return driver

# Lấy thông tin bài hát
def get_laptop_info(driver):
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
            print(f"Laptop: {name} - Links: {link_texts} - Time: {current_time}")
            count += 1
            
        except Exception as e:
            print(f"Error retrieving product information: {e}")
            print(product.get_attribute('outerHTML'))  # Print HTML nếu có lỗi
    
    return data

# Hàm chính
def main():
    driver = setup_driver()
    all_laptop_data = []  # Danh sách để lưu tất cả dữ liệu
    
    url = 'https://zingmp3.vn/album/Top-100-Nhac-Rap-Viet-Nam-Hay-Nhat-HIEUTHUHAI-Rhyder-Bray-Double2T/ZWZB96AI.html'
    driver.get(url)
    time.sleep(15)  # Chờ trang load ban đầu

    # Lấy dữ liệu
    laptop_data = get_laptop_info(driver)
    all_laptop_data.extend(laptop_data)

    # Tạo DataFrame
    df = pd.DataFrame(all_laptop_data, columns=['Top', 'Song Name', 'Artist', 'Time Get'])
    
    # Đặt tên file
    current_time = datetime.now().strftime('%Y-%m-%d')
    filename = os.path.join(os.getcwd(), f'top100_{current_time}.csv')  # Đường dẫn đầy đủ
    
    # Lưu file CSV
    df.to_csv(filename, index=False, encoding='utf-8-sig')
    driver.quit()
    
    return filename  # Trả về đường dẫn file CSV

if __name__ == "__main__":
    path_csv = main()
    print(f"File CSV đã được lưu tại: {path_csv}")
