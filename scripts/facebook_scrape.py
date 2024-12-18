import os
import time
import re
import random
import requests
from datetime import datetime
import random
import pandas as pd
import zipfile
from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.common.keys import Keys
from selenium.webdriver.common.action_chains import ActionChains
from selenium.common.exceptions import TimeoutException
from bs4 import BeautifulSoup
import json
import csv

# Setup directory paths
base_save_dir = r'C:\working\job_rcm\data\facebook'
group_href = "https://www.facebook.com/groups/3272112773116278/"
image_save_dir = os.path.join(base_save_dir, 'post_image')

# Create directory if it does not exist
if not os.path.exists(image_save_dir):
    os.makedirs(image_save_dir)

# Define the CSV file path
csv_file_path = os.path.join(base_save_dir, 'post_hrefs.csv')

# Function to read post hrefs from the CSV file
def read_post_hrefs_from_csv():
    if not os.path.exists(csv_file_path):
        return []
    with open(csv_file_path, 'r', newline='', encoding='utf-8') as file:
        reader = csv.reader(file)
        return [row[0] for row in reader]

# Function to save post href to the CSV file
def save_post_href_to_csv(post_href):
    with open(csv_file_path, 'a', newline='', encoding='utf-8') as file:
        writer = csv.writer(file)
        writer.writerow([post_href])


# Hàm chuyển đổi struct_time thành đối tượng datetime
def convert_scrape_time(scrape_time):
    return datetime.fromtimestamp(time.mktime(scrape_time))

# Hàm tạo thư mục theo thời gian scrape
def create_directory_structure(scrape_time):
    scrape_time = convert_scrape_time(scrape_time)  # Chuyển struct_time thành datetime
    year_dir = os.path.join(base_save_dir, scrape_time.strftime("%Y"))
    month_dir = os.path.join(year_dir, scrape_time.strftime("%m"))
    day_dir = os.path.join(month_dir, scrape_time.strftime("%d"))
    hour_dir = os.path.join(day_dir, scrape_time.strftime("%H"))

    for i in range(1, 100):  # Tối đa 100 phần
        part_dir = os.path.join(hour_dir, f'part_{i}')
        if not os.path.exists(part_dir):
            os.makedirs(part_dir)
            return part_dir

        if len(os.listdir(part_dir)) < 300:  # Mỗi thư mục chứa tối đa 300 post
            return part_dir

# Hàm lưu dữ liệu post
def save_post_data(post_data, post_html, images, scrape_time):
    part_dir = create_directory_structure(scrape_time)

    scrape_time_str = convert_scrape_time(scrape_time).strftime('%Y-%m-%d_%H%M%S')  # Thêm giây để đảm bảo tên duy nhất
    detail_folder_name = f"{scrape_time_str}_group_{post_data['group_id'].split('/')[-2]}"
    detail_folder = os.path.join(part_dir, detail_folder_name)
    os.makedirs(detail_folder, exist_ok=True)

    # Lưu file JSON
    json_path = os.path.join(detail_folder, 'data.json')
    with open(json_path, 'w', encoding='utf-8') as json_file:
        json.dump(post_data, json_file, ensure_ascii=False, indent=4)

    # Lưu file HTML
    html_path = os.path.join(detail_folder, 'html.txt')
    with open(html_path, 'w', encoding='utf-8') as html_file:
        html_file.write(post_html)

    # Lưu hình ảnh
    image_folder = os.path.join(detail_folder, 'images')
    os.makedirs(image_folder, exist_ok=True)
    for idx, img_url in enumerate(images):
        img_path = os.path.join(image_folder, f'image_{idx+1}.jpg')
        try:
            img_data = requests.get(img_url).content
            with open(img_path, 'wb') as img_file:
                img_file.write(img_data)
        except Exception as e:
            print(f"Error downloading image: {e}")

################## Set up Chrome options and driver
# Function to read proxy config from file
def read_proxy_config(file_path):
    config = {}
    with open(file_path, 'r') as f:
        for line in f:
            key, value = line.strip().split('=')
            config[key] = value
    return config

# Read proxy settings from text file
proxy_config = read_proxy_config(r'C:\working\job_rcm\job_rcm_code\config.txt')
PROXY_HOST = proxy_config['PROXY_HOST']
PROXY_PORT = proxy_config['PROXY_PORT']
PROXY_USER = proxy_config['PROXY_USER']
PROXY_PASS = proxy_config['PROXY_PASS']

# ChromeDriver path
chrome_driver_path = 'C:/Users/trand/chromedriver.exe'

# Manifest for proxy
manifest_json = """
{
    "version": "1.0.0",
    "manifest_version": 2,
    "name": "Chrome Proxy",
    "permissions": [
        "proxy",
        "tabs",
        "unlimitedStorage",
        "storage",
        "<all_urls>",
        "webRequest",
        "webRequestBlocking"
    ],
    "background": {
        "scripts": ["background.js"]
    },
    "minimum_chrome_version":"22.0.0"
}
"""

# Background script for proxy
background_js = """
var config = {
        mode: "fixed_servers",
        rules: {
        singleProxy: {
            scheme: "http",
            host: "%s",
            port: parseInt(%s)
        },
        bypassList: ["localhost"]
        }
    };

chrome.proxy.settings.set({value: config, scope: "regular"}, function() {});

function callbackFn(details) {
    return {
        authCredentials: {
            username: "%s",
            password: "%s"
        }
    };
}

chrome.webRequest.onAuthRequired.addListener(
            callbackFn,
            {urls: ["<all_urls>"]},
            ['blocking']
);
""" % (PROXY_HOST, PROXY_PORT, PROXY_USER, PROXY_PASS)

# Set up Chrome options
options = Options()
options.add_argument("--disable-infobars")
prefs = {"credentials_enable_service": False,
     "profile.password_manager_enabled": False}
options.add_experimental_option("prefs", prefs)
options.add_argument("--disable-notifications")
options.add_experimental_option("excludeSwitches", ["enable-automation"])
options.add_experimental_option('useAutomationExtension', False)

# Randomize User-Agent
user_agents = [
    'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/115.0.0.0 Safari/537.36',
    'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/114.0.0.0 Safari/537.36',
    'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/116.0.0.0 Safari/537.36',
]
options.add_argument(f"user-agent={random.choice(user_agents)}")
	
# Set up proxy
pluginfile = 'proxy_auth_plugin.zip'
with zipfile.ZipFile(pluginfile, 'w') as zp:
    zp.writestr("manifest.json", manifest_json)
    zp.writestr("background.js", background_js)
options.add_extension(pluginfile)


# Launch browser
driver = webdriver.Chrome(chrome_driver_path, options=options)
driver.get("http://www.facebook.com")

# Login 
time.sleep(4)
email = driver.find_element(By.NAME, "email")
password = driver.find_element(By.NAME, "pass")
email.send_keys("nguyenanhnguyen111666@gmail.com")
password.send_keys("tranduyluan11062003")

button = WebDriverWait(driver, 2).until(EC.element_to_be_clickable((By.CSS_SELECTOR, "button[type='submit']"))).click()

# Open a new tab
driver.execute_script("window.open('https://www.facebook.com/groups/160325109320298/');") 
### cần làm nhiều group trong 

driver.switch_to.window(driver.window_handles[-1])
# Scale screen to 60% for the new tab as well
driver.execute_script("document.body.style.zoom='60%'")

actions = ActionChains(driver)


# Read existing post hrefs
post_href_list = read_post_hrefs_from_csv()

post_count = 0  # Biến đếm số bài đã thu thập
n = 5

# Scraping loop
while True:
    feed_element = driver.find_element(By.XPATH, '//*[@role="feed"]')
    post_elements = driver.find_elements(By.XPATH, "//div[@class='x1yztbdb x1n2onr6 xh8yej3 x1ja2u2z']")
    # scrape_time = time.localtime()  # Record the scrape time

    for element in post_elements:
        if post_count >= n:  # Dừng lại sau khi đã thu thập đủ n bài
            print(f"Đã thu thập đủ {n} bài, dừng lại.")
            driver.quit()  # Thoát trình duyệt
            exit()  # Thoát chương trình

        
        scrape_time = time.localtime()  # Record the scrape time
        actions.move_to_element(element).perform()
        time.sleep(1)  

        # Get post href
        post_href_element = element.find_element(By.XPATH, ".//a[@role='link']")
        post_href_value = post_href_element.get_attribute('href')
        if post_href_value in post_href_list:
            continue

        # Get author name
        try:
            author_name_element = element.find_elements(By.CSS_SELECTOR, 'span.html-span.xdj266r.x11i5rnm.xat24cr.x1mh8g0r.xexx8yu.x4uap5.x18d9i69.xkhd6sd.x1hl2dhg.x16tdsg8.x1vvkbs')
            author_name_value = author_name_element[0].text.strip()
        except:
            author_name_value = None
        # Switch tabs and get HR ID
        actions.move_to_element(author_name_element[0]).key_down(Keys.CONTROL).click(author_name_element[0]).key_up(Keys.CONTROL).perform()
        time.sleep(2)
        current_tabs = driver.window_handles

        if len(current_tabs) > 2:
            driver.switch_to.window(current_tabs[-1])
            try :
                profile_elements = driver.find_elements(By.CSS_SELECTOR, 'a.x1i10hfl.xjbqb8w.x1ejq31n.xd10rxx')
                hr_id_value = profile_elements[6].get_attribute('href')
            except:
                hr_id_value = None
            time.sleep(2)
            driver.close()
            driver.switch_to.window(current_tabs[1])
        else:
            print("Only one tab, no extra tab to close.")

        # Get post date
        try:
                hover_elements = element.find_elements(By.CSS_SELECTOR, 'span.html-span.xdj266r.x11i5rnm.xat24cr.x1mh8g0r.xexx8yu.x4uap5.x18d9i69.xkhd6sd.x1hl2dhg.x16tdsg8.x1vvkbs')
                # time.sleep(5)
                actions.move_to_element(hover_elements[1]).perform()
                time.sleep(5)
                html =driver.page_source
                date_element = driver.find_element(By.CSS_SELECTOR, "span.x193iq5w.xeuugli.x13faqbe.x1vvkbs.x1xmvt09.x1nxh6w3.x1sibtaa.xo1l8bm.xzsf02u")
                post_date_value = date_element.text
                print(post_date_value)
        except Exception as e:
                post_date_value = None

        # Check for "see more" button and click if present
        try:
            see_more_button = element.find_element(By.XPATH, ".//div[contains(@class, 'x1i10hfl') and contains(@role, 'button') and contains(text(), 'Xem thêm')]")
            if see_more_button:
                see_more_button.click()
                time.sleep(1)
        except Exception as e:
            print("No 'see more' button found")

        # Get the content
        try :
            content_element_1s = element.find_elements(By.CSS_SELECTOR, "div.xdj266r.x11i5rnm.xat24cr.x1mh8g0r.x1vvkbs")

            if len(content_element_1s) > 1:
                    print('1')
                    content_value = content_element_1s[1].text
            else: 
                    time.sleep(1)
                    print('2')
                    content_element_2s = element.find_elements(By.CSS_SELECTOR, "span.x193iq5w.xeuugli.x13faqbe.x1vvkbs.x1xmvt09.x1lliihq.x1s928wv.xhkezso.x1gmr53x.x1cpjm7i.x1fgarty.x1943h6x.xudqn12.x3x7a5m.x6prxxf.xvq8zen.xo1l8bm.xzsf02u")

                    if content_element_2s:
                            content_value = content_element_2s[0].text
                    else:
                            content_value = None
                            print("No content found for this post")
        except:
            content_value = None

        # Get images
        image_elements = element.find_elements(By.TAG_NAME, 'img')
        image_urls = [img.get_attribute('src') for img in image_elements if int(img.get_attribute('width')) > 300 and int(img.get_attribute('height')) > 300]

        # Save post data and images
        post_data = {
            'post_href': post_href_value,
            'author_name': author_name_value,
            'hr_id': hr_id_value,
            'group_id': group_href,
            'scrape_date': time.strftime('%Y-%m-%d %H:%M:%S'),
            'post_date': post_date_value,
            'content': content_value,
            'images': image_urls
        }
        
        # # save_post_data(post_data, element.get_attribute('outerHTML'), image_urls, scrape_time)
        # save_post_data(post_data,html, image_urls, scrape_time)

        # Append post_href_value to the CSV file
        save_post_href_to_csv(post_href_value)
        # # Also append to the list for any further processing in the same run
        post_href_list.append(post_href_value)
        print(f"đang thao tác ở post {post_count}")
        post_count += 1


        # Sleep before the next post to avoid rate limiting
        time.sleep(1)

    # Scroll down the page to load more posts
    driver.execute_script("window.scrollTo(0, document.body.scrollHeight);")
    time.sleep(5)
