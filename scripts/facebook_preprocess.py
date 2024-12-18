import pandas as pd
import os
import json
import re

# Đường dẫn tới thư mục gốc và file kết quả
base_dir = r"C:\working\job_rcm\data\facebook"
output_path = r'C:\working\job_rcm\data\preprocessed\facebook\preprocessed_data.csv'
merged_output_path = r'C:\working\job_rcm\data\preprocessed\facebook\merged_data.json'
file_list_path = r'C:\working\job_rcm\data\preprocessed\facebook\file_list.txt'

# Hàm lấy danh sách tất cả các file JSON
def get_all_json_files(base_dir):
    json_files = []
    for root, dirs, files in os.walk(base_dir):
        for file in files:
            if file == 'data.json':
                json_files.append(os.path.join(root, file))
    return json_files

# Lấy danh sách file JSON hiện tại
current_file_list = get_all_json_files(base_dir)

# Kiểm tra xem file_list.txt có tồn tại không
if os.path.exists(file_list_path):
    # Đọc danh sách file cũ
    with open(file_list_path, 'r') as f:
        previous_file_list = f.read().splitlines()
else:
    # Nếu chưa có file_list.txt, coi như không có file nào trước đó
    previous_file_list = []
    # Tạo file_list.txt với nội dung trống
    with open(file_list_path, 'w') as f:
        f.write("")

# So sánh danh sách file để kiểm tra tệp mới
if set(current_file_list) != set(previous_file_list):
    # Có file mới, thực hiện gộp file (Task 1)
    all_posts = []
    for file_path in current_file_list:
        with open(file_path, 'r', encoding='utf-8') as f:
            data = json.load(f)
            all_posts.append(data)
    
    # Ghi dữ liệu hợp nhất ra file
    with open(merged_output_path, 'w', encoding='utf-8') as f:
        json.dump(all_posts, f, ensure_ascii=False, indent=4)
    
    # Cập nhật file_list.txt với danh sách file hiện tại
    with open(file_list_path, 'w') as f:
        f.write("\n".join(current_file_list))
    
    print(f"Dữ liệu mới đã được hợp nhất và lưu vào '{merged_output_path}'")
else:
    print("Không có file mới, đọc trực tiếp từ tệp JSON đã hợp nhất.")

# Đọc dữ liệu từ file JSON hợp nhất
facebook_df = pd.read_json(merged_output_path)
print("Dữ liệu đã được đọc thành công.")

facebook_df['post_date'] = facebook_df['post_date'].apply(
    lambda x: re.sub(r"Thứ \w+, (\d+) Tháng (\d+), (\d+) lúc (\d+:\d+)", r"\3-\2-\1 \4", x) if pd.notnull(x) else x
)
facebook_df['post_date'] = pd.to_datetime(facebook_df['post_date'], format='%Y-%m-%d %H:%M', errors='coerce')

def fill_nat_with_timedelta(group):
    last_valid = None
    result = []
    for date in group:
        if pd.isna(date):
            last_valid -= pd.Timedelta(minutes=30)
            result.append(last_valid)
        else:
            last_valid = date
            result.append(last_valid)
    return result

facebook_df['post_date'] = facebook_df.groupby('group_id')['post_date'].transform(fill_nat_with_timedelta)

facebook_df.to_csv(output_path, index=False, encoding='utf-8')
print(f"Dữ liệu đã được lưu vào: {output_path}")

