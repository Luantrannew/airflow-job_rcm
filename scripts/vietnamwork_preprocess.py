import pandas as pd
import os
import json

# Đường dẫn tới file JSON
vnw_json_path = r'C:\working\job_rcm\data\vietnamwork\detailview\data.json'
output_path = r'C:\working\job_rcm\data\preprocessed\vietnamwork\preprocessed_data.csv'

# Đọc dữ liệu từ file JSON
with open(vnw_json_path, 'r', encoding='utf-8') as f:
    data = json.load(f)

# Chuyển đổi thành DataFrame
vnw_df = pd.DataFrame(data)
vnw_df = vnw_df.drop(vnw_df[vnw_df['job_status'] == 'error_link'].index)
vnw_df = vnw_df.reset_index(drop=True)

vnw_df['scrape_date'] = pd.to_datetime(vnw_df['scrape_date'], format="%Y-%m-%dT%H:%M:%S.%f")
vnw_df['scrape_date'] = vnw_df['scrape_date'].dt.strftime("%Y-%m-%d %H:%M")


vnw_df.to_csv(output_path, index=False, encoding='utf-8')
print(f"Dữ liệu đã được lưu vào: {output_path}")
