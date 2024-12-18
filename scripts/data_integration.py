import pandas as pd
import os

# Đường dẫn
output_path = r'C:\working\job_rcm\data\preprocessed\final.csv'

fb_path = r'C:\working\job_rcm\data\preprocessed\facebook\preprocessed_data.csv'
ld_path = r'C:\working\job_rcm\data\preprocessed\linkedin\preprocessed_data.csv'
vnw_path = r'C:\working\job_rcm\data\preprocessed\vietnamwork\preprocessed_data.csv'

# Đọc dữ liệu
facebook_df = pd.read_csv(fb_path)
ld_df = pd.read_csv(ld_path)
vnw_df = pd.read_csv(vnw_path)

# Chuẩn hóa các DataFrame
# Facebook
facebook_df['source'] = 'facebook'
facebook_df = facebook_df.rename(columns={
    'post_href': 'job_link',
    'content': 'description'
})
facebook_df['hr_id'] = facebook_df.get('hr_id', None)
facebook_df['post_date'] = facebook_df.get('post_date', None)
facebook_df['company_name'] = None
facebook_df['company_href'] = None
facebook_df['job_name'] = None
facebook_df['salary'] = None
facebook_df['region'] = None
facebook_df['job_status'] = None
facebook_df['jd'] = None

# LinkedIn
ld_df['source'] = 'linkedin'
ld_df = ld_df.rename(columns={
    'job_link': 'job_link',
    'company_name': 'company_name',
    'company_href': 'company_href',
    'job_name': 'job_name',
    'region': 'region',
    'job_status': 'job_status',
    'scrape_date': 'scrape_date',
    'hr_id': 'hr_id',
    'jd': 'jd'
})
ld_df['description'] = None
ld_df['salary'] = None
ld_df['post_date'] = None

# VietnamWorks
vnw_df['source'] = 'vietnamwork'
vnw_df = vnw_df.rename(columns={
    'job_link': 'job_link',
    'company_name': 'company_name',
    'company_href': 'company_href',
    'job_name': 'job_name',
    'salary': 'salary',
    'region': 'region',
    'job_status': 'job_status',
    'scrape_date': 'scrape_date',
    'hr_id': 'hr_id',
    'jd': 'jd'
})
vnw_df['description'] = None
vnw_df['post_date'] = None

# Danh sách cột chuẩn hóa
columns = [
    'source', 'job_link', 'hr_id', 'company_name', 'company_href', 
    'job_name', 'salary', 'region', 'job_status','post_date', 'scrape_date', 'jd', 'description'
]

# Thêm cột thiếu và sắp xếp thứ tự
facebook_df = facebook_df.reindex(columns=columns)
ld_df = ld_df.reindex(columns=columns)
vnw_df = vnw_df.reindex(columns=columns)

# Gộp các DataFrame
merged_df = pd.concat([facebook_df, ld_df, vnw_df], ignore_index=True)

# Xuất dữ liệu
merged_df.to_csv(output_path, index=False, encoding='utf-8')
print(f"Dữ liệu đã được lưu vào: {output_path}")