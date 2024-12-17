from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.utils.dates import days_ago
from airflow.utils.trigger_rule import TriggerRule
from datetime import timedelta

# Default arguments for the DAG
default_args = {
    'owner': 'luan',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 0,  # Retry 1 lần nếu fail
    # 'retry_delay': timedelta(minutes=2),  # Thời gian chờ giữa các lần retry
}

# Define the DAG
with DAG(
    'job_data_pipeline',
    default_args=default_args,
    description='Pipepline dữ liệu công việc chạy vào 1h sáng',
    schedule_interval='0 1 * * *',  # Lịch chạy: 1h sáng hàng ngày
    start_date=days_ago(1),  # DAG bắt đầu từ hôm qua
    catchup=False,  # Không chạy backlog
) as dag:
    
    #########################################
    ####### SCRAPING ########################

    # Task 1: Facebook scraping
    facebook_scrape = BashOperator(
        task_id='facebook_scrape',
        bash_command='python /opt/airflow/job_rcm/job_rcm_code/job_scraping/updating_scrape/facebook/main.py',
    )

    # Task 2: LinkedIn scraping
    linkedin_scrape = BashOperator(
        task_id='linkedin_scrape',
        bash_command='python /opt/airflow/job_rcm/job_rcm_code/job_scraping/updating_scrape/linkedin/main.py',
        trigger_rule=TriggerRule.ALL_DONE,
    )

    # Task 3: VietnamWorks scraping
    vietnamwork_scrape = BashOperator(
        task_id='vietnamwork_scrape',
        bash_command='python /opt/airflow/job_rcm/job_rcm_code/job_scraping/updating_scrape/vietnamwork/main.py',
        trigger_rule=TriggerRule.ALL_DONE,
    )


    #########################################
    ####### PREPROCESSING ###################

    # Task 4: Facebook preprocessing
    facebook_preprocess = BashOperator(
        task_id='facebook_preprocess',
        bash_command='python /opt/airflow/job_rcm/job_rcm_code/data_preprocessing/update_code/facebook_preprocessed/main.py',
        trigger_rule=TriggerRule.ALL_DONE,
    )

    # Task 5: LinkedIn preprocessing
    linkedin_preprocess = BashOperator(
        task_id='linkedin_preprocess',
        bash_command='python /opt/airflow/job_rcm/job_rcm_code/data_preprocessing/update_code/linkedin_preprocessed/main.py',
        trigger_rule=TriggerRule.ALL_DONE,
    )

    # Task 6: VietnamWorks preprocessing
    vietnamwork_preprocess = BashOperator(
        task_id='vietnamwork_preprocess',
        bash_command='python /opt/airflow/job_rcm/job_rcm_code/data_preprocessing/update_code/vnw_proprocessed/main.py',
        trigger_rule=TriggerRule.ALL_DONE,
    )

    # Task 7: Data integration
    data_integration = BashOperator(
        task_id='data_integration',
        bash_command='python /opt/airflow/job_rcm/job_rcm_code/data_preprocessing/update_code/intergrate/main.py',
        trigger_rule=TriggerRule.ALL_DONE,
    )

    #########################################
    ####### DATA SAVING #####################

    # Task 8: Data upload
    data_upload = BashOperator(
        task_id='data_upload',
        bash_command='/opt/airflow/job_rcm/job_rcm_code/data_upload/main.bat',
        trigger_rule=TriggerRule.ALL_DONE,
    )

    # Define task dependencies (chạy tuần tự)
    facebook_scrape >> linkedin_scrape >> vietnamwork_scrape >> facebook_preprocess >> linkedin_preprocess >> vietnamwork_preprocess >> data_integration >> data_upload
    # facebook_scrape >> facebook_preprocess >> linkedin_preprocess >> vietnamwork_preprocess >> data_integration >> data_upload
