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
    'retries': 0,
}

# Define the DAG
with DAG(
    'job_data_pipeline',
    default_args=default_args,
    description='Pipeline dữ liệu công việc chạy vào 1h sáng',
    schedule_interval='0 1 * * *',
    start_date=days_ago(1),
    catchup=False,
) as dag:
    
    #########################################
    ####### SCRAPING ########################

    facebook_scrape = BashOperator(
        task_id='facebook_scrape',
        bash_command='python /opt/airflow/scripts/facebook_scrape.py',
    )

    linkedin_scrape = BashOperator(
        task_id='linkedin_scrape',
        bash_command='python /opt/airflow/scripts/linkedin_scrape.py',
        trigger_rule=TriggerRule.ALL_DONE,
    )

    vietnamwork_scrape = BashOperator(
        task_id='vietnamwork_scrape',
        bash_command='python /opt/airflow/scripts/vietnamwork_scrape.py',
        trigger_rule=TriggerRule.ALL_DONE,
    )


    #########################################
    ####### PREPROCESSING ###################

    facebook_preprocess = BashOperator(
        task_id='facebook_preprocess',
        bash_command='python /opt/airflow/scripts/facebook_preprocess.py',
        trigger_rule=TriggerRule.ALL_DONE,
    )

    linkedin_preprocess = BashOperator(
        task_id='linkedin_preprocess',
        bash_command='python /opt/airflow/scripts/linkedin_preprocess.py',
        trigger_rule=TriggerRule.ALL_DONE,
    )

    vietnamwork_preprocess = BashOperator(
        task_id='vietnamwork_preprocess',
        bash_command='python /opt/airflow/scripts/vietnamwork_preprocess.py',
        trigger_rule=TriggerRule.ALL_DONE,
    )

    data_integration = BashOperator(
        task_id='data_integration',
        bash_command='python /opt/airflow/scripts/data_integration.py',
        trigger_rule=TriggerRule.ALL_DONE,
    )


    #########################################
    ####### DATA SAVING #####################

    data_upload = BashOperator(
        task_id='data_upload',
        bash_command='bash /opt/airflow/scripts/data_upload.bat',
        trigger_rule=TriggerRule.ALL_DONE,
    )

    # Define task dependencies
    facebook_scrape >> linkedin_scrape >> vietnamwork_scrape >> \
    facebook_preprocess >> linkedin_preprocess >> vietnamwork_preprocess >> \
    data_integration >> data_upload
