from airflow import DAG
from airflow.operators.python_operator import BranchPythonOperator
from airflow.contrib.operators.ssh_operator import SSHOperator
from airflow.operators.bash_operator import BashOperator
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.python_operator import PythonOperator
from airflow.utils.trigger_rule import TriggerRule
from airflow.operators.subdag_operator import SubDagOperator
from airflow.models import Variable
from datetime import datetime, timedelta
from airflow.utils.dates import days_ago
import datetime
from dateutil.relativedelta import relativedelta


day = (datetime.datetime.now() - relativedelta(days=1)).strftime("%Y-%m-%d")
tiga_bulan = (datetime.datetime.strptime(day, "%Y-%m-%d") - relativedelta(months=3)).strftime("%Y-%m-%d")
enam_bulan = (datetime.datetime.strptime(day, "%Y-%m-%d") - relativedelta(months=6)).strftime("%Y-%m-%d")

# from task import so2w_di


'''
Main configuration for so2w Pipeline
'''

args = {
    'owner': 'so2w',
    # 'start_date' : datetime(2020, 5, 13),
    'start_date': days_ago(1),
    'depends_on_past': False,
    'email': ['DataRanger@office365.astra.co.id'],
    'email_on_failure': True,
    'email_on_retry': False,
    'retries': 3,
    'retry_delay': timedelta(minutes=7)
}

dag = DAG(
    dag_id='testing_icare',
    description='Ingest data sp to cloudera',
    default_args=args,
    schedule_interval='00 21 * * *'
)

start_daily = DummyOperator(
    task_id='start_daily',
    dag=dag)
start_monthly_prep = DummyOperator(
    task_id='start_monthly_prep',
    dag=dag)
start_monthly = DummyOperator(
    task_id='start_monthly',
    dag=dag)
start_quarterly = DummyOperator(
    task_id='start_quarterly',
    dag=dag)
start_var = DummyOperator(
    task_id='start_var',
    dag=dag)
start_car = DummyOperator(
    task_id='start_car',
    dag=dag)
start_churn = DummyOperator(
    task_id='start_churn',
    trigger_rule = 'one_success',
    dag=dag)
start_segmentation = DummyOperator(
    task_id='start_segmentation',
    dag=dag)
start_prodrec = DummyOperator(
    task_id='start_prodrec',
    trigger_rule = 'one_success',
    dag=dag)
quarterly_list = ['2', '5', '8', '10']
def check_monthly():
    dt_now = datetime.now()
    if str(dt_now.day) == str('1'):
        return ("start_monthly_prep", "start_daily")
    else:
        return "start_daily"
def check_quarterly():
    dt_now = datetime.now()
    if (str(dt_now.day) == str('1')) & (str(dt_now.month) in quarterly_list):
        return ("start_quarterly")
    else:
        return "start_monthly"
check_1st = BranchPythonOperator(
                    task_id='check_1st',
                    python_callable=check_monthly,
                    trigger_rule='all_done',
                    dag=dag)
check_2nd = BranchPythonOperator(
                    task_id='check_2nd',
                    python_callable=check_quarterly,
                    trigger_rule='all_done',
                    dag=dag)
check_1st >> [start_monthly_prep, start_daily]
start_monthly_prep >> [start_var, start_car] >> check_2nd
check_2nd >> [start_quarterly, start_monthly]
start_monthly >> [start_churn, start_prodrec]
start_quarterly >> [start_churn, start_segmentation]
start_segmentation >> start_prodrec
