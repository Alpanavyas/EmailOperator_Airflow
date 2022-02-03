# importing the required libraries
from datetime import timedelta, datetime
from airflow import DAG
from airflow.operators.email_operator import EmailOperator
from airflow.utils.dates import days_ago
from airflow.operators.bash import BashOperator
from airflow.operators.dummy_operator import DummyOperator


# These args will get passed on to the python operator
default_args = {
    'owner': 'alpana',
    'depends_on_past': False,
    'start_date': datetime(2021, 9, 10),
    'email': ['user@gmail.com'],
    'email_on_failure': True,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=1),
}

# define the DAG
dag = DAG(
    'My_Email_EmailOperator_S_F',
    default_args=default_args,
    description='Email Operator',
    schedule_interval=timedelta(days=1),
    start_date=datetime(2022, 2, 2),
    catchup=False
)


t1 = BashOperator(
        task_id='run_1',
        bash_command='spark-submit \
	--master yarn \
	--deploy-mode client \
	--driver-memory 1g \
	--executor-memory 2g \
	--num-executors 1 \
	--executor-cores 4 \
	/home/alpana/PycharmProjects/cohort_c8/jan_06_2022.py',
        dag=dag
	)



# define the first task
sendEmail = EmailOperator(
    task_id = 'sendEmail',
    to = ['user@gmail.com'],
    subject = 'Airflow_Alert',
    start_date = datetime(2022,1,10),
    html_content = """ <h3> Test Email from Airflow </h3> """,
    dag=dag
)

start = DummyOperator(task_id='start', dag=dag)
end = DummyOperator(task_id='end', dag=dag)


start >> t1 >> sendEmail >> end


