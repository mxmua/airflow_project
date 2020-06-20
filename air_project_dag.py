from airflow import DAG
from airflow.utils.dates import days_ago

from airflow.operators.python_operator import PythonOperator
# from airflow.operators.postgres_operator import PostgresOperator
# from airflow.operators.bash_operator import BashOperator
from airflow.operators.dummy_operator import DummyOperator
# from airflow.operators.python_operator import BranchPythonOperator
# from airflow.hooks.postgres_hook import PostgresHook

from datetime import timedelta
# from time import sleep
# from datetime import datetime
import requests
# import psycopg2
import random
import re
import csv
import os

import air_project as libs
import secur.hw04_credentials as ENV


GAUTH_FILENAME = 'key.json'
GSHEET_URL = 'https://docs.google.com/spreadsheets/d/1UK-aoLDoJ724KGUN0AzgOLKW1S05W2FLZmSYHdjjYig/'
STAGE_FILENAME = 'google_sheet_data.csv'
RESULT_FILENAME = 'parsed_result_data.csv'

STAGE_DIR = os.path.join(os.path.expanduser('~'), 'stage')

args = {
    'owner': 'air101',
    'start_date': days_ago(2),
    'retries': 0,
    'retry_delay': timedelta(minutes=1),
}

def bot_message(message_text: str, **kwargs) -> None:
    response = requests.post(
        url=f'https://api.telegram.org/bot{ENV.TG_BOT_TOKEN}/sendMessage',
        data={'chat_id': ENV.TG_BOT_CHAT_ID, 'text': message_text}
    ).json()
    print(response)


def get_report(file_name: str) -> dict:

    report_dict = {
        'rows_success_count': 0,
        'rows_failed_count': 0,
        'rows_failed_detail': [],
    }

    with open(file_name) as file_obj:
        reader = csv.DictReader(file_obj, delimiter=',')

        for line in reader:
            row_number = line['N']
            watchers_count = line['watchers_count'].strip()

            if not watchers_count:
                watchers_count = 'Error 404'

            if re.match(r'([0-9]+)(.?)([0-9]?[k]?)$', watchers_count):
                report_dict['rows_success_count'] += 1
            else:
                report_dict['rows_failed_count'] += 1
                report_dict['rows_failed_detail'].append([row_number, watchers_count])
    return report_dict


def send_report() -> None:
    report = get_report(file_name=os.path.join(STAGE_DIR, RESULT_FILENAME))
    report_str = f"rows_success_count: {report['rows_success_count']};\n  rows_failed_count: {report['rows_failed_count']}\n"

    for failed_row in report['rows_failed_detail']:
        report_str += f'{failed_row[0]}: {failed_row[1]} \n'

    bot_message(message_text=report_str)



def on_failure_action(*args, **kwargs):
    alert_text = '--- Failure ---'
    bot_message(alert_text)


def sla_miss_action(*args, **kwargs):
    alert_text = '--- SLA MISSED ---'
    bot_message(message_text=alert_text)


def load_links_from_gsheet(gsheet_url: str, stage_filename: str) -> None:
    libs.write_list_to_csv(['url'], \
        libs.get_url_from_gsheet(table_url=gsheet_url, auth_json_file=os.path.join(STAGE_DIR, GAUTH_FILENAME)), stage_filename)


def parse_links_watchers(stage_filename: str, result_filename: str) -> None:
    libs.csv_parser(csv_file_name=stage_filename, result_file_name=result_filename)




with DAG(dag_id='air101_project',
         default_args=args,
         schedule_interval=timedelta(days=1),
         sla_miss_callback=sla_miss_action,
         on_failure_callback=on_failure_action,
    ) as dag:

    load_links_from_gsheet = PythonOperator(
        task_id='load_links_from_gsheet',
        python_callable=load_links_from_gsheet,
        # provide_context=True,
        op_kwargs={'gsheet_url': GSHEET_URL, 'stage_filename': os.path.join(STAGE_DIR, STAGE_FILENAME)},
    )

    parse_links_watchers = PythonOperator(
        task_id='parse_links_watchers',
        python_callable=parse_links_watchers,
        # provide_context=True,
        op_kwargs={'stage_filename': os.path.join(STAGE_DIR, STAGE_FILENAME),\
            'result_filename': os.path.join(STAGE_DIR, RESULT_FILENAME)}
    )

    send_report = PythonOperator(
        task_id = 'send_report',
        python_callable=send_report,
        # provide_context=True,
    )

    load_links_from_gsheet >> parse_links_watchers >> send_report