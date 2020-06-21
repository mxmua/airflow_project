from airflow import DAG
from airflow.utils.dates import days_ago
from airflow.operators.python_operator import PythonOperator

from datetime import timedelta
import requests
import re
import csv
import os

import air_project as libs
import secur.credentials as ENV


args = {
    'owner': 'air101',
    'start_date': days_ago(2),
    'retries': 0,
    'retry_delay': timedelta(minutes=1),
}


def on_failure_action(*args, **kwargs):
    alert_text = '--- Failure ---'
    libs.bot_message(alert_text)


def sla_miss_action(*args, **kwargs):
    alert_text = '--- SLA MISSED ---'
    libs.bot_message(message_text=alert_text)


def load_links_from_gsheet(gsheet_url: str, stage_filename: str) -> None:
    libs.write_list_to_csv(['url'], \
        libs.get_url_from_gsheet(table_url=gsheet_url, auth_json_file=ENV.GSHEET_KEY_FILE), stage_filename)


def parse_links_watchers(stage_filename: str, result_filename: str) -> None:
    libs.csv_parser(uploaded_sheet_file=stage_filename, parsed_file_name=result_filename)


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
        op_kwargs={'gsheet_url': ENV.TABLE_URL, 'stage_filename': ENV.UPLOADED_GSHEET_FILE},
    )

    parse_links_watchers = PythonOperator(
        task_id='parse_links_watchers',
        python_callable=parse_links_watchers,
        # provide_context=True,
        op_kwargs={'stage_filename': ENV.UPLOADED_GSHEET_FILE,\
            'result_filename': ENV.PARSED_DATA_SET_FILE}
    )

    write_to_gsheet = PythonOperator(
        task_id='write_to_gsheet',
        python_callable=libs.write_to_gsheet
    )

    send_report = PythonOperator(
        task_id = 'send_report',
        python_callable=libs.render_and_send_report,
        op_kwargs={'parsed_file_name': ENV.PARSED_DATA_SET_FILE},
    )


    load_links_from_gsheet >> parse_links_watchers >> write_to_gsheet >> send_report
