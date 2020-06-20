import gspread
import csv
import time
import requests
import re
import json
from datetime import datetime
from pathlib import Path
from bs4 import BeautifulSoup
from random import randrange
from collections import OrderedDict

from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC

from bs4 import BeautifulSoup
from requests.exceptions import Timeout, ConnectTimeout, HTTPError, RequestException

TABLE_URL = 'https://docs.google.com/spreadsheets/d/1UK-aoLDoJ724KGUN0AzgOLKW1S05W2FLZmSYHdjjYig/'

FILES_PATH = Path('/home/dimk/Python/airflow_project')
UPLOADED_GSHEET_FILE = Path.joinpath(FILES_PATH, 'sheet.csv')
PARSED_DATA_SET_FILE = Path.joinpath(FILES_PATH, 'parsed.csv')
PARSED_LOG = Path.joinpath(FILES_PATH, 'parsed.log')
GSHEET_KEY_FILE = Path.joinpath(FILES_PATH, 'key.json')

SITE_NAME_WITH_TAGS = {
    'habr': {'tag': 'span', 'class': 'post-stats__views-count'},
    'rutube': {'tag': 'span', 'class': 'video-info-card__view-count'},
    'pornhub': {'tag': 'span', 'class': 'count'},
    'vimeo': {'tag': 'script', 'class': '', 'type': 'application/ld+json'},
}


def write_list_to_csv(table_headers, data_list,
                      file_name, add_number_row=True):
    with open(file_name, 'w+',  newline="", encoding='utf-8') as file:
        if add_number_row:
            table_headers.insert(0, 'N')
        write = csv.writer(file)
        write.writerow(table_headers)
        for row_number, row in enumerate(data_list):
            if not isinstance(row, list):
                row = [row]
            if add_number_row:
                row.insert(0, row_number + 3)
            write.writerow(row)


def write_dictlist_to_csv(data_list,
                          file_name):
    with open(file_name, 'w+',  newline="", encoding='utf-8') as file:
        columns = [row_name for row_name in data_list[0]]
        writer = csv.DictWriter(file, fieldnames=columns)
        writer.writeheader()
        writer.writerows(data_list)


def get_url_from_gsheet(table_url: str,
                        auth_json_file=GSHEET_KEY_FILE):
    gc = gspread.service_account(filename=auth_json_file)
    sh = gc.open_by_url(table_url)
    return sh.sheet1.col_values(1)[2:]


def remove_unnecessary(watch_count, site_name):

    if site_name == 'vimeo':
        watch_count = json.loads(watch_count.string.strip())[
            0]['interactionStatistic'][2]['userInteractionCount']
        return watch_count

    watch_count = watch_count.text

    if site_name == 'youtube':
        watch_count = ''.join(watch_count.split()[:-1])

    if site_name == 'rutube':
        watch_count = watch_count.replace(',', '')

    if site_name == 'pornhub':
        watch_count = ''.join(watch_count.split())

    return watch_count


def get_watchers_with_tag(response, site_name):
    soup = BeautifulSoup(response.content, 'html.parser')
    tag = SITE_NAME_WITH_TAGS[site_name]['tag']
    attr_class = SITE_NAME_WITH_TAGS[site_name]['class']
    attr_type = SITE_NAME_WITH_TAGS[site_name].get('type', '')
    watchers_count = soup.find(
        tag, attrs={"class": attr_class, "type": attr_type})
    if not watchers_count:
        return 'unavailable'
    watchers_count = remove_unnecessary(watchers_count, site_name)
    return watchers_count


def create_browser(url):
    # sudo apt install chromium-chromedriver

    options = webdriver.ChromeOptions()
    options.add_argument('--headless')
    browser = webdriver.Chrome(options=options)
    browser.get(url)
    return browser


def get_pikabu_watchers(url):

    browser = create_browser(url)
    generated_html = browser.page_source
    browser.quit()
    soup = BeautifulSoup(generated_html, 'html.parser')
    watchers_tag = soup.find('div', attrs={"class": 'story__views hint'})
    if not watchers_tag:
        return 'unavailable'
    watchers_count = ''.join(watchers_tag['aria-label'].split()[:-1])
    return watchers_count


def get_youtube_watchers(url):
    browser = create_browser(url)
    wait = WebDriverWait(browser, 10)
    element = wait.until(EC.presence_of_element_located(
        (By.CSS_SELECTOR, "span.yt-view-count-renderer"))).text
    watchers_count = re.sub('\D', '', ''.join(element.split()[:-1]))
    browser.quit()
    return watchers_count


def get_response(url: str, allow_redirects=True):
    headers = {"User-Agent": 'Mozilla/5.0 (X11 Linux x86_64) AppleWebKit/537.36 \
            (KHTML, like Gecko) Chrome/51.0.2704.103 Safari/537.36'}
    response = requests.get(
        url,   headers=headers,
        allow_redirects=allow_redirects,  timeout=5)
    response.raise_for_status()
    if response.status_code != 200:
        raise requests.exceptions.HTTPError
    return response


def csv_dict_reader(file_name: str, key_field):
    result_table = []
    with open(file_name) as file_obj:
        reader = csv.DictReader(file_obj, delimiter=',')
        for line in reader:
            result_table.append(line)
    return result_table


def parse_url(url):
    try:
        watchers_count = ''
        # habr, rutube, pornhub
        for site_name in SITE_NAME_WITH_TAGS:
            if site_name in url:
                response = get_response(url)
                watchers_count = get_watchers_with_tag(
                    response, site_name)

        # pikabu
        if 'pikabu' in url:
            watchers_count = get_pikabu_watchers(url)

        # youtube
        if 'youtube' in url:
            watchers_count = get_youtube_watchers(url)

    except (Timeout, ConnectTimeout, HTTPError,
            RequestException, KeyError) as ex:
        print(f'{url} - {ex}')
        watchers_count = 'unavailable'
    except Exception as ex:
        print(f'{url} - {ex}')
        watchers_count = 'unavailable'
    return watchers_count, int(
        datetime.now().timestamp())


def is_row_fresh(uploaded_row,
                 parsed_row_from_file):

    if parsed_row_from_file['url'] == uploaded_row['url']:
        if parsed_row_from_file['parsed_date']:
            time_difference = datetime.now().timestamp(
            ) - float(parsed_row_from_file['parsed_date'])
            if time_difference <= 172800:
                return True
    return False


def csv_parser(uploaded_sheet_file=UPLOADED_GSHEET_FILE,
               parsed_file_name=PARSED_DATA_SET_FILE):
    loaded_csv_data = csv_dict_reader(uploaded_sheet_file, 'N')
    # first load
    is_first = not Path(PARSED_DATA_SET_FILE).exists()
    if not is_first:
        parsed_data = csv_dict_reader(parsed_file_name, 'N')

    for row_number, uploaded_row in enumerate(loaded_csv_data):
        if not is_first:
            parsed_row_from_file = parsed_data[row_number]
            if is_row_fresh(uploaded_row, parsed_row_from_file):
                loaded_csv_data[row_number]['watchers_count'] = parsed_row_from_file['watchers_count']
                loaded_csv_data[row_number]['parsed_date'] = parsed_row_from_file['parsed_date']
                loaded_csv_data[row_number]['rechecked'] = False
                continue

        watchers_count, parsed_date = parse_url(uploaded_row['url'])
        loaded_csv_data[row_number]['watchers_count'] = watchers_count
        loaded_csv_data[row_number]['parsed_date'] = parsed_date
        loaded_csv_data[row_number]['rechecked'] = True

        # time.sleep(randrange(1, 4))
    write_dictlist_to_csv(loaded_csv_data, PARSED_DATA_SET_FILE)

    # write_dictlist_to_csv(loaded_csv_data, PARSED_DATA_SET_FILE)


def write_to_gsheet(parsed_file_name=PARSED_DATA_SET_FILE,
                    auth_json_file=GSHEET_KEY_FILE,
                    table_url=TABLE_URL):

    gc = gspread.service_account(filename=auth_json_file)
    sh = gc.open_by_url(table_url)
    loaded_csv_data = csv_dict_reader(parsed_file_name, 'N')
    watchers_list = []
    for row in loaded_csv_data:
        watchers_list.append([row['watchers_count']])

    first_cell = f'D{loaded_csv_data[0]["N"]}'
    end_cell = f'D{loaded_csv_data[-1]["N"]}'

    sh.sheet1.update(f'{first_cell}:{end_cell}', watchers_list)


def main():
    start_time = datetime.now()
    print('-------------------------')
    print(start_time)
    print('-------------------------')

    csv_file_name = UPLOADED_GSHEET_FILE
    write_list_to_csv(['url'], get_url_from_gsheet(TABLE_URL), csv_file_name)
    csv_parser()
    write_to_gsheet()

    print('-------------------------')
    print(datetime.now() - start_time)
    print('-------------------------')


if __name__ == '__main__':
    main()
