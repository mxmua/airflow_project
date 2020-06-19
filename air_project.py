import gspread
import csv
import time
import requests
import re
from bs4 import BeautifulSoup
from random import randrange
from typing import List
from collections import OrderedDict
from requests.exceptions import Timeout, ConnectTimeout, HTTPError, RequestException
TABLE_URL = 'https://docs.google.com/spreadsheets/d/1UK-aoLDoJ724KGUN0AzgOLKW1S05W2FLZmSYHdjjYig/'


SITE_NAME_WITH_TAGS = {
    'habr': {'tag': 'span', 'class': 'post-stats__views-count'},
    'rutube': {'tag': 'span', 'class': 'video-info-card__view-count'},
    'youtube': {'tag': 'div', 'class': 'watch-view-count'},
    'pikabu': {'tag': 'div', 'class': 'story__views hint'},
    'pornhub': {'tag': 'span', 'class': 'count'},
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
                row.insert(0, row_number + 1)
            write.writerow(row)


def write_dictlist_to_csv(data_list,
                          file_name):
    with open(file_name, 'w+',  newline="", encoding='utf-8') as file:
        columns = [row_name for row_name in data_list[0]]
        writer = csv.DictWriter(file, fieldnames=columns)
        writer.writeheader()
        writer.writerows(data_list)


def get_url_from_gsheet(table_url: str,
                        auth_json_file='key.json'):
    gc = gspread.service_account(filename=auth_json_file)
    sh = gc.open_by_url(table_url)
    return sh.sheet1.col_values(1)[2:]


def remove_unnecessary(count, site_name):
    if site_name == 'youtube':
        count = ''.join(count.split()[:-1])
    if site_name == 'rutube':
        count = count.replace(',', '')
    if site_name == 'pornhub':
        count = ''.join(count.split())
    return count


def get_watchers_with_tag(response, site_name):
    soup = BeautifulSoup(response.content, 'html.parser')
    watchers_count = soup.find(
        SITE_NAME_WITH_TAGS[site_name]['tag'], attrs={"class": SITE_NAME_WITH_TAGS[site_name]['class']})
    if not watchers_count:
        return 'unavailable'
    watchers_count = remove_unnecessary(watchers_count.text, site_name)
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


def csv_parser(csv_file_name='sheet.csv'):
    csv_data = csv_dict_reader(csv_file_name, 'N')

    for row_number, row in enumerate(csv_data):
        try:
            watchers_count = ''
            for site_name in SITE_NAME_WITH_TAGS:
                if site_name in row['url']:
                    response = get_response(row['url'])
                    watchers_count = get_watchers_with_tag(
                        response, site_name)
                    print(row['url'], watchers_count)
            csv_data[row_number]['watchers_count'] = watchers_count
        except (Timeout, ConnectTimeout, HTTPError, RequestException) as ex:
            print(f'{row["url"]} - {ex}')
            csv_data[row_number]['watchers_count'] = 'unavailable'
        except Exception as e:
            print(e)

        write_dictlist_to_csv(csv_data, 'parsed.csv')
    # time.sleep(randrange(1, 4))


def main():
    csv_file_name = 'sheet.csv'
    write_list_to_csv(['url'], get_url_from_gsheet(TABLE_URL), csv_file_name)
    csv_parser()


if __name__ == '__main__':
    main()
