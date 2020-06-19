import gspread
import csv
import requests
from bs4 import BeautifulSoup
from typing import List
from collections import OrderedDict
from requests.exceptions import Timeout, ConnectTimeout, HTTPError, RequestException
TABLE_URL = 'https://docs.google.com/spreadsheets/d/1UK-aoLDoJ724KGUN0AzgOLKW1S05W2FLZmSYHdjjYig/'


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
                        auth_json_file='key.json') -> List[str]:
    gc = gspread.service_account(filename=auth_json_file)
    sh = gc.open_by_url(table_url)
    return sh.sheet1.col_values(1)[2:]


def get_habr_seen_count(response):
    soup = BeautifulSoup(response.text, 'lxml')
    seems_count = soup.find(
        "span", class_="post-stats__views-count").text.replace('k', '000')
    return int(seems_count.replace(',', ''))


def get_response(url: str, use_headers=False, allow_redirects=True):
    #     headers = {}
    response = requests.get(
        url,  # headers=headers,
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

            seems_count = ''
            if 'habr.com' in row['url']:
                response = get_response(row['url'])
                seems_count = get_habr_seen_count(response)
            csv_data[row_number]['seems_count'] = seems_count
            # break
        except (Timeout, ConnectTimeout, HTTPError, RequestException) as ex:
            print(f'{row["url"]} - {ex}')
            csv_data[row_number]['seems_count'] = 'unavailable'
        except Exception as e:
            print(e)

    # print(csv_data[0])
        write_dictlist_to_csv(csv_data, 'parsed.csv')


def main():
    csv_file_name = 'sheet.csv'
    # write_list_to_csv(['url'], get_url_from_gsheet(TABLE_URL), csv_file_name)
    csv_parser()


if __name__ == '__main__':
    main()
