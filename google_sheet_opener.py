import gspread
import csv
from typing import List

TABLE_URL = 'https://docs.google.com/spreadsheets/d/1UK-aoLDoJ724KGUN0AzgOLKW1S05W2FLZmSYHdjjYig/'


def write_csv(table_headers: List[str], table_data, file_name: str):
    with open(file_name, 'w+',  newline="", encoding='utf-8') as file:
        writer = csv.writer(file)
        writer.writerows([table_headers])
        for row_number, row in enumerate(table_data):
            writer.writerow([row_number+1, row])


def get_url_from_gsheet(table_url: str,
                        auth_json_file='key.json') -> List[str]:
    gc = gspread.service_account(filename=auth_json_file)
    sh = gc.open_by_url(table_url)
    return sh.sheet1.col_values(1)[2:]


def main():
    csv_file_name = 'sheet.csv'
    write_csv(['N', 'url'], get_url_from_gsheet(TABLE_URL), csv_file_name)


if __name__ == '__main__':
    main()
