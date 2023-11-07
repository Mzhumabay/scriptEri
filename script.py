import os
import json
import requests
from time import sleep

def download_zip_files(successful_tables):
    # Создаем папку для сохранения файлов
    period = latest_period['name']
    folder_name = f'zip_files_{period}'
    if not os.path.exists(folder_name):
        os.makedirs(folder_name)

    for key, table_guid in successful_tables.items():
        unit_name, index_name = key
        subfolder = f'{folder_name}/{unit_name}/'
        if not os.path.exists(subfolder):
            os.makedirs(subfolder)

        filename = f'{subfolder}/{index_name}.zip'
        download_url = f'https://stat.gov.kz/api/sbr/download?bucket=SBR_UREQUEST&guid={table_guid}'

        try:
            response = requests.get(download_url)
            response.raise_for_status()
        except requests.exceptions.RequestException as e:
            print(f"Ошибка запроса для файла {filename}: {e}")
            continue

        with open(filename, 'wb') as file:
            file.write(response.content)
            print(f"Файл {filename} успешно загружен")

    print("Загрузка завершена")

def fetch_status(status_dict):
    processed_data = {}
    unprocessed_data = {}

    for status_key, status_id in status_dict.items():
        status_url = f'https://stat.gov.kz/api/sbr/requestResult/{status_id}/ru'

        try:
            response = requests.get(status_url)
            response.raise_for_status()
        except requests.exceptions.RequestException as e:
            print(f"Ошибка запроса: {e}")
            continue

        response_data = response.json()
        print(response_data)

        if response_data['description'] == 'Обработан':
            processed_data[status_key] = response_data['obj']['fileGuid']
        else:
            unprocessed_data[status_key] = f"статус {status_id} не обработан"

    return processed_data, unprocessed_data


def parse_json(file_path):
    with open(file_path, 'r', encoding='utf-8') as json_file:
        data = json.load(json_file)

    return data[0], data[1]

def get_latest_period():
    url = 'https://old.stat.gov.kz/api/rcut/ru'
    try:
        response = requests.get(url)
        response.raise_for_status()
    except requests.exceptions.RequestException as e:
        print(f"Ошибка запроса: {e}")
        sleep(1)
        return get_latest_period(url)

    return response.json()[0]

def fetch_status_id(unit, index, latest_period, url_post_request):
    data = {
        "conditions": [
            {
                "classVersionId": 2153,
                "itemIds": [unit['id']]
            }, {
                "classVersionId": 213,
                "itemIds": [741880]
            }, {
                "classVersionId": 1989,
                "itemIds": [39354, 39355, 39356]
            }, {
                "classVersionId": 4855,
                "itemIds": [index['number']]
            }
        ],
        "cutId": latest_period['id']
    }

    try:
        response = requests.post(url_post_request, json=data)
        response.raise_for_status()
    except requests.exceptions.RequestException as e:
        print(f"Ошибка запроса: {e}")
        sleep(1)  
        return fetch_status_id(unit, index, latest_period, url_post_request)

    response_data = response.json()
    print(response_data)

    return response_data['obj']

def main(Typeof_legal_unit, Okad_index, latest_period):
    url_post_request = 'https://old.stat.gov.kz/api/sbr/request/?api'
    objects = {}

    for unit in Typeof_legal_unit['data']:
        for index in Okad_index['data']:
            objects[(unit['name'], index['name'])] = fetch_status_id(unit, index, latest_period, url_post_request)
            sleep(60)  # Ожидание одну минуту перед следующим запросом

    processed_status_dict, unprocessed_status_dict = fetch_status(objects)
    download_zip_files(processed_status_dict)
    if unprocessed_status_dict:
        print(unprocessed_status_dict)

if __name__ == "__main__":

    Typeof_legal_unit, Okad_index = parse_json('data.json')
    latest_period = get_latest_period()

    main(Typeof_legal_unit, Okad_index, latest_period)