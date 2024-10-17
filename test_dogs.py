import os
import logging
from typing import List, Optional
import requests
import pytest
from dotenv import load_dotenv
from ratelimit import limits, sleep_and_retry

# Настройка логирования
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

# Загрузка переменных окружения
load_dotenv()
YANDEX_TOKEN = os.getenv('YANDEX_TOKEN')
if not YANDEX_TOKEN:
    raise ValueError("YANDEX_TOKEN не найден в переменных окружения")


class YaUploader:
    BASE_URL = 'https://cloud-api.yandex.net/v1/disk/resources'

    def __init__(self, token: str):
        self.token = token
        self.headers = {
            'Content-Type': 'application/json',
            'Accept': 'application/json',
            'Authorization': f'OAuth {self.token}'
        }

    @sleep_and_retry
    @limits(calls=10, period=1)  # Ограничение: 10 запросов в секунду
    def _make_request(self, method: str, url: str, **kwargs):
        response = requests.request(method, url, headers=self.headers, **kwargs)
        response.raise_for_status()
        return response

    def create_folder(self, path: str) -> None:
        try:
            self._make_request('PUT', f'{self.BASE_URL}?path={path}')
            logger.info(f"Папка '{path}' успешно создана")
        except requests.exceptions.HTTPError as e:
            if e.response.status_code != 409:  # 409 означает, что папка уже существует
                logger.error(f"Ошибка при создании папки '{path}': {e}")
                raise

    def upload_photo(self, folder: str, url: str, name: str) -> None:
        params = {"path": f'{folder}/{name}', "url": url, "overwrite": "true"}
        try:
            self._make_request('POST', f'{self.BASE_URL}/upload', params=params)
            logger.info(f"Файл '{name}' успешно загружен в папку '{folder}'")
        except requests.exceptions.HTTPError as e:
            logger.error(f"Ошибка при загрузке файла '{name}': {e}")
            raise


class DogAPI:
    BASE_URL = 'https://dog.ceo/api'

    @staticmethod
    @sleep_and_retry
    @limits(calls=10, period=1)  # Ограничение: 10 запросов в секунду
    def _make_request(url: str) -> dict:
        response = requests.get(url)
        response.raise_for_status()
        return response.json()

    @classmethod
    def get_sub_breeds(cls, breed: str) -> List[str]:
        data = cls._make_request(f'{cls.BASE_URL}/breed/{breed}/list')
        return data.get('message', [])

    @classmethod
    def get_random_image(cls, breed: str, sub_breed: Optional[str] = None) -> str:
        if sub_breed:
            url = f'{cls.BASE_URL}/breed/{breed}/{sub_breed}/images/random'
        else:
            url = f'{cls.BASE_URL}/breed/{breed}/images/random'
        data = cls._make_request(url)
        return data['message']


def upload_dog_images(breed: str) -> None:
    ya_uploader = YaUploader(YANDEX_TOKEN)
    folder = 'dog_images'
    ya_uploader.create_folder(folder)

    sub_breeds = DogAPI.get_sub_breeds(breed)
    if not sub_breeds:
        sub_breeds = [None]  # Если нет подпород, загрузим одно изображение основной породы

    for sub_breed in sub_breeds:
        try:
            image_url = DogAPI.get_random_image(breed, sub_breed)
            file_name = f"{breed}{'_' + sub_breed if sub_breed else ''}.jpg"
            ya_uploader.upload_photo(folder, image_url, file_name)
        except Exception as e:
            logger.error(f"Ошибка при обработке породы {breed}{f' (подпорода {sub_breed})' if sub_breed else ''}: {e}")


@pytest.mark.parametrize('breed', ['doberman', 'bulldog', 'collie'])
def test_upload_dog_images(breed):
    upload_dog_images(breed)

    # Проверка загруженных файлов
    ya_uploader = YaUploader(YANDEX_TOKEN)
    response = ya_uploader._make_request('GET', f'{YaUploader.BASE_URL}?path=/dog_images')

    uploaded_files = response.json()['_embedded']['items']
    sub_breeds = DogAPI.get_sub_breeds(breed)
    expected_count = max(len(sub_breeds), 1)

    assert len(uploaded_files) == expected_count, f"Ожидалось {expected_count} файлов, загружено {len(uploaded_files)}"

    for file in uploaded_files:
        assert file['type'] == 'file', f"Ожидался тип 'file', получен '{file['type']}'"
        assert file['name'].startswith(breed), f"Имя файла '{file['name']}' должно начинаться с '{breed}'"


if __name__ == "__main__":
    breed = input("Введите породу собаки: ")
    upload_dog_images(breed)