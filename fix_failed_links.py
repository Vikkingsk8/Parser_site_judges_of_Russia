import asyncio
import aiohttp
import pandas as pd
from bs4 import BeautifulSoup
from urllib.parse import urljoin
from tqdm.asyncio import tqdm as tqdm_asyncio
import time
import re
import random
import logging
from typing import List, Dict, Optional, Set
import sys
import backoff
from aiohttp import ClientSession, TCPConnector, ClientTimeout

# Настройка логирования
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s', datefmt='%H:%M:%S')
logger = logging.getLogger(__name__)

# Базовый URL
BASE_URL = "https://xn--d1aiaa2aleeao4h.xn--p1ai/"

class FailedUrlsParser:
    def __init__(self, max_concurrent_tasks: int = 15):
        self.base_url = BASE_URL
        self.session = None
        self.max_concurrent_tasks = max_concurrent_tasks
        self.semaphore = asyncio.Semaphore(max_concurrent_tasks)

        self.user_agents = [
            'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36',
            'Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:109.0) Gecko/20100101 Firefox/121.0',
            'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36',
            'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/121.0.0.0 Safari/537.36',
            'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/119.0.0.0 Safari/537.36',
        ]

        self.month_map = {
            'января': '01', 'февраля': '02', 'марта': '03', 'апреля': '04',
            'мая': '05', 'июня': '06', 'июля': '07', 'августа': '08',
            'сентября': '09', 'октября': '10', 'ноября': '11', 'декабря': '12'
        }

        # Статистика
        self.stats = {
            'total_urls': 0,
            'successfully_parsed': 0,
            'failed_parsed': 0,
            'failed_urls': []
        }

    async def __aenter__(self):
        timeout = ClientTimeout(total=120, connect=30, sock_read=60)
        connector = TCPConnector(
            ssl=False,
            limit=20,
            limit_per_host=5,
            force_close=True,
            enable_cleanup_closed=True
        )

        self.session = ClientSession(
            connector=connector,
            timeout=timeout,
            cookie_jar=aiohttp.CookieJar(),
            headers=self.get_random_headers()
        )
        logger.info("Инициализация сессии для парсинга failed URLs...")
        return self

    async def __aexit__(self, exc_type, exc_val, exc_tb):
        if self.session:
            await self.session.close()

    def get_random_headers(self) -> dict:
        return {
            'User-Agent': random.choice(self.user_agents),
            'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8',
            'Accept-Language': 'ru-RU,ru;q=0.9,en-US;q=0.8,en;q=0.7',
            'Accept-Encoding': 'gzip, deflate, br',
            'Connection': 'keep-alive',
            'Upgrade-Insecure-Requests': '1',
            'Cache-Control': 'max-age=0',
            'Referer': self.base_url
        }

    @backoff.on_exception(
        backoff.expo,
        (aiohttp.ClientError, asyncio.TimeoutError),
        max_tries=5,
        max_time=60
    )
    async def fetch_with_retry(self, url: str, max_retries: int = 5) -> Optional[str]:
        for attempt in range(max_retries):
            try:
                delay = random.uniform(1.0, 3.0)
                await asyncio.sleep(delay)

                async with self.semaphore:
                    async with self.session.get(
                        url,
                        headers=self.get_random_headers(),
                        allow_redirects=True
                    ) as response:
                        if response.status == 200:
                            text = await response.text()
                            return text
                        elif response.status == 429:
                            wait_time = 10 * (attempt + 1)
                            logger.warning(f"Получен статус 429 для {url}. Ждем {wait_time} сек.")
                            await asyncio.sleep(wait_time)
                        elif response.status == 404:
                            logger.debug(f"Страница не найдена: {url}")
                            return None
                        elif response.status >= 500:
                            logger.warning(f"Ошибка сервера {response.status} для {url}")
                            await asyncio.sleep(5 * (attempt + 1))
                        else:
                            logger.warning(f"Неожиданный статус {response.status} для {url}")
                            await asyncio.sleep(3 * (attempt + 1))

            except (aiohttp.ClientError, asyncio.TimeoutError) as e:
                if attempt == max_retries - 1:
                    logger.warning(f"Ошибка '{type(e).__name__}' при загрузке {url} после {max_retries} попыток")
                    self.stats['failed_urls'].append(url)
                await asyncio.sleep(3 * (attempt + 1))
            except Exception as e:
                logger.error(f"Неожиданная ошибка при загрузке {url}: {e}")
                await asyncio.sleep(5)

        return None

    def _clean_text_for_excel(self, text: str) -> str:
        """Очищает текст от символов, недопустимых в Excel"""
        if not isinstance(text, str):
            return text

        cleaned = re.sub(r'[\x00-\x08\x0B\x0C\x0E-\x1F\x7F-\x9F]', '', text)
        cleaned = cleaned.replace('\xa0', ' ')
        cleaned = cleaned.replace('&nbsp;', ' ')
        cleaned = re.sub(r' +', ' ', cleaned)
        return cleaned.strip()

    def _is_valid_birth_year(self, year_str: str) -> bool:
        try:
            year = int(year_str)
            return 1920 <= year <= 2005
        except (ValueError, TypeError):
            return False

    def _extract_date_of_birth(self, text: str) -> str:
        """Извлечение даты рождения из текста"""
        if not text:
            return ''

        text_beginning = text[:300].strip()
        text_beginning = re.sub(r'\s+', ' ', text_beginning)
        text_lower = text_beginning.lower()

        # 1. ПОЛНАЯ ДАТА С МЕСЯЦЕМ: "12 июня 1990 года рождения"
        full_date_with_month = r'(\d{1,2})\s+([а-я]+)\s+(\d{4})\s+года\s+рождения'
        match = re.search(full_date_with_month, text_lower)
        if match:
            day, month_str, year = match.groups()
            month = self.month_map.get(month_str)
            if month and self._is_valid_birth_year(year):
                return f"{int(day):02d}.{month}.{year}"

        # 2. ПОЛНАЯ ДАТА В ФОРМАТЕ DD.MM.YYYY: "12.07.1990 года рождения"
        full_date_dd_mm_yyyy = r'(\d{1,2})[\.\-](\d{1,2})[\.\-](\d{4})\s+года\s+рождения'
        match = re.search(full_date_dd_mm_yyyy, text_lower)
        if match:
            day, month, year = match.groups()
            if self._is_valid_birth_year(year):
                return f"{int(day):02d}.{int(month):02d}.{year}"

        # 3. ТОЛЬКО ГОД: "1990 года рождения"
        year_only = r'(\d{4})\s+года\s+рождения'
        match = re.search(year_only, text_lower)
        if match:
            year = match.group(1)
            if self._is_valid_birth_year(year):
                return year

        # 4. ГОД С СОКРАЩЕНИЕМ: "1990 г.р." или "1990 г. р."
        year_gr = r'(\d{4})\s*г\.\s*р\.'
        match = re.search(year_gr, text_lower)
        if match:
            year = match.group(1)
            if self._is_valid_birth_year(year):
                return year

        # 5. "РОДИЛСЯ": "Родился 12 июня 1990 года" или "Родилась 12.06.1990"
        born_patterns = [
            (r'родился?\s+(\d{1,2})\s+([а-я]+)\s+(\d{4})\s+года', 'month'),
            (r'родился?\s+(\d{1,2})[\.\-](\d{1,2})[\.\-](\d{4})', 'date'),
            (r'родился?\s+в\s+(\d{4})\s+году', 'year'),
        ]

        for pattern, pattern_type in born_patterns:
            match = re.search(pattern, text_lower)
            if match:
                if pattern_type == 'month':
                    day, month_str, year = match.groups()
                    month = self.month_map.get(month_str)
                    if month and self._is_valid_birth_year(year):
                        return f"{int(day):02d}.{month}.{year}"
                elif pattern_type == 'date':
                    day, month, year = match.groups()
                    if self._is_valid_birth_year(year):
                        return f"{int(day):02d}.{int(month):02d}.{year}"
                else:  # pattern_type == 'year'
                    year = match.group(1)
                    if self._is_valid_birth_year(year):
                        return year

        # 6. "РОД.": "род. 1990"
        rod_pattern = r'род\.\s*(\d{4})'
        match = re.search(rod_pattern, text_lower)
        if match:
            year = match.group(1)
            if self._is_valid_birth_year(year):
                return year

        return ''

    def _parse_breadcrumbs(self, soup: BeautifulSoup) -> Dict[str, str]:
        """Парсинг хлебных крошек для получения типа суда, региона, названия суда и его URL"""
        result = {
            'region': '',
            'court_type': '',
            'court': '',
            'court_url': ''
        }
        
        breadcrumbs_div = soup.find('div', class_='breadcrumbs')
        if breadcrumbs_div:
            links = breadcrumbs_div.find_all('a')
            
            # Последняя ссылка ведёт на страницу суда
            if links:
                court_link = links[-1]
                result['court'] = court_link.get_text(strip=True)
                href = court_link.get('href')
                if href:
                    result['court_url'] = urljoin(self.base_url, href)
            
            # Тип суда — предпоследняя ссылка
            if len(links) >= 2:
                result['court_type'] = links[-2].get_text(strip=True)
            
            # Регион — пред-предпоследняя
            if len(links) >= 3:
                result['region'] = links[-3].get_text(strip=True)
        
        return result

    async def _get_judge_status_from_court_page(self, court_url: str, judge_url: str) -> Optional[str]:
        """
        Загружает страницу суда и определяет статус судьи по блоку, в котором находится ссылка.
        Возвращает 'Действующий', 'В отставке' или None, если не найдено.
        """
        html = await self.fetch_with_retry(court_url, max_retries=2)
        if not html:
            return None
        
        soup = BeautifulSoup(html, 'html.parser')
        
        # Проверяем блок действующих судей
        container = soup.find('div', id='sudiilistview')
        if container:
            items_div = container.find('div', class_='items')
            if items_div:
                for a in items_div.find_all('a', class_='browser_link', href=True):
                    full_url = urljoin(self.base_url, a['href'])
                    if full_url == judge_url:
                        return 'Действующий'
        
        # Проверяем блок судей в отставке
        container = soup.find('div', id='sudiilistview2')
        if container:
            items_div = container.find('div', class_='items')
            if items_div:
                for a in items_div.find_all('a', class_='browser_link', href=True):
                    full_url = urljoin(self.base_url, a['href'])
                    if full_url == judge_url:
                        return 'В отставке'
        
        return None

    async def parse_judge_profile(self, url: str) -> Optional[Dict]:
        """Парсинг профиля судьи из failed_urls"""
        try:
            html = await self.fetch_with_retry(url, max_retries=3)
            if not html:
                self.stats['failed_parsed'] += 1
                return None

            soup = BeautifulSoup(html, 'html.parser')
            content_div = soup.find('div', id='content')
            if not content_div:
                self.stats['failed_parsed'] += 1
                return None

            # Парсим хлебные крошки (теперь с URL суда)
            breadcrumbs_data = self._parse_breadcrumbs(soup)

            data = {
                'region': breadcrumbs_data['region'],
                'court_type': breadcrumbs_data['court_type'],
                'court': breadcrumbs_data['court'],
                'full_name': '',
                'status': '',  # будет заполнено
                'date_of_birth': '',
                'judge_info': '',
                'profile_url': url
            }

            # Извлечение ФИО
            h1_tag = content_div.find('h1')
            if h1_tag:
                data['full_name'] = h1_tag.get_text(strip=True)

            # Извлечение информации из вкладки #type-2
            info_tab = soup.find('div', id='type-2')
            if info_tab:
                paragraphs = info_tab.find_all('p')
                all_text_parts = []
                for p in paragraphs:
                    text = p.get_text(strip=False)
                    if text:
                        text = re.sub(r'\s+', ' ', text)
                        all_text_parts.append(text)
                if all_text_parts:
                    data['judge_info'] = '\n'.join(all_text_parts)
            else:
                sudya_info = content_div.find('div', id='sudya_info')
                if sudya_info:
                    data['judge_info'] = sudya_info.get_text(strip=True)

            # Извлечение даты рождения
            if data['judge_info']:
                data['date_of_birth'] = self._extract_date_of_birth(data['judge_info'])
            if not data['date_of_birth']:
                full_content_text = content_div.get_text()
                data['date_of_birth'] = self._extract_date_of_birth(full_content_text[:300])

            # ОПРЕДЕЛЕНИЕ СТАТУСА через страницу суда
            if breadcrumbs_data['court_url']:
                status_from_court = await self._get_judge_status_from_court_page(
                    breadcrumbs_data['court_url'], url
                )
                if status_from_court:
                    data['status'] = status_from_court
                else:
                    # Если не нашли на странице суда, оставляем пустым или ставим 'Действующий' по умолчанию
                    data['status'] = 'Действующий'
            else:
                # Нет URL суда — ставим по умолчанию
                data['status'] = 'Действующий'

            # Очистка данных
            for key in data:
                if data[key] and isinstance(data[key], str):
                    data[key] = self._clean_text_for_excel(data[key])

            self.stats['successfully_parsed'] += 1
            return data

        except Exception as e:
            logger.warning(f"Ошибка при парсинге профиля {url}: {e}")
            self.stats['failed_parsed'] += 1
            self.stats['failed_urls'].append(url)
            return None

    def load_failed_urls(self, filename: str) -> List[str]:
        """Загрузка URL из файла failed_urls.txt"""
        try:
            with open(filename, 'r', encoding='utf-8') as f:
                urls = [line.strip() for line in f if line.strip()]
            logger.info(f"Загружено {len(urls)} URL из {filename}")
            return urls
        except FileNotFoundError:
            logger.error(f"Файл {filename} не найден")
            return []

    async def process_failed_urls(self, failed_urls_file: str):
        """Основной метод для парсинга failed URLs"""
        urls = self.load_failed_urls(failed_urls_file)
        self.stats['total_urls'] = len(urls)

        if not urls:
            return []

        # Создаем задачи
        tasks = [self.parse_judge_profile(url) for url in urls]
        
        # Обрабатываем батчами для лучшего контроля
        results = []
        batch_size = 50
        
        for i in range(0, len(tasks), batch_size):
            batch = tasks[i:i+batch_size]
            batch_results = await asyncio.gather(*batch, return_exceptions=True)
            
            for result in batch_results:
                if isinstance(result, dict):
                    results.append(result)
                elif isinstance(result, Exception):
                    logger.warning(f"Ошибка в задаче: {result}")
                    self.stats['failed_parsed'] += 1
            
            # Прогресс
            logger.info(f"Обработано {min(i+batch_size, len(tasks))} из {len(tasks)} URL")
            
            # Небольшая пауза между батчами
            if i + batch_size < len(tasks):
                await asyncio.sleep(random.uniform(2, 4))
        
        return results

    async def save_results_to_excel(self, data: List[Dict], output_file: str):
        """Сохранение результатов в Excel-файл с переименованными колонками"""
        if not data:
            logger.warning("Нет данных для сохранения")
            return

        df = pd.DataFrame(data)
        
        # Переименовываем колонки как в исходном коде
        rename_map = {
            'region': 'Регион',
            'court_type': 'Тип суда',
            'court': 'Название суда',
            'full_name': 'ФИО Судьи',
            'date_of_birth': 'Дата рождения',
            'status': 'Статус',
            'judge_info': 'Информация о судье (Био)',
            'profile_url': 'Ссылка'
        }
        
        # Применяем переименование только для существующих колонок
        df.rename(columns={k: v for k, v in rename_map.items() if k in df.columns}, inplace=True)
        
        # Определяем порядок колонок
        column_order = ['Регион', 'Тип суда', 'Название суда', 'ФИО Судьи', 
                       'Дата рождения', 'Статус', 'Информация о судье (Био)', 'Ссылка']
        
        # Оставляем только существующие колонки в нужном порядке
        existing_columns = [col for col in column_order if col in df.columns]
        df = df[existing_columns]
        
        try:
            df.to_excel(output_file, index=False, engine='openpyxl')
            logger.info(f"Результаты сохранены в {output_file}")
            logger.info(f"Всего записей: {len(df)}")
        except Exception as e:
            logger.error(f"Ошибка при сохранении в Excel: {e}")
            # Пробуем сохранить в CSV как запасной вариант
            csv_file = output_file.replace('.xlsx', '.csv')
            df.to_csv(csv_file, index=False, encoding='utf-8-sig')
            logger.info(f"Данные сохранены в CSV: {csv_file}")

    async def run(self, failed_urls_file: str = 'failed_urls_16_56.txt', output_file: str = 'parsed_failed_judges.xlsx'):
        """Запуск полного цикла парсинга failed URLs"""
        logger.info("Запуск парсинга failed URLs...")

        async with self:
            results = await self.process_failed_urls(failed_urls_file)

            # Сохраняем результаты
            await self.save_results_to_excel(results, output_file)

            # Выводим статистику
            logger.info("="*50)
            logger.info("СТАТИСТИКА ПАРСИНГА FAILED URLS:")
            logger.info(f"Всего URL для обработки: {self.stats['total_urls']}")
            logger.info(f"Успешно обработано: {self.stats['successfully_parsed']}")
            logger.info(f"Не удалось обработать: {self.stats['failed_parsed']}")
            
            success_rate = (self.stats['successfully_parsed'] / max(self.stats['total_urls'], 1)) * 100
            logger.info(f"Процент успеха: {success_rate:.1f}%")
            
            if self.stats['failed_urls']:
                logger.info(f"Оставшиеся неудачные URL: {len(self.stats['failed_urls'])}")
                # Сохраняем оставшиеся неудачные URL
                with open('remaining_failed_urls.txt', 'w', encoding='utf-8') as f:
                    for url in self.stats['failed_urls']:
                        f.write(url + '\n')
                logger.info("Оставшиеся неудачные URL сохранены в 'remaining_failed_urls.txt'")
            logger.info("="*50)


# Функция для запуска парсера
async def main():
    parser = FailedUrlsParser(max_concurrent_tasks=10)
    # Можно указать другие имена файлов при необходимости
    await parser.run(
        failed_urls_file='failed_urls_16_56.txt', 
        output_file='parsed_failed_judges1.xlsx'
    )

if __name__ == '__main__':
    if sys.platform == 'win32':
        asyncio.set_event_loop_policy(asyncio.WindowsSelectorEventLoopPolicy())
    
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        logger.info("\nПрограмма завершена пользователем.")
    except Exception as e:
        logger.error(f"Фатальная ошибка: {e}")
