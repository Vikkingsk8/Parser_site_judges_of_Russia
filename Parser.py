import asyncio
import aiohttp
import pandas as pd
from bs4 import BeautifulSoup
from urllib.parse import urljoin
from tqdm.asyncio import tqdm
import time
import re
import random
import logging
from typing import List, Dict, Optional, Tuple, Set, Any
import sys
import backoff
from aiohttp import ClientSession, TCPConnector, ClientTimeout
import argparse

# Настройка логирования
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s', datefmt='%H:%M:%S')
logger = logging.getLogger(__name__)

# Базовый URL
BASE_URL = "https://xn--d1aiaa2aleeao4h.xn--p1ai/"

class JudgeParser:
    def __init__(
        self, 
        region_start: int = 1,
        region_end: Optional[int] = None,
        max_court_types_per_region=None, 
        max_courts_per_type=None, 
        max_judges_per_court=None,
        max_concurrent_tasks: int = 15
    ):
        self.base_url = BASE_URL
        self.session = None
        self.region_start = region_start
        self.region_end = region_end
        self.max_court_types_per_region = max_court_types_per_region
        self.max_courts_per_type = max_courts_per_type
        self.max_judges_per_court = max_judges_per_court
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
            'total_judges_found': 0,
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
        logger.info("Инициализация сессии...")
        try:
            await self.fetch_with_retry(self.base_url, max_retries=3)
            logger.info("Сессия готова.")
        except Exception as e:
            logger.warning(f"Проверка подключения не удалась: {e}")
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

    async def get_region_urls(self) -> List[Tuple[str, str]]:
        url = urljoin(self.base_url, "suds/index")
        html = await self.fetch_with_retry(url, max_retries=3)
        if not html: 
            return []
            
        soup = BeautifulSoup(html, 'html.parser')
        region_data = []
        region_table = soup.find('table', id='region_table')
        
        if region_table:
            for a_tag in region_table.find_all('a', href=True):
                region_name = a_tag.get_text(strip=True)
                region_url = urljoin(self.base_url, a_tag['href'])
                region_data.append((region_name, region_url))
        
        logger.info(f"Найдено {len(region_data)} регионов.")
        
        # Применяем диапазон регионов
        if self.region_start or self.region_end:
            start_idx = self.region_start - 1 if self.region_start > 0 else 0
            end_idx = self.region_end if self.region_end else len(region_data)
            region_data = region_data[start_idx:end_idx]
            logger.info(f"Выбран диапазон регионов: с {self.region_start} по {end_idx} (всего {len(region_data)})")
        
        return region_data

    async def get_court_types_from_region(self, region_url: str) -> List[Tuple[str, str]]:
        html = await self.fetch_with_retry(region_url)
        if not html: 
            return []
            
        soup = BeautifulSoup(html, 'html.parser')
        court_types = []
        browser_div = soup.find('div', id='browser')
        
        if browser_div:
            for a_tag in browser_div.find_all('a', href=True):
                court_type_name = a_tag.get_text(strip=True)
                type_url = urljoin(self.base_url, a_tag['href'])
                court_types.append((court_type_name, type_url))
        
        if self.max_court_types_per_region: 
            return court_types[:self.max_court_types_per_region]
        return court_types

    async def get_courts_from_type_page(self, type_url: str) -> List[Tuple[str, str]]:
        html = await self.fetch_with_retry(type_url)
        if not html: 
            return []
            
        soup = BeautifulSoup(html, 'html.parser')
        courts = []
        items_div = soup.find('div', class_='items')
        
        if items_div:
            for div in items_div.find_all('div', class_='browser_view_suds'):
                a_tag = div.find('a', class_='browser_link')
                if a_tag:
                    court_name = a_tag.get_text(strip=True)
                    court_url = urljoin(self.base_url, a_tag['href'])
                    courts.append((court_name, court_url))
        
        if self.max_courts_per_type: 
            return courts[:self.max_courts_per_type]
        return courts

    async def get_all_judges_from_court(self, court_url: str, visited_urls: Set[str]) -> List[Tuple[str, str]]:
        if court_url in visited_urls: 
            return []
        visited_urls.add(court_url)
        
        html = await self.fetch_with_retry(court_url)
        if not html: 
            return []
        
        soup = BeautifulSoup(html, 'html.parser')
        judges_with_status = []

        blocks = [
            ('sudiilistview', 'Действующий'), 
            ('sudiilistview2', 'В отставке')
        ]

        found_on_page_urls = set()

        for div_id, status in blocks:
            container = soup.find('div', id=div_id)
            if container:
                items_div = container.find('div', class_='items')
                if items_div:
                    for a_tag in items_div.find_all('a', class_='browser_link', href=True):
                        full_url = urljoin(self.base_url, a_tag['href'])
                        
                        if full_url not in found_on_page_urls:
                            judges_with_status.append((full_url, status))
                            found_on_page_urls.add(full_url)
        
        self.stats['total_judges_found'] += len(found_on_page_urls)

        pager = soup.find('div', class_='pager')
        if pager:
            page_tasks = []
            for page_link in pager.find_all('a', href=True):
                next_page_url = urljoin(court_url, page_link['href'])
                if next_page_url not in visited_urls:
                    task = self.get_all_judges_from_court(next_page_url, visited_urls)
                    page_tasks.append(task)
            
            if page_tasks:
                results = await asyncio.gather(*page_tasks, return_exceptions=True)
                for result in results:
                    if isinstance(result, list):
                        judges_with_status.extend(result)
        
        unique_judges_map = {judge[0]: judge for judge in judges_with_status}
        unique_judges_list = list(unique_judges_map.values())
        
        if self.max_judges_per_court: 
            return unique_judges_list[:self.max_judges_per_court]
        return unique_judges_list

    def _is_valid_birth_year(self, year_str: str) -> bool:
        """Проверяет, является ли строка валидным годом рождения"""
        try:
            year = int(year_str)
            return 1920 <= year <= 2005
        except (ValueError, TypeError):
            return False

    def _extract_date_of_birth(self, text: str) -> str:
        """
        Извлечение даты рождения из текста.
        Ищем ТОЛЬКО по четким паттернам в начале текста (первые 300 символов):
        1. "12 июня 1990 года рождения" или "12.07.1990 года рождения"
        2. "1990 года рождения"
        3. "1990 г. р." или "1990 г.р."
        4. "Родился 12 июня 1990 года" или "Родилась 12.06.1990"
        """
        if not text: 
            return ''
        
        # Берем только первые 300 символов, так как дата рождения всегда в начале
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
            # Родился с месяцем: "родился 12 июня 1990 года"
            r'родился?\s+(\d{1,2})\s+([а-я]+)\s+(\d{4})\s+года',
            # Родился с цифровой датой: "родился 12.06.1990"
            r'родился?\s+(\d{1,2})[\.\-](\d{1,2})[\.\-](\d{4})',
            # Родился только с годом: "родился в 1990 году"
            r'родился?\s+в\s+(\d{4})\s+году',
        ]
        
        for pattern in born_patterns:
            match = re.search(pattern, text_lower)
            if match:
                if len(match.groups()) == 3:
                    # С днем и месяцем
                    if pattern == born_patterns[0]:  # с месяцем словами
                        day, month_str, year = match.groups()
                        month = self.month_map.get(month_str)
                        if month and self._is_valid_birth_year(year):
                            return f"{int(day):02d}.{month}.{year}"
                    else:  # с цифровой датой
                        day, month, year = match.groups()
                        if self._is_valid_birth_year(year):
                            return f"{int(day):02d}.{int(month):02d}.{year}"
                else:  # только год
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
        
        # Если ничего не нашли - возвращаем пустую строку
        return ''

    async def parse_judge_profile(self, url: str, status: str, region: str, court_type: str, court_name: str) -> Optional[Dict]:
        """Парсинг профиля судьи"""
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

            data = {
                'region': region, 
                'court_type': court_type, 
                'court': court_name, 
                'full_name': '', 
                'status': status, 
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
            bio_text = ''
            all_info_parts = []
            
            if info_tab:
                # Берем весь текст из info_tab, включая все параграфы
                paragraphs = info_tab.find_all('p')
                for p in paragraphs:
                    text = p.get_text(strip=True)
                    if text and text.lower() != 'регистрация':
                        all_info_parts.append(text)
                
                # Объединяем всю информацию
                data['judge_info'] = '\n'.join(all_info_parts)
                bio_text = data['judge_info']
            else:
                # Если нет вкладки #type-2, пробуем найти информацию в других местах
                sudya_info = content_div.find('div', id='sudya_info')
                if sudya_info:
                    info_text = sudya_info.get_text(strip=True)
                    data['judge_info'] = info_text
                    bio_text = info_text
            
            # Извлечение даты рождения - ТОЛЬКО из первых 300 символов judge_info
            if bio_text:
                data['date_of_birth'] = self._extract_date_of_birth(bio_text)
            
            # Если не нашли в judge_info, пробуем поискать в начале всего контента
            if not data['date_of_birth']:
                full_content_text = content_div.get_text()
                data['date_of_birth'] = self._extract_date_of_birth(full_content_text[:300])
            
            # Очистка данных
            if data['date_of_birth']:
                data['date_of_birth'] = data['date_of_birth'].strip().replace('\n', '').replace('\r', '')
            
            self.stats['successfully_parsed'] += 1
            return data
            
        except Exception as e:
            logger.warning(f"Ошибка при парсинге профиля {url}: {e}")
            self.stats['failed_parsed'] += 1
            self.stats['failed_urls'].append(url)
            return None

    def print_stats(self):
        """Вывод статистики"""
        logger.info("\n" + "="*50)
        logger.info("СТАТИСТИКА ПАРСИНГА:")
        logger.info(f"Всего найдено ссылок на судей: {self.stats['total_judges_found']}")
        logger.info(f"Успешно спарсено: {self.stats['successfully_parsed']}")
        logger.info(f"Не удалось спарсить: {self.stats['failed_parsed']}")
        success_rate = (self.stats['successfully_parsed'] / max(self.stats['total_judges_found'], 1)) * 100
        logger.info(f"Процент успеха: {success_rate:.1f}%")
        
        if self.stats['failed_urls']:
            logger.info(f"Проблемные URL: {len(self.stats['failed_urls'])}")
            with open(f'failed_urls_{self.region_start}_{self.region_end or "end"}.txt', 'w', encoding='utf-8') as f:
                for url in self.stats['failed_urls']:
                    f.write(f"{url}\n")
            logger.info(f"Список проблемных URL сохранен в failed_urls_{self.region_start}_{self.region_end or 'end'}.txt")
        logger.info("="*50)

async def process_in_batches(tasks, batch_size=50):
    """Обработка задач батчами для контроля памяти"""
    results = []
    for i in range(0, len(tasks), batch_size):
        batch = tasks[i:i+batch_size]
        batch_results = await asyncio.gather(*batch, return_exceptions=True)
        
        for result in batch_results:
            if isinstance(result, dict):
                results.append(result)
            elif isinstance(result, Exception):
                logger.warning(f"Ошибка в задаче: {result}")
        
        if i + batch_size < len(tasks):
            await asyncio.sleep(random.uniform(2, 5))
    
    return results

async def main():
    # Парсинг аргументов командной строки
    parser = argparse.ArgumentParser(description='Парсер судей')
    parser.add_argument('--start', type=int, default=1, help='Начальный регион (по умолчанию: 1)')
    parser.add_argument('--end', type=int, default=1, help='Конечный регион (по умолчанию: все)')
    parser.add_argument('--tasks', type=int, default=15, help='Количество одновременных задач (по умолчанию: 15)')
    
    args = parser.parse_args()
    
    logger.info("="*60)
    logger.info(f"=== Запуск парсера судей ===")
    logger.info(f"=== Диапазон регионов: с {args.start} по {args.end or 'все'} ===")
    logger.info(f"=== Макс. одновременных задач: {args.tasks} ===")
    logger.info("="*60)
    
    all_judges_data = []
    success_flag = False
    
    async with JudgeParser(
        region_start=args.start,
        region_end=args.end,
        max_concurrent_tasks=args.tasks
    ) as parser:
        
        try:
            region_data_list = await parser.get_region_urls()
            if not region_data_list:
                logger.error("Не удалось получить список регионов")
                return
            
            logger.info(f"Начало сбора данных по {len(region_data_list)} регионам")
            
            all_tasks = []
            for region_name, region_url in tqdm(region_data_list, desc="Регионы"):
                court_types = await parser.get_court_types_from_region(region_url)
                
                for court_type_name, type_url in court_types:
                    courts = await parser.get_courts_from_type_page(type_url)
                    
                    for court_name, court_url in courts:
                        visited_pages_for_court = set()
                        judges_with_status = await parser.get_all_judges_from_court(
                            court_url, 
                            visited_pages_for_court
                        )
                        
                        logger.info(f"Найдено {len(judges_with_status)} судей в {court_name}")
                        
                        for judge_url, status in judges_with_status:
                            task = parser.parse_judge_profile(
                                judge_url, 
                                status, 
                                region_name, 
                                court_type_name, 
                                court_name
                            )
                            all_tasks.append(task)
            
            logger.info(f"Всего собрано {len(all_tasks)} задач для парсинга")
            
            if all_tasks:
                all_judges_data = await process_in_batches(all_tasks, batch_size=50)
                all_judges_data = [data for data in all_judges_data if data]
                logger.info(f"Успешно спарсено {len(all_judges_data)} профилей")
            
            logger.info("\n" + "="*25 + " СОХРАНЕНИЕ " + "="*25)
            
            if all_judges_data:
                # Дедупликация по URL
                unique_judges = {}
                for judge in all_judges_data:
                    if judge and 'profile_url' in judge:
                        unique_judges[judge['profile_url']] = judge
                
                df = pd.DataFrame(list(unique_judges.values()))
                
                # Новый порядок столбцов - БЕЗ должности
                column_order = [
                    'region', 'court_type', 'court', 'full_name', 
                    'status', 'date_of_birth', 'judge_info', 'profile_url'
                ]
                
                existing_columns = [col for col in column_order if col in df.columns]
                df = df[existing_columns]
                
                rename_map = {
                    'region': 'Регион',
                    'court_type': 'Тип суда',
                    'court': 'Название суда',
                    'full_name': 'ФИО Судьи',
                    'status': 'Статус',
                    'date_of_birth': 'Дата рождения',
                    'judge_info': 'Информация о судье (Био)',
                    'profile_url': 'Ссылка'
                }
                
                df.rename(columns=rename_map, inplace=True)
                
                # Имя файла с указанием диапазона регионов
                timestamp = time.strftime('%Y%m%d_%H%M%S')
                region_range = f"regions_{args.start}_{args.end or 'all'}"
                output_file = f"judges_data_{region_range}_{timestamp}.xlsx"
                df.to_excel(output_file, index=False, engine='openpyxl')
                
                logger.info(f"✓ Данные сохранены в файл: {output_file}")
                logger.info(f"✓ Всего записей: {len(df)}")
                
                csv_file = f"judges_data_{region_range}_{timestamp}.csv"
                df.to_csv(csv_file, index=False, encoding='utf-8-sig')
                logger.info(f"✓ Резервная копия в CSV: {csv_file}")
                
                success_flag = True
                parser.print_stats()
            else:
                logger.error("✗ Не удалось собрать ни одной записи")
                
        except (KeyboardInterrupt, asyncio.CancelledError):
            logger.info("\nПарсинг прерван пользователем.")
        except Exception as e:
            logger.error(f"✗ Критическая ошибка: {e}", exc_info=True)
            
        finally:
            if all_judges_data and not success_flag:
                logger.info("Сохранение частичных данных...")
                try:
                    unique_judges = {}
                    for judge in all_judges_data:
                        if judge and 'profile_url' in judge:
                            unique_judges[judge['profile_url']] = judge
                    
                    if unique_judges:
                        df = pd.DataFrame(list(unique_judges.values()))
                        
                        rename_map = {
                            'region': 'Регион', 
                            'court_type': 'Тип суда', 
                            'court': 'Название суда',
                            'full_name': 'ФИО Судьи', 
                            'status': 'Статус',
                            'date_of_birth': 'Дата рождения', 
                            'judge_info': 'Информация о судье (Био)',
                            'profile_url': 'Ссылка'
                        }
                        
                        df.rename(columns={k: v for k, v in rename_map.items() if k in df.columns}, inplace=True)
                        
                        region_range = f"regions_{args.start}_{args.end or 'all'}"
                        output_file = f"judges_data_PARTIAL_{region_range}_{time.strftime('%Y%m%d_%H%M%S')}.xlsx"
                        df.to_excel(output_file, index=False, engine='openpyxl')
                        logger.info(f"✓ Частичные данные сохранены в {output_file} ({len(df)} записей)")
                except Exception as e:
                    logger.error(f"Ошибка при сохранении частичных данных: {e}")

if __name__ == "__main__":
    if sys.platform == 'win32':
        asyncio.set_event_loop_policy(asyncio.WindowsSelectorEventLoopPolicy())
    
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        logger.info("\nПрограмма завершена пользователем.")
    except Exception as e:
        logger.error(f"Фатальная ошибка: {e}")
