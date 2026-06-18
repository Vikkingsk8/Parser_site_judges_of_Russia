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

class ArbitrationParser:
    def __init__(
        self, 
        max_concurrent_tasks: int = 15,
        categories: List[str] = None  # Можно выбрать конкретные категории
    ):
        self.base_url = BASE_URL
        self.session = None
        self.max_concurrent_tasks = max_concurrent_tasks
        self.semaphore = asyncio.Semaphore(max_concurrent_tasks)
        
        # Категории арбитражных судов
        self.categories = categories or ['all']
        self.category_urls = {
            'vysshiy': '/suds/vysshiy-arbitrazhnyy-sud-rossiyskoy-federacii-85',
            'federalnye_okruga': '/site/arbitration/page/2',
            'apellyacionnye': '/site/arbitration/page/3',
            'subekty_rf': '/site/arbitration/page/4',
            'intellektualnye_prava': '/suds/sud-po-intellektual-nym-pravam-85'
        }
        
        self.user_agents = [
            'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36',
            'Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:109.0) Gecko/20100101 Firefox/121.0',
            'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36',
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
        """Проверяет, является ли строка валидным годом рождения"""
        try:
            year = int(year_str)
            return 1920 <= year <= 2005
        except (ValueError, TypeError):
            return False

    def _extract_date_of_birth(self, text: str) -> str:
        """
        Извлечение даты рождения из текста.
        """
        if not text: 
            return ''
        
        text_beginning = text[:500].strip()
        text_beginning = re.sub(r'\s+', ' ', text_beginning)
        text_lower = text_beginning.lower()
        
        # Полная дата с месяцем
        full_date_with_month = r'(\d{1,2})\s+([а-я]+)\s+(\d{4})\s+года\s+рождения'
        match = re.search(full_date_with_month, text_lower)
        if match:
            day, month_str, year = match.groups()
            month = self.month_map.get(month_str)
            if month and self._is_valid_birth_year(year):
                return f"{int(day):02d}.{month}.{year}"
        
        # Полная дата в формате DD.MM.YYYY
        full_date_dd_mm_yyyy = r'(\d{1,2})[\.\-](\d{1,2})[\.\-](\d{4})\s+года\s+рождения'
        match = re.search(full_date_dd_mm_yyyy, text_lower)
        if match:
            day, month, year = match.groups()
            if self._is_valid_birth_year(year):
                return f"{int(day):02d}.{int(month):02d}.{year}"
        
        # Только год
        year_only = r'(\d{4})\s+года\s+рождения'
        match = re.search(year_only, text_lower)
        if match:
            year = match.group(1)
            if self._is_valid_birth_year(year):
                return year
        
        # Год с сокращением
        year_gr = r'(\d{4})\s*г\.\s*р\.'
        match = re.search(year_gr, text_lower)
        if match:
            year = match.group(1)
            if self._is_valid_birth_year(year):
                return year
        
        # Родился/родилась
        born_patterns = [
            (r'родил[ая]сь?\s+(\d{1,2})\s+([а-я]+)\s+(\d{4})\s+года', 'month'),
            (r'родил[ая]сь?\s+(\d{1,2})\s+([а-я]+)\s+(\d{4})\s+г\.', 'month'),
            (r'родил[ая]сь?\s+(\d{1,2})[\.\-](\d{1,2})[\.\-](\d{4})', 'date'),
            (r'родил[ая]сь?\s+в\s+(\d{4})\s+году', 'year'),
            (r'родил[ая]сь?\s+в\s+(\d{4})\s+г\.', 'year'),
            (r'родил[ая]сь?\s+в\s+(\d{4})(?:\s|$|\.)', 'year'),
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
                else:
                    year = match.group(1)
                    if self._is_valid_birth_year(year):
                        return year
        
        # Род.
        rod_pattern = r'род\.\s*(\d{4})'
        match = re.search(rod_pattern, text_lower)
        if match:
            year = match.group(1)
            if self._is_valid_birth_year(year):
                return year
        
        return ''

    async def parse_judge_profile(self, url: str, status: str, category: str, court_name: str) -> Optional[Dict]:
        """Парсинг профиля арбитражного судьи"""
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
                'category': category,
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
            
            if info_tab:
                # Ищем параграф с классом copy-disable (основная информация)
                copy_disable = info_tab.find('p', class_='copy-disable')
                if copy_disable:
                    bio_text = copy_disable.get_text(strip=False)
                else:
                    # Если нет, берем все параграфы
                    paragraphs = info_tab.find_all('p')
                    all_text_parts = []
                    for p in paragraphs:
                        text = p.get_text(strip=False)
                        if text:
                            text = re.sub(r'\s+', ' ', text)
                            all_text_parts.append(text)
                    if all_text_parts:
                        bio_text = '\n'.join(all_text_parts)
                
                data['judge_info'] = bio_text
            
            # Если нет #type-2, пробуем найти #sudya_info
            if not bio_text:
                sudya_info = content_div.find('div', id='sudya_info')
                if sudya_info:
                    info_text = sudya_info.get_text(strip=True)
                    data['judge_info'] = info_text
                    bio_text = info_text
            
            # Извлечение даты рождения
            if bio_text:
                data['date_of_birth'] = self._extract_date_of_birth(bio_text)
            
            # Если не нашли, пробуем поискать в начале всего контента
            if not data['date_of_birth']:
                full_content_text = content_div.get_text()
                data['date_of_birth'] = self._extract_date_of_birth(full_content_text[:500])
            
            # Очистка данных
            for key in ['date_of_birth', 'judge_info', 'full_name', 'category', 'court', 'status']:
                if data[key]:
                    data[key] = self._clean_text_for_excel(data[key])
            
            self.stats['successfully_parsed'] += 1
            return data
            
        except Exception as e:
            logger.warning(f"Ошибка при парсинге профиля {url}: {e}")
            self.stats['failed_parsed'] += 1
            self.stats['failed_urls'].append(url)
            return None

    async def get_judges_from_court_page(self, court_url: str, category: str, court_name: str, visited_urls: Set[str]) -> List[Dict]:
        """Получение списка судей со страницы суда (с учетом пагинации)"""
        if court_url in visited_urls:
            return []
        visited_urls.add(court_url)
        
        logger.info(f"Загрузка страницы суда: {court_url}")
        html = await self.fetch_with_retry(court_url)
        if not html:
            return []
        
        soup = BeautifulSoup(html, 'html.parser')
        judges = []
        
        # Обработка действующих судей и судей в отставке
        for div_id, status in [('sudiilistview', 'Действующий'), ('sudiilistview2', 'В отставке')]:
            container = soup.find('div', id=div_id)
            if container:
                items_div = container.find('div', class_='items')
                if items_div:
                    # Ищем ссылки на судей
                    for a_tag in items_div.find_all('a', class_='browser_link', href=True):
                        judge_url = urljoin(self.base_url, a_tag['href'])
                        judge_name = a_tag.get_text(strip=True)
                        
                        # Создаем задачу на парсинг профиля
                        judge_data = await self.parse_judge_profile(
                            judge_url, status, category, court_name
                        )
                        if judge_data:
                            judges.append(judge_data)
        
        # Обработка пагинации внутри страницы суда
        pager = soup.find('div', class_='pager')
        if pager:
            next_link = pager.find('li', class_='next')
            if next_link:
                a_tag = next_link.find('a', href=True)
                if a_tag:
                    next_page_url = urljoin(court_url, a_tag['href'])
                    if next_page_url not in visited_urls:
                        logger.info(f"Переход на следующую страницу суда: {next_page_url}")
                        next_judges = await self.get_judges_from_court_page(
                            next_page_url, category, court_name, visited_urls
                        )
                        judges.extend(next_judges)
        
        return judges

    async def get_courts_from_category_page(self, category_url: str) -> List[Tuple[str, str]]:
        """Получение списка судов со страницы категории (с учетом пагинации)"""
        courts = []
        current_url = category_url
        visited_urls = set()
        
        while current_url and current_url not in visited_urls:
            visited_urls.add(current_url)
            logger.info(f"Загрузка страницы категории: {current_url}")
            html = await self.fetch_with_retry(current_url)
            if not html:
                break
            
            soup = BeautifulSoup(html, 'html.parser')
            list_view = soup.find('div', id='sudiilistview')
            
            if list_view:
                items_div = list_view.find('div', class_='items')
                if items_div:
                    # Ищем ссылки на суды
                    for a_tag in items_div.find_all('a', class_='browser_link', href=True):
                        court_name = a_tag.get_text(strip=True)
                        court_url = urljoin(self.base_url, a_tag['href'])
                        courts.append((court_name, court_url))
                        logger.debug(f"Найден суд: {court_name}")
            
            # Ищем следующую страницу в пагинации
            pager = soup.find('div', class_='pager')
            next_url = None
            if pager:
                next_link = pager.find('li', class_='next')
                if next_link:
                    a_tag = next_link.find('a', href=True)
                    if a_tag:
                        next_url = urljoin(current_url, a_tag['href'])
                        if next_url in visited_urls:
                            next_url = None
            
            current_url = next_url
            if current_url:
                await asyncio.sleep(random.uniform(1, 2))
        
        logger.info(f"Найдено {len(courts)} судов в категории")
        return courts

    async def parse_direct_court(self, court_url: str, category_name: str) -> List[Dict]:
        """Парсинг прямого суда (без списка судов, сразу судьи)"""
        logger.info(f"Парсинг прямого суда: {court_url}")
        html = await self.fetch_with_retry(court_url)
        if not html:
            return []
        
        soup = BeautifulSoup(html, 'html.parser')
        
        # Получаем название суда
        court_name_tag = soup.find('h1', class_='mtop')
        if not court_name_tag:
            court_name_tag = soup.find('h1')
        court_name = court_name_tag.get_text(strip=True) if court_name_tag else category_name
        
        visited_urls = set()
        judges = await self.get_judges_from_court_page(court_url, category_name, court_name, visited_urls)
        
        return judges

    async def run(self):
        """Основной метод запуска парсинга"""
        all_judges_data = []
        
        # Определяем какие категории парсить
        categories_to_parse = []
        if 'all' in self.categories:
            categories_to_parse = list(self.category_urls.keys())
        else:
            categories_to_parse = [c for c in self.categories if c in self.category_urls]
        
        category_names = {
            'vysshiy': 'Высший арбитражный суд РФ',
            'federalnye_okruga': 'Федеральные арбитражные суды округов',
            'apellyacionnye': 'Арбитражные апелляционные суды',
            'subekty_rf': 'Арбитражные суды субъектов РФ',
            'intellektualnye_prava': 'Суд по интеллектуальным правам'
        }
        
        for cat_key in categories_to_parse:
            cat_name = category_names[cat_key]
            cat_url = urljoin(self.base_url, self.category_urls[cat_key])
            
            logger.info(f"\n{'='*60}")
            logger.info(f"Парсинг категории: {cat_name}")
            logger.info(f"{'='*60}")
            
            # Для прямых судов (без списка)
            if cat_key in ['vysshiy', 'intellektualnye_prava']:
                judges = await self.parse_direct_court(cat_url, cat_name)
                all_judges_data.extend(judges)
                logger.info(f"В категории {cat_name} найдено {len(judges)} судей")
            
            # Для категорий со списком судов
            else:
                courts = await self.get_courts_from_category_page(cat_url)
                logger.info(f"В категории {cat_name} найдено {len(courts)} судов")
                
                for court_name, court_url in tqdm(courts, desc=f"Обработка судов ({cat_name})"):
                    visited_urls = set()
                    judges = await self.get_judges_from_court_page(court_url, cat_name, court_name, visited_urls)
                    all_judges_data.extend(judges)
                    logger.info(f"В суде {court_name} найдено {len(judges)} судей")
                    
                    # Небольшая задержка между судами
                    await asyncio.sleep(random.uniform(0.5, 1.5))
        
        return all_judges_data

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
            with open(f'arbitration_failed_urls_{time.strftime("%Y%m%d_%H%M%S")}.txt', 'w', encoding='utf-8') as f:
                for url in self.stats['failed_urls']:
                    f.write(f"{url}\n")
            logger.info(f"Список проблемных URL сохранен в arbitration_failed_urls_*.txt")
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
    parser = argparse.ArgumentParser(description='Парсер арбитражных судей')
    parser.add_argument('--tasks', type=int, default=15, help='Количество одновременных задач (по умолчанию: 15)')
    parser.add_argument('--category', type=str, default='all', 
                        help='Категория для парсинга (all, vysshiy, federalnye_okruga, apellyacionnye, subekty_rf, intellektualnye_prava)')
    
    args = parser.parse_args()
    
    logger.info("="*60)
    logger.info(f"=== Запуск парсера АРБИТРАЖНЫХ судей ===")
    logger.info(f"=== Категория: {args.category} ===")
    logger.info(f"=== Макс. одновременных задач: {args.tasks} ===")
    logger.info("="*60)
    
    categories = args.category.split(',') if ',' in args.category else [args.category]
    
    all_judges_data = []
    
    async with ArbitrationParser(
        max_concurrent_tasks=args.tasks,
        categories=categories
    ) as parser:
        
        try:
            all_judges_data = await parser.run()
            
            logger.info(f"\nВсего собрано {len(all_judges_data)} профилей арбитражных судей")
            
            if all_judges_data:
                # Дедупликация по URL
                unique_judges = {}
                for judge in all_judges_data:
                    if judge and 'profile_url' in judge:
                        unique_judges[judge['profile_url']] = judge
                
                df = pd.DataFrame(list(unique_judges.values()))
                
                # Порядок столбцов
                column_order = [
                    'category', 'court', 'full_name', 
                    'date_of_birth', 'status', 'judge_info', 'profile_url'
                ]
                
                existing_columns = [col for col in column_order if col in df.columns]
                df = df[existing_columns]
                
                rename_map = {
                    'category': 'Категория суда',
                    'court': 'Название суда',
                    'full_name': 'ФИО Судьи',
                    'date_of_birth': 'Дата рождения',
                    'status': 'Статус',
                    'judge_info': 'Информация о судье',
                    'profile_url': 'Ссылка'
                }
                
                df.rename(columns=rename_map, inplace=True)
                
                # Имя файла
                timestamp = time.strftime('%Y%m%d_%H%M%S')
                category_suffix = args.category.replace(',', '_')
                output_file = f"arbitration_judges_{category_suffix}_{timestamp}.xlsx"
                
                # Сохраняем в Excel
                try:
                    df.to_excel(output_file, index=False, engine='openpyxl')
                    logger.info(f"✓ Данные сохранены в файл: {output_file}")
                except Exception as e:
                    logger.error(f"Ошибка при сохранении в Excel: {e}")
                    logger.info("Пробуем сохранить в CSV...")
                    csv_file = f"arbitration_judges_{category_suffix}_{timestamp}.csv"
                    df.to_csv(csv_file, index=False, encoding='utf-8-sig')
                    logger.info(f"✓ Данные сохранены в CSV: {csv_file}")
                
                # Резервная копия в CSV
                csv_file = f"arbitration_judges_{category_suffix}_{timestamp}.csv"
                df.to_csv(csv_file, index=False, encoding='utf-8-sig')
                logger.info(f"✓ Резервная копия в CSV: {csv_file}")
                logger.info(f"✓ Всего записей: {len(df)}")
                
                parser.print_stats()
            else:
                logger.error("✗ Не удалось собрать ни одной записи")
                
        except (KeyboardInterrupt, asyncio.CancelledError):
            logger.info("\nПарсинг прерван пользователем.")
        except Exception as e:
            logger.error(f"✗ Критическая ошибка: {e}", exc_info=True)
            
        finally:
            if all_judges_data:
                logger.info("Сохранение частичных данных...")
                try:
                    unique_judges = {}
                    for judge in all_judges_data:
                        if judge and 'profile_url' in judge:
                            unique_judges[judge['profile_url']] = judge
                    
                    if unique_judges:
                        df = pd.DataFrame(list(unique_judges.values()))
                        
                        rename_map = {
                            'category': 'Категория суда',
                            'court': 'Название суда',
                            'full_name': 'ФИО Судьи',
                            'date_of_birth': 'Дата рождения',
                            'status': 'Статус',
                            'judge_info': 'Информация о судье',
                            'profile_url': 'Ссылка'
                        }
                        
                        df.rename(columns={k: v for k, v in rename_map.items() if k in df.columns}, inplace=True)
                        
                        timestamp = time.strftime('%Y%m%d_%H%M%S')
                        csv_file = f"arbitration_judges_PARTIAL_{timestamp}.csv"
                        df.to_csv(csv_file, index=False, encoding='utf-8-sig')
                        logger.info(f"✓ Частичные данные сохранены в {csv_file} ({len(df)} записей)")
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
