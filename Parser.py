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

# Настройка логирования
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s', datefmt='%H:%M:%S')
logger = logging.getLogger(__name__)

# Базовый URL
BASE_URL = "https://xn--d1aiaa2aleeao4h.xn--p1ai/"

class JudgeParser:
    def __init__(
        self, 
        max_regions=None, 
        max_court_types_per_region=None, 
        max_courts_per_type=None, 
        max_judges_per_court=None,
        max_concurrent_tasks: int = 10
    ):
        self.base_url = BASE_URL
        self.session = None
        self.max_regions = max_regions
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
        
        if self.max_regions and self.max_regions > 0: 
            return region_data[:self.max_regions]
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
        """Извлечение даты рождения из текста с улучшенной логикой"""
        if not text: 
            return ''
        
        text = re.sub(r'\s+', ' ', text.strip())
        text_lower = text.lower()
        
        # 1. Сначала ищем полную дату рождения
        # "25 сентября 1963 года рождения"
        full_date_pattern = r'(\d{1,2})\s+([а-я]+)\s+(\d{4})\s+года\s+рождения'
        full_match = re.search(full_date_pattern, text_lower)
        if full_match:
            day, month_str, year = full_match.groups()
            month = self.month_map.get(month_str)
            if month and self._is_valid_birth_year(year):
                return f"{int(day):02d}.{month}.{year}"
        
        # 2. Ищем год с явными маркерами рождения в начале текста
        first_200 = text_lower[:200]
        
        birth_year_patterns = [
            # "1967 года рождения,"
            r'(\d{4})\s+года\s+рождения[,\s]',
            # "1990 г.р."
            r'(\d{4})\s*г\.\s*р\.[,\s]',
            # "1990 г. р."
            r'(\d{4})\s*г\.\s*р\.[,\s]',
            # "родился в 1990 году"
            r'родился?\s+(?:в\s+)?(\d{4})\s+году',
            # "год рождения: 1990"
            r'год\s+рождения[:\s]+(\d{4})',
            # "род. 1990"
            r'род\.\s*(\d{4})',
            # "1990 года,"
            r'(\d{4})\s+года,',
            # "1990 г.,"
            r'(\d{4})\s+г\.,',
        ]
        
        for pattern in birth_year_patterns:
            match = re.search(pattern, first_200)
            if match:
                year = match.group(1)
                if self._is_valid_birth_year(year):
                    return year
        
        # 3. Ищем год рождения в контексте "рождения" во всем тексте
        birth_context_patterns = [
            r'(\d{4})\s+год[а]?\s+рождения',
            r'\((\d{4})\s+г\.\s*р\.\)',
            r'\((\d{4})\s+года\s+рождения\)',
        ]
        
        for pattern in birth_context_patterns:
            matches = list(re.finditer(pattern, text_lower))
            for match in matches:
                year = match.group(1)
                if self._is_valid_birth_year(year):
                    # Проверяем контекст - не должен быть годом окончания учебы
                    context_start = max(0, match.start() - 30)
                    context_end = min(len(text_lower), match.end() + 30)
                    context = text_lower[context_start:context_end]
                    
                    # Исключаем годы окончания учебы
                    if not any(word in context for word in ['окончил', 'окончила', 'закончил', 'закончила', 'университет', 'институт', 'академия']):
                        return year
        
        # 4. Ищем год в начале текста, который может быть годом рождения
        # Проверяем первый год в тексте, если он в первых 100 символах
        year_match = re.search(r'\b(19\d{2}|20[0-2]\d)\b', text_lower[:150])
        if year_match:
            year = year_match.group(1)
            if self._is_valid_birth_year(year):
                # Проверяем контекст
                context_start = max(0, year_match.start() - 20)
                context_end = min(len(text_lower), year_match.end() + 20)
                context = text_lower[context_start:context_end]
                
                # Проверяем, что это не год окончания учебы или другого события
                is_likely_birth_year = (
                    any(word in context for word in ['рождения', 'года', 'г.', 'род.']) and
                    not any(word in context for word in ['окончил', 'окончила', 'закончил', 'закончила', 'поступил', 'поступила'])
                )
                
                if is_likely_birth_year:
                    return year
        
        return ''

    async def parse_judge_profile(self, url: str, status: str, region: str, court_type: str, court_name: str) -> Optional[Dict]:
        """Парсинг профиля судьи с улучшенным извлечением информации"""
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
                'position': '', 
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
                
                # Извлечение должности
                position_found = False
                for text in all_info_parts:
                    text_lower = text.lower()
                    if text_lower.startswith('должность:'):
                        data['position'] = text.split(':', 1)[1].strip()
                        position_found = True
                        break
                
                # Если не нашли через "должность:", ищем другие указания
                if not position_found:
                    for text in all_info_parts:
                        text_lower = text.lower()
                        if any(pos in text_lower for pos in ['председатель', 'судья', 'заместитель']):
                            # Проверяем, что это описание должности
                            words = text_lower.split()
                            if any(pos_word in words[:3] for pos_word in ['председатель', 'судья', 'замествитель']):
                                data['position'] = text
                                break
            else:
                # Если нет вкладки #type-2, пробуем найти информацию в других местах
                # Ищем блок с информацией о судье
                sudya_info = content_div.find('div', id='sudya_info')
                if sudya_info:
                    info_text = sudya_info.get_text(strip=True)
                    data['judge_info'] = info_text
                    bio_text = info_text
            
            # Извлечение даты рождения
            if bio_text:
                data['date_of_birth'] = self._extract_date_of_birth(bio_text)
            
            # Дополнительная проверка: если не нашли дату, пробуем поискать во всем контенте
            if not data['date_of_birth']:
                full_content_text = content_div.get_text()
                data['date_of_birth'] = self._extract_date_of_birth(full_content_text[:500])  # Только первые 500 симв
            
            # Очистка данных
            if data['date_of_birth']:
                # Удаляем нежелательные символы
                data['date_of_birth'] = data['date_of_birth'].strip().replace('\n', '').replace('\r', '')
            
            if data['position']:
                data['position'] = data['position'].strip().replace('\n', ' ')
            
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
            with open('failed_urls.txt', 'w', encoding='utf-8') as f:
                for url in self.stats['failed_urls']:
                    f.write(f"{url}\n")
            logger.info("Список проблемных URL сохранен в failed_urls.txt")
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
    logger.info("=== Запуск улучшенного парсера судей ===")
    
    # Настройки
    MAX_REGIONS = 1  # 1 для теста, None для всех
    MAX_CONCURRENT_TASKS = 15
    
    all_judges_data = []
    success_flag = False
    
    async with JudgeParser(
        max_regions=MAX_REGIONS,
        max_concurrent_tasks=MAX_CONCURRENT_TASKS
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
                unique_judges = {}
                for judge in all_judges_data:
                    if judge and 'profile_url' in judge:
                        unique_judges[judge['profile_url']] = judge
                
                df = pd.DataFrame(list(unique_judges.values()))
                
                column_order = [
                    'region', 'court_type', 'court', 'full_name', 
                    'status', 'position', 'date_of_birth', 'judge_info', 'profile_url'
                ]
                
                existing_columns = [col for col in column_order if col in df.columns]
                df = df[existing_columns]
                
                rename_map = {
                    'region': 'Регион',
                    'court_type': 'Тип суда',
                    'court': 'Название суда',
                    'full_name': 'ФИО Судьи',
                    'status': 'Статус',
                    'position': 'Должность',
                    'date_of_birth': 'Дата рождения',
                    'judge_info': 'Информация о судье (Био)',
                    'profile_url': 'Ссылка'
                }
                
                df.rename(columns=rename_map, inplace=True)
                
                timestamp = time.strftime('%Y%m%d_%H%M%S')
                output_file = f"judges_data_{timestamp}.xlsx"
                df.to_excel(output_file, index=False, engine='openpyxl')
                
                logger.info(f"✓ Данные сохранены в файл: {output_file}")
                logger.info(f"✓ Всего записей: {len(df)}")
                
                csv_file = f"judges_data_{timestamp}.csv"
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
                            'region': 'Регион', 'court_type': 'Тип суда', 'court': 'Название суда',
                            'full_name': 'ФИО Судьи', 'status': 'Статус', 'position': 'Должность',
                            'date_of_birth': 'Дата рождения', 'judge_info': 'Информация о судье (Био)',
                            'profile_url': 'Ссылка'
                        }
                        
                        df.rename(columns={k: v for k, v in rename_map.items() if k in df.columns}, inplace=True)
                        
                        output_file = f"judges_data_PARTIAL_{time.strftime('%Y%m%d_%H%M%S')}.xlsx"
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
