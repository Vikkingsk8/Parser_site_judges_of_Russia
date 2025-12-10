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
from typing import List, Dict, Optional, Set
import sys

# Настройка логирования
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    datefmt='%H:%M:%S'
)
logger = logging.getLogger(__name__)

# Базовый URL сайта
BASE_URL = "https://xn--d1aiaa2aleeao4h.xn--p1ai/"

class JudgeParser:
    def __init__(self, max_regions=2, max_courts_per_region=None, max_judges_per_court=None):
        self.base_url = BASE_URL
        self.session = None
        self.request_count = 0
        self.max_regions = max_regions
        self.max_courts_per_region = max_courts_per_region
        self.max_judges_per_court = max_judges_per_court
        
        self.user_agents = [
            'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36',
            'Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:109.0) Gecko/20100101 Firefox/121.0',
            'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36',
            'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36 Edg/120.0.0.0'
        ]
        
    async def __aenter__(self):
        timeout = aiohttp.ClientTimeout(total=60, connect=30)
        connector = aiohttp.TCPConnector(limit=3, ssl=False)
        self.session = aiohttp.ClientSession(
            timeout=timeout,
            connector=connector,
            headers=self.get_random_headers()
        )
        return self
        
    async def __aexit__(self, exc_type, exc_val, exc_tb):
        if self.session:
            await self.session.close()
    
    def get_random_headers(self) -> dict:
        """Генерируем случайные заголовки"""
        return {
            'User-Agent': random.choice(self.user_agents),
            'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8',
            'Accept-Language': 'ru-RU,ru;q=0.9,en-US;q=0.8,en;q=0.7',
            'Accept-Encoding': 'gzip, deflate',
            'Connection': 'keep-alive',
            'Upgrade-Insecure-Requests': '1',
            'Cache-Control': 'max-age=0',
            'Referer': self.base_url,
            'DNT': '1',
        }
    
    async def fetch_with_retry(self, url: str, max_retries: int = 3) -> Optional[str]:
        """Загружаем страницу с повторными попытками"""
        for attempt in range(max_retries):
            try:
                # Случайная задержка от 0.5 до 1.5 секунд
                delay = random.uniform(0.5, 1.5)
                await asyncio.sleep(delay)
                
                # Меняем заголовки
                self.session.headers.update(self.get_random_headers())
                
                async with self.session.get(url) as response:
                    if response.status == 200:
                        text = await response.text()
                        return text
                    elif response.status == 429:  # Too Many Requests
                        wait_time = 5 * (attempt + 1)
                        logger.warning(f"429 Too Many Requests для {url}. Ждем {wait_time} сек...")
                        await asyncio.sleep(wait_time)
                        continue
                    else:
                        logger.warning(f"Ошибка {response.status} для {url}, попытка {attempt + 1}")
                        await asyncio.sleep(3)
                        
            except asyncio.TimeoutError:
                logger.warning(f"Таймаут для {url}, попытка {attempt + 1}")
                await asyncio.sleep(5)
            except Exception as e:
                logger.warning(f"Ошибка при загрузке {url}: {e}, попытка {attempt + 1}")
                await asyncio.sleep(3)
        
        logger.error(f"Не удалось загрузить {url} после {max_retries} попыток")
        return None
    
    async def get_region_urls(self) -> List[str]:
        """Получаем URL регионов"""
        logger.info("Получение списка регионов...")
        url = urljoin(self.base_url, "suds/index")
        
        html = await self.fetch_with_retry(url, max_retries=5)
        if not html:
            logger.error("Не удалось загрузить страницу регионов")
            return []
            
        soup = BeautifulSoup(html, 'html.parser')
        region_urls = []
        
        # Ищем таблицу с регионами
        region_table = soup.find('table', id='region_table')
        if region_table:
            for a_tag in region_table.find_all('a', href=re.compile(r'/suds/index/region/')):
                full_url = urljoin(self.base_url, a_tag['href'])
                region_urls.append(full_url)
        
        logger.info(f"Найдено {len(region_urls)} регионов")
        return region_urls
    
    async def get_court_urls_from_region(self, region_url: str) -> List[str]:
        """Получаем суды из региона"""
        html = await self.fetch_with_retry(region_url)
        if not html:
            return []
            
        soup = BeautifulSoup(html, 'html.parser')
        court_urls = []
        
        # Проверяем, есть ли выбор типа суда
        browser_div = soup.find('div', id='browser')
        if browser_div:
            # Получаем URL типов судов
            type_urls = []
            for a_tag in browser_div.find_all('a', href=re.compile(r'/suds/index/type/')):
                type_url = urljoin(self.base_url, a_tag['href'])
                type_urls.append(type_url)
            
            # Обрабатываем типы судов последовательно
            for type_url in type_urls:
                courts = await self.get_courts_from_type_page(type_url)
                court_urls.extend(courts)
                await asyncio.sleep(1)
        else:
            # Ищем прямые ссылки на суды
            content_div = soup.find('div', id='content')
            if content_div:
                for a_tag in content_div.find_all('a', href=re.compile(r'/suds/')):
                    href = a_tag['href']
                    if '/suds/index' not in href and not href.startswith('#'):
                        full_url = urljoin(self.base_url, href)
                        court_urls.append(full_url)
        
        # Удаляем дубликаты и применяем лимит
        court_urls = list(set(court_urls))
        if self.max_courts_per_region:
            court_urls = court_urls[:self.max_courts_per_region]
        
        return court_urls
    
    async def get_courts_from_type_page(self, type_url: str) -> List[str]:
        """Получаем суды со страницы типа суда"""
        html = await self.fetch_with_retry(type_url)
        if not html:
            return []
            
        soup = BeautifulSoup(html, 'html.parser')
        court_urls = []
        
        # Ищем таблицу с судами
        browser_table = soup.find('table', id='browser')
        if browser_table:
            # Находим все td с классом list (в них ссылки на суды)
            list_tds = browser_table.find_all('td', class_='list')
            for td in list_tds:
                for a_tag in td.find_all('a', href=re.compile(r'/suds/')):
                    href = a_tag['href']
                    if '/suds/index' not in href:
                        full_url = urljoin(self.base_url, href)
                        court_urls.append(full_url)
        
        return list(set(court_urls))
    
    async def get_all_judge_urls_from_court(self, court_url: str) -> List[str]:
        """Получаем ВСЕХ судей из суда (без дубликатов)"""
        html = await self.fetch_with_retry(court_url)
        if not html:
            return []
            
        soup = BeautifulSoup(html, 'html.parser')
        judge_urls_set = set()  # Используем set для автоматического удаления дубликатов
        
        # Ищем таблицу с судьями
        browser_table = soup.find('table', id='browser')
        if browser_table:
            # Находим все td с классом list (в них содержатся судьи)
            list_tds = browser_table.find_all('td', class_='list')
            for td in list_tds:
                # В каждом td ищем ссылки на судей
                for a_tag in td.find_all('a', href=re.compile(r'/sudii/')):
                    href = a_tag['href']
                    full_url = urljoin(self.base_url, href)
                    judge_urls_set.add(full_url)  # Set автоматически удаляет дубликаты
        
        # Преобразуем set обратно в list
        judge_urls = list(judge_urls_set)
        
        # Применяем лимит, если задан
        if self.max_judges_per_court:
            judge_urls = judge_urls[:self.max_judges_per_court]
        
        return judge_urls
    
    async def parse_judge_profile(self, url: str) -> Optional[Dict]:
        """Парсим профиль судьи"""
        html = await self.fetch_with_retry(url)
        if not html:
            return None
            
        soup = BeautifulSoup(html, 'html.parser')
        
        data = {
            'full_name': '',
            'court': '',
            'position': '',
            'appointment_date': '',
            'judge_info': '',
            'profile_url': url
        }
        
        # 1. ФИО из h1
        h1_tag = soup.find('h1')
        if h1_tag:
            data['full_name'] = h1_tag.get_text(strip=True)
        
        # 2. Информация о суде
        sudya_info = soup.find('div', id='sudya_info')
        if sudya_info:
            for span in sudya_info.find_all('span'):
                text = span.get_text(strip=True)
                if 'Региональный суд:' in text:
                    a_tag = span.find('a')
                    data['court'] = a_tag.get_text(strip=True) if a_tag else text.replace('Региональный суд:', '').strip()
        
        # 3. Информация о судье
        for tab in soup.find_all('div', class_='tab-box'):
            h1_in_tab = tab.find('h1', id='comt')
            if h1_in_tab and 'Информация о судье' in h1_in_tab.get_text():
                paragraphs = tab.find_all('p')
                info_text = '\n'.join([p.get_text(strip=True) for p in paragraphs])
                data['judge_info'] = info_text
                
                # Извлекаем первую дату назначения из текста
                date_match = re.search(r'(\d{1,2}\.\d{1,2}\.\d{4})', info_text)
                if date_match:
                    data['appointment_date'] = date_match.group(1)
        
        return data

async def main():
    """Главная функция"""
    logger.info("=== ПАРСЕР СУДЕЙ РОССИИ ===")
    logger.info("=" * 40)
    
    # Настройки (можно менять)
    MAX_REGIONS = 2           # Сколько регионов обрабатывать (None = все)
    MAX_COURTS_PER_REGION = None  # Сколько судов в каждом регионе (None = все)  
    MAX_JUDGES_PER_COURT = None  # Сколько судей в каждом суде (None = всех)
    
    async with JudgeParser(
        max_regions=MAX_REGIONS,
        max_courts_per_region=MAX_COURTS_PER_REGION,
        max_judges_per_court=MAX_JUDGES_PER_COURT
    ) as parser:
        try:
            # 1. Получаем регионы
            logger.info("1. Получаем список регионов...")
            all_region_urls = await parser.get_region_urls()
            
            if not all_region_urls:
                logger.error("Не удалось получить регионы")
                return
            
            # Берем только указанное количество регионов
            if MAX_REGIONS:
                region_urls = all_region_urls[:MAX_REGIONS]
            else:
                region_urls = all_region_urls
            
            logger.info(f"Будем обрабатывать {len(region_urls)} регионов")
            
            all_judges_data = []
            
            # 2. Обрабатываем регионы с прогресс-баром
            for region_idx, region_url in enumerate(tqdm(region_urls, desc="Регионы"), 1):
                logger.info(f"\nРегион {region_idx}/{len(region_urls)}")
                
                # Получаем суды из региона
                court_urls = await parser.get_court_urls_from_region(region_url)
                logger.info(f"  Найдено судов: {len(court_urls)}")
                
                if not court_urls:
                    logger.warning("  В регионе не найдено судов")
                    continue
                
                # 3. Обрабатываем суды в регионе
                for court_idx, court_url in enumerate(tqdm(court_urls, desc="Суды", leave=False), 1):
                    logger.info(f"\n  Суд {court_idx}/{len(court_urls)}")
                    
                    # Получаем ВСЕХ судей из суда
                    judge_urls = await parser.get_all_judge_urls_from_court(court_url)
                    logger.info(f"    Найдено судей: {len(judge_urls)}")
                    
                    if not judge_urls:
                        logger.warning("    В суде не найдено судей")
                        continue
                    
                    # 4. Обрабатываем судей с прогресс-баром
                    judge_tasks = []
                    for judge_url in judge_urls:
                        task = parser.parse_judge_profile(judge_url)
                        judge_tasks.append(task)
                    
                    # Выполняем задачи с прогресс-баром
                    results = []
                    for task in tqdm(asyncio.as_completed(judge_tasks), 
                                   total=len(judge_tasks), 
                                   desc="Судьи", 
                                   leave=False):
                        result = await task
                        if result:
                            results.append(result)
                    
                    all_judges_data.extend(results)
                    
                    # Пауза между судами
                    if court_idx < len(court_urls):
                        await asyncio.sleep(random.uniform(1, 2))
                
                # Пауза между регионами
                if region_idx < len(region_urls):
                    await asyncio.sleep(random.uniform(2, 3))
            
            # 5. Сохраняем результаты
            logger.info("\n" + "="*50)
            logger.info("СОХРАНЕНИЕ РЕЗУЛЬТАТОВ")
            logger.info("="*50)
            
            if all_judges_data:
                # Удаляем возможные дубликаты по profile_url
                unique_judges = {}
                for judge in all_judges_data:
                    url = judge.get('profile_url', '')
                    if url and url not in unique_judges:
                        unique_judges[url] = judge
                
                unique_judges_list = list(unique_judges.values())
                
                df = pd.DataFrame(unique_judges_list)
                
                # Определяем порядок колонок
                column_order = ['full_name', 'court', 'position', 'appointment_date', 'judge_info', 'profile_url']
                existing_columns = [col for col in column_order if col in df.columns]
                
                # Переупорядочиваем колонки
                df = df[existing_columns + [col for col in df.columns if col not in existing_columns]]
                
                # Сохраняем в Excel
                timestamp = time.strftime("%Y%m%d_%H%M%S")
                output_file = f"judges_data_{timestamp}_{len(unique_judges_list)}_records.xlsx"
                
                with pd.ExcelWriter(output_file, engine='openpyxl') as writer:
                    df.to_excel(writer, index=False, sheet_name='Судьи')
                    
                    # Добавляем сводный лист со статистикой
                    stats_df = pd.DataFrame({
                        'Метрика': ['Всего собрано', 'Уникальных записей', 'Регионов обработано', 'Время сбора'],
                        'Значение': [len(all_judges_data), len(unique_judges_list), len(region_urls), time.strftime("%Y-%m-%d %H:%M:%S")]
                    })
                    stats_df.to_excel(writer, index=False, sheet_name='Статистика')
                
                logger.info(f"✓ УСПЕХ! Данные сохранены в файл: {output_file}")
                logger.info(f"✓ Всего собрано записей: {len(all_judges_data)}")
                logger.info(f"✓ Уникальных записей: {len(unique_judges_list)}")
                
                # Показываем несколько примеров
                logger.info("\n=== ПРИМЕРЫ ДАННЫХ ===")
                for i, row in enumerate(df.head(3).to_dict('records')):
                    logger.info(f"\nПример {i+1}:")
                    logger.info(f"  ФИО: {row.get('full_name', 'N/A')}")
                    logger.info(f"  Суд: {row.get('court', 'N/A')}")
                    if row.get('appointment_date'):
                        logger.info(f"  Дата назначения: {row.get('appointment_date')}")
                    if row.get('judge_info'):
                        info_preview = row['judge_info'][:100] + "..." if len(row['judge_info']) > 100 else row['judge_info']
                        logger.info(f"  Информация: {info_preview}")
                
            else:
                logger.error("✗ Не удалось собрать ни одной записи")
                
        except KeyboardInterrupt:
            logger.info("\nПарсинг прерван пользователем")
            
            # Сохраняем то, что успели собрать
            if all_judges_data:
                # Удаляем дубликаты
                unique_judges = {}
                for judge in all_judges_data:
                    url = judge.get('profile_url', '')
                    if url and url not in unique_judges:
                        unique_judges[url] = judge
                
                df = pd.DataFrame(list(unique_judges.values()))
                output_file = f"judges_data_INTERRUPTED_{len(unique_judges)}_records.xlsx"
                df.to_excel(output_file, index=False, engine='openpyxl')
                logger.info(f"✓ Сохранены частичные данные в {output_file}")
                
        except Exception as e:
            logger.error(f"✗ Критическая ошибка: {e}")
            import traceback
            traceback.print_exc()

if __name__ == "__main__":
    # Для Windows
    import platform
    if platform.system() == 'Windows':
        asyncio.set_event_loop_policy(asyncio.WindowsSelectorEventLoopPolicy())
    
    # Запускаем парсер
    asyncio.run(main())