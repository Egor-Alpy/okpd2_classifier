from anthropic import AsyncAnthropic
from typing import List, Dict, Any, Optional
import logging
import re
import httpx
import os
from src.core.config import settings

logger = logging.getLogger(__name__)


class AnthropicClient:
    def __init__(self, api_key: str, model: str):
        self.proxy_url = settings.proxy_url
        self.api_key = api_key
        self.model = model
        self.client = None
        self._http_client = None
        self.enable_caching = settings.enable_prompt_caching

    async def _ensure_client(self):
        """Создать клиент при необходимости"""
        if self.client is None:
            if self.proxy_url:
                logger.info(f"Using proxy for Anthropic API: {self.proxy_url}")
                self._http_client = httpx.AsyncClient(
                    proxy=self.proxy_url,
                    timeout=httpx.Timeout(30.0, connect=10.0)
                )
                self.client = AsyncAnthropic(
                    api_key=self.api_key,
                    http_client=self._http_client
                )
            else:
                logger.info("No proxy configured for Anthropic API")
                self.client = AsyncAnthropic(api_key=self.api_key)

    async def classify_batch(self, prompt: str, cached_content: str = None, max_tokens: int = 4000) -> str:
        """Отправить запрос на классификацию с поддержкой кэширования"""
        await self._ensure_client()

        try:
            # Формируем сообщение с кэшированием
            if self.enable_caching and cached_content:
                messages = [{
                    "role": "user",
                    "content": [
                        {
                            "type": "text",
                            "text": cached_content,
                            "cache_control": {"type": "ephemeral"}
                        },
                        {
                            "type": "text",
                            "text": prompt  # Динамическая часть
                        }
                    ]
                }]

                # Добавляем header для prompt caching
                extra_headers = {"anthropic-beta": "prompt-caching-2024-07-31"}

                logger.debug("Sending request with prompt caching enabled")
            else:
                # Обычный запрос без кэширования
                messages = [{"role": "user", "content": prompt}]
                extra_headers = None

            response = await self.client.messages.create(
                model=self.model,
                max_tokens=max_tokens,
                messages=messages,
                temperature=0.0,
                extra_headers=extra_headers
            )

            # Логируем информацию о кэше
            if hasattr(response, 'usage'):
                usage = response.usage
                if hasattr(usage, 'cache_creation_input_tokens'):
                    logger.info(f"Cache creation tokens: {usage.cache_creation_input_tokens}")
                if hasattr(usage, 'cache_read_input_tokens'):
                    logger.info(f"Cache read tokens: {usage.cache_read_input_tokens}")
                logger.info(f"Total input tokens: {usage.input_tokens}")

            return response.content[0].text

        except httpx.ProxyError as e:
            logger.error(f"Proxy error when calling Anthropic API: {e}")
            logger.error(f"Please check your proxy settings: {self.proxy_url}")
            raise
        except httpx.ConnectError as e:
            logger.error(f"Connection error when calling Anthropic API: {e}")
            logger.error("If you need a proxy/VPN, please configure HTTP_PROXY, HTTPS_PROXY or SOCKS_PROXY in .env")
            raise
        except Exception as e:
            logger.error(f"Error calling Anthropic API: {e}")
            raise

    async def __aenter__(self):
        await self._ensure_client()
        return self

    async def __aexit__(self, exc_type, exc_val, exc_tb):
        """Закрыть клиент при выходе из контекста"""
        if self.client:
            await self.client.close()
        if self._http_client:
            await self._http_client.aclose()


class PromptBuilder:
    """Построитель промптов для классификации с поддержкой кэширования"""

    # Путь к файлу со списком групп ОКПД2 (сокращенный)
    OKPD2_GROUPS_PATH = "src/data/okpd2_5digit_groups_optimized.txt"

    def __init__(self):
        self._okpd2_groups = None
        self._cached_content = None
        self._load_resources()

    def _load_resources(self):
        """Загрузить список групп ОКПД2"""
        try:
            if os.path.exists(self.OKPD2_GROUPS_PATH):
                with open(self.OKPD2_GROUPS_PATH, 'r', encoding='utf-8') as f:
                    self._okpd2_groups = f.read()
                logger.info(f"Loaded OKPD2 groups from {self.OKPD2_GROUPS_PATH}")

                # Проверяем размер
                groups_size = len(self._okpd2_groups)
                logger.info(f"OKPD2 groups size: {groups_size:,} characters")

                if groups_size > 130000:
                    logger.warning(
                        f"OKPD2 groups file is too large ({groups_size} chars). Please optimize to ~130k chars")
            else:
                logger.error(f"OKPD2 groups file not found at {self.OKPD2_GROUPS_PATH}")
                logger.error("Please create optimized file with ~130k characters (40k tokens)")
                self._okpd2_groups = "# ERROR: OKPD2 groups file not found"
        except Exception as e:
            logger.error(f"Failed to load OKPD2 groups: {e}")
            self._okpd2_groups = "# ERROR loading OKPD2 groups"

    def get_cached_content(self) -> str:
        """Получить кэшируемую часть промпта"""
        if not self._cached_content:
            self._cached_content = f"""ЗАДАЧА: Определить ТОП-5 НАИБОЛЕЕ ПОДХОДЯЩИХ групп ОКПД2 для каждого товара (первые 5 цифр кода в формате XX.XX.X).

ИНСТРУКЦИИ:
1. Для каждого товара определите от 1 до 5 НАИБОЛЕЕ ПОДХОДЯЩИХ групп (XX.XX.X)
2. Расположите группы В ПОРЯДКЕ УБЫВАНИЯ РЕЛЕВАНТНОСТИ (первая - самая подходящая)
3. Возвращайте в формате: "Название товара|XX.XX.X|YY.YY.Y|ZZ.ZZ.Z" (максимум 5 групп)
4. Если товар НЕ подходит НИ ПОД ОДНУ группу - НЕ выводите его
5. НЕ добавляйте пояснения или комментарии
6. Старайтесь включить все потенциально подходящие группы (до 5 штук)
7. Используйте ТОЛЬКО коды из предоставленного списка ОКПД2

ПРАВИЛА ВЫБОРА ТОП-5:
- Первая группа - наиболее точно описывающая товар
- Следующие группы - альтернативные варианты классификации
- Учитывайте различные аспекты товара (материал, назначение, сфера применения)
- Если товар явно подходит только под 1-2 группы - укажите только их
- НЕ добавляйте группы "для количества" - только реально подходящие

ФОРМАТ ВЫВОДА:
Название товара|XX.XX.X
Название товара|XX.XX.X|YY.YY.Y
Название товара|XX.XX.X|YY.YY.Y|ZZ.ZZ.Z|AA.AA.A|BB.BB.B

ПРИМЕРЫ:
Хлеб пшеничный|10.71.1
Ноутбук HP|26.20.1|26.20.3|26.20.2
Услуги по ремонту компьютеров|95.11.1|33.12.1|62.09.1
Кабель электрический|27.32.1|25.93.1|27.90.3
Принтер с функцией сканера|26.20.4|28.23.2|26.20.3|26.30.1
Стол офисный деревянный|31.01.1|31.09.1|25.99.2
Программное обеспечение|58.29.1|58.29.2|58.29.3|58.29.4|62.01.1
Услуги IT-консалтинга|62.02.1|62.02.2|62.09.2|70.22.1|62.03.1
Молоко пастеризованное|10.51.1|10.51.5|10.51.2
Журнал бухгалтерский|17.23.1|58.14.1|17.12.1
Услуги грузоперевозок|49.41.1|49.41.2|52.29.1|52.24.1

ГРУППЫ ОКПД2 (5 ЦИФР):
{self._okpd2_groups}"""

        return self._cached_content

    def build_products_prompt(self, products: List[str]) -> str:
        """Построить промпт только с товарами (динамическая часть)"""
        return f"\nСПИСОК ТОВАРОВ:\n" + "\n".join(products)

    @staticmethod
    def parse_classification_response(response: str, product_map: Dict[str, str]) -> Dict[str, List[str]]:
        """Парсинг ответа от AI с поддержкой топ-5 групп ОКПД2"""
        results = {}

        # Регулярное выражение для 5-значных кодов ОКПД2
        okpd2_pattern = re.compile(r'^\d{2}\.\d{2}\.\d$')

        for line in response.strip().split('\n'):
            if '|' not in line:
                continue

            parts = line.split('|')
            if len(parts) < 2:
                continue

            product_name = parts[0].strip()

            # Ищем товар в маппинге
            product_id = None

            # Сначала точное совпадение
            if product_name in product_map:
                product_id = product_map[product_name]
            else:
                # Затем частичное совпадение
                for name, pid in product_map.items():
                    name_lower = name.lower().strip()
                    product_lower = product_name.lower().strip()

                    if (product_lower in name_lower or
                            name_lower in product_lower or
                            (name_lower.split() and product_lower.split() and
                             name_lower.split()[0] == product_lower.split()[0])):
                        product_id = pid
                        break

            if product_id:
                # Извлекаем группы с валидацией формата (максимум 5)
                groups = []
                for group in parts[1:6]:  # Берем максимум 5 групп
                    group = group.strip()
                    if okpd2_pattern.match(group):
                        groups.append(group)
                    else:
                        logger.warning(f"Invalid OKPD2 group format: {group}")

                if groups:
                    # Сохраняем порядок (первая группа - самая релевантная)
                    results[product_id] = groups
                    logger.debug(f"Product '{product_name}' classified with top groups: {groups}")
                else:
                    logger.warning(f"No valid groups found for product '{product_name}'")

        return results