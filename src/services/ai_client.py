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

    async def classify_batch(self, prompt: str, max_tokens: int = 4000) -> str:
        """Отправить запрос на классификацию"""
        await self._ensure_client()

        try:
            logger.debug(f"Sending request to Anthropic API via {'proxy' if self.proxy_url else 'direct connection'}")

            response = await self.client.messages.create(
                model=self.model,
                max_tokens=max_tokens,
                messages=[{"role": "user", "content": prompt}],
                temperature=0.0
            )

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
    """Построитель промптов для классификации"""

    # Путь к файлу с шаблоном промпта
    PROMPT_TEMPLATE_PATH = "prompts/stage1_prompt_template.txt"

    # Путь к файлу со списком групп ОКПД2 (будет создан пользователем)
    OKPD2_GROUPS_PATH = "data/okpd2_5digit_groups.txt"

    def __init__(self):
        self._prompt_template = None
        self._okpd2_groups = None
        self._load_resources()

    def _load_resources(self):
        """Загрузить шаблон промпта и список групп ОКПД2"""
        # Загружаем шаблон промпта
        try:
            with open(self.PROMPT_TEMPLATE_PATH, 'r', encoding='utf-8') as f:
                self._prompt_template = f.read()
            logger.info(f"Loaded prompt template from {self.PROMPT_TEMPLATE_PATH}")
        except Exception as e:
            logger.error(f"Failed to load prompt template: {e}")
            # Используем встроенный шаблон как fallback
            self._prompt_template = self._get_fallback_template()

        # Загружаем список групп ОКПД2
        try:
            if os.path.exists(self.OKPD2_GROUPS_PATH):
                with open(self.OKPD2_GROUPS_PATH, 'r', encoding='utf-8') as f:
                    self._okpd2_groups = f.read()
                logger.info(f"Loaded OKPD2 groups from {self.OKPD2_GROUPS_PATH}")
            else:
                logger.warning(f"OKPD2 groups file not found at {self.OKPD2_GROUPS_PATH}")
                logger.warning("Please create this file with format: XX.XX.X - Group Name")
                self._okpd2_groups = "# PLACEHOLDER: Please add OKPD2 5-digit groups here"
        except Exception as e:
            logger.error(f"Failed to load OKPD2 groups: {e}")
            self._okpd2_groups = "# ERROR loading OKPD2 groups"

    def _get_fallback_template(self) -> str:
        """Встроенный шаблон промпта как fallback"""
        return """ЗАДАЧА: Определить ВСЕ ВОЗМОЖНЫЕ группы ОКПД2 для каждого товара (первые 5 цифр кода в формате XX.XX.X).

ИНСТРУКЦИИ:
1. Для каждого товара определите ВСЕ подходящие группы (XX.XX.X)
2. Если товар может относиться к НЕСКОЛЬКИМ группам - укажите ВСЕ через символ "|"
3. Возвращайте в формате: "Название товара|XX.XX.X" или "Название товара|XX.XX.X|YY.YY.Y"
4. Если товар НЕ подходит НИ ПОД ОДНУ группу - НЕ выводите его
5. НЕ добавляйте пояснения или комментарии
6. Лучше указать больше потенциальных групп, чем пропустить подходящую

ФОРМАТ ВЫВОДА:
Название товара|XX.XX.X
Название товара|XX.XX.X|YY.YY.Y

ГРУППЫ ОКПД2:
{OKPD2_GROUPS_PLACEHOLDER}

СПИСОК ТОВАРОВ:
{PRODUCTS_LIST}"""

    def build_stage_one_prompt(self, products: List[str]) -> str:
        """Построить промпт для первого этапа с 5-значными группами"""
        products_text = "\n".join(products)

        # Заменяем плейсхолдеры в шаблоне
        prompt = self._prompt_template.replace(
            "{OKPD2_GROUPS_PLACEHOLDER}",
            self._okpd2_groups
        ).replace(
            "{PRODUCTS_LIST}",
            products_text
        )

        return prompt

    @staticmethod
    def parse_classification_response(response: str, product_map: Dict[str, str]) -> Dict[str, List[str]]:
        """
        Парсинг ответа от AI с поддержкой 5-значных групп ОКПД2

        Args:
            response: Ответ от AI
            product_map: Маппинг {название товара: id товара}

        Returns:
            Dict с результатами {product_id: [группы]}
        """
        results = {}

        # Регулярное выражение для 5-значных кодов ОКПД2 (XX.XX.X)
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
                # Извлекаем ВСЕ группы с валидацией формата
                groups = []
                for group in parts[1:]:
                    group = group.strip()
                    if okpd2_pattern.match(group):
                        groups.append(group)
                    else:
                        logger.warning(f"Invalid OKPD2 group format: {group}")

                if groups:
                    # Убираем дубликаты
                    seen = set()
                    unique_groups = []
                    for g in groups:
                        if g not in seen:
                            seen.add(g)
                            unique_groups.append(g)

                    results[product_id] = unique_groups
                    logger.debug(f"Product '{product_name}' classified with groups: {unique_groups}")
                else:
                    logger.warning(f"No valid groups found for product '{product_name}'")

        return results