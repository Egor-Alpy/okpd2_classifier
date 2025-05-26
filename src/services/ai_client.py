from anthropic import AsyncAnthropic
from typing import List, Dict, Any, Optional
import logging
import re
import httpx
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

    @staticmethod
    def build_stage_one_prompt(products: List[str]) -> str:
        """Построить промпт для первого этапа"""
        products_text = "\n".join(products)

        prompt = f"""ЗАДАЧА: Определить ВСЕ ВОЗМОЖНЫЕ основные классы ОКПД2 для каждого товара (первые 2 цифры кода).

ИНСТРУКЦИИ:
1. Для каждого товара определите ВСЕ подходящие классы (XX)
2. Если товар может относиться к НЕСКОЛЬКИМ классам - укажите ВСЕ через символ "|"
3. Возвращайте в формате: "Название товара|XX" или "Название товара|XX|YY|ZZ"
4. Если товар НЕ подходит НИ ПОД ОДИН класс - НЕ выводите его
5. НЕ добавляйте пояснения или комментарии
6. Лучше указать больше потенциальных классов, чем пропустить подходящий

ПРАВИЛА МНОЖЕСТВЕННОЙ КЛАССИФИКАЦИИ:
- Указывайте несколько классов, если товар:
  • Может использоваться в разных сферах
  • Имеет характеристики нескольких категорий
  • Является комплексным изделием
  • Может классифицироваться по-разному в зависимости от контекста
- Приоритет: лучше перестраховаться на первом этапе

ФОРМАТ ВЫВОДА:
Название товара|XX
Название товара|XX|YY
Название товара|XX|YY|ZZ

ОСНОВНЫЕ КЛАССЫ ОКПД2:
01 - Продукция сельского хозяйства
02 - Продукция лесоводства
03 - Рыба и рыбоводство
05 - Уголь
06 - Нефть и газ
07 - Руды металлические
08 - Полезные ископаемые прочие
10 - Продукты пищевые
11 - Напитки
12 - Табачные изделия
13 - Текстиль
14 - Одежда
15 - Кожа и изделия из кожи
16 - Древесина и изделия из дерева
17 - Бумага и бумажные изделия
18 - Услуги печатные
19 - Нефтепродукты
20 - Химические вещества
21 - Лекарственные средства
22 - Изделия резиновые и пластмассовые
23 - Минеральные продукты неметаллические
24 - Металлы основные
25 - Металлические изделия готовые
26 - Компьютеры и электроника
27 - Оборудование электрическое
28 - Машины и оборудование
29 - Автотранспортные средства
30 - Транспортные средства прочие
31 - Мебель
32 - Изделия готовые прочие
33 - Ремонт машин и оборудования
35 - Электроэнергия, газ, пар
36 - Водоснабжение
38 - Утилизация отходов
41 - Здания
42 - Сооружения
43 - Строительные работы специальные
45 - Торговля автотранспортом
46 - Оптовая торговля
47 - Розничная торговля
49 - Наземный транспорт
50 - Водный транспорт
51 - Воздушный транспорт
52 - Складирование
53 - Почта и курьерские услуги
55 - Гостиницы
56 - Общественное питание
58 - Издательская деятельность
59 - Кино и видео
60 - Теле- и радиовещание
61 - Телекоммуникации
62 - Программное обеспечение
63 - Информационные услуги
64 - Финансовые услуги
65 - Страхование
68 - Недвижимость
69 - Юридические и бухгалтерские услуги
70 - Консалтинг
71 - Архитектура и инжиниринг
72 - Научные исследования
73 - Реклама
74 - Прочие профессиональные услуги
75 - Ветеринария
77 - Аренда и лизинг
78 - Трудоустройство
79 - Туризм
80 - Охранная деятельность
81 - Обслуживание зданий
82 - Административные услуги
84 - Государственное управление
85 - Образование
86 - Здравоохранение
87 - Социальный уход
88 - Социальные услуги
90 - Творчество и развлечения
91 - Библиотеки, архивы, музеи
92 - Азартные игры
93 - Спорт и отдых
94 - Общественные организации
95 - Ремонт компьютеров и бытовых товаров
96 - Персональные услуги

СПИСОК ТОВАРОВ:
{products_text}"""

        return prompt

    @staticmethod
    def parse_classification_response(response: str, product_map: Dict[str, str]) -> Dict[str, List[str]]:
        """
        Парсинг ответа от AI с поддержкой множественных групп

        Args:
            response: Ответ от AI
            product_map: Маппинг {название товара: id товара}

        Returns:
            Dict с результатами {product_id: [группы]}
        """
        results = {}

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
                # Извлекаем ВСЕ группы
                groups = []
                for group in parts[1:]:
                    group = group.strip()
                    if re.match(r'^\d{2}$', group):
                        groups.append(group)

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

        return results