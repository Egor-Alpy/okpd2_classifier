import logging
import re
import json
import os
from typing import List, Dict, Any, Optional, Set

logger = logging.getLogger(__name__)


class PromptBuilderStage2:
    """Построитель промптов для второго этапа классификации с кэшированием"""

    # Путь к файлу с полным деревом ОКПД2
    OKPD2_FULL_TREE_PATH = "src/data/okpd2_full_tree.json"

    def __init__(self):
        self._okpd2_tree = None
        self._class_caches = {}  # Кэши для каждого класса
        self._load_resources()

    def _load_resources(self):
        """Загрузить дерево ОКПД2"""
        try:
            if os.path.exists(self.OKPD2_FULL_TREE_PATH):
                with open(self.OKPD2_FULL_TREE_PATH, 'r', encoding='utf-8') as f:
                    self._okpd2_tree = json.load(f)
                logger.info(f"Loaded OKPD2 tree from {self.OKPD2_FULL_TREE_PATH}")

                # Создаем кэши для каждого класса
                self._prepare_class_caches()
            else:
                logger.warning(f"OKPD2 tree file not found at {self.OKPD2_FULL_TREE_PATH}")
                logger.warning("Please create this file with the full OKPD2 hierarchy")
                self._okpd2_tree = {}
        except Exception as e:
            logger.error(f"Failed to load OKPD2 tree: {e}")
            self._okpd2_tree = {}

    def _prepare_class_caches(self):
        """Подготовить кэшированный контент для каждого класса"""
        base_prompt = """ЗАДАЧА: Найти ОДИН МАКСИМАЛЬНО ТОЧНЫЙ код ОКПД2 для каждого товара из предоставленных веток.

КОНТЕКСТ:
На первом этапе для товаров были определены топ-5 подходящих групп ОКПД2 (5-значные коды).
Теперь нужно найти САМЫЙ ТОЧНЫЙ код из ВСЕХ возможных продолжений этих групп.

ИНСТРУКЦИИ:
1. Для каждого товара изучите ВСЕ предоставленные коды из разных веток
2. Выберите ОДИН НАИБОЛЕЕ ТОЧНЫЙ и СПЕЦИФИЧНЫЙ код
3. Код должен максимально точно описывать конкретный товар
4. Возвращайте в формате: "Название товара|Полный код"
5. Если НИ ОДИН код не подходит точно - НЕ выводите товар

АЛГОРИТМ ВЫБОРА:
1. Изучите все доступные коды из всех веток
2. Отдайте предпочтение наиболее специфичному коду (с большим количеством цифр)
3. Код должен точно соответствовать всем характеристикам товара
4. При выборе между кодами разных веток - выбирайте более точный
5. НЕ выбирайте общие коды, если есть более специфичные

КРИТЕРИИ ТОЧНОГО СООТВЕТСТВИЯ:
- Товар полностью подпадает под описание кода
- Код учитывает ключевые характеристики товара
- Нет противоречий между товаром и описанием кода
- Выбран максимально детальный код из доступных

ПРАВИЛА ОТСЕИВАНИЯ:
- НЕ используйте коды "прочие" (.9, .99, .190, .999) если есть альтернативы
- НЕ выбирайте общие коды при наличии специфичных
- НЕ выводите товар при отсутствии точного соответствия
- При сомнениях - лучше не классифицировать

ФОРМАТ ВЫВОДА:
Название товара|XX.XX.XX.XXX

ПРИМЕРЫ:
Журнал регистрации входящих документов|17.23.11.120
Принтер лазерный цветной A4|26.20.40.110
Услуги по разработке мобильных приложений|62.01.21.000
[Товар "Канцелярские принадлежности" - НЕ выводится, слишком общее]
[Товар "Неизвестное оборудование" - НЕ выводится, нет данных]

"""

        # Для каждого класса создаем кэшированный контент
        for class_code, class_data in self._okpd2_tree.items():
            codes_text = self._format_class_codes(class_code, class_data)
            self._class_caches[class_code] = base_prompt + f"\nДОСТУПНЫЕ КОДЫ КЛАССА {class_code}:\n{codes_text}"

            # Логируем размер кэша
            cache_size = len(self._class_caches[class_code])
            logger.debug(f"Cache for class {class_code}: {cache_size:,} chars")

    def _format_class_codes(self, class_code: str, class_data: Dict[str, str]) -> str:
        """Форматировать коды класса для кэша"""
        lines = []

        # Группируем по 5-значным группам
        groups = {}
        for code, description in class_data.items():
            if isinstance(description, str) and len(code) >= 7:
                group = code[:7]
                if group not in groups:
                    groups[group] = []
                groups[group].append((code, description))

        # Форматируем
        for group in sorted(groups.keys()):
            lines.append(f"\n# Группа {group}")
            for code, desc in sorted(groups[group]):
                lines.append(f"{code} - {desc}")

        return "\n".join(lines)

    def get_cached_content_for_groups(self, okpd_groups: List[str]) -> Optional[str]:
        """Получить объединенный кэшированный контент для списка групп"""
        # Извлекаем уникальные классы из групп
        classes = set()
        for group in okpd_groups:
            if group and len(group) >= 2:
                classes.add(group[:2])

        if not classes:
            return None

        # Если только один класс - возвращаем его кэш
        if len(classes) == 1:
            class_code = list(classes)[0]
            return self._class_caches.get(class_code)

        # Если несколько классов - объединяем коды
        # (это менее эффективно, но редкий случай)
        all_codes = {}
        for class_code in classes:
            if class_code in self._okpd2_tree:
                class_data = self._okpd2_tree[class_code]
                for group in okpd_groups:
                    if group.startswith(class_code):
                        for code, desc in class_data.items():
                            if code.startswith(group) and isinstance(desc, str):
                                all_codes[code] = desc

        if not all_codes:
            return None

        # Создаем временный промпт
        codes_text = self._format_codes_text("MULTI", all_codes)
        return self._get_base_prompt() + f"\nДОСТУПНЫЕ КОДЫ:\n{codes_text}"

    def _get_base_prompt(self) -> str:
        """Получить базовый промпт без кодов"""
        return """ЗАДАЧА: Найти ОДИН МАКСИМАЛЬНО ТОЧНЫЙ код ОКПД2 для каждого товара.

ИНСТРУКЦИИ:
1. Выберите ОДИН НАИБОЛЕЕ ТОЧНЫЙ код
2. Возвращайте в формате: "Название товара|Полный код"
3. Если НИ ОДИН код не подходит - НЕ выводите товар

ФОРМАТ ВЫВОДА:
Название товара|XX.XX.XX.XXX"""

    def build_products_prompt_stage2(self, products: List[Dict[str, Any]]) -> str:
        """Построить динамическую часть промпта для второго этапа"""
        products_text = "\n".join([p["title"] for p in products])
        return f"\nСПИСОК ТОВАРОВ:\n{products_text}"

    @staticmethod
    def parse_stage2_response(response: str, product_map: Dict[str, str]) -> Dict[str, Dict[str, str]]:
        """Парсинг ответа от AI для второго этапа"""
        results = {}

        # Регулярное выражение для полных кодов ОКПД2
        okpd2_pattern = re.compile(r'^\d{2}\.\d{2}\.\d+(\.\d+)*$')

        for line in response.strip().split('\n'):
            if '|' not in line:
                continue

            parts = line.split('|')
            if len(parts) != 2:
                continue

            product_name = parts[0].strip()
            code = parts[1].strip()

            # Проверяем формат кода
            if not okpd2_pattern.match(code):
                logger.warning(f"Invalid OKPD2 code format: {code}")
                continue

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
                results[product_id] = {
                    "code": code,
                    "name": product_name
                }
                logger.debug(f"Product '{product_name}' classified with code: {code}")
            else:
                logger.warning(f"Product '{product_name}' not found in mapping")

        return results

    def get_code_description(self, code: str) -> Optional[str]:
        """Получить описание кода из дерева ОКПД2"""
        if not self._okpd2_tree:
            return None

        # Получаем класс
        okpd_class = code[:2]

        if okpd_class not in self._okpd2_tree:
            return None

        class_data = self._okpd2_tree[okpd_class]

        # Проверяем, есть ли код в словаре класса
        if code in class_data and isinstance(class_data[code], str):
            return class_data[code]

        return None