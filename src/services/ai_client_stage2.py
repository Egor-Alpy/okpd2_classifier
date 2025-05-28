import logging
import re
import json
import os
from typing import List, Dict, Any, Optional, Set

logger = logging.getLogger(__name__)


class PromptBuilderStage2:
    """Построитель промптов для второго этапа классификации"""

    # Путь к файлу с шаблоном промпта второго этапа
    PROMPT_TEMPLATE_PATH = "src/prompts/stage2_prompt_template.txt"

    # Путь к файлу с полным деревом ОКПД2
    OKPD2_FULL_TREE_PATH = "src/data/okpd2_full_tree.json"

    def __init__(self):
        self._prompt_template = None
        self._okpd2_tree = None
        self._load_resources()

    def _load_resources(self):
        """Загрузить шаблон промпта и дерево ОКПД2"""
        # Загружаем шаблон промпта
        try:
            with open(self.PROMPT_TEMPLATE_PATH, 'r', encoding='utf-8') as f:
                self._prompt_template = f.read()
            logger.info(f"Loaded stage 2 prompt template from {self.PROMPT_TEMPLATE_PATH}")
        except Exception as e:
            logger.error(f"Failed to load prompt template: {e}")
            self._prompt_template = self._get_fallback_template()

        # Загружаем дерево ОКПД2
        try:
            if os.path.exists(self.OKPD2_FULL_TREE_PATH):
                with open(self.OKPD2_FULL_TREE_PATH, 'r', encoding='utf-8') as f:
                    self._okpd2_tree = json.load(f)
                logger.info(f"Loaded OKPD2 tree from {self.OKPD2_FULL_TREE_PATH}")
            else:
                logger.warning(f"OKPD2 tree file not found at {self.OKPD2_FULL_TREE_PATH}")
                logger.warning("Please create this file with the full OKPD2 hierarchy")
                self._okpd2_tree = {}
        except Exception as e:
            logger.error(f"Failed to load OKPD2 tree: {e}")
            self._okpd2_tree = {}

    def _get_fallback_template(self) -> str:
        """Встроенный шаблон промпта как fallback"""
        return """ЗАДАЧА: Найти ОДИН МАКСИМАЛЬНО ТОЧНЫЙ код ОКПД2 для каждого товара из предоставленных веток.

ИНСТРУКЦИИ:
1. Для каждого товара изучите ВСЕ предоставленные коды
2. Выберите ОДИН НАИБОЛЕЕ ТОЧНЫЙ код
3. Возвращайте в формате: "Название товара|Полный код"
4. Если НИ ОДИН код не подходит - НЕ выводите товар

ФОРМАТ ВЫВОДА:
Название товара|XX.XX.XX.XXX

ДОСТУПНЫЕ КОДЫ:
{OKPD2_CODES}

СПИСОК ТОВАРОВ:
{PRODUCTS_LIST}"""

    def build_stage_two_prompt(self, products: List[Dict[str, Any]]) -> str:
        """
        Построить промпт для второго этапа

        Args:
            products: Список товаров с их топ-5 группами
                     [{"title": "...", "okpd_groups": ["XX.XX.X", ...]}]
        """
        # Собираем все уникальные группы из всех товаров
        all_groups = set()
        for product in products:
            if product.get("okpd_group"):
                all_groups.update(product["okpd_group"])

        # Получаем все коды для этих групп
        all_codes = self._get_all_codes_for_groups(list(all_groups))

        # Формируем список товаров
        products_text = "\n".join([p["title"] for p in products])

        # Формируем текст с кодами
        codes_text = self._format_codes_text(all_codes)

        # Заменяем плейсхолдеры в шаблоне
        prompt = self._prompt_template.replace("{OKPD2_CODES}", codes_text)
        prompt = prompt.replace("{PRODUCTS_LIST}", products_text)

        return prompt

    def _get_all_codes_for_groups(self, groups: List[str]) -> Dict[str, str]:
        """
        Получить все коды-продолжения для списка 5-значных групп

        Args:
            groups: Список 5-значных групп (XX.XX.X)

        Returns:
            Dict {код: описание} для всех продолжений
        """
        all_codes = {}

        for group in groups:
            # Получаем 2-значный класс из группы
            class_code = group[:2]

            if class_code not in self._okpd2_tree:
                logger.warning(f"Class {class_code} not found in OKPD2 tree")
                continue

            class_data = self._okpd2_tree[class_code]

            # Ищем все коды, начинающиеся с нашей группы
            for code, description in class_data.items():
                if code.startswith(group) and isinstance(description, str):
                    all_codes[code] = description

        logger.info(f"Found {len(all_codes)} codes for {len(groups)} groups")
        return all_codes

    def _format_codes_text(self, codes: Dict[str, str]) -> str:
        """Форматировать коды для промпта"""
        # Группируем коды по 5-значным группам для удобства
        groups = {}
        for code, description in codes.items():
            # Извлекаем 5-значную группу
            if len(code) >= 7:  # XX.XX.X
                group = code[:7]
                if group not in groups:
                    groups[group] = []
                groups[group].append((code, description))

        # Формируем текст
        lines = []
        for group in sorted(groups.keys()):
            lines.append(f"\n# Группа {group}")
            # Сортируем коды внутри группы
            for code, desc in sorted(groups[group]):
                lines.append(f"{code} - {desc}")

        return "\n".join(lines)

    @staticmethod
    def parse_stage2_response(response: str, product_map: Dict[str, str]) -> Dict[str, Dict[str, str]]:
        """
        Парсинг ответа от AI для второго этапа

        Args:
            response: Ответ от AI
            product_map: Маппинг {название товара: id товара}

        Returns:
            Dict с результатами {product_id: {"code": "XX.XX.XX.XXX", "name": "..."}}
        """
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

        # Получаем класс (первые 2 цифры)
        okpd_class = code[:2]

        if okpd_class not in self._okpd2_tree:
            return None

        class_data = self._okpd2_tree[okpd_class]

        # Проверяем, есть ли код в словаре класса
        if code in class_data and isinstance(class_data[code], str):
            return class_data[code]

        return None