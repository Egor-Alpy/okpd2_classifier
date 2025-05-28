import logging
import re
import json
import os
from typing import List, Dict, Any, Optional

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
        return """ЗАДАЧА: Найти МАКСИМАЛЬНО ТОЧНЫЙ код ОКПД2 для товаров внутри класса [{CLASS_CODE} - {CLASS_NAME}] или НЕ ВОЗВРАЩАТЬ товар, если точного соответствия нет.

ИНСТРУКЦИИ:
1. Все товары предварительно отнесены к классу {CLASS_CODE}
2. Найдите НАИБОЛЕЕ СПЕЦИФИЧНЫЙ и ТОЧНЫЙ код внутри данного класса
3. Используйте СТРОГИЙ ИЕРАРХИЧЕСКИЙ подход: класс → подкласс → группа → подгруппа → вид → категория → подкатегория
4. Возвращайте ТОЛЬКО если есть ТОЧНОЕ соответствие описанию в классификаторе
5. Формат вывода: "Название товара|Полный код"
6. Если НЕТ точного соответствия - НЕ ВЫВОДИТЕ товар вообще

АЛГОРИТМ ПОИСКА:
1. Начните с изучения всех подклассов ({CLASS_CODE}.X)
2. Выберите наиболее подходящий подкласс
3. Внутри подкласса изучите все группы ({CLASS_CODE}.XX)
4. Продолжайте углубляться до самого детального уровня
5. ВАЖНО: Выбирайте код только если описание ТОЧНО соответствует товару
6. При сомнениях - лучше НЕ классифицировать

КРИТЕРИИ ТОЧНОГО СООТВЕТСТВИЯ:
- Товар подпадает под описание кода
- Учтены все ключевые характеристики товара
- Нет противоречий между товаром и описанием кода
- Если есть выбор между общим и специфичным кодом - выбирайте специфичный

ПРАВИЛА ОТСЕИВАНИЯ:
- НЕ используйте коды "прочие" (.9, .99, .190, .999 и т.д.) без крайней необходимости
- НЕ классифицируйте, если товар лишь частично соответствует описанию
- НЕ выводите товар, если его характеристики противоречат описанию кода
- НЕ угадывайте - при неуверенности товар исключается

ФОРМАТ ВЫВОДА:
Название товара|XX.XX.XX.XXX

ДЕТАЛЬНАЯ СТРУКТУРА КЛАССА [{CLASS_CODE}]:
{CLASS_STRUCTURE}

СПИСОК ТОВАРОВ:
{PRODUCTS_LIST}"""

    def build_stage_two_prompt(self, products: List[str], okpd_class: str) -> str:
        """
        Построить промпт для второго этапа

        Args:
            products: Список названий товаров
            okpd_class: 2-значный код класса ОКПД2
        """
        # Получаем структуру класса из дерева
        class_structure = self._get_class_structure(okpd_class)
        class_name = self._get_class_name(okpd_class)

        # Формируем список товаров
        products_text = "\n".join(products)

        # Заменяем плейсхолдеры в шаблоне
        prompt = self._prompt_template.replace("{CLASS_CODE}", okpd_class)
        prompt = prompt.replace("{CLASS_NAME}", class_name)
        prompt = prompt.replace("{CLASS_STRUCTURE}", class_structure)
        prompt = prompt.replace("{PRODUCTS_LIST}", products_text)

        return prompt

    def _get_class_structure(self, okpd_class: str) -> str:
        """Получить иерархическую структуру класса"""
        if not self._okpd2_tree or okpd_class not in self._okpd2_tree:
            logger.warning(f"Class {okpd_class} not found in OKPD2 tree")
            return f"# Структура класса {okpd_class} не загружена"

        # Получаем данные класса
        class_data = self._okpd2_tree[okpd_class]

        # Строим структуру
        lines = []
        self._build_structure_lines(okpd_class, class_data, lines, 0)

        return "\n".join(lines)

    def _build_structure_lines(self, current_code: str, data: Dict, lines: List[str], level: int):
        """Рекурсивно построить строки структуры"""
        # Сортируем ключи по длине и алфавиту для правильного порядка
        sorted_keys = sorted(data.keys(), key=lambda x: (len(x.split('.')), x))

        for key in sorted_keys:
            value = data[key]

            # Определяем отступ
            indent = "  " * level

            # Если это строка - это описание кода
            if isinstance(value, str):
                lines.append(f"{indent}{key} - {value}")
            # Если это словарь - рекурсивно обрабатываем (не должно быть в вашем формате)
            elif isinstance(value, dict):
                # В вашем формате не должно быть вложенных словарей
                logger.warning(f"Unexpected nested dict for key {key}")

    def _get_class_name(self, okpd_class: str) -> str:
        """Получить название класса"""
        if not self._okpd2_tree or okpd_class not in self._okpd2_tree:
            return "Неизвестный класс"

        class_data = self._okpd2_tree[okpd_class]

        # Ищем название класса - это должен быть ключ с тем же кодом
        if okpd_class in class_data and isinstance(class_data[okpd_class], str):
            return class_data[okpd_class]

        return "Класс без названия"

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
        # Поддерживаем форматы: XX.XX.X, XX.XX.XX, XX.XX.XX.XXX
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
                    "name": product_name  # Сохраняем название для отладки
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

        # В вашем формате все коды класса хранятся на одном уровне
        class_data = self._okpd2_tree[okpd_class]

        # Просто проверяем, есть ли код в словаре класса
        if code in class_data and isinstance(class_data[code], str):
            return class_data[code]

        return None