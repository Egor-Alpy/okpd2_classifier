from fastapi import APIRouter, Depends, HTTPException
from typing import Dict, Any, List
import logging
import uuid

from src.api.dependencies import verify_api_key
from src.services.ai_client import AnthropicClient
from src.services.classifier import StageOneClassifier
from src.services.classifier_stage2 import StageTwoClassifier
from src.core.config import settings

router = APIRouter()
logger = logging.getLogger(__name__)


@router.post("/classify-tender")
async def classify_tender(
        tender_data: Dict[str, Any],
        api_key: str = Depends(verify_api_key)
):
    """
    Классифицировать товары тендера по ОКПД2

    Принимает полный JSON тендера, классифицирует товары без кода ОКПД2
    и возвращает обновленный JSON с добавленными кодами.
    """
    try:
        if not tender_data or "items" not in tender_data:
            raise HTTPException(status_code=400, detail="Invalid tender data format")

        items = tender_data.get("items", [])
        if not items:
            raise HTTPException(status_code=400, detail="No items found in tender")

        logger.info(f"Received tender with {len(items)} items for classification")

        # Фильтруем товары для классификации
        items_to_classify = []
        item_indices = {}  # Мапинг внутренних ID на индексы в исходном массиве

        for idx, item in enumerate(items):
            # Пропускаем товары, у которых уже есть код ОКПД2
            if item.get("okpd2Code") and item.get("okpd2Code") != "":
                logger.info(f"Item {item.get('id', idx)} already has OKPD2 code, skipping")
                continue

            # Проверяем наличие названия товара
            if not item.get("name"):
                logger.warning(f"Item {item.get('id', idx)} has no name, skipping")
                continue

            internal_id = str(uuid.uuid4())
            items_to_classify.append({
                "_id": internal_id,
                "title": item["name"]
            })
            item_indices[internal_id] = idx

        if not items_to_classify:
            logger.info("All items already have OKPD2 codes")
            return {
                "tender": tender_data,
                "statistics": {
                    "total": len(items),
                    "already_classified": len(items),
                    "newly_classified": 0,
                    "failed": 0
                }
            }

        logger.info(f"Found {len(items_to_classify)} items without OKPD2 codes")

        # Инициализируем AI клиент
        ai_client = AnthropicClient(
            settings.anthropic_api_key,
            settings.anthropic_model
        )

        # Создаем временный store-заглушку для классификаторов
        class MockStore:
            async def bulk_update_products(self, updates):
                pass

            async def get_statistics(self):
                return {"total": 0}

        mock_store = MockStore()

        # Классифицируем только если есть товары без кодов
        classified_count = 0
        failed_count = 0

        if items_to_classify:
            # Первый этап - определение топ-5 групп
            classifier_stage1 = StageOneClassifier(
                ai_client,
                mock_store,
                batch_size=min(50, len(items_to_classify))
            )

            logger.info("Starting stage 1 classification...")
            stage1_result = await classifier_stage1.process_batch(items_to_classify)

            # Подготовка для второго этапа
            items_for_stage2 = []
            for internal_id, groups in stage1_result["results"].items():
                if groups and len(groups) > 0:
                    # Находим товар и добавляем группы
                    for item in items_to_classify:
                        if item["_id"] == internal_id:
                            item["okpd_groups"] = groups  # Используем новое имя поля
                            items_for_stage2.append(item)
                            break
                else:
                    failed_count += 1

            # Второй этап - определение точного кода
            if items_for_stage2:
                classifier_stage2 = StageTwoClassifier(
                    ai_client,
                    mock_store,
                    batch_size=min(15, len(items_for_stage2))
                )

                logger.info(f"Starting stage 2 classification for {len(items_for_stage2)} items...")
                stage2_result = await classifier_stage2.process_batch(items_for_stage2)

                # Обновляем исходные товары тендера
                for internal_id, result_data in stage2_result["results"].items():
                    if internal_id in item_indices:
                        idx = item_indices[internal_id]
                        if result_data["code"]:
                            # Обновляем товар в исходном массиве
                            tender_data["items"][idx]["okpd2Code"] = result_data["code"]
                            # Получаем описание кода
                            code_name = classifier_stage2.prompt_builder.get_code_description(result_data["code"])
                            if code_name:
                                tender_data["items"][idx]["okpd2Name"] = code_name
                            classified_count += 1
                        else:
                            failed_count += 1
                else:
                    # Товары без точного кода тоже считаем failed
                    for item in items_for_stage2:
                        internal_id = item["_id"]
                        if internal_id not in stage2_result["results"]:
                            failed_count += 1

        # Закрываем AI клиент
        await ai_client.__aexit__(None, None, None)

        # Статистика
        already_classified = len(items) - len(items_to_classify)

        logger.info(
            f"Tender classification completed: "
            f"{already_classified} already had codes, "
            f"{classified_count} newly classified, "
            f"{failed_count} failed"
        )

        return {
            "tender": tender_data,
            "statistics": {
                "total": len(items),
                "already_classified": already_classified,
                "newly_classified": classified_count,
                "failed": failed_count
            }
        }

    except Exception as e:
        logger.error(f"Error classifying tender: {e}")
        raise HTTPException(status_code=500, detail=str(e))