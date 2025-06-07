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


@router.post("/classify-positions")
async def classify_tender_positions(
        positions: List[Dict[str, Any]],
        api_key: str = Depends(verify_api_key)
):
    """
    Классифицировать позиции тендера по ОКПД2

    Принимает список позиций в формате:
    [
        {
            "id": "position_id",
            "title": "Наименование товара"
        },
        ...
    ]

    Возвращает:
    {
        "position_id": {
            "okpd_groups": ["XX.XX.X", ...],
            "okpd2_code": "XX.XX.XX.XXX",
            "okpd2_name": "Название по ОКПД2"
        },
        ...
    }
    """
    try:
        if not positions:
            raise HTTPException(status_code=400, detail="No positions provided")

        logger.info(f"Received {len(positions)} positions for classification")

        # Подготавливаем данные для классификации
        items_to_classify = []
        id_mapping = {}

        for pos in positions:
            if "id" not in pos or "title" not in pos:
                raise HTTPException(
                    status_code=400,
                    detail="Each position must have 'id' and 'title' fields"
                )

            internal_id = str(uuid.uuid4())
            items_to_classify.append({
                "_id": internal_id,
                "title": pos["title"]
            })
            id_mapping[internal_id] = pos["id"]

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

        # Первый этап - определение топ-5 групп
        classifier_stage1 = StageOneClassifier(
            ai_client,
            mock_store,
            batch_size=min(50, len(items_to_classify))
        )

        logger.info("Starting stage 1 classification...")
        stage1_result = await classifier_stage1.process_batch(items_to_classify)

        # Подготовка результатов
        results = {}

        # Обновляем товары с группами для второго этапа
        items_for_stage2 = []
        for internal_id, groups in stage1_result["results"].items():
            original_id = id_mapping[internal_id]
            results[original_id] = {
                "okpd_groups": groups,
                "okpd2_code": None,
                "okpd2_name": None
            }

            if groups and len(groups) > 0:
                # Находим товар и добавляем группы
                for item in items_to_classify:
                    if item["_id"] == internal_id:
                        item["okpd_groups"] = groups
                        items_for_stage2.append(item)
                        break

        # Второй этап - определение точного кода
        if items_for_stage2:
            classifier_stage2 = StageTwoClassifier(
                ai_client,
                mock_store,
                batch_size=min(15, len(items_for_stage2))
            )

            logger.info(f"Starting stage 2 classification for {len(items_for_stage2)} items...")
            stage2_result = await classifier_stage2.process_batch(items_for_stage2)

            # Обновляем результаты точными кодами
            for internal_id, result_data in stage2_result["results"].items():
                original_id = id_mapping[internal_id]
                if original_id in results:
                    results[original_id]["okpd2_code"] = result_data["code"]
                    # Получаем описание кода
                    code_name = classifier_stage2.prompt_builder.get_code_description(result_data["code"])
                    results[original_id]["okpd2_name"] = code_name

        # Закрываем AI клиент
        await ai_client.__aexit__(None, None, None)

        # Статистика
        total_classified = sum(1 for r in results.values() if r["okpd_groups"])
        with_exact_code = sum(1 for r in results.values() if r["okpd2_code"])

        logger.info(
            f"Classification completed: {total_classified}/{len(positions)} classified, "
            f"{with_exact_code} with exact codes"
        )

        return {
            "results": results,
            "statistics": {
                "total": len(positions),
                "classified": total_classified,
                "with_exact_code": with_exact_code
            }
        }

    except Exception as e:
        logger.error(f"Error classifying tender positions: {e}")
        raise HTTPException(status_code=500, detail=str(e))