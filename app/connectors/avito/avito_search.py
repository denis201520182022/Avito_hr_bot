# app/connectors/avito/avito_search.py

import logging
from sqlalchemy import select
from app.db.session import AsyncSessionLocal
from app.db.models import Account, JobContext, Candidate, Dialogue
from app.connectors.avito.client import avito
from app.core.rabbitmq import mq
from app.utils.redis_lock import get_redis_client # Импортируем наш редис

logger = logging.getLogger("AvitoSearch")

class AvitoSearchService:
    async def discover_and_propose(self):
        async with AsyncSessionLocal() as db:
            stmt = select(Account).filter_by(platform="avito", is_active=True)
            accounts = (await db.execute(stmt)).scalars().all()

            for acc in accounts:
                vac_stmt = select(JobContext).filter_by(account_id=acc.id)
                vacancies = (await vac_stmt).scalars().all()

                for vac in vacancies:
                    await self._search_for_vacancy(acc, vac, db)

    async def _search_for_vacancy(self, account, vacancy, db):
        try:
            redis = get_redis_client()
            # Уникальный ключ для этой вакансии
            cursor_key = f"avito_search_cursor:{account.id}:{vacancy.id}"
            
            # 1. Достаем последний курсор из Редиса
            last_cursor = await redis.get(cursor_key)

            search_params = {
                "query": vacancy.title,
                "age_min": 30,
                "age_max": 55,
                "per_page": 20, # Берем по 20 за раз
                "fields": "location,address_details,nationality,age"
            }
            
            # Если курсор есть — добавляем его в запрос
            if last_cursor:
                search_params["cursor"] = last_cursor

            # 2. Вызываем поиск
            results = await avito.search_cvs(account, db, search_params)
            
            # 3. Сохраняем НОВЫЙ курсор из ответа Авито в Редис
            new_cursor = results.get("meta", {}).get("cursor")
            if new_cursor:
                # Храним курсор 7 дней, чтобы если вакансия долго висит, не начинать с начала
                await redis.set(cursor_key, str(new_cursor), ex=604800)

            resumes = results.get("resumes", [])
            if not resumes:
                logger.info(f"Ничего не нашли по вакансии {vacancy.title}, сбрасываем курсор.")
                await redis.delete(cursor_key) # Если дошли до конца — в след. раз начнем сначала
                return

            for cv in resumes:
                resume_id = str(cv.get("id"))

                # Проверка на дубликат (чтобы не тратить деньги на контакты)
                exists = await db.scalar(select(Candidate).filter_by(platform_user_id=resume_id))
                if exists: continue

                # Фильтрация по городу
                cv_city = cv.get("address_details", {}).get("location") or cv.get("location", {}).get("title", "")
                if (vacancy.city or "").lower() not in cv_city.lower():
                    continue

                # 4. Получаем chat_id
                try:
                    contacts_data = await avito.get_resume_contacts(account, db, resume_id)
                    chat_id = next((c["value"] for c in contacts_data.get("contacts", []) if c["type"] == "chat_id"), None)
                    
                    if not chat_id: continue

                    # Подготовка payload для унификатора (см. прошлый шаг)
                    search_payload = {
                        "search_full_name": contacts_data.get("name"),
                        "search_phone": next((c["value"] for c in contacts_data.get("contacts", []) if c["type"] == "phone"), None),
                        "cv_data": cv
                    }

                    await mq.publish("avito_inbound", {
                        "source": "avito_search_found",
                        "account_id": account.id,
                        "payload": search_payload,
                        "chat_id": chat_id,
                        "resume_id": resume_id,
                        "vacancy_id": vacancy.id
                    })
                except Exception as e:
                    logger.error(f"Ошибка контактов {resume_id}: {e}")

        except Exception as e:
            logger.error(f"Ошибка поиска: {e}")

avito_search_service = AvitoSearchService()