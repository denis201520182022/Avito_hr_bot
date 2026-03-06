import logging
import datetime
from sqlalchemy import select, and_, update
from app.db.session import AsyncSessionLocal
from app.db.models import (
    Account, 
    JobContext, 
    Candidate, 
    AvitoSearchStatus, 
    AvitoSearchStat
)
from app.connectors.avito.client import avito
from app.core.rabbitmq import mq
from app.utils.redis_lock import get_redis_client

logger = logging.getLogger("AvitoSearch")

class AvitoSearchService:
    async def discover_and_propose(self):
        """Основной цикл: обход аккаунтов и запуск поиска по вакансиям"""
        async with AsyncSessionLocal() as db:
            # 1. Получаем все активные аккаунты Авито
            stmt = select(Account).filter_by(platform="avito", is_active=True)
            accounts = (await db.execute(stmt)).scalars().all()

            for acc in accounts:
                # 2. ПРОВЕРКА ГЛОБАЛЬНОГО РУБИЛЬНИКА (из таблицы AvitoSearchStatus)
                status_stmt = select(AvitoSearchStatus).filter_by(account_id=acc.id)
                status = await db.scalar(status_stmt)
                
                if not status or not status.is_enabled:
                    logger.info(f"⏸ Поиск для аккаунта '{acc.name}' (ID: {acc.id}) выключен пользователем.")
                    continue

                # 3. Получаем активные вакансии аккаунта, у которых остались квоты
                vac_stmt = select(JobContext).filter(
                    and_(
                        JobContext.account_id == acc.id,
                        JobContext.is_active == True,
                        JobContext.search_remaining_quota > 0
                    )
                )
                vacancies = (await db.execute(vac_stmt)).scalars().all()

                if not vacancies:
                    logger.debug(f"Нет активных вакансий с лимитами для аккаунта {acc.name}")
                    continue

                for vac in vacancies:
                    # 4. Проверка: не закрыта ли вакансия на самом Авито?
                    is_still_active = await self._check_avito_vacancy_status(acc, vac, db)
                    
                    if is_still_active:
                        await self._search_for_vacancy(acc, vac, db)
                    else:
                        logger.info(f"🚫 Вакансия '{vac.title}' ({vac.external_id}) деактивирована на Авито. Пропускаем.")
            
            await db.commit()

    async def _check_avito_vacancy_status(self, account, vacancy, db) -> bool:
        """Синхронизация статуса вакансии с API Авито"""
        try:
            vac_data = await avito.get_job_details(vacancy.external_id, account, db)
            status_from_api = vac_data.raw_json.get("is_active", False)
            
            if not status_from_api:
                vacancy.is_active = False # Синхронизируем нашу БД
                return False
            return True
        except Exception as e:
            logger.error(f"Ошибка проверки статуса вакансии {vacancy.external_id}: {e}")
            return True # В случае ошибки API считаем вакансию активной, чтобы не прерывать поиск

    async def _update_daily_stats(self, db, account_id, vacancy_id):
        """Обновление ежедневной статистики затрат"""
        today = datetime.date.today()
        stat_stmt = select(AvitoSearchStat).filter(
            and_(
                AvitoSearchStat.account_id == account_id,
                AvitoSearchStat.vacancy_id == vacancy_id,
                AvitoSearchStat.date == today
            )
        )
        stat = await db.scalar(stat_stmt)

        if stat:
            stat.spent_count += 1
        else:
            new_stat = AvitoSearchStat(
                account_id=account_id,
                vacancy_id=vacancy_id,
                date=today,
                spent_count=1
            )
            db.add(new_stat)

    async def _search_for_vacancy(self, account, vacancy, db):
        """Логика поиска резюме под конкретную вакансию"""
        try:
            redis = get_redis_client()
            cursor_key = f"avito_search_cursor:{account.id}:{vacancy.id}"
            last_cursor = await redis.get(cursor_key)

            # --- ФОРМИРОВАНИЕ ПАРАМЕТРОВ ПОИСКА ИЗ БД ---
            f = vacancy.search_filters or {}
            
            # Базовый набор (всегда есть)
            search_params = {
                "query": f.get("query") or vacancy.title,
                "per_page": 20,
                "fields": "location,address_details,nationality,age,education,experience,driving"
            }

            # Маппинг всех гибких фильтров из вашей таблицы
            # (Синхронизатор уже подготовил ID, здесь просто прокидываем их в API)
            # Маппинг всех гибких фильтров из вашей таблицы
            optional_fields = [
                "location", "specialization", "schedule", "age_min", "age_max",
                "gender", "education_level", "experience_min", "experience_max",
                "nationality", "medical_book", "salary_min", "salary_max",
                "driver_licence", "driver_licence_category", "driving_experience",
                "own_transport", "business_trip_readiness", "relocation_readiness"
            ]

            for field in optional_fields:
                val = f.get(field)
                
                # ПРОВЕРКА: 
                # 1. Значение не None (уже обработано синхронизатором)
                # 2. Значение не пустая строка
                # 3. Значение не строка 'null'
                if val is not None:
                    str_val = str(val).strip()
                    if str_val != "" and str_val.lower() != 'null':
                        search_params[field] = val

            if last_cursor: 
                search_params["cursor"] = last_cursor

            # Выполнение запроса к API Авито
            results = await avito.search_cvs(account, db, search_params)
            new_cursor = results.get("meta", {}).get("cursor")
            resumes = results.get("resumes", [])

            if not resumes:
                logger.info(f"Поиск '{vacancy.title}' не дал результатов, сброс курсора.")
                await redis.delete(cursor_key)
                return

            # Сохраняем новый курсор для следующего прогона
            if new_cursor:
                await redis.set(cursor_key, str(new_cursor), ex=604800) # 7 дней жизни курсора

            opened_count = 0
            MAX_PER_RUN = 5 # Ограничение на один прогон для экономии

            for cv in resumes:
                if opened_count >= MAX_PER_RUN:
                    break

                resume_id = str(cv.get("id"))

                # 1. Проверка на дубликаты (Бесплатно)
                exists = await db.scalar(select(Candidate).filter_by(platform_user_id=resume_id))
                if exists:
                    continue

                # 2. АТОМАРНОЕ СПИСАНИЕ КВОТЫ ВАКАНСИИ
                # Используем SQL update с условием, чтобы избежать race condition
                quota_stmt = (
                    update(JobContext)
                    .where(and_(
                        JobContext.id == vacancy.id,
                        JobContext.search_remaining_quota > 0
                    ))
                    .values(search_remaining_quota=JobContext.search_remaining_quota - 1)
                    .returning(JobContext.search_remaining_quota)
                )
                
                res = await db.execute(quota_stmt)
                new_limit = res.scalar()

                if new_limit is None:
                    logger.warning(f"🛑 Квоты вакансии '{vacancy.title}' исчерпаны.")
                    break

                try:
                    # 3. ПОЛУЧЕНИЕ КОНТАКТОВ (Платно)
                    logger.info(f"💰 Открываем контакты {resume_id} для вакансии '{vacancy.title}'. Остаток: {new_limit}")
                    contacts_data = await avito.get_resume_contacts(account, db, resume_id)
                    
                    contacts_list = contacts_data.get("contacts", [])
                    chat_id = next((c["value"] for c in contacts_list if c["type"] == "chat_id"), None)
                    
                    if not chat_id:
                        logger.warning(f"У резюме {resume_id} нет chat_id. Лимит списан Авито за телефон.")
                        continue

                    # 4. Обновление статистики и отправка в Engine
                    await self._update_daily_stats(db, account.id, vacancy.id)

                    await mq.publish("avito_inbound", {
                        "source": "avito_search_found",
                        "account_id": account.id,
                        "avito_user_id": account.auth_data.get("user_id"),
                        "payload": {
                            "search_full_name": contacts_data.get("name"),
                            "search_phone": next((c["value"] for c in contacts_list if c["type"] == "phone"), None),
                            "cv_data": cv # Полные данные резюме для LLM
                        },
                        "chat_id": chat_id,
                        "resume_id": resume_id,
                        "vacancy_id": vacancy.id
                    })
                    
                    opened_count += 1
                    await db.commit() # Фиксируем списание каждой квоты немедленно

                except Exception as api_err:
                    # 5. ВОЗВРАТ КВОТЫ при системной ошибке запроса
                    logger.error(f"❌ Ошибка API для {resume_id}, возвращаем квоту: {api_err}")
                    await db.execute(
                        update(JobContext)
                        .where(JobContext.id == vacancy.id)
                        .values(search_remaining_quota=JobContext.search_remaining_quota + 1)
                    )
                    await db.commit()
                    continue

        except Exception as e:
            logger.error(f"Ошибка в процессе поиска по вакансии {vacancy.id}: {e}", exc_info=True)

avito_search_service = AvitoSearchService()