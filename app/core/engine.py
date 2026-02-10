# app/core/engine.py
import logging
from sqlalchemy import select
from app.db.session import AsyncSessionLocal
from app.db.models import Account, JobContext, Candidate, Dialogue
from app.connectors.avito.client import avito
from app.core.schemas import EngineTaskDTO
from app.core.rabbitmq import mq
from app.services.llm import llm_service
import datetime

logger = logging.getLogger(__name__)

class InboundDispatcher:
    async def process_avito_event(self, payload: dict):
        """Разбор сырого вебхука Авито и обновление БД"""
        
        # Авито присылает разные типы вебхуков. 
        # 1. Отклик (Job API) содержит 'applyId'
        # 2. Сообщение (Messenger API) содержит 'content', 'chat_id'
        
        if "applyId" in payload:
            await self._handle_new_application(payload)
        elif "content" in payload and payload.get("direction") == "in":
            await self._handle_new_message(payload)

    async def _handle_new_application(self, payload: dict):
        """Обработка нового отклика (п. 5 и 6 ТЗ)"""
        apply_id = payload["applyId"]
        
        async with AsyncSessionLocal() as db:
            # 1. Получаем детали отклика через API
            try:
                candidate_data = await avito.get_candidate_details(apply_id)
                vacancy_id = payload.get("vacancyId") # или тянем из деталей
                
                # 2. Ищем или создаем вакансию (JobContext)
                job = await db.scalar(select(JobContext).filter_by(external_id=str(vacancy_id)))
                if not job:
                    job_details = await avito.get_job_details(vacancy_id)
                    job = JobContext(
                        external_id=str(vacancy_id),
                        title=job_details.title,
                        description_data={"text": job_details.description}
                    )
                    db.add(job)
                    await db.flush()

                # 3. Ищем или создаем кандидата
                candidate = await db.scalar(select(Candidate).filter_by(platform_user_id=candidate_data.platform_user_id))
                if not candidate:
                    candidate = Candidate(
                        platform_user_id=candidate_data.platform_user_id,
                        full_name=candidate_data.full_name,
                        phone_number=candidate_data.phone,
                        profile_data={"location": candidate_data.location}
                    )
                    db.add(candidate)
                    await db.flush()

                # 4. Создаем диалог
                # external_chat_id для Авито берем из payload отклика (если есть) или позже из сообщения
                chat_id = payload.get("chatId", f"apply_{apply_id}")
                
                dialogue = await db.scalar(select(Dialogue).filter_by(external_chat_id=chat_id))
                if not dialogue:
                    account = await db.scalar(select(Account).filter_by(platform="avito"))
                    dialogue = Dialogue(
                        external_chat_id=chat_id,
                        account_id=account.id,
                        candidate_id=candidate.id,
                        vacancy_id=job.id,
                        current_state="initial", # Начальное состояние из конфига
                        status="new"
                    )
                    db.add(dialogue)
                    await db.commit()
                
                # 5. Кидаем задачу в Engine (на первый ответ)
                task = EngineTaskDTO(
                    dialogue_id=dialogue.id,
                    external_chat_id=chat_id,
                    text="[SYSTEM: NEW_APPLICATION]", # Сигнал боту поздороваться
                    account_id=dialogue.account_id,
                    platform="avito",
                    event_type="new_lead"
                )
                await mq.publish("engine_tasks", task.model_dump())
                logger.info(f"🆕 Создан новый лид: {candidate.full_name} (ID диалога: {dialogue.id})")

            except Exception as e:
                logger.error(f"❌ Ошибка при создании лида: {e}")
                await db.rollback()

    async def _handle_new_message(self, payload: dict):
        """Обработка входящего текстового сообщения"""
        chat_id = payload["chat_id"]
        text = payload["content"]["text"]
        user_id = str(payload["author_id"])

        async with AsyncSessionLocal() as db:
            # Ищем существующий диалог
            dialogue = await db.scalar(select(Dialogue).filter_by(external_chat_id=chat_id))
            
            if not dialogue:
                # Если диалога нет, а сообщение пришло (редкий кейс для Авито, но бывает)
                logger.warning(f"⚠️ Получено сообщение в неизвестный чат {chat_id}. Игнорирую до появления отклика.")
                return

            # Обновляем историю и время последнего сообщения
            history = list(dialogue.history)
            history.append({"role": "user", "content": text, "timestamp": payload.get("created")})
            dialogue.history = history
            dialogue.last_message_at = datetime.datetime.now(datetime.timezone.utc)
            
            await db.commit()

            # Отправляем задачу на обработку ИИ
            task = EngineTaskDTO(
                dialogue_id=dialogue.id,
                external_chat_id=chat_id,
                text=text,
                account_id=dialogue.account_id,
                platform="avito",
                event_type="new_message"
            )
            await mq.publish("engine_tasks", task.model_dump())
            logger.info(f"📩 Сообщение от чата {chat_id} отправлено в Engine")

    async def process_engine_task(self, task_data: dict):
        """Обработка задачи на генерацию ответа ИИ"""
        dialogue_id = task_data["dialogue_id"]
        user_text = task_data["text"]
        
        async with AsyncSessionLocal() as db:
            # 1. Берем диалог и связанные данные
            dialogue = await db.get(Dialogue, dialogue_id)
            if not dialogue:
                return

            # Загружаем кандидата и вакансию для контекста
            candidate = await db.get(Candidate, dialogue.candidate_id)
            job = await db.get(JobContext, dialogue.vacancy_id)

            # 2. Формируем системный промпт (простая версия)
            system_prompt = (
                f"Ты {settings.bot_role_name}. Твоя задача — первичный отбор кандидатов.\n"
                f"Вакансия: {job.title}.\n"
                f"Кандидат: {candidate.full_name}.\n"
                f"Общайся вежливо, на 'Вы', кратко и по делу."
            )

            # 3. Получаем ответ от ИИ
            # Если это новый лид, user_text будет системным маркером
            clean_user_text = "" if user_text.startswith("[SYSTEM") else user_text
            
            ai_response = await llm_service.get_response(
                system_prompt=system_prompt,
                history=dialogue.history,
                user_message=clean_user_text or "Привет! Я по поводу вакансии."
            )

            # 4. Сохраняем ответ ИИ в историю БД
            history = list(dialogue.history)
            history.append({
                "role": "assistant", 
                "content": ai_response, 
                "timestamp": datetime.datetime.now(datetime.timezone.utc).isoformat()
            })
            dialogue.history = history
            await db.commit()

            # 5. Кидаем задачу в очередь на отправку пользователю
            outbound_payload = {
                "external_chat_id": dialogue.external_chat_id,
                "text": ai_response,
                "platform": task_data["platform"],
                "account_id": dialogue.account_id
            }
            await mq.publish("outbound_messages", outbound_payload)
            logger.info(f"🤖 ИИ сгенерировал ответ для чата {dialogue.external_chat_id}")

dispatcher = InboundDispatcher()