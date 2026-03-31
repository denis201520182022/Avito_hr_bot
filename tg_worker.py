import asyncio
import json
import logging
import io
import datetime
import time
from aiogram import Bot
import html  # Импортируем стандартную библиотеку для экранирования HTML
from aiogram.types import BufferedInputFile # (предполагая использование aiogram 3.x)
from aiogram.types import BufferedInputFile
from sqlalchemy import select
from sqlalchemy.orm import selectinload
from aiogram import Dispatcher
from aiogram.fsm.storage.memory import MemoryStorage
from app.tg_bot.handlers import router as main_router
from app.tg_bot.middlewares import DbSessionMiddleware

from app.core.config import settings
from app.utils import tg_alerts
from app.core.rabbitmq import mq
from app.db.session import AsyncSessionLocal
from app.db.models import Dialogue, Candidate, Account, JobContext
from app.services.sheets import sheets_service
from app.utils.logger import logger, set_log_context, log_context
# Настройка логирования


bot = Bot(token=settings.TELEGRAM_BOT_TOKEN)
storage = MemoryStorage()
dp = Dispatcher(storage=storage)

# Регистрируем мидлварь для базы данных
dp.update.middleware(DbSessionMiddleware(AsyncSessionLocal))

# Подключаем все хендлеры
dp.include_router(main_router)



async def handle_alert_task(message_body: dict):
    """Диспетчер системных алертов (ошибки, верификация, галлюцинации)"""
    alert_type = message_body.get("type") # 'system', 'verification', 'hallucination'
    
    try:
        if alert_type == 'system':
            await tg_alerts.send_system_alert(
                message_text=message_body.get("text"),
                alert_type=message_body.get("alert_type", "admin_only")
            )
        
        elif alert_type == 'verification':
            await tg_alerts.send_verification_alert(
                dialogue_id=message_body.get("dialogue_id"),
                external_chat_id=message_body.get("external_chat_id"),
                db_data=message_body.get("db_data"),
                llm_data=message_body.get("llm_data"),
                history_text=message_body.get("history_text"),
                reasoning=message_body.get("reasoning")
            )
            
        elif alert_type == 'hallucination':
            await tg_alerts.send_hallucination_alert(
                dialogue_id=message_body.get("dialogue_id"),
                external_chat_id=message_body.get("external_chat_id"),
                user_said=message_body.get("user_said"),
                llm_suggested=message_body.get("llm_suggested"),
                corrected_val=message_body.get("corrected_val"),
                history_text=message_body.get("history_text"),
                reasoning=message_body.get("reasoning")
            )
            
        logger.info(f"🔔 Алерт типа '{alert_type}' успешно обработан")
    except Exception as e:
        logger.error(f"💥 Ошибка при обработке алертов в воркере: {e}")




def format_history_txt(dialogue: Dialogue, candidate: Candidate, vacancy: JobContext) -> str:
    """Формирует текстовый файл истории диалога"""
    lines = []
    lines.append(f"=== ИСТОРИЯ ДИАЛОГА (АВИТО) ===")
    lines.append(f"ID чата: {dialogue.external_chat_id}")
    lines.append(f"Кандидат: {candidate.full_name or 'Аноним'}")
    lines.append(f"Вакансия: {vacancy.title if vacancy else 'Не указана'}")
    lines.append(f"Дата создания отклика: {dialogue.created_at.strftime('%d.%m.%Y %H:%M')}")
    lines.append("-" * 50 + "\n")

    for entry in (dialogue.history or []):
        role = entry.get('role')
        content = entry.get('content', '')
        content_str = str(content)
        
        # ФИЛЬТР: Пропускаем пустые, системные команды [SYSTEM и мусор [Системное сообщение]
        if not content_str or content_str.startswith('[SYSTEM') or content_str.startswith('[Системное сообщение]'):
            continue
            
        ts = entry.get('timestamp_utc', '')
        if ts:
            try:
                dt = datetime.datetime.fromisoformat(ts.replace('Z', '+00:00'))
                # Конвертируем в МСК для файла (+3 часа)
                msk_dt = dt + datetime.timedelta(hours=3)
                ts_str = msk_dt.strftime('[%H:%M:%S] ')
            except: ts_str = ""
        else: ts_str = ""

        label = "👤 Кандидат" if role == 'user' else "🤖 Бот"
        lines.append(f"{ts_str}{label}: {content}\n")

    return "\n".join(lines)



async def send_tg_notification(dialogue: Dialogue, candidate: Candidate, vacancy: JobContext, account: Account):
    """Логика формирования и отправки карточки в Telegram (HTML version)"""
    profile = candidate.profile_data or {}
    tg_settings = account.settings or {}
    target_chat_id = tg_settings.get("tg_chat_id")
    target_topic_id = tg_settings.get("topic_qualified_id")

    if not target_chat_id:
        logger.warning(f"Для аккаунта {account.name} не настроен tg_chat_id.")
        return

    def esc(text):
        """Безопасное экранирование текста для HTML"""
        if text is None or text == "": 
            return "—"
        return html.escape(str(text))
    
    meta = dialogue.metadata_json or {}
    avito_link = f"https://www.avito.ru/profile/messenger/channel/{dialogue.external_chat_id}"
    
    # Формируем текст с использованием HTML-тегов
    message_text = (
        f"🚀 <b>Новый кандидат (Авито)</b>\n\n"
        f"📌 <b>Вакансия:</b> {esc(vacancy.title if vacancy else 'Не указана')}\n"
        f"📍 <b>Город:</b> {esc(profile.get('city', 'Не указан'))}\n\n"
        f"👤 <b>ФИО:</b> {esc(candidate.full_name)}\n"
        f"📞 <b>Телефон:</b> <code>{esc(candidate.phone_number)}</code>\n"
        f"📅 <b>Дата рождения:</b> {esc(profile.get('birth_date'))}\n"
        f"🎂 <b>Возраст:</b> {esc(profile.get('age'))}\n"
        f"🌍 <b>Гражданство:</b> {esc(profile.get('citizenship'))}\n"
        f"📜 <b>Патент:</b> {esc(profile.get('has_patent'))}\n\n"
        f"📅 <b>Собеседование:</b> {esc(meta.get('interview_date'))} в {esc(meta.get('interview_time'))}\n\n"
        f"🔗 <a href='{avito_link}'>Открыть чат в Авито</a>"
    )

    history_text = format_history_txt(dialogue, candidate, vacancy)
    file_name = f"chat_{dialogue.external_chat_id}.txt"
    document = BufferedInputFile(history_text.encode('utf-8'), filename=file_name)

    try:
        await bot.send_document(
            chat_id=target_chat_id,
            document=document,
            caption=message_text,
            message_thread_id=target_topic_id,
            parse_mode="HTML"  # Указываем HTML вместо MarkdownV2
        )
        logger.info(
            "✅ Карточка кандидата отправлена в TG", 
            extra={
                "target_chat": target_chat_id,
                "candidate": candidate.full_name
            }
        )
    except Exception as e:
        logger.exception(f"❌ Ошибка отправки карточки в TG: {e}")

async def handle_reporting_task(message_body: dict):
    """Диспетчер задач отчетности (TG + Google Sheets)"""
    dialogue_id = message_body.get("dialogue_id")
    event_type = message_body.get("type", "qualified") # 'qualified', 'rescheduled', 'cancelled'
    
    async with AsyncSessionLocal() as db:
        stmt = (
            select(Dialogue)
            .where(Dialogue.id == dialogue_id)
            .options(
                selectinload(Dialogue.candidate),
                selectinload(Dialogue.vacancy),
                selectinload(Dialogue.account)
            )
        )
        result = await db.execute(stmt)
        dialogue = result.scalar_one_or_none()

        if not dialogue:
            logger.error(f"Диалог {dialogue_id} не найден")
            return

        candidate = dialogue.candidate
        vacancy = dialogue.vacancy
        account = dialogue.account
        meta = dialogue.metadata_json or {}

        # --- ОБРАБОТКА ПО ТИПАМ СОБЫТИЙ ---

        try:
            if event_type == 'qualified':
                # 1. Google Sheets: Бронируем слот
                await sheets_service.book_slot(
                    target_date=meta.get("interview_date"),
                    target_time=meta.get("interview_time"),
                    candidate_name=candidate.full_name or "Аноним Авито"
                )
                logger.info("📊 Слот в Google Sheets забронирован")
                # 2. Google Sheets: Добавляем кандидата в список
                await sheets_service.append_candidate({
                    "full_name": candidate.full_name,
                    "phone": candidate.phone_number,
                    "vacancy": vacancy.title if vacancy else "Не указана",
                    "chat_link": f"https://www.avito.ru/profile/messenger/channel/{dialogue.external_chat_id}",
                    "interview_dt": f"{meta.get('interview_date')} {meta.get('interview_time')}",
                    "status": "Записан ботом"
                })
                logger.info("📊 Кандидат добавлен в таблицу")
                # 3. Telegram: Отправляем уведомление
                await send_tg_notification(dialogue, candidate, vacancy, account)

            elif event_type == 'rescheduled':
                # 1. Google Sheets: Освобождаем старый слот
                old_date = message_body.get("old_date")
                old_time = message_body.get("old_time")
                if old_date and old_time:
                    await sheets_service.release_slot(old_date, old_time)
                
                # 2. Google Sheets: Бронируем новый слот
                await sheets_service.book_slot(
                    target_date=meta.get("interview_date"),
                    target_time=meta.get("interview_time"),
                    candidate_name=f"{candidate.full_name or 'Аноним'} (ПЕРЕНОС)"
                )
                logger.info(f"🔄 Таблицы: Перенос для диалога {dialogue_id} выполнен")

            elif event_type == 'cancelled':
                # Google Sheets: Освобождаем текущий слот при отказе
                await sheets_service.release_slot(
                    target_date=meta.get("interview_date"),
                    target_time=meta.get("interview_time")
                )
                logger.info(f"🚫 Таблицы: Слот освобожден (отказ) для диалога {dialogue_id}")

        except Exception as e:
            logger.error(f"💥 Ошибка Reporting Worker при обработке {event_type}: {e}", exc_info=True)


async def run_alerts_consumer():
    """Слушатель очереди системных алертов"""
    queue = await mq.channel.get_queue("tg_alerts")
    logger.info("👷 Alerts Consumer запущен...")
    
    async with queue.iterator() as queue_iter:
        async for message in queue_iter:
            # ДОБАВЛЯЕМ ignore_processed=True
            async with message.process(ignore_processed=True):
                log_context.set({})
                try:
                    payload = json.loads(message.body.decode())
                    set_log_context(
                        service="alerts",
                        alert_type=payload.get("type"),
                        dialogue_id=payload.get("dialogue_id")
                    )
                    await handle_alert_task(payload)
                    # Если дошли сюда - все ок, process() сам отправит ack() при выходе
                    
                except json.JSONDecodeError:
                    # Если пришел мусор вместо JSON - нет смысла возвращать, удаляем
                    logger.error("❌ Получен некорректный JSON в алертах, сообщение отброшено.")
                    await message.reject(requeue=False)

                except Exception as e:
                    logger.exception("💥 Ошибка обработки системного алерта")
                    logger.info("♻️ Возвращаем сообщение в очередь (NACK)...")
                    # ВОТ ОНО: Возвращаем в очередь
                    await message.nack(requeue=True)
                    # Добавляем небольшую паузу, чтобы не спамить логами, если сервис лежит
                    await asyncio.sleep(1)

async def run_rabbitmq_consumer():
    """Фоновая задача для прослушивания очереди RabbitMQ"""
    await mq.connect()
    queue = await mq.channel.get_queue("tg_notifications")
    logger.info("👷 Reporting Worker (RabbitMQ) запущен...")
    
    async with queue.iterator() as queue_iter:
        async for message in queue_iter:
            # ДОБАВЛЯЕМ ignore_processed=True
            async with message.process(ignore_processed=True):
                # --- ДОБАВЛЕНА ЗАДЕРЖКА ---
                log_context.set({})
                await asyncio.sleep(10) 
                    # --- КОНЕЦ ДОБАВЛЕННОЙ ЗАДЕРЖКИ ---
                try:
                    payload = json.loads(message.body.decode())
                    set_log_context(
                        service="reporting",
                        event_type=payload.get("type"),
                        dialogue_id=payload.get("dialogue_id")
                    )
                    await handle_reporting_task(payload)
                    
                except json.JSONDecodeError:
                    logger.error("❌ Некорректный JSON в уведомлениях, сообщение отброшено.")
                    await message.reject(requeue=False)

                except Exception as e:
                    logger.exception("💥 Ошибка в Reporting Worker")
                    logger.info("♻️ Возвращаем сообщение в очередь (NACK)...")
                    # ВОТ ОНО: Возвращаем в очередь
                    await message.nack(requeue=True)
                    await asyncio.sleep(1)

async def main():
    """Запуск бота, отчетности и алертов одновременно"""
    await mq.connect() # Подключаемся один раз на старте
    
    # 1. Задача для уведомлений о кандидатах (Reporting)
    reporting_task = asyncio.create_task(run_rabbitmq_consumer())
    
    # 2. Задача для системных алертов (Alerts)
    alerts_task = asyncio.create_task(run_alerts_consumer())
    
    # 3. Интерактивная часть бота (Polling)
    logger.info("🤖 Interactive TG Bot запущен...")
    try:
        await bot.delete_webhook(drop_pending_updates=True)
        await dp.start_polling(bot)
    finally:
        reporting_task.cancel()
        alerts_task.cancel()
        await mq.close()

if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        logger.info("Воркер и бот остановлены вручную")