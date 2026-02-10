# worker.py
import asyncio
import json
import logging
import signal
from aio_pika import IncomingMessage
from app.core.rabbitmq import mq
from app.core.engine import dispatcher
from app.connectors.avito.client import avito
from app.db.session import AsyncSessionLocal
from app.db.models import Account

# Настройка логирования
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger("Worker")

# --- ОБРАБОТЧИКИ ОЧЕРЕДЕЙ (CALLBACKS) ---

async def on_avito_inbound(message: IncomingMessage):
    """
    1. Обработка входящих вебхуков от Авито.
    Здесь происходит парсинг, создание кандидатов и диалогов в БД.
    """
    async with message.process():
        try:
            body = json.loads(message.body.decode())
            payload = body.get("payload")
            
            logger.info(f"📥 [Inbound] Получено событие от Avito")
            await dispatcher.process_avito_event(payload)
            
        except Exception as e:
            logger.error(f"❌ Ошибка в on_avito_inbound: {e}", exc_info=True)


async def on_engine_task(message: IncomingMessage):
    """
    2. Обработка задач для 'Мозга' (ИИ).
    Здесь мы идем в OpenAI, формируем ответ и кладем его в очередь отправки.
    """
    async with message.process():
        try:
            task_data = json.loads(message.body.decode())
            dialogue_id = task_data.get("dialogue_id")
            
            logger.info(f"🧠 [Engine] Обработка ИИ для диалога ID: {dialogue_id}")
            await dispatcher.process_engine_task(task_data)
            
        except Exception as e:
            logger.error(f"❌ Ошибка в on_engine_task: {e}", exc_info=True)


async def on_outbound_msg(message: IncomingMessage):
    """
    3. Физическая отправка сообщений в Авито.
    Достает задачу из очереди и дергает Avito API.
    """
    async with message.process():
        try:
            data = json.loads(message.body.decode())
            chat_id = data.get("external_chat_id")
            text = data.get("text")
            account_id = data.get("account_id")

            logger.info(f"📤 [Outbound] Отправка сообщения в чат {chat_id}")

            async with AsyncSessionLocal() as db:
                account = await db.get(Account, account_id)
                # Вытаскиваем user_id из auth_data или ставим по умолчанию
                # Авито API требует ID кабинета (user_id) для отправки
                user_id = account.auth_data.get("user_id") if account.auth_data else "me"

            await avito.send_message(
                user_id=user_id,
                chat_id=chat_id,
                text=text
            )
            logger.info(f"✅ Сообщение успешно доставлено в чат {chat_id}")

        except Exception as e:
            logger.error(f"❌ Ошибка в on_outbound_msg: {e}", exc_info=True)


# --- ГЛАВНЫЙ ЦИКЛ ---

async def main():
    # 1. Подключаемся к RabbitMQ
    await mq.connect()
    channel = mq.channel

    # 2. Настраиваем QoS (чтобы воркер не хватал слишком много задач сразу)
    # prefetch_count=10 означает, что воркер берет 10 задач и пока их не подтвердит (ack), новые не получит
    await channel.set_qos(prefetch_count=10)

    # 3. Подписываемся на очереди
    
    # Очередь входящих
    inbound_queue = await channel.get_queue("avito_inbound")
    await inbound_queue.consume(on_avito_inbound)

    # Очередь для Мозга (LLM)
    engine_queue = await channel.get_queue("engine_tasks")
    await engine_queue.consume(on_engine_task)

    # Очередь исходящих
    outbound_queue = await channel.get_queue("outbound_messages")
    await outbound_queue.consume(on_outbound_msg)

    logger.info("👷 Worker запущен. Слушаю очереди: avito_inbound, engine_tasks, outbound_messages")

    # 4. Обработка сигналов для корректного завершения
    stop_event = asyncio.Event()
    
    def ask_exit():
        logger.info("🛑 Получен сигнал остановки...")
        stop_event.set()

    loop = asyncio.get_running_loop()
    for sig in (signal.SIGINT, signal.SIGTERM):
        loop.add_signal_handler(sig, ask_exit)

    # Держим воркер активным до получения сигнала
    await stop_event.wait()
    
    # 5. Завершение работы
    await mq.close()
    logger.info("👋 Воркер полностью остановлен.")

if __name__ == "__main__":
    try:
        asyncio.run(main())
    except (KeyboardInterrupt, SystemExit):
        pass