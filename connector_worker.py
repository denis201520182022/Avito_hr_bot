# connector_worker.py
import asyncio
import json
import logging
from aio_pika import IncomingMessage
from app.core.rabbitmq import mq
from app.connectors.avito.client import avito
from app.db.session import AsyncSessionLocal
from app.db.models import Account

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')
logger = logging.getLogger("ConnectorWorker")

async def on_outbound_message(message: IncomingMessage):
    async with message.process():
        try:
            data = json.loads(message.body.decode())
            platform = data.get("platform")
            
            if platform == "avito":
                # Для Авито нам нужен user_id (из Account.auth_data)
                async with AsyncSessionLocal() as db:
                    account = await db.get(Account, data["account_id"])
                    # В реальном API Авито нужно знать ID пользователя (клиента)
                    # Обычно он прилетает в вебхуке, мы его сохраним или вытащим из токена
                    # Для упрощения пока предположим, что мы его знаем
                    user_id = account.auth_data.get("user_id") or "me"
                    
                await avito.send_message(
                    user_id=user_id,
                    chat_id=data["external_chat_id"],
                    text=data["text"]
                )
                logger.info(f"📤 Сообщение отправлено в Авито (чат: {data['external_chat_id']})")

        except Exception as e:
            logger.error(f"❌ Ошибка отправки сообщения: {e}")

async def main():
    await mq.connect()
    channel = await mq.connection.channel()
    await channel.set_qos(prefetch_count=10)
    
    queue = await channel.get_queue("outbound_messages")
    logger.info("🚀 ConnectorWorker (Отправка) запущен...")
    
    await queue.consume(on_outbound_message)
    await asyncio.Future()

if __name__ == "__main__":
    asyncio.run(main())