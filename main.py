# main.py
import logging
from contextlib import asynccontextmanager
from fastapi import FastAPI, Request, Header, Response
from app.connectors.avito.client import avito
from app.core.rabbitmq import mq
from app.core.config import settings

# Настройка логов
logging.basicConfig(level=logging.DEBUG, format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

@asynccontextmanager
async def lifespan(app: FastAPI):
    # --- ДЕЙСТВИЯ ПРИ СТАРТЕ ---
    logger.info("🚀 Запуск HR-бота...")
    
    # 1. Подключаемся к RabbitMQ
    await mq.connect()
    
    # 2. Авторизация и настройка вебхуков Авито
    # Это гарантирует, что при каждом перезапуске бот проверяет свою "связь" с Авито
    await avito.setup_webhooks()
    
    yield
    
    # --- ДЕЙСТВИЯ ПРИ ОСТАНОВКЕ ---
    await mq.close()
    logger.info("🛑 Бот остановлен")

app = FastAPI(title="AI HR Platform", lifespan=lifespan)

@app.post("/webhooks/avito")
async def avito_webhook_handler(
    request: Request, 
    x_secret: str = Header(None)
):
    """
    Эндпоинт для приема вебхуков от Авито.
    Обрабатывает и подтверждение, и входящие данные.
    """
    payload = await request.json()
    
    # 1. Проверка на пустое тело (запрос от Авито на проверку доступности эндпоинта)
    if not payload:
        return Response(status_code=200)

    # 2. Логика безопасности (X-Secret)
    import os
    if x_secret != os.getenv("AVITO_WEBHOOK_SECRET"):
        logger.warning("⚠️ Получен вебхук с неверным X-Secret")
        # В режиме разработки можно закомментировать, в продакшне - обязательно

    # 3. Отправляем сырое событие в RabbitMQ
    # Мы не разбираем его здесь, это сделает Воркер
    await mq.publish("avito_inbound", {
        "source": "avito",
        "payload": payload
    })

    return Response(status_code=200)

@app.get("/health")
async def health_check():
    return {"status": "ok", "bot_id": settings.bot_id}