# main.py
import logging
import os
from contextlib import asynccontextmanager
from fastapi import FastAPI, Request, Header, Response

from app.connectors.avito import avito_connector, avito
from app.core.rabbitmq import mq
from app.core.config import settings

# Настройка логирования
logging.basicConfig(
    level=logging.INFO, 
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger("FastAPI")

@asynccontextmanager
async def lifespan(app: FastAPI):
    """
    Управление жизненным циклом приложения.
    Здесь запускаются и останавливаются все фоновые процессы.
    """
    logger.info("🚀 Запуск HR-платформы...")
    
    try:
        # 1. Подключаемся к RabbitMQ
        await mq.connect()
    except Exception as e:
        # Если MQ не работает, шлем алерт напрямую (fallback)
        from app.utils.tg_alerts import send_system_alert
        await send_system_alert(f"🚨 КРИТИЧЕСКАЯ ОШИБКА: RabbitMQ не доступен!\n{e}")
        raise e # Останавливаем запуск приложения

    try:
        # 2. Запускаем коннектор Авито
        await avito_connector.start()
    except Exception as e:
        error_msg = f"❌ Не удалось запустить Avito Connector: {e}"
        logger.error(error_msg)
        # Шлем через очередь, так как MQ уже подключен
        await mq.publish("tg_alerts", {"type": "system", "text": error_msg})

    yield

    
    # --- ДЕЙСТВИЯ ПРИ ОСТАНОВКЕ ---
    logger.info("🛑 Остановка приложения...")
    
    # Останавливаем коннектор (поллер и т.д.)
    await avito_connector.stop()
    
    # Закрываем HTTP сессии клиента Авито
    await avito.close()
    
    # Закрываем соединение с очередью
    await mq.close()
    
    logger.info("👋 Бот полностью остановлен")

# Инициализация FastAPI
app = FastAPI(
    title="AI HR Platform", 
    version="2.0.0",
    lifespan=lifespan
)

@app.post("/webhooks/avito")
async def avito_webhook_handler(
    request: Request, 
    x_secret: str = Header(None)
):
    """
    Единый эндпоинт для приема вебхуков от Авито (Messenger API v3).
    Служит только для приема сообщений. Отклики приходят через Поллер.
    """
    try:
        payload = await request.json()
    except Exception:
        return Response(status_code=400)

    # 1. Проверка на пустой запрос (Авито иногда шлет для проверки связи)
    if not payload:
        return Response(status_code=200)

    # 2. Проверка безопасности (X-Secret)
    # --- ИСПРАВЛЕННАЯ ПРОВЕРКА БЕЗОПАСНОСТИ ---
    # В Messenger API v3 заголовок X-Secret не приходит.
    # Поэтому мы проверяем его ТОЛЬКО если он был передан.
    expected_secret = settings.AVITO_WEBHOOK_SECRET
    
    if x_secret and expected_secret and x_secret != expected_secret:
        error_msg = f"⚠️ Ошибка настроек! Неверный X-Secret от IP: {request.client.host}"
        logger.warning(error_msg)
        await mq.publish("tg_alerts", {"type": "system", "text": error_msg})
        return Response(status_code=403)
    # ------------------------------------------

    # 3. Определяем владельца (наш account)
    # В Messenger API v3 поле 'user_id' — это ID нашего кабинета (рекрутера)
    avito_user_id = payload.get("user_id")

    try:
        # 4. Отправляем событие в RabbitMQ
        await mq.publish("avito_inbound", {
            "source": "avito_webhook",
            "type": "new_message",
            "avito_user_id": str(avito_user_id) if avito_user_id else None,
            "payload": payload
        })
    except Exception as e:
        error_msg = f"❌ ПОТЕРЯ ДАННЫХ: Не удалось записать вебхук Авито в очередь!\n{e}"
        logger.error(error_msg)
        
        # Пытаемся отправить алерт через другую очередь
        await mq.publish("tg_alerts", {"type": "system", "text": error_msg})
        return Response(status_code=500) # Сообщаем Авито, что мы не приняли данные

    # Авито требует быстрый ответ 200 OK
    return Response(status_code=200)

@app.get("/health")
async def health_check():
    """Проверка жизнеспособности сервиса"""
    return {
        "status": "ok", 
        "bot_id": settings.bot_id,
        "mq_connected": mq.connection is not None and not mq.connection.is_closed
    }