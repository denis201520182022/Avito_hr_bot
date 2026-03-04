# main.py
import logging
import os
from contextlib import asynccontextmanager
from fastapi import FastAPI, Request, Header, Response

from app.connectors.avito import avito_connector, avito
from app.core.rabbitmq import mq
from app.core.config import settings
import uuid # понадобится для request_id
from app.utils.logger import logger, set_log_context, log_context



# --- ДОБАВЬТЕ ЭТУ СТРОКУ ---
logging.getLogger("httpx").setLevel(logging.WARNING)

@asynccontextmanager
async def lifespan(app: FastAPI):
    """
    Управление жизненным циклом приложения.
    Здесь запускаются и останавливаются все фоновые процессы.
    """
    logger.info("🚀 Запуск HR-платформы...", extra={"env": os.getenv("DOCKER_MODE")})
    
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
        logger.exception("Сообщение об ошибке")
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

@app.middleware("http")
async def log_requests_middleware(request: Request, call_next):
    # 1. Очищаем контекст перед новым запросом
    log_context.set({})
    
    # 2. Устанавливаем базовый контекст запроса
    set_log_context(
        request_id=str(uuid.uuid4()),
        method=request.method,
        path=request.url.path,
        ip=request.client.host if request.client else "unknown"
    )
    
    response = await call_next(request)
    return response




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

    # 1. Проверка на пустой запрос
    if not payload:
        return Response(status_code=200)

    # 2. Проверка безопасности (X-Secret)
    expected_secret = settings.AVITO_WEBHOOK_SECRET
    if x_secret and expected_secret and x_secret != expected_secret:
        error_msg = f"⚠️ Ошибка настроек! Неверный X-Secret от IP: {request.client.host}"
        logger.warning(error_msg)
        await mq.publish("tg_alerts", {"type": "system", "text": error_msg})
        return Response(status_code=403)

    # 3. ОПРЕДЕЛЯЕМ ВЛАДЕЛЬЦА (Наш account)
    # В Messenger API v3 user_id лежит по пути: payload -> value -> user_id
    # Используем безопасный get(), чтобы не упасть с ошибкой, если структура изменится
    inner_payload = payload.get("payload", {})
    inner_value = inner_payload.get("value", {})
    avito_user_id = inner_value.get("user_id")
    if avito_user_id:
        set_log_context(avito_user_id=str(avito_user_id))
    # Резервный вариант: если Авито пришлет ID в корне (как в старых версиях)
    if not avito_user_id:
        avito_user_id = payload.get("user_id")

    # 4. Отправляем событие в RabbitMQ
    try:
        # Важно: приводим к str, так как в БД мы ищем через .astext (строковое сравнение)
        formatted_user_id = str(avito_user_id) if avito_user_id else None
        
        await mq.publish("avito_inbound", {
            "source": "avito_webhook",
            "type": "new_message",
            "avito_user_id": formatted_user_id,
            "payload": payload
        })
        
        # Лог для отладки (потом можно убрать)
        if formatted_user_id:
            logger.info(f"✅ Вебхук принят для avito_user_id: {formatted_user_id}")
        else:
            logger.warning(f"❓ Получен вебхук без user_id. Payload: {payload}")

    except Exception as e:
        error_msg = f"❌ ПОТЕРЯ ДАННЫХ: Не удалось записать вебхук Авито в очередь!\n{e}"
        logger.exception("Сообщение об ошибке")
        await mq.publish("tg_alerts", {"type": "system", "text": error_msg})
        return Response(status_code=500)

    return Response(status_code=200)

@app.get("/health")
async def health_check():
    """Проверка жизнеспособности сервиса"""
    return {
        "status": "ok", 
        "bot_id": settings.bot_id,
        "mq_connected": mq.connection is not None and not mq.connection.is_closed
    }