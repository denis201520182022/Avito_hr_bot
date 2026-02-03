from fastapi import FastAPI, Request, BackgroundTasks
from src.ai_core.openai_client import ai_service
from src.transport.avito_client import avito_client
import logging
import json
import time

# Ставим уровень DEBUG для максимума инфы
logging.basicConfig(
    level=logging.DEBUG,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

app = FastAPI()

# Middleware для логирования ВСЕГО
@app.middleware("http")
async def log_requests(request: Request, call_next):
    start_time = time.time()
    path = request.url.path
    method = request.method
    logger.info(f"🚀 ВХОДЯЩИЙ ЗАПРОС: {method} {path}")
    
    response = await call_next(request)
    
    process_time = time.time() - start_time
    logger.info(f"✅ ОТВЕТ: {method} {path} Статус: {response.status_code} Время: {process_time:.4f}s")
    return response

@app.get("/")
async def health_check():
    return {"status": "working", "time": time.time()}

async def process_avito_message(data: dict):
    try:
        payload = data.get("payload", {})
        msg_value = payload.get("value", {})
        
        chat_id = msg_value.get("chat_id")
        my_user_id = msg_value.get("user_id")
        author_id = msg_value.get("author_id")
        text = msg_value.get("content", {}).get("text")

        logger.info(f"📥 ОБРАБОТКА: chat={chat_id}, from={author_id}, text={text}")

        if author_id == my_user_id:
            logger.info("Self-message detected, skipping.")
            return

        if chat_id and text:
            ai_response = await ai_service.generate_response(text)
            logger.info(f"🤖 AI: {ai_response}")
            
            res = await avito_client.send_message(chat_id, my_user_id, ai_response)
            logger.info(f"📤 ОТПРАВКА: {res}")
    except Exception as e:
        logger.error(f"❌ Ошибка в process_avito_message: {e}", exc_info=True)

@app.post("/avito/webhook")
async def webhook(request: Request, background_tasks: BackgroundTasks):
    try:
        body = await request.body()
        if not body:
            logger.warning("Пустое тело запроса (возможно проверка Авито)")
            return {"ok": True}
            
        raw_data = json.loads(body)
        
        # Печатаем вообще всё в консоль
        print("\n" + "="*50)
        print("FULL JSON FROM AVITO:")
        print(json.dumps(raw_data, indent=2, ensure_ascii=False))
        print("="*50 + "\n")
        
        # Авито V3 шлет сообщение внутри payload -> type
        payload = raw_data.get("payload", {})
        if payload.get("type") == "message":
            background_tasks.add_task(process_avito_message, raw_data)
        else:
            logger.info(f"Получено событие типа: {payload.get('type')}")
            
    except Exception as e:
        logger.error(f"❌ Ошибка парсинга вебхука: {e}")
        
    return {"ok": True}

if __name__ == "__main__":
    import uvicorn
    # Слушаем на 0.0.0.0, чтобы тоннель точно видел
    uvicorn.run(app, host="0.0.0.0", port=8003, log_level="debug")