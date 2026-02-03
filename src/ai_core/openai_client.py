# src/ai_core/openai_client.py
import httpx
from openai import AsyncOpenAI
from config.settings import settings
import logging

# Настройка простого логгера
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

class AIService:
    def __init__(self):
        # 1. Формируем URL прокси по твоему методу
        self.proxy_url = (
            f"http://{settings.SQUID_PROXY_USER}:{settings.SQUID_PROXY_PASSWORD}@"
            f"{settings.SQUID_PROXY_HOST}:{settings.SQUID_PROXY_PORT}"
        )
        
        logger.info(f"Настройка прокси: {settings.SQUID_PROXY_HOST}:{settings.SQUID_PROXY_PORT}")

        # 2. Создаем HTTP клиента (исправлен аргумент на proxy)
        self.http_client = httpx.AsyncClient(
            proxy=self.proxy_url, 
            timeout=60.0
        )
        
        # 3. Инициализируем OpenAI
        self.client = AsyncOpenAI(
            api_key=settings.OPENAI_API_KEY,
            http_client=self.http_client
        )

    async def generate_response(self, user_text: str) -> str:
        try:
            # Простой вызов без сложной истории (пока)
            response = await self.client.chat.completions.create(
                model=settings.OPENAI_MODEL,
                messages=[
                    {"role": "system", "content": "Ты помощник."},
                    {"role": "user", "content": user_text}
                ],
                max_tokens=500
            )
            return response.choices[0].message.content
        except Exception as e:
            logger.error(f"Ошибка OpenAI: {e}")
            return "Ошибка генерации ответа."

    async def close(self):
        await self.http_client.aclose()

ai_service = AIService()