# app/services/llm.py
import logging
from openai import AsyncOpenAI
import os
from app.core.config import settings

logger = logging.getLogger(__name__)

class LLMService:
    def __init__(self):
        # API ключ берем из .env
        self.client = AsyncOpenAI(api_key=os.getenv("OPENAI_API_KEY"))
        self.model = settings.llm.model
        self.temperature = settings.llm.temperature

    async def get_response(self, system_prompt: str, history: list, user_message: str) -> str:
        """Получение простого ответа от модели"""
        messages = [{"role": "system", "content": system_prompt}]
        
        # Добавляем историю (последние 10 сообщений для экономии токенов)
        for msg in history[-10:]:
            messages.append({"role": msg["role"], "content": msg["content"]})
            
        # Добавляем новое сообщение пользователя
        messages.append({"role": "user", "content": user_message})

        try:
            response = await self.client.chat.completions.create(
                model=self.model,
                messages=messages,
                temperature=self.temperature
            )
            return response.choices[0].message.content
        except Exception as e:
            logger.error(f"❌ Ошибка OpenAI API: {e}")
            return "Извините, я сейчас не могу ответить. Попробуйте позже."

llm_service = LLMService()