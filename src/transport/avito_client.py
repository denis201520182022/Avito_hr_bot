import httpx
from config.settings import settings
import logging

logger = logging.getLogger(__name__)

class AvitoClient:
    def __init__(self):
        self.base_url = "https://api.avito.ru"
        
    async def get_token(self):
        async with httpx.AsyncClient() as client:
            payload = {
                "grant_type": "client_credentials",
                "client_id": settings.AVITO_CLIENT_ID,
                "client_secret": settings.AVITO_CLIENT_SECRET
            }
            resp = await client.post(
                f"{self.base_url}/token", 
                data=payload,
                headers={"Content-Type": "application/x-www-form-urlencoded"}
            )
            if resp.status_code != 200:
                logger.error(f"Ошибка токена: {resp.text}")
                return None
            return resp.json().get("access_token")

    async def get_subscriptions(self):
        """Проверка текущих подписок вебхука (POST согласно Swagger)"""
        token = await self.get_token()
        headers = {"Authorization": f"Bearer {token}"}
        async with httpx.AsyncClient() as client:
            # Сваггер говорит POST /messenger/v1/subscriptions
            resp = await client.post(f"{self.base_url}/messenger/v1/subscriptions", headers=headers)
            return resp.json()

    async def subscribe_webhook(self, webhook_url: str):
        token = await self.get_token()
        headers = {"Authorization": f"Bearer {token}", "Content-Type": "application/json"}
        payload = {"url": webhook_url}
        
        async with httpx.AsyncClient() as client:
            resp = await client.post(f"{self.base_url}/messenger/v3/webhook", json=payload, headers=headers)
            # Если не 200 OK, выводим текст ошибки
            if resp.status_code not in [200, 201]:
                print(f"Ошибка Авито ({resp.status_code}): {resp.text}")
                return {"error": resp.text}
            return resp.json()

    async def send_message(self, chat_id: str, user_id: int, text: str):
        token = await self.get_token()
        headers = {"Authorization": f"Bearer {token}", "Content-Type": "application/json"}
        payload = {"message": {"text": text[:1000]}, "type": "text"}
        url = f"{self.base_url}/messenger/v1/accounts/{user_id}/chats/{chat_id}/messages"
        async with httpx.AsyncClient() as client:
            resp = await client.post(url, json=payload, headers=headers)
            return resp.json()

avito_client = AvitoClient()