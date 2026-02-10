# app/connectors/avito/client.py
import logging
import httpx
import datetime
from typing import Optional
from sqlalchemy import select
from app.db.session import AsyncSessionLocal
from app.db.models import Account
from app.core.config import settings

logger = logging.getLogger(__name__)

class AvitoClient:
    def __init__(self):
        self.base_url = "https://api.avito.ru"
        self.token_url = f"{self.base_url}/token"
        # Будем использовать один клиент для всех запросов (пул соединений)
        self.http_client = httpx.AsyncClient(timeout=30.0)

    async def _get_account_from_db(self, db) -> Account:
        """Получаем запись аккаунта Авито из БД"""
        result = await db.execute(select(Account).filter_by(platform="avito"))
        account = result.scalar_one_or_none()
        if not account:
            # Если в БД еще нет аккаунта, создаем пустую запись
            account = Account(
                platform="avito",
                name="Основной аккаунт Авито",
                auth_data={}
            )
            db.add(account)
            await db.commit()
            await db.refresh(account)
        return account

    async def get_access_token(self) -> str:
        """
        Умное получение токена:
        1. Проверяет БД.
        2. Если токен живой — возвращает.
        3. Если протух или его нет — запрашивает новый по Client Credentials.
        """
        async with AsyncSessionLocal() as db:
            account = await self._get_account_from_db(db)
            auth = account.auth_data or {}
            
            # Проверяем срок годности (с запасом 5 минут)
            expires_at = auth.get("expires_at")
            now = datetime.datetime.now(datetime.timezone.utc).timestamp()

            if auth.get("access_token") and expires_at and expires_at > (now + 300):
                return auth["access_token"]

            # Токен нужен новый
            logger.info("🔑 Запрашиваю новый Access Token для Авито...")
            
            client_id = auth.get("client_id") or settings.integrations.google_sheets.get("AVITO_CLIENT_ID") # Пример забора из конфига
            client_secret = auth.get("client_secret") or settings.integrations.google_sheets.get("AVITO_CLIENT_SECRET")
            
            # Если нет в БД, берем из переменных окружения (в продакшне лучше через БД)
            import os
            client_id = client_id or os.getenv("AVITO_CLIENT_ID")
            client_secret = client_secret or os.getenv("AVITO_CLIENT_SECRET")

            data = {
                "grant_type": "client_credentials",
                "client_id": client_id,
                "client_secret": client_secret
            }

            response = await self.http_client.post(self.token_url, data=data)
            response.raise_for_status()
            token_data = response.json()

            # Обновляем данные в БД
            new_auth = {
                "client_id": client_id,
                "client_secret": client_secret,
                "access_token": token_data["access_token"],
                "expires_at": now + token_data["expires_in"],
                "token_type": token_data["token_type"]
            }
            account.auth_data = new_auth
            await db.commit()
            
            logger.info("✅ Токен Авито успешно обновлен")
            return token_data["access_token"]

    async def get_headers(self):
        token = await self.get_access_token()
        return {"Authorization": f"Bearer {token}"}

    async def setup_webhooks(self):
        """
        Проверка и автоматическая подписка на вебхуки.
        Согласно ТЗ: проверяем наличие подписки на наш URL, если нет - создаем.
        """
        import os
        target_url = os.getenv("WEBHOOK_BASE_URL") + "/webhooks/avito"
        secret = os.getenv("AVITO_WEBHOOK_SECRET", "super_secret_key")

        if not os.getenv("WEBHOOK_BASE_URL"):
            logger.error("❌ WEBHOOK_BASE_URL не задан в .env. Авто-подписка невозможна.")
            return

        headers = await self.get_headers()

        # 1. Проверяем подписки на ОТКЛИКИ (Job API)
        try:
            job_hook_res = await self.http_client.get(
                f"{self.base_url}/job/v1/applications/webhooks", 
                headers=headers
            )
            job_hook_res.raise_for_status()
            current_hooks = job_hook_res.json().get("webhooks", [])
            
            is_subscribed = any(h["url"] == target_url for h in current_hooks)

            if not is_subscribed:
                logger.info(f"📣 Подписываюсь на вебхуки откликов: {target_url}")
                subscribe_res = await self.http_client.put(
                    f"{self.base_url}/job/v1/applications/webhook",
                    headers=headers,
                    json={"url": target_url, "secret": secret}
                )
                subscribe_res.raise_for_status()
            else:
                logger.info("✅ Подписка на отклики уже активна")

            # 2. Проверяем подписки на СООБЩЕНИЯ (Messenger V3)
            # У Авито Мессенджера метод GET /messenger/v1/subscriptions
            msg_hook_res = await self.http_client.get(
                f"{self.base_url}/messenger/v1/subscriptions",
                headers=headers
            )
            msg_hook_res.raise_for_status()
            msg_subs = msg_hook_res.json().get("subscriptions", [])
            
            msg_subscribed = any(s["url"] == target_url for s in msg_subs)

            if not msg_subscribed:
                logger.info(f"💬 Подписываюсь на вебхуки сообщений: {target_url}")
                await self.http_client.post(
                    f"{self.base_url}/messenger/v3/webhook",
                    headers=headers,
                    json={"url": target_url}
                )
            else:
                logger.info("✅ Подписка на сообщения мессенджера активна")

        except Exception as e:
            logger.error(f"❌ Ошибка при настройке вебхуков: {e}")

    async def get_candidate_details(self, apply_id: str) -> CandidateDTO:
        """Получение инфо об отклике (скрин 'Работа с содержимым откликов')"""
        headers = await self.get_headers()
        # В Авито инфа об отклике тянется по applyId (ids может быть списком)
        url = f"{self.base_url}/job/v1/applications/ids={apply_id}"
        
        response = await self.http_client.get(url, headers=headers)
        response.raise_for_status()
        data = response.json().get("applies", [{}])[0]
        
        # Вытаскиваем телефон и имя
        contacts = data.get("contacts", {})
        applicant = data.get("applicant", {})
        
        return CandidateDTO(
            full_name=applicant.get("name") or "Не указано",
            phone=contacts.get("phones", [None])[0],
            platform_user_id=str(contacts.get("user_id")),
            location=applicant.get("city")
        )

    async def get_job_details(self, vacancy_id: str) -> JobContextDTO:
        """Получение инфо о вакансии (скрин 'Просмотр данных вакансий')"""
        headers = await self.get_headers()
        url = f"{self.base_url}/job/v2/vacancies/batch"
        
        payload = {
            "ids": [int(vacancy_id)],
            "fields": ["title", "description"]
        }
        
        response = await self.http_client.post(url, headers=headers, json=payload)
        response.raise_for_status()
        vac_data = response.json()[0]
        
        return JobContextDTO(
            external_id=str(vac_data["id"]),
            title=vac_data["title"],
            description=vac_data["description"]
        )

    async def send_message(self, user_id: str, chat_id: str, text: str):
        """Отправка сообщения (скрин 'Messenger API / Отправка сообщения')"""
        headers = await self.get_headers()
        url = f"{self.base_url}/messenger/v1/accounts/{user_id}/chats/{chat_id}/messages"
        
        payload = {
            "message": {"text": text},
            "type": "text"
        }
        
        response = await self.http_client.post(url, headers=headers, json=payload)
        response.raise_for_status()
        return response.json()

# Экземпляр клиента
avito = AvitoClient()