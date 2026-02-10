# app/connectors/avito/client.py
import logging
import httpx
import datetime
import os
from typing import Optional
from sqlalchemy import select
from app.db.session import AsyncSessionLocal
from app.db.models import Account
from app.core.config import settings
from app.core.schemas import CandidateDTO, JobContextDTO

# Включаем подробное логирование HTTP-запросов
logging.getLogger("httpx").setLevel(logging.DEBUG)
logger = logging.getLogger(__name__)

class AvitoClient:
    def __init__(self):
        self.base_url = "https://api.avito.ru"
        self.token_url = f"{self.base_url}/token"
        self._http_client: Optional[httpx.AsyncClient] = None

    @property
    def http_client(self) -> httpx.AsyncClient:
        if self._http_client is None:
            self._http_client = httpx.AsyncClient(timeout=30.0)
        return self._http_client

    async def _get_account_from_db(self, db) -> Account:
        result = await db.execute(select(Account).filter_by(platform="avito"))
        account = result.scalar_one_or_none()
        if not account:
            account = Account(platform="avito", name="Основной аккаунт Авито", auth_data={})
            db.add(account)
            await db.commit()
            await db.refresh(account)
        return account

    async def get_access_token(self) -> str:
        async with AsyncSessionLocal() as db:
            account = await self._get_account_from_db(db)
            auth = account.auth_data or {}
            
            expires_at = auth.get("expires_at")
            now = datetime.datetime.now(datetime.timezone.utc).timestamp()

            if auth.get("access_token") and expires_at and expires_at > (now + 300):
                logger.debug("Используем токен из кэша БД")
                return auth["access_token"]

            logger.info("🔑 Запрашиваю новый Access Token для Авито...")
            client_id = os.getenv("AVITO_CLIENT_ID")
            client_secret = os.getenv("AVITO_CLIENT_SECRET")

            if not client_id or not client_secret:
                raise ValueError("AVITO_CLIENT_ID или AVITO_CLIENT_SECRET не заданы в .env")

            data = {
                "grant_type": "client_credentials",
                "client_id": client_id,
                "client_secret": client_secret
            }
            
            # Явное логирование запроса токена
            logger.info(f"--> POST {self.token_url}")
            response = await self.http_client.post(self.token_url, data=data)
            logger.info(f"<-- {response.status_code} {response.reason_phrase}")
            response.raise_for_status()
            
            token_data = response.json()
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
        base_url = os.getenv("WEBHOOK_BASE_URL")
        if not base_url:
            logger.error("❌ WEBHOOK_BASE_URL не задан. Авто-подписка невозможна.")
            return

        target_url = base_url.rstrip('/') + "/webhooks/avito"
        secret = os.getenv("AVITO_WEBHOOK_SECRET", "super_secret_key")
        headers = await self.get_headers()

        try:
            # 1. Вебхуки ОТКЛИКОВ (Job API)
            job_hook_url = f"{self.base_url}/job/v1/applications/webhooks"
            job_hook_res = await self.http_client.get(job_hook_url, headers=headers)
            job_hook_res.raise_for_status()
            current_hooks = job_hook_res.json().get("webhooks", [])
            
            if not any(h["url"] == target_url for h in current_hooks):
                logger.info(f"📣 Подписываюсь на вебхуки откликов: {target_url}")
                await self.http_client.put(
                    f"{self.base_url}/job/v1/applications/webhook",
                    headers=headers,
                    json={"url": target_url, "secret": secret}
                )
            else:
                logger.info("✅ Подписка на отклики уже активна")

            # 2. Вебхуки СООБЩЕНИЙ (Messenger API v3) - ИСПРАВЛЕННЫЙ ЭНДПОИНТ
            # Проверяем подписки через GET /messenger/v1/subscriptions
            msg_check_url = f"{self.base_url}/messenger/v1/subscriptions"
            msg_hook_res = await self.http_client.get(msg_check_url, headers=headers)
            msg_hook_res.raise_for_status()
            msg_subs = msg_hook_res.json().get("subscriptions", [])
            
            if not any(s["url"] == target_url for s in msg_subs):
                logger.info(f"💬 Подписываюсь на вебхуки сообщений: {target_url}")
                # Подписываемся через POST /messenger/v3/webhook
                await self.http_client.post(
                    f"{self.base_url}/messenger/v3/webhook",
                    headers=headers,
                    json={"url": target_url}
                )
            else:
                logger.info("✅ Подписка на сообщения мессенджера активна")

        except httpx.HTTPStatusError as e:
            # Выводим тело ответа при ошибке
            response_body = e.response.text
            logger.error(
                f"❌ Ошибка при настройке вебхуков: {e}\n"
                f"URL: {e.request.url}\n"
                f"Response Body: {response_body}"
            )
        except Exception as e:
            logger.error(f"❌ Неизвестная ошибка при настройке вебхуков: {e}", exc_info=True)

    async def get_candidate_details(self, apply_id: str) -> CandidateDTO:
        """Получение инфо об отклике"""
        headers = await self.get_headers()
        url = f"{self.base_url}/job/v1/applications/{apply_id}"
        
        response = await self.http_client.get(url, headers=headers)
        response.raise_for_status()
        data = response.json()
        
        contacts = data.get("contacts", {})
        applicant = data.get("applicant", {})
        
        return CandidateDTO(
            full_name=applicant.get("name") or "Не указано",
            phone=contacts.get("phones", [None])[0],
            platform_user_id=str(contacts.get("user_id")),
            location=applicant.get("city"),
            raw_payload=data
        )

    async def get_job_details(self, vacancy_id: str) -> JobContextDTO:
        """Получение инфо о вакансии"""
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
        """Отправка сообщения в Авито"""
        headers = await self.get_headers()
        url = f"{self.base_url}/messenger/v1/accounts/{user_id}/chats/{chat_id}/messages"
        
        payload = {
            "message": {"text": text},
            "type": "text"
        }
        
        response = await self.http_client.post(url, headers=headers, json=payload)
        response.raise_for_status()
        return response.json()

# Создаем экземпляр
avito = AvitoClient()