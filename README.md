.\cloudflared.exe tunnel --protocol http2 --url http://localhost:8000

source .venv/bin/activate


curl -L -X POST 'https://api.avito.ru/token/' \
    -H 'Content-Type: application/x-www-form-urlencoded' \
    --data-urlencode 'grant_type=client_credentials' \
    --data-urlencode 'client_id=gNc5Bbkplxurm_XXgcy_' \
    --data-urlencode 'client_secret=0gDwAmJX7ErJROSfxcBC3U4WBc-NdEoxMLLTUw94'

{"access_token":"s2aAuEUBS5aBUJDaexixOwhdLuqtA6DjHcchuF-E","expires_in":86400,"token_type":"Bearer"}


curl -X POST "https://api.avito.ru/messenger/v1/subscriptions" \
-H "Authorization: Bearer s2aAuEUBS5aBUJDaexixOwhdLuqtA6DjHcchuF-E" \
-H "Content-Type: application/json"


{"subscriptions":[
    {"url":"https://added-stem-seeing-injuries.trycloudflare.com/avito/webhook","version":"3"},{"url":"https://amojo.amocrm.ru/v1/~external/hooks/avito","version":"3"},
    {"url":"https://deaf-rankings-necessary-cathedral.trycloudflare.com/avito/webhook","version":"3"},{"url":"https://editors-ticket-dare-bidding.trycloudflare.com/avito/webhook","version":"3"},{"url":"http://212.193.26.118:8003/avito/webhook","version":"3"}]}




curl -X POST "https://api.avito.ru/messenger/v1/webhook/unsubscribe" \
-H "Authorization: Bearer s2aAuEUBS5aBUJDaexixOwhdLuqtA6DjHcchuF-E" \
-H "Content-Type: application/json" \
-d '{"url": "https://added-stem-seeing-injuries.trycloudflare.com/avito/webhook"}'

curl -X POST "https://api.avito.ru/messenger/v1/webhook/unsubscribe" \
-H "Authorization: Bearer s2aAuEUBS5aBUJDaexixOwhdLuqtA6DjHcchuF-E" \
-H "Content-Type: application/json" \
-d '{"url": "https://deaf-rankings-necessary-cathedral.trycloudflare.com/avito/webhook"}'

curl -X POST "https://api.avito.ru/messenger/v1/webhook/unsubscribe" \
-H "Authorization: Bearer s2aAuEUBS5aBUJDaexixOwhdLuqtA6DjHcchuF-E" \
-H "Content-Type: application/json" \
-d '{"url": "https://editors-ticket-dare-bidding.trycloudflare.com/avito/webhook"}'

curl -X POST "https://api.avito.ru/messenger/v1/webhook/unsubscribe" \
-H "Authorization: Bearer s2aAuEUBS5aBUJDaexixOwhdLuqtA6DjHcchuF-E" \
-H "Content-Type: application/json" \
-d '{"url": "http://212.193.26.118:8003/avito/webhook"}'


curl -X POST "https://api.avito.ru/messenger/v3/webhook" \
-H "Authorization: Bearer s2aAuEUBS5aBUJDaexixOwhdLuqtA6DjHcchuF-E" \
-H "Content-Type: application/json" \
-d '{"url": "http://212.193.26.118:8003/avito/webhook"}'





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





Нужен ИИ Бот не сложный, но подвох в том, что его надо спроектировать так, чтобы можно было легко менять под клиента.
Потому что я движусь в сторону саас сервиса, где можно будет провести автоматизированный опрос клиенту и на основе него составить конфиг для нового бота, и запускать очень быстро.

То есть что требуется - нужно чтобы бот был построен блоками, которые бы можно было включать, выключать, ставить другие и тд. Короче как конструктор ботов.
Сейчас требуется выполнить заказ для конкретного клиента, а не платформу сделать, но надо уже этот проект сделать так, чтобы он стал началом для платформенного универсального решения.

Что именно должно/может меняться:
1)Канал взаимодействия. Авито или hh например
2) Способ взаимодействия с каналом вебхук или полинг. В авито можно через вебхук чтобы они сами присылали новые отклики или сообщения, а вот в hh нет вебхуков, приходится бесконечный полинг включать чтобы забирать все новые отклики и сообщения
3) Информация о вакансии берется или из самого объявления (как в этом проекте), или из промпта (отдельного гугл дока)
4) Бот только отвечает на отклики или сам ищет в базе и пишет первым (когда написал первым то дальше все одинаково идет)
5) Список вопросов от бота разный, у каждого клиента он свой. Надо как то сделать так чтобы бот шел по вопросам и состояниям вне зависимости от их списка
6) Критерии подходит не подходит кандидат. Они у всех разные и связаны с вопросами
8) Должна быть возможность подрубать различные llm от различных провайдеров
7) Осальные моменты, например отправка в тг, запись в гугл таблицы и тд
) Ну и само собой промпт разный у всех и FAQ (их надо из гугл дока тянуть)


Да, нужнет json или yaml конфиг, куда выносим все настройки, ключи и тд
Нужны конекторы и общие интерфейсы
FSM обязательно нужна, но llm сам должен вести диалог, чтобы это был как живой человек. Мы должны направлять и валидировать действия llm везде где только возможно. Данные вычленяет тоже llm. Но всю важную логику решения в коде делаем

Архитектуру я вижу примерно так:
Есть конфиг
Есть конекторы для avito, hh. Они взаимодействуют с каналами. Данные от них обрабатываются и приходят к нужному виду и далее кладутся в очереди (наверное rabbit)
Есть воркеры, которые берут инфу из очередей, и с помощью движка обрабатывают
Воркеры движка ничего не знают откуда данные пришли, к ним данные поступают унифицированные. В нем происходит вся логика диалога
Есть конектор для llm
Есть модуль принятия решений
Есть БД постгрес
Есть редис для кеша
Есть планировщик (для напоминаний, касаний до и после собеса, инициация первичного контакта для исходящего бота наверное тоже тут и тд)
Есть воркер тг, который мониторит очередь и отправляет в тг
Также есть тг бот, в котором происходит управление тарифами, статистика и тд но о нем пока не думаем






source venv/bin/activate