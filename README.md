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


curl -X GET \
  'https://api.avito.ru/job/v1/applications/webhooks' \
  -H 'Authorization: Bearer KesLQo6-QlGmn3t_nKOOrAL1cymkCL-mbjkyjxSw' \
  -H 'Content-Type: application/json'



curl -X PUT \
  'https://api.avito.ru/job/v1/applications/webhook' \
  -H 'Authorization: Bearer KesLQo6-QlGmn3t_nKOOrAL1cymkCL-mbjkyjxSw' \
  -H 'Content-Type: application/json' \
  -d '{
    "url": "http://212.193.26.118:8004/webhooks/avito",
    "secret": "super_secret_key"
  }'




📊 Шпаргалка по статистике (AnalyticsEvent)
Все события хранятся в таблице analytics_events. Каждая запись — это свершившийся факт в жизни диалога.
1. Карта событий (Типы event_type)
Событие	Кто пишет	Когда происходит	Что в event_data
lead_created	Унификатор	Создан новый диалог и списаны деньги.	{"cost": 19.0}
first_contact	Движок	Кандидат отправил первое ответное сообщение.	{}
qualified	Движок	Кандидат прошел все проверки и записан на собес.	{"interview_date": "..."}
rejected_by_bot	Движок	Бот отказал по критериям (возраст, гражданство и т.д.).	{"reason": "age"}
rejected_by_candidate	Движок	Кандидат сам сказал «не интересно» или «отказываюсь».	{"reason_state": "..."}
timed_out	Шедулер	Кандидат замолчал и бот прекратил дожимы.	{"final_level": 2}





Окей, супер
Теперь перейдем к другой задачи
Ты уже видел мой файлы, видел как я пользуюсь алертами в тг

Вот код алертов:
import logging
from typing import Optional, Dict, Any
from aiogram import Bot
from aiogram.client.default import DefaultBotProperties
from aiogram.enums import ParseMode
from aiogram.types import BufferedInputFile
from sqlalchemy import select

from app.core.config import settings
from app.db.session import AsyncSessionLocal
from app.db.models import TelegramUser

logger = logging.getLogger(__name__)

def esc(text: Any) -> str:
    """Экранирование спецсимволов для MarkdownV2 (упрощенное)"""
    return str(text).replace('_', '\\_').replace('*', '\\*').replace('[', '\\[').replace('`', '\\`').replace('>', '\\>')

async def _get_recipients(alert_type: str) -> list[int]:
    """Вспомогательная функция для получения ID получателей из БД"""
    async with AsyncSessionLocal() as session:
        if alert_type in ["balance", "all"]:
            # Все пользователи бота
            stmt = select(TelegramUser.telegram_id)
        else:
            # Только админы
            stmt = select(TelegramUser.telegram_id).where(TelegramUser.role == 'admin')
        
        result = await session.execute(stmt)
        return list(result.scalars().all())

async def send_system_alert(message_text: str, alert_type: str = "admin_only"):
    """
    Отправляет системное уведомление (ошибки, баланс, анонсы).
    """
    recipients = await _get_recipients(alert_type)
    if not recipients:
        return

    async with Bot(token=settings.TELEGRAM_BOT_TOKEN) as bot:
        for chat_id in recipients:
            try:
                await bot.send_message(chat_id=chat_id, text=message_text)
            except Exception as e:
                logger.warning(f"Ошибка отправки алерта в {chat_id}: {e}")

async def send_verification_alert(
    dialogue_id: int,
    external_chat_id: str,
    db_data: Dict[str, Any],
    llm_data: Dict[str, Any],
    history_text: Optional[str] = None,
    reasoning: str = "не указано"
):
    """
    Алерт о несовпадении анкетных данных (например, возраст или гражданство).
    """
    # Используем твой ID как основной для инцидентов или шлем всем админам
    admin_id = 1975808643 
    
    alert_text = (
        f"🚨 *INCIDENT: Ошибка верификации данных*\n\n"
        f"Диалог ID: `{dialogue_id}`\n"
        f"Avito Chat ID: `{esc(external_chat_id)}`\n\n"
        f"📉 *Данные в БД:* {esc(db_data)}\n"
        f"🤖 *Deep Check LLM:* {esc(llm_data)}\n\n"
        f"🧐 *Обоснование:* _{esc(reasoning)}_\n\n"
        f"⛔ *Данные в БД НЕ! обновлены на основе Deep Check.*"
    )

    async with Bot(
        token=settings.TELEGRAM_BOT_TOKEN, 
        default=DefaultBotProperties(parse_mode=ParseMode.MARKDOWN)
    ) as bot:
        try:
            await bot.send_message(chat_id=admin_id, text=alert_text)
            
            if history_text:
                file = BufferedInputFile(
                    history_text.encode('utf-8'), 
                    filename=f"verify_error_{external_chat_id}.txt"
                )
                await bot.send_document(chat_id=admin_id, document=file, caption="📜 История для анализа")
        except Exception as e:
            logger.error(f"Ошибка отправки алерта верификации: {e}")

async def send_hallucination_alert(
    dialogue_id: int,
    external_chat_id: str,
    user_said: str,
    llm_suggested: str,
    corrected_val: str,
    history_text: Optional[str] = None,
    reasoning: str = "не указано"
):
    """
    Алерт о галлюцинации или ошибке извлечения (даты, телефоны и т.д.).
    """
    admin_id = 1975808643

    alert_text = (
        f"📅 *INCIDENT: Ошибка извлечения (Галлюцинация)*\n\n"
        f"Диалог ID: `{dialogue_id}`\n"
        f"Avito Chat: `{esc(external_chat_id)}`\n\n"
        f"👤 *Кандидат:* _{esc(user_said)}_\n"
        f"🤖 *LLM:* `{esc(llm_suggested)}`\n"
        f"✅ *Аудитор исправил:* `{esc(corrected_val)}`\n\n"
        f"🧐 *Обоснование:* _{esc(reasoning)}_\n\n"
        f"🔄 *Диалог отправлен на перегенерацию.*"
    )

    async with Bot(
        token=settings.TELEGRAM_BOT_TOKEN, 
        default=DefaultBotProperties(parse_mode=ParseMode.MARKDOWN)
    ) as bot:
        try:
            await bot.send_message(chat_id=admin_id, text=alert_text)
            
            if history_text:
                file = BufferedInputFile(
                    history_text.encode('utf-8'), 
                    filename=f"hallucination_{external_chat_id}.txt"
                )
                await bot.send_document(chat_id=admin_id, document=file, caption="📜 История диалога")
        except Exception as e:
            logger.error(f"Ошибка отправки алерта галлюцинации: {e}")




Я сейчас следующим шагом дам тебе код самого движка и необходимо нам с тобой будет внедрить алерты (ты мне должен будешь сказать где и какой код добавить, но не писать весь файл)
А также необходимо будет проверить, 




docker compose up -d --build
docker compose down
docker logs avito_hr_bot


tail -f logs/fastapi.log

tail -f logs/engine.log
tail -n 20 logs/tg_worker_err.log

tail -f logs/connector.log
tail -f logs/connector_err.log
tail -n 20 logs/engine_err.log
tail -n 20 logs/scheduler_err.log
tail -f logs/tg_worker.log



docker logs -f avito_hr_bot

tail -f logs/*.log

docker compose exec rabbitmq rabbitmqctl purge_queue engine_tasks
docker compose exec redis redis-cli FLUSHALL




{
  "tg_chat_id": "-5281527918"
}





Ресетнуть диалог:
docker exec -it avito_hr_bot python reset_test.py u2i-NyF0fdvl9bDIzxRgvbA61Q



Удалить диалог

DO $$
DECLARE
    -- === НАСТРОЙКА ===
    -- Укажите здесь ID диалога, который нужно удалить
    target_dialogue_id INTEGER := 5; 
    
    -- Переменная для хранения ID кандидата
    target_candidate_id INTEGER;
BEGIN
    -- 1. Получаем ID кандидата перед тем, как удалить диалог
    SELECT candidate_id INTO target_candidate_id
    FROM dialogues
    WHERE id = target_dialogue_id;

    -- Если диалог не найден, выходим
    IF target_candidate_id IS NULL THEN
        RAISE NOTICE 'Диалог с ID % не найден.', target_dialogue_id;
        RETURN;
    END IF;

    -- 2. Удаляем все данные, ссылающиеся на диалог (Дети)
    DELETE FROM llm_logs WHERE dialogue_id = target_dialogue_id;
    DELETE FROM interview_reminders WHERE dialogue_id = target_dialogue_id;
    DELETE FROM interview_followups WHERE dialogue_id = target_dialogue_id;
    DELETE FROM analytics_events WHERE dialogue_id = target_dialogue_id;

    -- 3. Удаляем сам диалог
    DELETE FROM dialogues WHERE id = target_dialogue_id;
    
    RAISE NOTICE 'Диалог % удален.', target_dialogue_id;

    -- 4. Удаляем кандидата (Родитель)
    -- Удаляем ТОЛЬКО если у этого кандидата больше нет записей в таблице dialogues.
    -- (Мы уже удалили текущий диалог выше, поэтому проверяем, остались ли другие).
    DELETE FROM candidates
    WHERE id = target_candidate_id
    AND NOT EXISTS (
        SELECT 1 FROM dialogues WHERE candidate_id = target_candidate_id
    );

    -- Проверяем, был ли удален кандидат
    IF FOUND THEN
        RAISE NOTICE 'Кандидат (ID %) также был удален, так как у него нет других диалогов.', target_candidate_id;
    ELSE
        RAISE NOTICE 'Кандидат (ID %) ОСТАВЛЕН в базе, так как у него есть другие активные диалоги.', target_candidate_id;
    END IF;

END $$;