import asyncio
import sys
from src.transport.avito_client import avito_client

async def main():
    # Твоя ссылка
    url = "212.193.26.118:8003/avito/webhook" 
    
    print("\n1. Проверяем текущие подписки...")
    current = await avito_client.get_subscriptions()
    print(f"Сейчас в Авито: {current}")

    print(f"\n2. Регистрируем новую ссылку: {url}")
    result = await avito_client.subscribe_webhook(url)
    print(f"Результат регистрации: {result}")

    print("\n3. Финальная проверка подписок...")
    final = await avito_client.get_subscriptions()
    print(f"Итого в Авито: {final}")

if __name__ == "__main__":
    asyncio.run(main())