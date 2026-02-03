# config/settings.py
from pydantic_settings import BaseSettings
from typing import Optional

class Settings(BaseSettings):
    # Proxy Settings
    SQUID_PROXY_HOST: str
    SQUID_PROXY_PORT: str
    SQUID_PROXY_USER: str
    SQUID_PROXY_PASSWORD: str

    # OpenAI Settings
    OPENAI_API_KEY: str
    OPENAI_MODEL: str = "gpt-4o-mini"

    # Avito Settings
    AVITO_CLIENT_ID: Optional[str] = None
    AVITO_CLIENT_SECRET: Optional[str] = None

    class Config:
        env_file = ".env"
        extra = "ignore" # Игнорировать лишние переменные в .env

settings = Settings()