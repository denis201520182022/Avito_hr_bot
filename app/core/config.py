import yaml
import os
from typing import List, Dict, Any, Optional
from pydantic import BaseModel, Field
from pathlib import Path
from dotenv import load_dotenv
from typing import List, Literal

load_dotenv()

# --- Вспомогательные схемы для YAML ---

class SilenceReminderLevel(BaseModel):
    delay_minutes: int
    text: str
    stop_bot: bool = False

class SilenceConfig(BaseModel):
    enabled: bool
    levels: List[SilenceReminderLevel]

class QuietTimeConfig(BaseModel):
    enabled: bool
    start: str
    end: str
    default_timezone: str

class SilenceConfig(BaseModel):
    enabled: bool
    quiet_time: QuietTimeConfig # Добавили вложенную схему
    levels: List[SilenceReminderLevel]

class InterviewReminderItem(BaseModel):
    id: str
    type: Literal["fixed_time", "relative"]
    days_before: Optional[int] = 0        # Для fixed_time (1 = день до, 0 = в день собеса)
    at_time: Optional[str] = None        # Для fixed_time (например "20:00")
    minutes_before: Optional[int] = None # Для relative (например 120)
    text: str

class InterviewConfig(BaseModel):
    enabled: bool
    items: List[InterviewReminderItem] # Теперь это список

class RemindersConfig(BaseModel):
    silence: SilenceConfig
    interview: InterviewConfig

class GoogleSheetsConfig(BaseModel):
    spreadsheet_url: str
    calendar_sheet_name: str
    candidates_sheet_name: str

class KBConfig(BaseModel):
    prompt_doc_url: str
    credentials_json: str
    cache_ttl: int

class FeaturesConfig(BaseModel):
    enable_outbound_search: bool
    vacancy_description_source: str
    send_tg_interview_cards: bool

class LLMConfig(BaseModel):
    main_model: str
    smart_model: str
    temperature: float
    max_tokens: int # Добавлено согласно config.yaml
    request_timeout: int # Добавлено согласно config.yaml

class MessagesConfig(BaseModel): # НОВАЯ СХЕМА
    initial_greeting: str
    qualification_failed_farewell: str

# --- Главный класс настроек ---

class Settings(BaseModel):
    bot_id: str
    bot_role_name: str
    
    channels: Dict[str, List[str]] # active_platforms внутри channels
    llm: LLMConfig # Блок llm
    features: FeaturesConfig
    knowledge_base: KBConfig
    google_sheets: GoogleSheetsConfig
    reminders: RemindersConfig
    messages: MessagesConfig # НОВЫЙ БЛОК

    # Параметры из .env
    DATABASE_URL: str = Field(default_factory=lambda: os.getenv("DATABASE_URL", ""))
    RABBITMQ_URL: str = Field(default_factory=lambda: os.getenv("RABBITMQ_URL", ""))
    REDIS_URL: str = Field(default_factory=lambda: os.getenv("REDIS_URL", ""))
    TELEGRAM_BOT_TOKEN: str = Field(default_factory=lambda: os.getenv("TELEGRAM_BOT_TOKEN", ""))
        # как мы решили в llm.py, поэтому их здесь нет.
        
    # WEBHOOK_BASE_URL также берется напрямую из ENV в main.py, как и раньше.

    @classmethod
    def load(cls, path: str = "config.yaml"):
        if not Path(path).exists():
            raise FileNotFoundError(f"Файл конфигурации не найден: {path}")
            
        with open(path, "r", encoding="utf-8") as f:
            config_data = yaml.safe_load(f)
            
        return cls(**config_data)

# Создаем глобальный объект настроек
try:
    settings = Settings.load()
except Exception as e:
    print(f"❌ Ошибка загрузки конфигурации: {e}")
    raise