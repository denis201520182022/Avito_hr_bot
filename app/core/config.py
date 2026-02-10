# app/core/config.py
import yaml
from typing import List, Optional, Dict, Any
from pydantic import BaseModel, HttpUrl, Field
from pathlib import Path

# --- Подсхемы для блоков YAML ---

class ChannelConfig(BaseModel):
    type: str  # avito, hh
    mode: str  # inbound, outbound
    vacancies: List[str]

class LLMConfig(BaseModel):
    provider: str
    model: str
    smart_model: str
    temperature: float = 0.2

class KnowledgeBaseConfig(BaseModel):
    vacancy_info_source: str
    main_prompt_url: str
    faq_url: str
    vacancy_doc_url: Optional[str] = None

class Question(BaseModel):
    id: str
    text: str
    required: bool = True

class QualificationRules(BaseModel):
    age: Dict[str, int]  # {"min": 30, "max": 55}
    citizenship: Dict[str, Any]
    criminal_record: Dict[str, List[str]]

class ReminderItem(BaseModel):
    delay_minutes: Optional[int] = None
    delay_hours: Optional[int] = None
    send_at_local_time: Optional[str] = None
    text: str
    stop_bot: bool = False

class RemindersConfig(BaseModel):
    silence: List[ReminderItem]
    interview: Dict[str, ReminderItem]

class IntegrationsConfig(BaseModel):
    telegram: Dict[str, bool]
    google_sheets: Dict[str, str]

# --- Главный класс конфига ---

class BotConfig(BaseModel):
    bot_id: str
    bot_role_name: str
    channel: ChannelConfig
    llm: LLMConfig
    knowledge_base: KnowledgeBaseConfig
    screening: Dict[str, List[Question]]
    qualification_rules: QualificationRules
    reminders: RemindersConfig
    integrations: IntegrationsConfig
    system_messages: Dict[str, str]

    @classmethod
    def load(cls, path: str = "config.yaml"):
        if not Path(path).exists():
            raise FileNotFoundError(f"Конфиг не найден по пути: {path}")
            
        with open(path, "r", encoding="utf-8") as f:
            data = yaml.safe_load(f)
            
        return cls(**data)

# Создаем глобальный объект настроек, который будем импортировать везде
settings = BotConfig.load()