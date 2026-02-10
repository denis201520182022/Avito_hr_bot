# app/core/schemas.py
from pydantic import BaseModel
from typing import Optional, Dict, Any

class IncomingEventDTO(BaseModel):
    """То, что прилетает из очереди inbound"""
    platform: str
    external_chat_id: str
    text: Optional[str] = None
    user_id: str
    item_id: str  # ID вакансии/объявления
    raw_payload: Dict[str, Any]

class CandidateDTO(BaseModel):
    """Универсальный профиль, собранный коннектором"""
    full_name: Optional[str] = None
    phone: Optional[str] = None
    platform_user_id: str
    location: Optional[str] = None

class JobContextDTO(BaseModel):
    """Данные о вакансии, собранные с платформы"""
    external_id: str
    title: str
    description: str

# app/core/schemas.py (дополнение)

class EngineTaskDTO(BaseModel):
    """Задача для ИИ-воркера"""
    dialogue_id: int        # ID из нашей БД
    external_chat_id: str
    text: Optional[str] = None
    account_id: int
    platform: str
    event_type: str         # 'new_lead' или 'new_message'