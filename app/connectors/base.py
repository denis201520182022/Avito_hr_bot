# app/connectors/base.py
from abc import ABC, abstractmethod
from app.core.schemas import IncomingEventDTO, CandidateDTO, JobContextDTO

class BaseConnector(ABC):
    @abstractmethod
    async def parse_webhook(self, payload: dict) -> IncomingEventDTO:
        """Переводит сырой JSON платформы в наш IncomingEventDTO"""
        pass

    @abstractmethod
    async def get_candidate_details(self, user_id: str, context_id: str) -> CandidateDTO:
        """Идет в API платформы и вытягивает инфу о человеке"""
        pass

    @abstractmethod
    async def get_job_details(self, item_id: str) -> JobContextDTO:
        """Тянет инфу о вакансии (название, описание)"""
        pass

    @abstractmethod
    async def send_message(self, chat_id: str, text: str):
        """Отправляет текстовое сообщение в чат платформы"""
        pass