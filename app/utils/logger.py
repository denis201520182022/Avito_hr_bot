import logging
import sys
from datetime import datetime
from pythonjsonlogger import jsonlogger
import os
import contextvars

# Переменная, которая будет хранить контекст логов для текущего потока/таски
log_context = contextvars.ContextVar("log_context", default={})

class CustomJsonFormatter(jsonlogger.JsonFormatter):
    def add_fields(self, log_record, record, message_dict):
        super(CustomJsonFormatter, self).add_fields(log_record, record, message_dict)
        
        # 1. Добавляем данные из контекста (если они есть)
        current_context = log_context.get()
        for key, value in current_context.items():
            log_record[key] = value

        # 2. Стандартные поля
        if not log_record.get('timestamp'):
            log_record['timestamp'] = datetime.utcnow().strftime('%Y-%m-%dT%H:%M:%S.%fZ')
        log_record['level'] = str(log_record.get('level', record.levelname) or "INFO").upper()

# Функция-помощник для установки контекста
def set_log_context(**kwargs):
    # Берем текущий контекст, обновляем его новыми данными
    new_context = log_context.get().copy()
    new_context.update(kwargs)
    log_context.set(new_context)


# Добавь этот класс перед функцией setup_logger
class ContextFilter(logging.Filter):
    def filter(self, record):
        # Добавляем данные из контекста в объект записи лога (для локального режима)
        record.log_context = str(log_context.get())
        return True

def setup_logger(name: str):
    logger = logging.getLogger(name)
    logger.setLevel(logging.INFO)
    if logger.hasHandlers():
        logger.handlers.clear()

    handler = logging.StreamHandler(sys.stdout)
    
    if os.getenv("DOCKER_MODE") == "true":
        formatter = CustomJsonFormatter('%(timestamp)s %(level)s %(name)s %(message)s')
    else:
        # Добавляем наш фильтр, чтобы record.log_context стал доступен
        logger.addFilter(ContextFilter())
        formatter = logging.Formatter('%(asctime)s | %(levelname)s | %(name)s | %(message)s | ctx: %(log_context)s')
        
    handler.setFormatter(formatter)
    logger.addHandler(handler)
    logger.propagate = False
    return logger

logger = setup_logger("avito_bot")