import gspread
import logging
from sqlalchemy import select, update
from app.db.session import AsyncSessionLocal
from app.db.models import JobContext, Account
from app.core.config import settings

logger = logging.getLogger("GoogleSyncSearch")

class GoogleSyncSearchService:
    def __init__(self):
        # Авторизация в Google через файл сервисного аккаунта
        self.gc = gspread.service_account(filename=settings.knowledge_base.credentials_json)
        
    def _clean_value(self, val):
        """Очистка значения ячейки: превращает пустоту или 'Null' в настоящий None"""
        if val is None:
            return None
        
        s_val = str(val).strip()
        # Если ячейка пустая, содержит только пробелы или написано 'null' (в любом регистре)
        if s_val == "" or s_val.lower() == 'null':
            return None
            
        return s_val

    def _extract_ids(self, val):
        """Извлекает цифровые ID из строки (напр. '10173 - Туризм 16844 - Дом' -> ['10173', '16844'])"""
        if not val: return None
        # Разбиваем по пробелам или переносам строк (в случае мультивыбора в ячейке)
        parts = str(val).replace('\n', ' ').split(' ')
        ids = []
        for p in parts:
            # Берем только часть до дефиса, если она числовая
            potential_id = p.split('-')[0].strip()
            if potential_id.isdigit():
                ids.append(potential_id)
        return ",".join(ids) if ids else None

    async def sync_all(self):
        try:
            # 1. Открываем таблицу и лист
            sh = self.gc.open_by_url(settings.google_sheets.spreadsheet_url)
            ws = sh.worksheet(settings.google_sheets.search_sheet_name)
            
            all_values = ws.get_all_values()
            if not all_values: return

            async with AsyncSessionLocal() as db:
                # 2. Получаем активные вакансии из БД (только Авито)
                stmt = select(JobContext).join(Account).where(
                    Account.platform == 'avito',
                    JobContext.is_active == True
                )
                db_vacancies = (await db.execute(stmt)).scalars().all()
                db_vac_map = {v.external_id: v for v in db_vacancies}
                
                num_cols = len(all_values[0]) if len(all_values) > 0 else 0
                new_sheet_columns = [] 
                processed_ids = set()

                # 3. Читаем существующие колонки (C, D, E...)
                for col_idx in range(2, num_cols):
                    # ID вакансии в СТРОКЕ 2 (индекс 1 в get_all_values)
                    ext_id = all_values[1][col_idx].strip()
                    if not ext_id: continue
                    
                    processed_ids.add(ext_id)
                    vacancy = db_vac_map.get(ext_id)

                    if not vacancy: continue

                    # КВОТЫ: СТРОКА 4 (индекс 3)
                    quota_to_add = self._clean_value(all_values[3][col_idx])
                    if quota_to_add and quota_to_add.isdigit():
                        vacancy.search_remaining_quota += int(quota_to_add)
                        logger.info(f"Добавлено {quota_to_add} квот вакансии {ext_id}")
                    
                    # ПАРАМЕТРЫ: читаем из соответствующих строк (индекс = строка - 1)
                    filters = {
                        "query": self._clean_value(all_values[5][col_idx]),        # Стр 6
                        "location": self._clean_value(all_values[6][col_idx]),     # Стр 7
                        "metro": self._clean_value(all_values[7][col_idx]),        # Стр 8
                        "district": self._clean_value(all_values[8][col_idx]),     # Стр 9
                        "specialization": self._extract_ids(all_values[9][col_idx]),# Стр 10
                        "schedule": self._clean_value(all_values[10][col_idx]),     # Стр 11
                        "business_trip_readiness": self._clean_value(all_values[11][col_idx]),
                        "relocation_readiness": self._clean_value(all_values[12][col_idx]),
                        "gender": self._clean_value(all_values[13][col_idx]),
                        "age_min": self._clean_value(all_values[14][col_idx]),
                        "age_max": self._clean_value(all_values[15][col_idx]),
                        "education_level": self._clean_value(all_values[16][col_idx]),
                        "experience_min": self._clean_value(all_values[17][col_idx]),
                        "experience_max": self._clean_value(all_values[18][col_idx]),
                        "salary_min": self._clean_value(all_values[19][col_idx]),
                        "salary_max": self._clean_value(all_values[20][col_idx]),
                        "nationality": self._extract_ids(all_values[21][col_idx]),
                        "driver_licence": self._clean_value(all_values[22][col_idx]),
                        "driver_licence_category": self._clean_value(all_values[23][col_idx]),
                        "driving_experience": self._clean_value(all_values[24][col_idx]),
                        "own_transport": self._clean_value(all_values[25][col_idx]),
                        "medical_book": self._clean_value(all_values[26][col_idx]),
                    }
                    vacancy.search_filters = filters
                    
                    # ПОДГОТОВКА КОЛОНКИ ДЛЯ ЗАПИСИ (начиная со строки 2)
                    col_data = [""] * 26 # Индексы 0-25 соответствуют строкам 2-27
                    col_data[0] = vacancy.external_id                 # Строка 2
                    col_data[1] = vacancy.title                       # Строка 3
                    col_data[2] = ""                                  # Строка 4 (сброс ввода)
                    col_data[3] = str(vacancy.search_remaining_quota) # Строка 5 (остаток)
                    
                    # Копируем визуальные значения параметров обратно (строки 6-27 -> индексы 4-25)
                    for r in range(5, 27):
                        if r < len(all_values):
                            col_data[r-1] = all_values[r][col_idx]
                    
                    new_sheet_columns.append(col_data)

                # 4. ДОБАВЛЯЕМ НОВЫЕ ВАКАНСИИ
                for ext_id, vac in db_vac_map.items():
                    if ext_id not in processed_ids:
                        new_col = [""] * 26
                        new_col[0] = vac.external_id
                        new_col[1] = vac.title
                        new_col[2] = ""
                        new_col[3] = str(vac.search_remaining_quota)
                        new_sheet_columns.append(new_col)
                        logger.info(f"Добавлена новая вакансия: {vac.title}")

                # 5. ОЧИСТКА И ЗАПИСЬ (C2:ZZ27)
                ws.batch_clear(["C2:ZZ27"])
                
                if new_sheet_columns:
                    # Транспонируем: список колонок в список строк для записи
                    rows_to_update = list(zip(*new_sheet_columns))
                    ws.update("C2", rows_to_update)

                await db.commit()
                logger.info("✅ Синхронизация AvitoSearch завершена (индексы исправлены)")

        except Exception as e:
            logger.error(f"❌ Ошибка синхронизации AvitoSearch: {e}", exc_info=True)

google_sync_search_service = GoogleSyncSearchService()