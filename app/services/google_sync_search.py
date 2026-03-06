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
            
            # Читаем все данные листа (матрица)
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
                
                # 3. Парсим текущие столбцы таблицы (начиная с колонки C, индекс 2)
                # Колонки в Google Sheets: A=0, B=1, C=2...
                num_rows = len(all_values)
                num_cols = len(all_values[0]) if num_rows > 0 else 0
                
                new_sheet_columns = [] # Здесь будем хранить обновленные данные для записи назад
                processed_ids = set()

                for col_idx in range(2, num_cols):
                    # Извлекаем ID вакансии из строки 2 (индекс 1)
                    ext_id = all_values[1][col_idx].strip()
                    if not ext_id: continue
                    
                    processed_ids.add(ext_id)
                    vacancy = db_vac_map.get(ext_id)

                    # Если вакансия в БД неактивна или удалена - мы просто не добавим её в new_sheet_columns
                    if not vacancy:
                        continue

                    # --- ОБРАБОТКА КВОТ ---
                    quota_to_add = self._clean_value(all_values[3][col_idx]) # Строка 4
                    if quota_to_add and quota_to_add.isdigit():
                        vacancy.search_remaining_quota += int(quota_to_add)
                        logger.info(f"Добавлено {quota_to_add} квот вакансии {ext_id}")
                    
                    # --- ОБРАБОТКА ПАРАМЕТРОВ ПОИСКА ---
                    filters = {
                        "query": self._clean_value(all_values[5][col_idx]),
                        "location": self._clean_value(all_values[6][col_idx]),
                        "metro": self._clean_value(all_values[7][col_idx]),
                        "district": self._clean_value(all_values[8][col_idx]),
                        # Применяем форматирование для ID
                        "specialization": self._extract_ids(all_values[9][col_idx]),
                        "schedule": self._clean_value(all_values[10][col_idx]),
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
                    
                    # Подготавливаем данные столбца для записи обратно в таблицу
                    col_data = [""] * 27 # 27 строк
                    col_data[1] = vacancy.external_id
                    col_data[2] = vacancy.title
                    col_data[3] = "" # Обнуляем "Квоты задать"
                    col_data[4] = str(vacancy.search_remaining_quota) # Пишем "Квоты осталось"
                    # Копируем остальные фильтры как есть (чтобы не портить визуализацию в таблице)
                    for r in range(5, 27):
                        col_data[r] = all_values[r][col_idx]
                    
                    new_sheet_columns.append(col_data)

                # 4. ДОБАВЛЯЕМ НОВЫЕ ВАКАНСИИ, которых еще нет в таблице
                for ext_id, vac in db_vac_map.items():
                    if ext_id not in processed_ids:
                        new_col = [""] * 27
                        new_col[1] = vac.external_id
                        new_col[2] = vac.title
                        new_col[3] = ""
                        new_col[4] = str(vac.search_remaining_quota)
                        new_sheet_columns.append(new_col)
                        logger.info(f"Добавлена новая вакансия в таблицу: {vac.title}")

                # 5. ЗАПИСЫВАЕМ ОБНОВЛЕННЫЕ ДАННЫЕ В ТАБЛИЦУ (Обновление со сдвигом)
                # Очищаем старые данные справа от заголовков (C2:ZZ27)
                ws.batch_clear(["C2:ZZ27"])
                
                if new_sheet_columns:
                    # Транспонируем список столбцов обратно в строки для gspread
                    # zip(*new_sheet_columns) превратит список столбцов в список строк
                    rows_to_update = list(zip(*new_sheet_columns))
                    # Записываем в диапазон, начиная с C2 (строка 2, колонка 3)
                    start_cell = gspread.utils.rowcol_to_a1(2, 3)
                    ws.update(start_cell, rows_to_update)

                await db.commit()
                logger.info("✅ Синхронизация с AvitoSearch завершена успешно")

        except Exception as e:
            logger.error(f"❌ Ошибка синхронизации AvitoSearch: {e}", exc_info=True)

google_sync_search_service = GoogleSyncSearchService()