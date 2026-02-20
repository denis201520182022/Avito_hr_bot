
source .venv/bin/activate






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