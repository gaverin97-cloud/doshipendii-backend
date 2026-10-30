# До стипендии (Before Scholarship) — Backend Service

Полноценный бэкенд сервиса «До стипендии» — тренажёра личного бюджета для студентов.

## Архитектура
- **Framework**: FastAPI (Python 3.11+)
- **Database**: Embedded SQLite (схема полностью адаптирована под Postgres)
- **Engine**: Игровой движок как чистая функция `(состояние, решение) → (новое состояние, события)`
- **Event Sourcing**: Таблица `session_events` сохраняет лог решений как источник истины.

## Быстрый запуск

1. Установите зависимости:
```bash
pip install -r requirements.txt
```

2. Запустите сервер:
```bash
python main.py
```

3. Откройте интерактивную документацию Swagger в браузере:
`http://localhost:8000/docs`

## Тестовые данные
- Код публичной группы: `K7M2QX4P`
- Эндпоинт входа: `POST /v1/cohorts/join`