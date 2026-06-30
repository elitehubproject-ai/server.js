# Supabase Database Setup Guide

## Переменные окружения для подключения к Supabase

Добавьте следующие переменные окружения в ваш рендер-сервис:

### Обязательные переменные:
```
SUPABASE_URL=https://your-project.supabase.co
SUPABASE_KEY=your_supabase_anon_key_or_service_role_key
```

### Опциональные переменные:
```
PG_POOL_MAX=10
PG_CONNECT_TIMEOUT_MS=20000
MESSENGER_MYSQL_EXIT_ON_FAIL=1
```

## Пример для Render.com:

В панели Render.com → Environment Variables добавьте:

```
SUPABASE_URL=https://mykozjcyeojscusbhiiz.supabase.co
SUPABASE_KEY=your_supabase_service_role_key
PG_POOL_MAX=20
PG_CONNECT_TIMEOUT_MS=30000
MESSENGER_MYSQL_EXIT_ON_FAIL=1
```

## Как получить данные Supabase:

### 1. Создайте проект на Supabase:
1. Зайдите на https://supabase.com
2. Создайте новый проект
3. Дождитесь создания базы данных

### 2. Получите данные подключения:
1. В проекте → Settings → Database
2. Найдите "Connection string"
3. Скопируйте "URI" или используйте отдельные параметры:
   - **Project URL**: https://your-project.supabase.co
   - **API Key**: в Settings → API → service_role (рекомендуется) или anon_key

### 3. Формат подключения:
```
SUPABASE_URL=https://your-project.supabase.co
SUPABASE_KEY=eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9...
```

## Установка зависимостей:

```bash
cd backend
npm install
```

## Структура таблиц:

Таблицы будут созданы автоматически при первом запуске:

- `users` - профили пользователей
- `settings` - настройки приватности  
- `chats` - чаты между пользователями
- `messages` - сообщения

## Особенности Supabase:

1. **Только PostgreSQL** - Supabase не поддерживает MySQL
2. **JSONB поля** - используются вместо JSON для лучшей производительности
3. **Автоматические индексы** - создаются для оптимизации запросов
4. **Внешние подключения** - разрешены через REST API или прямое подключение

## Проверка подключения:

При запуске сервера вы увидите:
```
[messenger_mysql] connected (Supabase PostgreSQL)
[messenger] storage backend: supabase-postgresql
```

Если подключение не удалось:
```
[messenger_mysql] init failed: [error message]
[messenger] storage backend: unavailable
```

## Настройка безопасности в Supabase:

### 1. Рекомендации:
- Используйте `service_role` ключ для серверных операций
- Настройте RLS (Row Level Security) если нужно
- Ограничьте доступ по IP в настройках Supabase

### 2. Таблицы создаются автоматически:
- Все таблицы создаются с правильными индексами
- JSONB поля для хранения массивов и объектов
- Уникальные ключи для предотвращения дубликатов

## Преимущества Supabase:

- ✅ **Полностью бесплатно** до 500MB
- ✅ **Внешние подключения** разрешены  
- ✅ **Автоматические бэкапы**
- ✅ **Real-time возможности**
- ✅ **Простая настройка**
- ✅ **Надежная инфраструктура**

## Отладка:

Если есть проблемы с подключением:
1. Проверьте правильность `SUPABASE_URL`
2. Убедитесь что `SUPABASE_KEY` действительный
3. Проверьте что проект Supabase активен
4. Посмотрите логи Render для детальной ошибки

## Готово к работе!

После настройки переменных окружения и запуска сервера, все таблицы будут созданы автоматически и мессенджер будет работать с Supabase PostgreSQL базой данных.
