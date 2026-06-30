# MySQL Database Setup Guide

## Переменные окружения для подключения к MySQL

Добавьте следующие переменные окружения в ваш рендер-сервис:

### Обязательные переменные:
```
MYSQL_HOST=your-mysql-host.com
MYSQL_PORT=3306
MYSQL_USER=your_mysql_username
MYSQL_PASSWORD=your_mysql_password
MYSQL_DATABASE=seychmessenger
```

### Опциональные переменные:
```
MYSQL_POOL_MAX=10
MYSQL_CONNECT_TIMEOUT_MS=20000
MYSQL_TIMEOUT_MS=60000
MESSENGER_MYSQL_EXIT_ON_FAIL=1
```

## Пример для Render.com:

В панели Render.com → Environment Variables добавьте:

```
MYSQL_HOST=your-render-mysql-instance.render.com
MYSQL_PORT=3306
MYSQL_USER=your_username
MYSQL_PASSWORD=your_secure_password
MYSQL_DATABASE=seychmessenger
MYSQL_POOL_MAX=20
MYSQL_CONNECT_TIMEOUT_MS=30000
MESSENGER_MYSQL_EXIT_ON_FAIL=1
```

## Установка зависимостей:

```bash
cd backend
npm install
```

## Создание базы данных MySQL:

```sql
CREATE DATABASE seychmessenger CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci;
CREATE USER 'seychmessenger'@'%' IDENTIFIED BY 'your_password';
GRANT ALL PRIVILEGES ON seychmessenger.* TO 'seychmessenger'@'%';
FLUSH PRIVILEGES;
```

## Структура таблиц:

Таблицы будут созданы автоматически при первом запуске:

- `users` - профили пользователей
- `settings` - настройки приватности
- `chats` - чаты между пользователями
- `messages` - сообщения

## Миграция данных с PostgreSQL:

Данные из PostgreSQL полностью совместимы с MySQL. Все JSON поля сохраняются как JSON, все текстовые поля как TEXT/VARCHAR.

## Отличия от PostgreSQL:

1. **JSON поля**: MySQL использует `JSON` вместо `JSONB`
2. **Индексы**: MySQL использует синтаксис `CREATE INDEX` вместо `CREATE INDEX IF NOT EXISTS`
3. **UPSERT**: MySQL использует `ON DUPLICATE KEY UPDATE` вместо `ON CONFLICT`
4. **Подключение**: Используются отдельные переменные `MYSQL_*` вместо `DATABASE_URL`

## Проверка подключения:

При запуске сервера вы увидите:
```
[messenger_mysql] connected (MySQL)
[messenger] storage backend: mysql
```

Если подключение не удалось:
```
[messenger_mysql] init failed: [error message]
[messenger] storage backend: unavailable
```

## Внешнее подключение:

Теперь WebSocket сервер может подключаться к MySQL извне, а не только внутри одного сервера.
