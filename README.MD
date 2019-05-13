**Запуск с помощью docker-compose:**

Запуск контенера с БД postgresql - `docker-compose up -d db`

Подгрузка rar архива ФИАС - `docker-compose up download_rar`

Распаковка архива - `docker-compose up unarchive_rar`

Импорт распакованых файлов в базу - `docker-compose up import`

Подгрузка, распаковка и импорт должны происходить последовательно.