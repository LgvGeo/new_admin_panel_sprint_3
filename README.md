# Заключительное задание первого модуля

Ваша задача в этом уроке — загрузить данные в Elasticsearch из PostgreSQL. Подробности задания в папке `etl`.

Запуск проекта:  
задаем env file с переменными окружения  

DB_NAME=postgres  
DB_USER=postgres  
DB_PASSWORD=postgres  
DB_HOST=db  
DB_PORT=5432  
ELASTICSEARCH_CONN_URI=http://elastic:9200  
REDIS_HOST=redis  
REDIS_PORT=6379  
POSTGRES_PASSWORD=postgres  
POSTGRES_USER=postgres  
POSTGRES_DB=postgres  

Запускаем через docker compose  
После этого инициализируем бд заполняем данными и создаем индекс в эластике  
перезапускаем через docker compose