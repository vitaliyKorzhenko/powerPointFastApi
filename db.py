import psycopg2
import urllib.parse as up

# External Database URL
database_url = "postgres://govorikapresentations_user:gf7LBmzPmQubL6flpA5CzcpDpFWmEi0Y@dpg-crnvgul6l47c73al9tpg-a.oregon-postgres.render.com:5432/govorikapresentations"

# Парсинг URL
url = up.urlparse(database_url)

# Подключение к базе данных
try:
    conn = psycopg2.connect(
        dbname=url.path[1:],  # убираем первый символ "/"
        user=url.username,
        password=url.password,
        host=url.hostname,
        port=url.port
    )
    print("Successfully connected to the database!")

    # создаем курсор
    cur = conn.cursor()

    # SQL-запрос для создания таблицы
    create_table_query = '''
    CREATE TABLE files (
        id SERIAL PRIMARY KEY,
        file_name VARCHAR(255),
        status VARCHAR(50),
        total_elements INT,
        processed_elements INT,
        result_file VARCHAR(255),
        created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
    )
    '''

    # выполнение запроса
    cur.execute(create_table_query)
    conn.commit()
    print("Table created successfully!")

    # закрываем соединение
    cur.close()
    conn.close()

except Exception as e:
    print(f"Error connect to DB: {e}")
