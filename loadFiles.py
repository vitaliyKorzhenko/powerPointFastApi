import json
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

    # Чтение данных из JSON файла
    with open('infoFile.json', 'r') as f:
        data = json.load(f)
        state_info = data.get('stateInfo', [])

        # Вставка данных по 100 записей
        batch_size = 100
        for i in range(0, len(state_info), batch_size):
            batch = state_info[i:i + batch_size]  # Получаем следующую партию из 100 элементов
            for item in batch:
                insert_query = '''
                INSERT INTO files (file_name, status, total_elements, processed_elements, result_file)
                VALUES (%s, %s, %s, %s, %s)
                '''
                cur.execute(insert_query, (
                    item['file_name'],
                    item['status'],
                    item['total_elements'],
                    item['processed_elements'],
                    item['result_file']
                ))
            conn.commit()  # Фиксация изменений после каждой партии
            print(f"Inserted {min(i + batch_size, len(state_info))} records successfully!")

    # закрываем соединение
    cur.close()
    conn.close()

except Exception as e:
    print(f"Error connect to DB: {e}")
