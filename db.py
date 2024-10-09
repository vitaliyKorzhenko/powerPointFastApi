import psycopg2
import urllib.parse as up

# External Database URL
database_url = "postgres://govorikapresentations_user:gf7LBmzPmQubL6flpA5CzcpDpFWmEi0Y@dpg-crnvgul6l47c73al9tpg-a.oregon-postgres.render.com:5432/govorikapresentations"

# url
url = up.urlparse(database_url)

# connect to db
try:
    conn = psycopg2.connect(
       dbname="python-db",  # название базы данных на RDS
       user="tskmd",  # имя пользователя
       password="nY9q+x4Hf6!-z",  # пароль
       host="python-db.c3btgkurkirn.eu-north-1.rds.amazonaws.com",  # хост RDS
       port="5432"  # порт
    )
    print("Successfully connected to the database!")

    # create cursor
    cur = conn.cursor()

    # SQL-create table
    create_table_query = '''
    CREATE TABLE files (
        id SERIAL PRIMARY KEY,
        file_name VARCHAR(255),
        status VARCHAR(50),
        total_elements INT,
        processed_elements INT,
        result_file VARCHAR(255),
        aws_status INT,
        created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
    )
    '''

    cur.execute(create_table_query)
    conn.commit()
    print("Table created successfully!")

    cur.close()
    conn.close()

except Exception as e:
    print(f"Error connect to DB: {e}")
