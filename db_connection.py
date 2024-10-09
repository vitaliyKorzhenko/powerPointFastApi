import psycopg2

def get_connection():
    try:
        # Подключение к базе данных AWS RDS
        conn = psycopg2.connect(
            dbname="python-db",  # название базы данных на RDS
            user="tskmd",  # имя пользователя
            password="nY9q+x4Hf6!-z",  # пароль
            host="python-db.c3btgkurkirn.eu-north-1.rds.amazonaws.com",  # хост RDS
            port="5432"  # порт
        )
        print("Successfully connected to the database!")
        return conn
    except Exception as e:
        print(f"Error connecting to database: {e}")
        return None
    
get_connection()
