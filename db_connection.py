import psycopg2
import urllib.parse as up

# External Database URL
database_url = "postgres://govorikapresentations_user:gf7LBmzPmQubL6flpA5CzcpDpFWmEi0Y@dpg-crnvgul6l47c73al9tpg-a.oregon-postgres.render.com:5432/govorikapresentations"

# Парсинг URL
url = up.urlparse(database_url)

def get_connection():
    try:
        conn = psycopg2.connect(
            dbname=url.path[1:],  # убираем первый символ "/"
            user=url.username,
            password=url.password,
            host=url.hostname,
            port=url.port
        )
        print("Successfully connected to the database!")
        return conn
    except Exception as e:
        print(f"Error connecting to database: {e}")
        return None
