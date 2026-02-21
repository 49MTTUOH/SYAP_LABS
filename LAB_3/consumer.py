import json
from kafka import KafkaConsumer
import psycopg2
from psycopg2 import sql
from psycopg2.extensions import ISOLATION_LEVEL_AUTOCOMMIT

consumer = KafkaConsumer(
    'etl-topic',
    bootstrap_servers=['localhost:9092'],
    value_deserializer=lambda v: json.loads(v.decode('utf-8'))
)

DB_HOST = "localhost"
DB_PORT = 5432
DB_NAME = "kafkaDB"
DB_USER = "postgres"
DB_PASSWORD = "123456"

try:
    conn = psycopg2.connect(dbname='postgres', user=DB_USER, password=DB_PASSWORD,
                            host=DB_HOST, port=DB_PORT)
    conn.set_isolation_level(ISOLATION_LEVEL_AUTOCOMMIT)
    cur = conn.cursor()

    cur.execute(f"SELECT 1 FROM pg_catalog.pg_database WHERE datname = '{DB_NAME}';")
    exists = cur.fetchone()
    if not exists:
        cur.execute(sql.SQL("CREATE DATABASE {}").format(sql.Identifier(DB_NAME)))
        print(f"[INFO] База '{DB_NAME}' создана.")
    else:
        print(f"[INFO] База '{DB_NAME}' уже существует.")
    cur.close()
    conn.close()
except Exception as e:
    print(f"[ERROR] Ошибка при создании базы: {e}")
    exit(1)

try:
    conn = psycopg2.connect(dbname=DB_NAME, user=DB_USER, password=DB_PASSWORD,host=DB_HOST, port=DB_PORT)
    cursor = conn.cursor()
    print(f"[INFO] Подключено к базе '{DB_NAME}'.")
except Exception as e:
    print(f"[ERROR] Ошибка подключения к PostgreSQL: {e}")
    exit(1)

def create_table_if_not_exists(table_name, columns):
    col_defs = ", ".join([f"{col['name']} {col['type'].upper()}" for col in columns])
    query = sql.SQL("CREATE TABLE IF NOT EXISTS {} ({})").format(
        sql.Identifier(table_name),
        sql.SQL(col_defs)
    )
    cursor.execute(query)
    conn.commit()
    print(f"[INFO] Таблица '{table_name}' готова (создана или уже существует).")

def insert_data(table_name, columns, rows):
    col_names = [col['name'] for col in columns]
    placeholders = ", ".join(["%s"] * len(col_names))
    insert_query = sql.SQL("INSERT INTO {} ({}) VALUES ({})").format(
        sql.Identifier(table_name),
        sql.SQL(", ").join(map(sql.Identifier, col_names)),
        sql.SQL(placeholders)
    )
    for row in rows:
        cursor.execute(insert_query, row)
    conn.commit()
    print(f"[INFO] В таблицу '{table_name}' вставлено {len(rows)} строк.")
    if len(rows) > 0:
        print(f"[DEBUG] Примеры данных: {rows[:3]}{'...' if len(rows) > 3 else ''}")

print("[INFO] Ожидание сообщений от Kafka...")

for message in consumer:
    data = message.value
    table_name = data.get('table_name', 'untitled')
    columns = data.get('columns', [])
    rows = data.get('rows', [])

    if not columns or not rows:
        print(f"[WARNING] Получено сообщение без столбцов или строк для таблицы '{table_name}' => пропускаем.")
        continue

    for col in columns:
        if col['type'] == 'int':
            col['type'] = 'INTEGER'
        elif col['type'] == 'float':
            col['type'] = 'REAL'
        elif col['type'] == 'bool':
            col['type'] = 'BOOLEAN'
        else:
            col['type'] = 'TEXT'

    try:
        create_table_if_not_exists(table_name, columns)
        insert_data(table_name, columns, rows)
        print(f"[SUCCESS] Данные из '{table_name}' успешно добавлены в PostgreSQL.\n")
    except Exception as e:
        print(f"[ERROR] Ошибка при добавлении данных в таблицу '{table_name}': {e}\n")