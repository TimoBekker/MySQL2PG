import os
import json
import psycopg2
from psycopg2 import sql

# Настройки подключения к PostgreSQL
pg_config = {
    'host': 'localhost',
    'user': '###',
    'password': '###',
    'dbname': '###'
}

# Папка с JSON файлами
json_dir = 'databases_json'

# Создаем подключение
conn = psycopg2.connect(**pg_config)
cur = conn.cursor()

def create_table_if_not_exists(table_name, columns):
    """
    Создает таблицу, если она еще не существует.
    columns — список названий колонок.
    Все имена колонок оборачиваются в двойные кавычки.
    """
    columns_with_types = ', '.join([f'"{col}" TEXT' for col in columns])
    create_query = sql.SQL("CREATE TABLE IF NOT EXISTS {} ({})").format(
        sql.Identifier(table_name),
        sql.SQL(columns_with_types)
    )
    cur.execute(create_query)
    conn.commit()

def clean_value(value):
    """
    Удаляет нулевые байты из строки.
    """
    if isinstance(value, str):
        return value.replace('\x00', '')
    return value

def insert_rows(table_name, columns, rows):
    """
    Вставляет строки в таблицу.
    """
    columns_identifiers = [sql.Identifier(col) for col in columns]
    placeholders = ', '.join(['%s'] * len(columns))
    insert_query = sql.SQL("INSERT INTO {} ({}) VALUES ({})").format(
        sql.Identifier(table_name),
        sql.SQL(', ').join(columns_identifiers),
        sql.SQL(placeholders)
    )
    for row in rows:
        values = [clean_value(row.get(col)) for col in columns]
        cur.execute(insert_query, values)
    conn.commit()

# Обрабатываем все файлы JSON в папке
for filename in os.listdir(json_dir):
    if filename.endswith('.json'):
        file_path = os.path.join(json_dir, filename)
        with open(file_path, 'r', encoding='utf-8') as f:
            db_json = json.load(f)
            # Предполагается структура: { "table": [ {row}, ... ], ... }
            for table, rows in db_json.items():
                if not rows:
                    continue  # пропускаем пустые таблицы
                columns = list(rows[0].keys())
                create_table_if_not_exists(table, columns)
                insert_rows(table, columns, rows)
                print(f"Импортировано таблица: {table} из файла {filename}")

# Закрываем соединение
cur.close()
conn.close()

print("Импорт завершен.")