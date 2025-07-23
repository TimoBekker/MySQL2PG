import mysql.connector
import json
import os
from datetime import date, datetime

# Настройки подключения
config = {
    'user': '###',
    'password': '###',
    'host': '###',
}

# Создаем папку для хранения файлов, если её нет
output_dir = 'databases_json'
os.makedirs(output_dir, exist_ok=True)

# Подключение к MySQL
cnx = mysql.connector.connect(**config)
cursor = cnx.cursor()

# Получаем список всех баз данных
cursor.execute("SHOW DATABASES;")
databases = [db[0] for db in cursor.fetchall()]

def default_serializer(obj):
    if isinstance(obj, (date, datetime)):
        return obj.isoformat()
    raise TypeError(f"Object of type {type(obj)} is not JSON serializable")

for db in databases:
    # Игнорируем системные базы данных
    if db in ('information_schema', 'mysql', 'performance_schema', 'sys'):
        continue
    
    print(f"Обработка базы данных: {db}")
    
    # выбираем базу данных
    cursor.execute(f"USE {db};")
    
    # Получаем список таблиц
    cursor.execute("SHOW TABLES;")
    tables = [table[0] for table in cursor.fetchall()]
    
    db_data = {}
    
    for table in tables:
        # Получаем все строки таблицы
        cursor.execute(f"SELECT * FROM {table};")
        columns = [desc[0] for desc in cursor.description]
        rows = cursor.fetchall()
        # Формируем список словарей для каждой строки
        table_data = [dict(zip(columns, row)) for row in rows]
        db_data[table] = table_data
    
    # Сохраняем данные базы в JSON-файл
    file_path = os.path.join(output_dir, f"{db}.json")
    with open(file_path, 'w', encoding='utf-8') as f:
        json.dump(db_data, f, ensure_ascii=False, indent=4, default=default_serializer)

print("Все базы данных успешно экспортированы в папку:", output_dir)

# Закрываем соединение
cursor.close()
cnx.close()