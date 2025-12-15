import sqlite3
import csv
import os

# ⚙️ Paths
csv_path = r'C:\Users\juan_\Downloads\Libro1.csv'  # ← Change this
db_path = r'C:\Users\juan_\Downloads\basegeneral.sqlite'  # ← Shared network path

# 📁 Check if DB already exists
db_exists = os.path.exists(db_path)

# 🔌 Connect (will create the file if it doesn't exist)
conn = sqlite3.connect(db_path)
cursor = conn.cursor()

# 📖 Read CSV columns
with open(csv_path, newline='', encoding='utf-8') as file:
    reader = csv.reader(file, delimiter=';')
    columns = next(reader)  # Read header (field names)

    # 🏗️ Create table only if it doesn't exist
    if not db_exists:
        columns_sql = ', '.join([f'"{col.strip()}" TEXT' for col in columns])
        cursor.execute(f'CREATE TABLE IF NOT EXISTS datos ({columns_sql})')
        print("🧱 Table created.")

# 🔁 Insert data
with open(csv_path, newline='', encoding='utf-8') as file_2:
    lector = csv.DictReader(file_2, delimiter=';')
    for fila in lector:
        values = [fila.get(col, "") for col in columns]
        placeholders = ', '.join(['?'] * len(columns))
        cursor.execute(f'INSERT INTO datos VALUES ({placeholders})', values)

# 💾 Save and close
conn.commit()
conn.close()

print("✅ Database updated successfully.")