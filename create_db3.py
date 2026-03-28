import pandas as pd
import sqlite3
import math
import sys
import os

file_path = r"c:\Users\Estudiante\Documents\APLICACIONES WEB\1_AHPC_PJ_Escribania_Crimen_Capital_1664a1894_Inventario_2025.xls"
db_path = r"c:\Users\Estudiante\Documents\APLICACIONES WEB\ahpc_crimen_app\ahpc_crimen.db"

def clean_val(val):
    if pd.isna(val):
        return None
    return str(val).strip()

def clean_int(val):
    if pd.isna(val) or val == 'nan':
        return None
    try:
        return int(float(val))
    except (ValueError, TypeError):
        return str(val).strip() # Sometimes it's a string

def main():
    print("Reading excel...")
    df = pd.read_excel(file_path, header=4)
    
    os.makedirs(os.path.dirname(db_path), exist_ok=True)
    conn = sqlite3.connect(db_path)
    cursor = conn.cursor()

    cursor.execute("DROP TABLE IF EXISTS indice_crimen")
    cursor.execute("DROP TABLE IF EXISTS indice_crimen_fts")
    
    cursor.execute("""
    CREATE TABLE indice_crimen (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        inventario TEXT,
        fondo TEXT,
        subfondo TEXT,
        serie TEXT,
        año TEXT,
        legajo TEXT,
        expediente TEXT,
        partes TEXT,
        causa TEXT
    )
    """)

    cursor.execute("""
    CREATE VIRTUAL TABLE indice_crimen_fts USING fts5(
        partes, causa, año,
        content='indice_crimen', content_rowid='id'
    )
    """)

    cursor.executescript("""
    CREATE TRIGGER t_indice_crimen_ai AFTER INSERT ON indice_crimen BEGIN
        INSERT INTO indice_crimen_fts(rowid, partes, causa, año)
        VALUES (new.id, new.partes, new.causa, new.año);
    END;
    """)

    inserted = 0
    for idx, row in df.iterrows():
        try:
            val_partes = clean_val(row.iloc[7])
            val_causa = clean_val(row.iloc[8])
            
            if not val_partes and not val_causa:
                continue

            inv = clean_val(row.iloc[0])
            fondo = clean_val(row.iloc[1])
            sub = clean_val(row.iloc[2])
            serie = clean_val(row.iloc[3])
            año = clean_int(row.iloc[4])
            leg = clean_val(row.iloc[5])
            exp = clean_val(row.iloc[6])

            cursor.execute("""
                INSERT INTO indice_crimen (
                    inventario, fondo, subfondo, serie, año, legajo, expediente, partes, causa
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
            """, (inv, fondo, sub, serie, año, leg, exp, val_partes, val_causa))
            inserted += 1
        except Exception as e:
            pass

    conn.commit()
    conn.close()
    print(f"Inserted {inserted} records")

if __name__ == '__main__':
    main()
