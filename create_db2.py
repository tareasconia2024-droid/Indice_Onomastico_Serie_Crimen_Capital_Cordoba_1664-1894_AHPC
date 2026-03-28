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
    if isinstance(val, float) and math.isnan(val):
        return None
    return str(val).strip()

def clean_int(val):
    if pd.isna(val):
        return None
    try:
        return int(float(val))
    except (ValueError, TypeError):
        return None

def find_data(file_path):
    xl = pd.ExcelFile(file_path)
    for sheet in xl.sheet_names:
        df_test = pd.read_excel(xl, sheet_name=sheet, header=None, nrows=20)
        for idx, row in df_test.iterrows():
            # Check if this row contains something like "ORDEN" or "COMPLETO"
            row_str = " ".join([str(x).upper() for x in row.values])
            if "N° ORDEN" in row_str or "COMPLETO" in row_str or "ACTO" in row_str:
                print(f"Found headers in sheet '{sheet}' at row {idx}")
                return sheet, idx
    return None, None

def main():
    print(f"Reading excel file: {file_path}")
    sheet, header_row = find_data(file_path)
    
    if sheet is None:
        print("Could not find the correct headers in any sheet!")
        sys.exit(1)
        
    df = pd.read_excel(file_path, sheet_name=sheet, header=header_row)
    print(f"Read {len(df)} rows. Columns: {list(df.columns)[:5]}...")
    
    # ... (rest of the db creation code)
    print("Creating SQLite database...")
    os.makedirs(os.path.dirname(db_path), exist_ok=True)
    conn = sqlite3.connect(db_path)
    cursor = conn.cursor()

    cursor.execute("DROP TABLE IF EXISTS indice_crimen")
    cursor.execute("DROP TABLE IF EXISTS indice_crimen_fts")
    
    cursor.execute("""
    CREATE TABLE indice_crimen (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        numero_orden INTEGER,
        numero_completo TEXT,
        año INTEGER,
        mes TEXT,
        dia TEXT,
        tipo_acto TEXT,
        apellido TEXT,
        nombre TEXT,
        otros_datos TEXT,
        caratula_completa TEXT,
        fojas TEXT,
        signatura TEXT,
        observaciones TEXT
    )
    """)

    cursor.execute("""
    CREATE VIRTUAL TABLE indice_crimen_fts USING fts5(
        tipo_acto, apellido, nombre, otros_datos, caratula_completa, observaciones,
        content='indice_crimen', content_rowid='id'
    )
    """)

    cursor.executescript("""
    CREATE TRIGGER t_indice_crimen_ai AFTER INSERT ON indice_crimen BEGIN
        INSERT INTO indice_crimen_fts(rowid, tipo_acto, apellido, nombre, otros_datos, caratula_completa, observaciones)
        VALUES (new.id, new.tipo_acto, new.apellido, new.nombre, new.otros_datos, new.caratula_completa, new.observaciones);
    END;
    """)

    print("Inserting data into SQLite...")
    inserted_count = 0

    for idx, row in df.iterrows():
        try:
            def safe_get(idx):
                if idx < len(row):
                    return row.iloc[idx]
                return None
                
            col0 = safe_get(0)
            col6 = safe_get(6)
            col9 = safe_get(9)
            
            if pd.isna(col0) and pd.isna(col6) and pd.isna(col9):
                continue

            numero_orden = clean_int(safe_get(0))
            numero_completo = clean_val(safe_get(1))
            año = clean_int(safe_get(2))
            mes = clean_val(safe_get(3))
            dia = clean_val(safe_get(4))
            tipo_acto = clean_val(safe_get(5))
            apellido = clean_val(safe_get(6))
            nombre = clean_val(safe_get(7))
            otros_datos = clean_val(safe_get(8))
            caratula = clean_val(safe_get(9))
            fojas = clean_val(safe_get(10))
            signatura = clean_val(safe_get(11))
            observaciones = clean_val(safe_get(12))

            cursor.execute("""
                INSERT INTO indice_crimen (
                    numero_orden, numero_completo, año, mes, dia, 
                    tipo_acto, apellido, nombre, otros_datos, 
                    caratula_completa, fojas, signatura, observaciones
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """, (
                numero_orden, numero_completo, año, mes, dia,
                tipo_acto, apellido, nombre, otros_datos,
                caratula, fojas, signatura, observaciones
            ))
            inserted_count += 1
        except Exception as e:
            print(f"Error at index {idx}: {e}")

    conn.commit()
    conn.close()
    print(f"Successfully inserted {inserted_count} records into the database.")

if __name__ == '__main__':
    main()
