import psycopg2
import os

def get_db_connection():
    return psycopg2.connect(
        host=os.environ['POSTGRES_HOST'],
        database=os.environ['POSTGRES_DB'],
        user=os.environ['POSTGRES_USER'],
        password=os.environ['POSTGRES_PASSWORD']
    )

def list_records(table_name):
    """Generic SELECT * from app.table"""
    # Verify table exists in 'app' schema to prevent injection
    # (Though main.py checks introspection, double check here or trust caller? Trust caller for MVP speed, 
    # but use SQL parameters for values. Table name cannot be parametrized in Psycopg2 directly without AsIs,
    # so we must validate it strictly.)
    
    # Strict validation of table_name
    from app.introspection import get_tables
    if table_name not in get_tables():
         raise ValueError("Table does not exist")

    conn = get_db_connection()
    cur = conn.cursor()
    cur.execute(f'SELECT * FROM app."{table_name}"') # Quote table name for safety
    rows = cur.fetchall()
    cur.close()
    conn.close()
    return rows

def create_record(table_name, data):
    """Generic INSERT into app.table"""
    from app.introspection import get_tables, get_table_details
    if table_name not in get_tables():
         raise ValueError("Table does not exist")
         
    # Filter data to match columns
    columns = get_table_details(table_name)
    col_names = {c['name'] for c in columns}
    
    clean_data = {k: v for k, v in data.items() if k in col_names and v}
    
    if not clean_data:
        raise ValueError("No valid data provided")

    cols = list(clean_data.keys())
    values = list(clean_data.values())
    
    # Construct SQL
    col_str = ", ".join([f'"{c}"' for c in cols])
    val_placeholders = ", ".join(["%s"] * len(values))
    sql = f'INSERT INTO app."{table_name}" ({col_str}) VALUES ({val_placeholders})'
    
    conn = get_db_connection()
    cur = conn.cursor()
    cur.execute(sql, values)
    conn.commit()
    cur.close()
    conn.close()
