import psycopg2
import os

def get_db_connection():
    return psycopg2.connect(
        host=os.environ['POSTGRES_HOST'],
        database=os.environ['POSTGRES_DB'],
        user=os.environ['POSTGRES_USER'],
        password=os.environ['POSTGRES_PASSWORD']
    )

def list_records(table_name, filters=None, sort_by=None, order='ASC'):
    """Generic SELECT * from app.table with optional filtering and sorting"""
    from app.introspection import get_tables, get_table_details
    if table_name not in get_tables():
         raise ValueError("Table does not exist")
    
    columns = [c['name'] for c in get_table_details(table_name)]
    
    query = f'SELECT * FROM app."{table_name}"'
    params = []
    
    # Filtering
    if filters:
        filter_clauses = []
        for col, val in filters.items():
            if col in columns:
                filter_clauses.append(f'"{col}" = %s')
                params.append(val)
        if filter_clauses:
            query += " WHERE " + " AND ".join(filter_clauses)
            
    # Sorting
    if sort_by and sort_by in columns:
        if order.upper() not in ['ASC', 'DESC']:
            order = 'ASC'
        query += f' ORDER BY "{sort_by}" {order}'

    conn = get_db_connection()
    cur = conn.cursor()
    cur.execute(query, params)
    rows = cur.fetchall()
    cur.close()
    conn.close()
    return rows

def get_record(table_name, record_id):
    """Retrieve a single record by its primary key."""
    from app.introspection import get_tables, get_table_details
    if table_name not in get_tables():
         raise ValueError("Table does not exist")
         
    details = get_table_details(table_name)
    pk_col = next((c['name'] for c in details if c['is_pk']), None)
    if not pk_col:
        raise ValueError("Table has no primary key")
        
    conn = get_db_connection()
    cur = conn.cursor()
    cur.execute(f'SELECT * FROM app."{table_name}" WHERE "{pk_col}" = %s', (record_id,))
    row = cur.fetchone()
    cur.close()
    conn.close()
    return row

def create_record(table_name, data):
    """Generic INSERT into app.table"""
    from app.introspection import get_tables, get_table_details
    if table_name not in get_tables():
         raise ValueError("Table does not exist")
         
    columns = get_table_details(table_name)
    col_names = {c['name'] for c in columns}
    
    clean_data = {k: v for k, v in data.items() if k in col_names and v is not None}
    
    if not clean_data:
        raise ValueError("No valid data provided")

    cols = list(clean_data.keys())
    values = list(clean_data.values())
    
    col_str = ", ".join([f'"{c}"' for c in cols])
    val_placeholders = ", ".join(["%s"] * len(values))
    sql = f'INSERT INTO app."{table_name}" ({col_str}) VALUES ({val_placeholders})'
    
    conn = get_db_connection()
    cur = conn.cursor()
    cur.execute(sql, values)
    conn.commit()
    cur.close()
    conn.close()

def update_record(table_name, record_id, data):
    """Generic UPDATE for app.table"""
    from app.introspection import get_tables, get_table_details
    if table_name not in get_tables():
         raise ValueError("Table does not exist")
         
    details = get_table_details(table_name)
    pk_col = next((c['name'] for c in details if c['is_pk']), None)
    col_names = {c['name'] for c in details}
    
    if not pk_col:
        raise ValueError("Table has no primary key")

    clean_data = {k: v for k, v in data.items() if k in col_names and k != pk_col}
    
    if not clean_data:
        return # Nothing to update
        
    set_clauses = [f'"{k}" = %s' for k in clean_data.keys()]
    values = list(clean_data.values())
    values.append(record_id)
    
    sql = f'UPDATE app."{table_name}" SET {", ".join(set_clauses)} WHERE "{pk_col}" = %s'
    
    conn = get_db_connection()
    cur = conn.cursor()
    cur.execute(sql, values)
    conn.commit()
    cur.close()
    conn.close()

def delete_record(table_name, record_id):
    """Generic DELETE from app.table"""
    from app.introspection import get_tables, get_table_details
    if table_name not in get_tables():
         raise ValueError("Table does not exist")
         
    details = get_table_details(table_name)
    pk_col = next((c['name'] for c in details if c['is_pk']), None)
    if not pk_col:
        raise ValueError("Table has no primary key")
        
    conn = get_db_connection()
    cur = conn.cursor()
    cur.execute(f'DELETE FROM app."{table_name}" WHERE "{pk_col}" = %s', (record_id,))
    conn.commit()
    cur.close()
    conn.close()

def duplicate_record(table_name, record_id):
    """Duplicate a record, excluding original PK and timestamps if applicable."""
    from app.introspection import get_tables, get_table_details
    if table_name not in get_tables():
         raise ValueError("Table does not exist")
         
    details = get_table_details(table_name)
    pk_col = next((c['name'] for c in details if c['is_pk']), None)
    
    row = get_record(table_name, record_id)
    if not row:
        raise ValueError("Original record not found")
        
    # Map row values back to columns
    # We need to filter out the PK and auto-generated fields
    col_names = [c['name'] for c in details]
    record_dict = dict(zip(col_names, row))
    
    # Fields to exclude: pk_col and any common auto-generated fields
    # For now, let's just exclude the PK and 'id' if named differently
    exclude = {pk_col, 'created_at', 'updated_at'}
    clean_data = {k: v for k, v in record_dict.items() if k not in exclude and v is not None}
    
    create_record(table_name, clean_data)
