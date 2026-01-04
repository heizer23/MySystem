import psycopg2
from flask import current_app

import os

def get_db_connection():
    return psycopg2.connect(
        host=os.environ.get('POSTGRES_HOST', 'postgres'),
        database=os.environ.get('POSTGRES_DB', 'homeserver'),
        user=os.environ.get('POSTGRES_USER', 'homeserver'),
        password=os.environ.get('POSTGRES_PASSWORD', 'homeserver_secret')
    )

def get_tables():
    """Returns list of table names in the 'app' schema."""
    conn = get_db_connection()
    cur = conn.cursor()
    cur.execute("""
        SELECT table_name 
        FROM information_schema.tables 
        WHERE table_schema = 'app' 
        AND table_type = 'BASE TABLE';
    """)
    tables = [row[0] for row in cur.fetchall()]
    cur.close()
    conn.close()
    return tables

def get_table_details(table_name):
    """Returns columns and primary key info for a given table in 'app' schema."""
    conn = get_db_connection()
    cur = conn.cursor()
    
    # 1. Get columns basic info
    cur.execute("""
        SELECT column_name, data_type, is_nullable, column_default
        FROM information_schema.columns
        WHERE table_schema = 'app'
        AND table_name = %s
        ORDER BY ordinal_position;
    """, (table_name,))
    columns = []
    for row in cur.fetchall():
        columns.append({
            "name": row[0],
            "type": row[1],
            "nullable": row[2] == 'YES',
            "default": row[3],
            "is_pk": False  # Will update below
        })

    # 2. Identify Primary Key
    cur.execute("""
        SELECT kcu.column_name
        FROM information_schema.table_constraints tc
        JOIN information_schema.key_column_usage kcu
          ON tc.constraint_name = kcu.constraint_name
          AND tc.table_schema = kcu.table_schema
        WHERE tc.constraint_type = 'PRIMARY KEY'
        AND tc.table_schema = 'app'
        AND tc.table_name = %s;
    """, (table_name,))
    pks = {row[0] for row in cur.fetchall()}

    for col in columns:
        if col["name"] in pks:
            col["is_pk"] = True

    cur.close()
    conn.close()
    return columns
