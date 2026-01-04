import psycopg2
import os
import re

def get_db_connection():
    return psycopg2.connect(
        host=os.environ['POSTGRES_HOST'],
        database=os.environ['POSTGRES_DB'],
        user=os.environ['POSTGRES_USER'],
        password=os.environ['POSTGRES_PASSWORD']
    )

def init_db():
    """Idempotent initialization of schemas and audit table."""
    conn = get_db_connection()
    cur = conn.cursor()
    
    # Create schemas
    cur.execute("CREATE SCHEMA IF NOT EXISTS app;")
    cur.execute("CREATE SCHEMA IF NOT EXISTS internal;")
    
    # Create Audit Table
    cur.execute("""
        CREATE TABLE IF NOT EXISTS internal.ddl_audit (
            id SERIAL PRIMARY KEY,
            timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            sql_text TEXT NOT NULL,
            success BOOLEAN DEFAULT FALSE,
            error_message TEXT
        );
    """)
    
    conn.commit()
    cur.close()
    conn.close()

def validate_ddl(sql: str):
    """
    Strictly validates that the SQL is a single CREATE TABLE statement 
    in the 'app' schema and contains a Primary Key.
    """
    # 1. Basic cleanup
    sql = sql.strip()
    
    # 2. Check for multiple statements (semicolon check)
    # This is a naive check; assumes no semicolons in literals/comments for MVP.
    # A real parser would be better, but regex is acceptable for this constrained scope.
    if sql.count(';') > 1:
        # Allow one trailing semicolon
        if sql.count(';') == 1 and not sql.endswith(';'):
             raise ValueError("Multiple statements detected. Only one Statement allowed.")
        if sql.count(';') > 1: # More than one usually means multiple statements
             pass # heuristic, let's just ban inner semicolons for now for strict MVP
    
    statements = [s for s in sql.split(';') if s.strip()]
    if len(statements) != 1:
        raise ValueError("Must be exactly one statement.")

    statement = statements[0].strip()
    
    # 3. Check generic structure: CREATE TABLE app.<something>
    # Case insensitive match for CREATE TABLE
    match = re.match(r'^CREATE\s+TABLE\s+app\.([a-zA-Z0-9_]+)', statement, re.IGNORECASE)
    if not match:
        raise ValueError("Statement must start with 'CREATE TABLE app.<name>'")
        
    # 4. Check forbidden keywords (DROP, TRUNCATE, GRANT, etc) - simple safeguards
    forbidden = ['DROP ', 'TRUNCATE ', 'GRANT ', 'REVOKE ', 'ALTER ', 'COPY ', 'INSERT ', 'UPDATE ', 'DELETE ']
    upper_sql = statement.upper()
    for verb in forbidden:
        if verb in upper_sql:
            raise ValueError(f"Operation '{verb.strip()}' is not allowed.")

    # 5. Check for Primary Key
    if 'PRIMARY KEY' not in upper_sql:
        raise ValueError("Table definition must include a PRIMARY KEY.")
        
    return statement

def execute_ddl(sql: str):
    """Validates and executes DDL, logging result to audit table."""
    conn = get_db_connection()
    cur = conn.cursor()
    
    success = False
    error_msg = None
    
    try:
        # Validate
        clean_sql = validate_ddl(sql)
        
        # Execute DDL
        cur.execute(clean_sql)
        success = True
        
    except Exception as e:
        error_msg = str(e)
        raise e # Re-raise to let caller know
    finally:
        # Audit Log (in separate transaction block or same? strictly same if we want atomic, 
        # but for DDL in Postgres, DDL is transactional. 
        # However, if DDL fails, we still want to log the failure. 
        # So we rollback the DDL transaction and then insert audit log.)
        
        if success:
             conn.commit()
             # Log success
             try:
                 audit_conn = get_db_connection()
                 audit_cur = audit_conn.cursor()
                 audit_cur.execute("INSERT INTO internal.ddl_audit (sql_text, success) VALUES (%s, %s)", (sql, True))
                 audit_conn.commit()
                 audit_conn.close()
             except:
                 pass # Don't fail main flow if audit fails on success logging?
                 
        else:
             conn.rollback() # Rollback the failed DDL
             # Log Error
             try:
                 audit_conn = get_db_connection()
                 audit_cur = audit_conn.cursor()
                 audit_cur.execute("INSERT INTO internal.ddl_audit (sql_text, success, error_message) VALUES (%s, %s, %s)", (sql, False, error_msg))
                 audit_conn.commit()
                 audit_conn.close()
             except Exception as audit_e:
                 print(f"Failed to write audit log: {audit_e}")

    cur.close()
    conn.close()
