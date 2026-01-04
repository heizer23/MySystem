import os
from flask import Flask, render_template, request, redirect, url_for, jsonify
from app import ddl, introspection, crud, llm

app = Flask(__name__)

# --- Lifecycle ---
with app.app_context():
    # Initialize DB (Schemas, Audit Table)
    # Wrap in try/except in case DB isn't ready during build phase, 
    # though Docker Compose healthcheck should prevent this in runtime.
    try:
        ddl.init_db()
        print("Database initialized successfully.")
    except Exception as e:
        print(f"Warning: Database initialization failed: {e}")

# --- UI Routes ---

@app.route('/')
def index():
    tables = introspection.get_tables()
    return render_template('list_objects.html', tables=tables)

@app.route('/create-object', methods=['GET', 'POST'])
def create_object_ui():
    error = None
    if request.method == 'POST':
        prompt = request.form.get('prompt')
        sql = llm.generate_ddl_from_prompt(prompt)
        try:
            ddl.execute_ddl(sql)
            return redirect(url_for('index'))
        except Exception as e:
            error = f"DDL Execution Failed: {str(e)}"
    
    return render_template('create_object.html', error=error)

@app.route('/object/<table>')
def view_object_ui(table):
    try:
        columns = introspection.get_table_details(table)
        rows = crud.list_records(table)
        return render_template('view_object.html', table_name=table, columns=columns, rows=rows)
    except Exception as e:
        return f"Error: {e}", 404

@app.route('/object/<table>/create', methods=['GET', 'POST'])
def create_record_ui(table):
    try:
        columns = introspection.get_table_details(table)
        if request.method == 'POST':
            # Collect data from form
            data = request.form.to_dict()
            crud.create_record(table, data)
            return redirect(url_for('view_object_ui', table=table))
            
        return render_template('create_record.html', table_name=table, columns=columns)
    except Exception as e:
        return f"Error: {e}", 400

# --- JSON API Routes ---

@app.route('/api/objects', methods=['GET'])
def api_list_objects():
    return jsonify(introspection.get_tables())

@app.route('/api/objects/<table>', methods=['GET'])
def api_object_details(table):
    try:
        return jsonify(introspection.get_table_details(table))
    except:
        return jsonify({"error": "Table not found"}), 404

@app.route('/api/objects/<table>/records', methods=['GET'])
def api_list_records(table):
    try:
        rows = crud.list_records(table)
        return jsonify(rows)
    except Exception as e:
        return jsonify({"error": str(e)}), 400

@app.route('/api/objects/<table>/records', methods=['POST'])
def api_create_record(table):
    try:
        data = request.json
        crud.create_record(table, data)
        return jsonify({"status": "success"}), 201
    except Exception as e:
        return jsonify({"error": str(e)}), 400

@app.route('/health')
def health_check():
    return jsonify({"status": "healthy"})

if __name__ == '__main__':
    port = int(os.environ.get('PORT', 5000))
    app.run(host='0.0.0.0', port=port)
