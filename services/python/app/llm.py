import os
import requests
import json

def generate_ddl_from_prompt(prompt):
    """
    Generates a CREATE TABLE statement from a natural language prompt.
    Supports OpenAI (sk- prefix) and Gemini (Google API Key).
    """
    api_key = os.environ.get('LLM_API_KEY')
    
    # MOCK BEHAVIOR for Testing if no key
    if not api_key or api_key == 'change_me' or api_key.startswith('your_api_key'):
        words = prompt.split()
        table_name = "test_object"
        for i, w in enumerate(words):
            if w.lower() == 'table' and i > 0:
                 table_name = words[i-1].lower()
                 
        return f"""
        CREATE TABLE app.{table_name} (
            id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
            name TEXT NOT NULL,
            created_at TIMESTAMP DEFAULT NOW()
        );
        """

    try:
        if api_key.startswith('sk-'):
            # OpenAI Implementation
            url = "https://api.openai.com/v1/chat/completions"
            headers = {
                "Authorization": f"Bearer {api_key}",
                "Content-Type": "application/json"
            }
            payload = {
                "model": "gpt-4o-mini",
                "messages": [
                    {"role": "system", "content": "You are a PostgreSQL expert. Generate a single CREATE TABLE statement in the 'app' schema. Ensure it has a PRIMARY KEY. Return ONLY the SQL code, no markdown or explanation."},
                    {"role": "user", "content": prompt}
                ],
                "temperature": 0
            }
            response = requests.post(url, headers=headers, json=payload, timeout=10)
            response.raise_for_status()
            sql = response.json()['choices'][0]['message']['content'].strip()
        else:
            # Gemini Implementation
            url = f"https://generativelanguage.googleapis.com/v1beta/models/gemini-1.5-flash:generateContent?key={api_key}"
            headers = {'Content-Type': 'application/json'}
            payload = {
                "contents": [{
                    "parts": [{
                        "text": f"You are a PostgreSQL expert. Generate a single CREATE TABLE statement in the 'app' schema for the following request: '{prompt}'. Ensure it has a PRIMARY KEY. Return ONLY the SQL code, no markdown, no explanations."
                    }]
                }],
                "generationConfig": {
                    "temperature": 0
                }
            }
            response = requests.post(url, headers=headers, json=payload, timeout=10)
            response.raise_for_status()
            data = response.json()
            sql = data['candidates'][0]['content']['parts'][0]['text'].strip()

        # Clean up any markdown blocks
        if "```sql" in sql:
            sql = sql.split("```sql")[1].split("```")[0].strip()
        elif "```" in sql:
            sql = sql.split("```")[1].split("```")[0].strip()
            
        return sql

    except Exception as e:
        return f"-- Error calling LLM: {str(e)}"

