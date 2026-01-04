# HomeServer

A minimal, object-first data system running on Docker Compose. 
Define objects via natural language (simulated) or creating tables manually, and the system automatically generates a UI and API for them.

## Features
- **Object-First**: Data objects defined strictly by PostgreSQL tables in the `app` schema.
- **Introspection**: UI and API automatically discover tables and columns.
- **Strict DDL**: Safety guard allowing only clean `CREATE TABLE` statements with Primary Keys.
- **JSON API**: Full access to objects and records via `/api`.
- **Portable**: Runs anywhere Docker runs (amd64/arm64).

## Setup

1.  **Prerequisites**: Docker Desktop installed and running.
2.  **Configuration**:
    -   Copy `.env.example` to `.env`.
    -   (Optional) Set `LLM_API_KEY` in `.env` if you have one (or leave as stub).
3.  **Start**:
    ```bash
    docker compose up -d --build
    ```
4.  **Access**:
    -   Web UI: [http://localhost:5000](http://localhost:5000)
    -   API: [http://localhost:5000/api/objects](http://localhost:5000/api/objects)

## Usage

### Creating an Object
1.  Go to **New Object**.
2.  Type a prompt (e.g., "Create a Book table with title and author").
    -   *Note: Without a valid LLM key, this uses a mock generator.*
3.  The system executes the SQL (if safe) and the object appears in the list.

### Managing Records
-   Click an object to view records.
-   Click "Add Record" to use the dynamically generated form.

## Architecture
-   **App**: Python (Flask) service.
-   **Database**: PostgreSQL 15.
-   **Persistence**: Data stored in `postgres_data` volume.
