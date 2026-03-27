# nl2sql

Modular system for loading and querying structured data with SQLite.

## Current Modules

- **CSV Loader (`ingestion/`)**  
  Loads CSV files into SQLite, infers schema, and handles create vs append logic

- **Schema Manager (`schema/`)**  
  Inspects database structure, retrieves table schemas, and compares schemas

- **SQL Validator (`validation/`)**  
  Ensures queries are safe (SELECT-only, valid tables/columns, no dangerous SQL)

  - **Query Service (`query/`)**  
  Validates and executes SQL queries, and now also routes natural language queries through the LLM adapter

- **CLI (`cli.py`)**  
  Interactive interface for loading data, inspecting schema, running SQL queries, and asking natural language questions

- **LLM Adapter (`llm/`)**  
  Converts natural language into SQL using LLM API endpoint, while keeping SQL validation and execution in the existing pipeline

## Testing

- **Unit tests** for each module  
- **Integration tests** for end-to-end CLI workflows (CSV → DB → query)

## Status

Working system for:
- CSV ingestion → SQLite
- Schema-aware table management
- Safe SQL validation and execution
- CLI-based interaction
- Natural language → SQL through LLM adapter
