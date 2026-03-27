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
  Validates and executes SQL queries, returning structured results

- **CLI (`cli.py`)**  
  Interactive interface for loading data, inspecting schema, and running queries

## Testing

- **Unit tests** for each module  
- **Integration tests** for end-to-end CLI workflows (CSV → DB → query)

## Status

Working system for:
- CSV ingestion → SQLite  
- Schema-aware table management  
- Safe SQL validation and execution  
- CLI-based interaction  

Next: LLM adapter for natural language → SQL
