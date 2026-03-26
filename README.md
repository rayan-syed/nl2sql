# nl2sql

Modular system for loading and querying structured data with SQLite.

## Current Modules

- **CSV Loader (`ingestion/`)**  
  Loads CSV files into SQLite, infers schema, and handles create vs append logic

- **Schema Manager (`schema/`)**  
  Inspects database structure, retrieves table schemas, and compares schemas

- **SQL Validator (`validation/`)**  
  Ensures queries are safe (SELECT-only, valid tables/columns, no dangerous SQL)

## Status

Working ingestion + schema management + basic query validation. More components (query service, CLI, LLM) to be added.
