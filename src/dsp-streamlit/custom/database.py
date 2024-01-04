from typing import Optional
import duckdb
import streamlit as st
import pandas as pd


# Function to connect to DuckDB
def connect_to_duckdb(file_path) -> Optional[duckdb.DuckDBPyConnection]:
    try:
        # Establishing the connection
        conn = duckdb.connect(database=file_path, read_only=False)
        return conn
    except Exception as e:
        st.error(f"Error connecting to DuckDB: {e}")
        return None


# Function to get table information
def get_table_info(conn: duckdb.DuckDBPyConnection) -> pd.DataFrame:
    try:
        query = "SHOW ALL TABLES;"
        return conn.execute(query).df().drop(columns=["temporary", "database"])
    except Exception as e:
        st.error(f"Error fetching table info: {e}")
        return pd.DataFrame


# Function to get a table as DataFrame
def get_table_as_dataframe(
    conn: duckdb.DuckDBPyConnection, schema: str, table_name: str
) -> pd.DataFrame:
    try:
        print(schema, table_name)
        query = f"SELECT * FROM {schema}.{table_name};"
        return conn.execute(query).df()
    except Exception as e:
        st.error(f"Error fetching data from table {schema}.{table_name}: {e}")
        return None
