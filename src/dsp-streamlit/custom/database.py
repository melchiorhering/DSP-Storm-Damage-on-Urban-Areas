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
    conn: duckdb.DuckDBPyConnection, table_name: str
) -> pd.DataFrame:
    try:
        query = f"SELECT * FROM {table_name};"
        return conn.execute(query).df()
    except Exception as e:
        st.error(f"Error fetching data from table {table_name}: {e}")
        return pd.DataFrame()
