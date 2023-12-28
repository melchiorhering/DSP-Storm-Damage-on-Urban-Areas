import duckdb
import streamlit as st
import pandas as pd


# Function to connect to DuckDB
def connect_to_duckdb(file_path):
    try:
        # Establishing the connection
        conn = duckdb.connect(database=file_path, read_only=False)
        return conn
    except Exception as e:
        st.error(f"Error connecting to DuckDB: {e}")
        return None


# Function to get table information
def get_table_info(conn):
    try:
        query = "SHOW ALL TABLES;"
        return conn.execute(query).df()
    except Exception as e:
        st.error(f"Error fetching table info: {e}")
        return pd.DataFrame()
