import duckdb
import streamlit as st


# Function to connect to DuckDB
def connect_to_duckdb(file_path):
    try:
        # Establishing the connection
        conn = duckdb.connect(database=file_path, read_only=False)
        return conn
    except Exception as e:
        st.error(f"Error connecting to DuckDB: {e}")
        return None

# Function to get the list of tables
def get_tables(conn):
    try:
        query = "SELECT table_name FROM information_schema.tables WHERE table_schema = 'main'"
        return conn.execute(query).fetchall()
    except Exception as e:
        st.error(f"Error fetching table list: {e}")
        return []