import streamlit as st
from custom.database import connect_to_duckdb, get_table_as_dataframe

# Page Styling
st.set_page_config(layout="wide")


def main():
    # Path to your DuckDB file
    db_file_path = "../data-system-project.db"

    st.title("DuckDB Tables Viewer")
    conn = connect_to_duckdb(db_file_path)

    df = get_table_as_dataframe(
        conn=conn, schema="joined", table_name="incident_deployments_vehicles_weather"
    )

    # Show DF
    st.dataframe(df, use_container_width=True)


if __name__ == "__main__":
    main()
