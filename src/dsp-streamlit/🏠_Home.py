from typing import Union

import pandas as pd
import polars as pl
import streamlit as st
from custom.database import connect_to_duckdb, get_table_as_dataframe, get_table_info
from pygwalker.api.streamlit import StreamlitRenderer, init_streamlit_comm

# Page Styling
st.set_page_config(layout="wide")


# Establish communication between pygwalker and streamlit
init_streamlit_comm()


def main():
    # Adding a sidebar for navigation
    page = st.sidebar.selectbox("Choose a page", ["View Tables", "Usage Guide"])

    # Path to your DuckDB file
    db_file_path = "data_systems_project.duckdb"

    if page == "View Tables":
        view_tables_page(db_file_path)
    elif page == "Usage Guide":
        usage_guide_page()


@st.cache_resource
def get_pyg_renderer(df: Union[pl.DataFrame, pd.DataFrame]) -> StreamlitRenderer:
    # When you need to publish your app to the public, you should set the debug parameter to False to prevent other users from writing to your chart configuration file.
    return StreamlitRenderer(df, spec="./gw_config.json", debug=False)


def showcase_data(df: Union[pl.DataFrame, pd.DataFrame]):
    st.header("Use PyGWalker To Analyze data")
    renderer = get_pyg_renderer(df)
    # Render your data exploration interface. Developers can use it to build charts by drag and drop.
    renderer.render_explore(width=1600)


def view_tables_page(db_file_path):
    st.title("DuckDB Tables Viewer")
    conn = connect_to_duckdb(db_file_path)

    if conn:
        # Get table information
        table_info = get_table_info(conn)

        if not table_info.empty:
            st.write("Table Information:")
            st.dataframe(table_info, use_container_width=True)

            # Create two columns for input
            col1, col2 = st.columns(2)

            with col1:
                schema = st.selectbox(
                    "Select a schema!",
                    tuple(table_info["schema"].unique()),
                    index=None,
                    placeholder="Select schema...",
                )

                st.write("You selected:", schema)

            with col2:
                # Filter to only retrieve the right schema and (table)name combinations
                df = table_info[table_info["schema"] == schema]

                table = st.selectbox(
                    "Select a table!",
                    tuple(df["name"].unique()),
                    index=None,
                    placeholder="Select table...",
                )
                st.write("You selected:", table)

            if schema and table:
                df = get_table_as_dataframe(conn, schema, table)
                if not df.empty:  # Changed this line
                    showcase_data(df)

                else:
                    st.write(
                        "⚠️ - No table information found in the database.",
                    )

        conn.close()


def usage_guide_page():
    st.title("Usage Guide for Retrieving Data")

    st.markdown(
        """
    ## How to Retrieve Data
    To retrieve data from a specific table in the DuckDB database, use the `get_table_as_dataframe` function.

    ### Function Syntax
    ```python
    get_table_as_dataframe(connection, schema, table_name)
    ```

    - `connection`: A connection object to the DuckDB database, obtained via the `connect_to_duckdb` function.
    - `schema`: The schema name used for the table.
    - `table_name`: The name of the table you want to retrieve data from.

    ### Example
    ```python
    conn = connect_to_duckdb("../data-system-project.db")
    if conn:
        data_frame = get_table_as_dataframe(conn, <schema>, <your_table_name>)
        print(data_frame)
        conn.close()
    ```

    ### Important Notes
    - Ensure that the table name is correct and exists in the database.
    - Always close the connection after you are done retrieving data to avoid database locking issues.
    - Handle exceptions properly to catch and understand any errors during database operations.

    ## Common Pitfalls
    - Forgetting to close the database connection.
    - Misspelling the table name.
    - Not handling exceptions, which may lead to a lack of understanding of what went wrong during the operation.
    """
    )


if __name__ == "__main__":
    main()


# Sample Data
# areas = ["Area 1", "Area 2", "Area 3", "Area 4"]
# incident_counts = df["Date"].value_counts()
# dates_with_20_or_more_incidents = incident_counts[incident_counts >= 20].index
# times = [f"{hour}:00" for hour in range(24)]

# # Title
# st.title("Storm Damage Prediction Dashboard")

# # Storm Selection
# st.sidebar.header("Previous Storms")
# selected_storm = st.sidebar.selectbox(
#     "Select a Previous Storm", dates_with_20_or_more_incidents
# )
# df_selected_storm = df[(df["Date"] == selected_storm)]
# # st.write(df_selected_storm)


# # Time Slider
# # st.sidebar.header("Time Slider")
# # selected_time = st.sidebar.selectbox("Select Time", times)

# # Date Selection
# st.sidebar.header("Date and Damage Type Selection")
# selected_date = st.sidebar.date_input("Select a Date", date.today())
# print(selected_date)

# # if len(selected_storm) > 0:
# #     selected_date = selected_storm
# # else:
# #     selected_date = selected_date

# # Map and Area Information
# col1, col2 = st.columns(2)

# # Map
# # col1.header("Amsterdam-Amstelland")

# available_damage_types = df["Damage_Type"].unique()
# damage_type = st.sidebar.multiselect("Select Damage Types", available_damage_types)
# print(damage_type)
# # # Sidebar to get user input
# # st.sidebar.header("Select Options")
# # selected_date = st.sidebar.date_input("Select Date", value=pd.to_datetime('today'))


# def display_map(df, date, damage_type):
#     # Filter the DataFrame based on selected date and damage type
#     if len(damage_type) < 2:
#         df_filtered = df[(df["Date"] == date) & (df["Damage_Type"] == damage_type[0])]
#     else:
#         df_filtered = df[(df["Date"] == date) & (df["Damage_Type"].isin(damage_type))]

#     # Create a Scatter Mapbox to display the incidents
#     fig = px.scatter_mapbox(
#         df_filtered,
#         lat="LAT",
#         lon="LON",
#         color="Damage_Type",
#         size_max=15,
#         zoom=10,
#         mapbox_style="carto-positron",
#     )

#     # Show plot in Streamlit
#     st.plotly_chart(fig, use_container_width=True)


# if len(damage_type) > 0:
#     display_map(df, selected_date.strftime("%Y-%m-%d"), damage_type)
# else:
#     df3 = df[(df["Date"] == selected_date.strftime("%Y-%m-%d"))]
#     # Load the GeoJSON file
#     with open("converted_serviceareas.geojson") as f:
#         geojson_data = json.load(f)

#     # Create the Choropleth map with Plotly
#     fig = px.choropleth_mapbox(
#         df3,
#         geojson=geojson_data,
#         locations="id",
#         featureidkey="properties.id",
#         color="your_color_column",  # Replace with the column you want to base the color on
#         mapbox_style="carto-positron",
#         zoom=10,
#         center={"lat": 52.360001, "lon": 4.885278},
#         opacity=0.5,
#     )

#     # Add scatter points on the map for each data point in df3
#     fig.add_trace(px.scatter_mapbox(df3, lat="LAT", lon="LON", size_max=15).data[0])

#     # Show plot in Streamlit
#     st.plotly_chart(fig, use_container_width=True)

# # Filter the DataFrame for the selected date
# df_filtered = df[df["Date"] == selected_date.strftime("%Y-%m-%d")]

# # Check if df_filtered is empty
# if df_filtered.empty:
#     st.write("No data available for the selected date.")
# else:
#     # Extract hour from Incident_Starttime
#     df_filtered["Hour"] = pd.to_datetime(df_filtered["Incident_Starttime"]).dt.hour

#     # Create a summary DataFrame with counts of each damage type per hour
#     hourly_damage = (
#         df_filtered.groupby(["Hour", "Damage_Type"])
#         .size()
#         .reset_index(name="Incidents")
#     )

#     # Create a DataFrame for total incidents per hour
#     total_incidents = hourly_damage.groupby("Hour")["Incidents"].sum().reset_index()

#     # Ensure all hours (0-23) are present
#     all_hours = pd.DataFrame({"Hour": range(24)})
#     total_incidents = all_hours.merge(total_incidents, on="Hour", how="left").fillna(0)

#     # Prepare hover text which excludes damage types with 0 incidents
#     hover_text = []
#     for hour in all_hours["Hour"]:
#         hour_data = hourly_damage[hourly_damage["Hour"] == hour]
#         hover_info = "<br>".join(
#             [
#                 f"- {row['Damage_Type']}: {int(row['Incidents'])}"
#                 for i, row in hour_data.iterrows()
#                 if row["Incidents"] > 0
#             ]
#         )
#         hover_text.append(
#             f"Damage type(s):<br>{hover_info}" if hover_info else "No incidents"
#         )

#     # Create the bar plot
#     fig = go.Figure(
#         data=[
#             go.Bar(
#                 x=total_incidents["Hour"],
#                 y=total_incidents["Incidents"],
#                 hovertext=hover_text,
#                 hoverinfo="text",
#                 marker=dict(color="blue"),
#             )
#         ]
#     )

#     # Update the layout to match your dashboard style and display all hours
#     fig.update_layout(
#         title="Number of Incidents per Hour",
#         xaxis=dict(title="Hour of the Day", tickmode="linear", tick0=0, dtick=1),
#         yaxis=dict(title="Number of Incidents"),
#         plot_bgcolor="#F5F2F0",
#         paper_bgcolor="#F5F2F0",
#         bargap=0.1,  # Adjust the gap between bars if needed
#     )

#     # Show plot in Streamlit
#     st.plotly_chart(fig, use_container_width=True)

# # map_data = pd.DataFrame({'lat': [52.349876235310184], 'lon': [4.914844775990842]})
# # st.map(map_data)

# # st.write(df.shape)
# # st.write(df.head())
# # st.write(df.columns)
# # st.write(len(df))
