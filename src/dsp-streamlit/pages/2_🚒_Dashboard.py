import streamlit as st
import numpy as np
import pandas as pd
from datetime import date
from datetime import datetime
import matplotlib.pyplot as plt
import plotly.express as px
from shapely import wkt
import plotly.graph_objs as go
from shapely.geometry import Polygon
from PIL import Image
import geopandas as gpd
import geodatasets
import plotly.express as px
import plotly.graph_objects as go
from shapely.geometry import Point
import time
from custom.database import connect_to_duckdb, get_table_as_dataframe


# Set the page layout to wide
st.set_page_config(layout="wide")
# st.sidebar.image('brandweerlogorond.png', use_column_width=True)

# Data
db_file_path = "data_systems_project.duckdb"
conn = connect_to_duckdb(db_file_path)




df_service_areas = get_table_as_dataframe(
    conn=conn, schema="public", table_name="service_areas"
)

df_wijken = get_table_as_dataframe(
    conn=conn, schema="public", table_name="cbs_wijken"
)

df_buurten = get_table_as_dataframe(
    conn=conn, schema="public", table_name="cbs_buurten"
)




vertalingen = {'Tree': 'Bomen'
               , 'Building': 'Gebouwen'
               , 'Fence, Road signs, Scaffolding': 'Hekwerk, Verkeersborden, Steigers'
               , 'Unknown' : 'Onbekend'}


# Storm names 
# Creating a DataFrame for the storm data from 2007 and later
storm_data_2007_later = {
    "Jaar": [2007, 2013, 2013, 2015, 2016, 2017, 2018, 2018, 2020, 2020, 2020, 2021, 2021, 2021, 2022, 2022, 2022, 2022, 2023, 2023, 2023, 2023, 2023, 2023, 2024, 2024, 2024, 2024],
    "Datum": ["18 jan", "28 okt", "5 dec", "25 jul", "20 nov", "13 sep", "3 jan", "18 jan", "25 sep", "27 dec", "9 feb", "20 jan", "21 jan", "7 feb", "31 jan", "16 feb", "18 feb", "20 feb", "5 juli", "18 okt", "2 nov", "9 dec", "21 dec", "27 dec", "2 jan", "3 jan", "21 jan", "22 jan"],
    "Naam": ["", "", "", "", "", "", "", "", "Odetta", "Bella", "Ciara", "Christoph", "Christoph", "Darcy", "Corrie", "Dudley", "Eunice", "Franklin" , "Poly", "Babet", "Ciarán", "Elin", "Pia", "Gerrit", "Henk", "Henk", "Isha", "Isha"]
}

df_storms_2007_later_df = pd.DataFrame(storm_data_2007_later)

# Adjusting the function to account for the "juli" month
def convert_to_full_date_numeric(year, date_str):
    # Handle special cases with two dates (e.g., "26/27 dec")
    if '/' in date_str:
        date_str = date_str.split('/')[0] + ' ' + date_str.split(' ')[-1]

    # Convert Dutch month abbreviations to numeric format
    dutch_months = {
        "jan": "01", "feb": "02", "mrt": "03", "apr": "04", "mei": "05", "jun": "06",
        "jul": "07", "aug": "08", "sep": "09", "okt": "10", "nov": "11", "dec": "12", "juli": "07"
    }
    day, month_abbr = date_str.split(' ')
    month = dutch_months[month_abbr.lower()]

    # Construct the full date string
    full_date_str = f"{year}-{month}-{day}"

    return full_date_str

# Applying the function to each row in the DataFrame
df_storms_2007_later_df['Datum'] = df_storms_2007_later_df.apply(lambda row: convert_to_full_date_numeric(row['Jaar'], row['Datum']), axis=1)

# Dropping the 'Jaar' column as it's no longer needed
df_storm_dates = df_storms_2007_later_df.drop(columns=['Jaar'])

df_storm_dates['Datum'] = pd.to_datetime(df_storm_dates['Datum'], format="%Y-%m-%d")
df_storm_dates['Datum'] = df_storm_dates['Datum'].dt.date
df_storm_dates = df_storm_dates.sort_values(by='Datum', ascending=False)


# Title
st.title("Stormschade Amsterdam-Amstelland")

selectbox_options = df_storm_dates.apply(lambda row: f"{row['Datum']} ({row['Naam']})" if row['Naam'] else f"{row['Datum']}", axis=1).tolist()

# Storm Selection
st.sidebar.header("Storm")

# Initialize session state variables if they don't exist
if 'last_date_interaction' not in st.session_state:
    st.session_state['last_date_interaction'] = None
if 'selected_date_dropdown' not in st.session_state:
    st.session_state['selected_date_dropdown'] = None
if 'selected_date_calendar' not in st.session_state:
    st.session_state['selected_date_calendar'] = datetime.today().date()  # default to today's date

# Function to update session state for dropdown
def update_dropdown():
    st.session_state.last_date_interaction = 'dropdown'

# Function to update session state for calendar
def update_calendar():
    st.session_state.last_date_interaction = 'calendar'

# Dropdown for selecting date
selected_date_str = st.sidebar.selectbox(
    "Selecteer storm", selectbox_options, on_change=update_dropdown)
# Update the dropdown state after selection
st.session_state['selected_date_dropdown'] = selected_date_str

# Calendar for selecting date
selected_date_calendar = st.sidebar.date_input(
    "Selecteer datum", value=st.session_state['selected_date_calendar'], on_change=update_calendar)

# Determine the final date selection based on the last interaction
selected_storm = None
if st.session_state.last_date_interaction == 'dropdown':
    selected_storm = datetime.strptime(selected_date_str.split(' ')[0], "%Y-%m-%d").date()
elif st.session_state.last_date_interaction == 'calendar':
    selected_storm = selected_date_calendar
else:
    # Default to dropdown value if no interaction has occurred
    selected_storm = datetime.strptime(selected_date_str.split(' ')[0], "%Y-%m-%d").date()

st.sidebar.write(f"Geselecteerde datum: {selected_storm}")


if selected_storm.strftime("%Y-%m-%d") == "2024-01-22": #prediction file
    df_incidents = pd.read_csv('bigger_sample_output.csv')
    df_knmi = pd.read_csv('maandag.csv')

else: #history files
    df_incidents = get_table_as_dataframe(
    conn=conn, schema="cleaned", table_name="cleaned_storm_incidents"
    )

    df_knmi = get_table_as_dataframe(
        conn=conn, schema="cleaned", table_name="cleaned_knmi_weather_data"
    )

# Map View
map_view_levels = ['Incident', 'Verzorgingsgebied', 'Wijk', 'Buurt']
# Map Selection
st.sidebar.header("Map")
selected_view = st.sidebar.radio(
    "Selecteer regionaal niveau", map_view_levels
)

# Time Slider
st.sidebar.header("Simulatie")
selected_hour = st.sidebar.slider(
    "Selecteer uur", 0, 23, value=23
)  # Values will be 0 to 23

df_incidents['Damage_Type'] = df_incidents['Damage_Type'].map(vertalingen)

# Continue with your operations using the selected_hour
df_incidents["Hour"] = pd.to_datetime(
    df_incidents["Incident_Starttime"], format="%H:%M:%S"
).dt.hour

df_accumulated = df_incidents[
    (df_incidents["Date"] == selected_storm.strftime("%Y-%m-%d"))
    & (df_incidents["Hour"] <= selected_hour)
]

damage_type = df_incidents["Damage_Type"].unique()
df_knmi_selected = df_knmi[df_knmi["Date"] == selected_storm.strftime("%Y-%m-%d")]

# Convert df_serviceareas into gpd
df_service_areas["geometry"] = df_service_areas["geometry"].apply(wkt.loads)
gdf_service_areas = gpd.GeoDataFrame(df_service_areas, geometry="geometry")

# Set the CRS to EPSG:28992 for RD New (Rijksdriehoekstelsel)
gdf_service_areas.set_crs("EPSG:28992", inplace=True)

# Convert it to EPSG:4326
gdf_service_areas = gdf_service_areas.to_crs(epsg=4326)

# Convert df_wijken into gpd
df_wijken["geometry"] = df_wijken["geometry"].apply(wkt.loads)
gdf_wijken = gpd.GeoDataFrame(df_wijken, geometry="geometry")
gdf_wijken.drop(0, inplace=True)
gdf_wijken.reset_index(drop=True, inplace=True)

# Convert df_buurten into gpd
df_buurten["geometry"] = df_buurten["geometry"].apply(wkt.loads)
gdf_buurten = gpd.GeoDataFrame(df_buurten, geometry="geometry")
gdf_buurten.drop([0,1], inplace=True)
gdf_buurten.reset_index(drop=True, inplace=True)

gdf_incidents_accumulated = gpd.GeoDataFrame(df_accumulated, geometry=gpd.points_from_xy(df_accumulated.LON, df_accumulated.LAT))

# df for dynamic figures

joined_df_filtered_wijken = gpd.sjoin(gdf_incidents_accumulated, gdf_wijken, how="left", op="within")
joined_df_filtered_wijken_buurten = gpd.sjoin(gdf_incidents_accumulated, gdf_buurten, how="left", op="within")
joined_df_filtered_wijken_buurten_area = gpd.sjoin(gdf_incidents_accumulated, gdf_service_areas, how="left", op="within")

df_accumulated['Wijk'] = joined_df_filtered_wijken['wijknaam']
df_accumulated['Buurt'] = joined_df_filtered_wijken_buurten['buurtnaam']
df_accumulated['Verzorgingsgebied'] = joined_df_filtered_wijken_buurten_area['Verzorgingsgebied']
df_accumulated.fillna("Onbekend", inplace=True)

# df for static figures
df_filtered = df_incidents[df_incidents['Date'] == selected_storm.strftime('%Y-%m-%d')]
gdf_incidents_filtered = gpd.GeoDataFrame(df_filtered, geometry=gpd.points_from_xy(df_filtered.LON, df_filtered.LAT))

joined_df_filtered_wijken = gpd.sjoin(gdf_incidents_filtered, gdf_wijken, how="left", op="within")
joined_df_filtered_wijken_buurten = gpd.sjoin(gdf_incidents_filtered, gdf_buurten, how="left", op="within")
joined_df_filtered_wijken_buurten_area = gpd.sjoin(gdf_incidents_filtered, gdf_service_areas, how="left", op="within")


df_filtered['Wijk'] = joined_df_filtered_wijken['wijknaam']
df_filtered['Buurt'] = joined_df_filtered_wijken_buurten['buurtnaam']
df_filtered['Verzorgingsgebied'] = joined_df_filtered_wijken_buurten_area['Verzorgingsgebied']
df_filtered.fillna("Onbekend", inplace=True)
df_filtered['Hour'] = pd.to_datetime(df_filtered['Incident_Starttime']).dt.hour



# Function to display the map using Plotly in Streamlit
def display_map_plotly_streamlit(gdf_service_areas, gdf_wijken, gdf_buurten, df_accumulated, damage_type):
    # show_intensity = st.checkbox("Show Intensity of Service Areas")

    if selected_view == 'Verzorgingsgebied':
        incidents_per_area = df_accumulated.groupby("Verzorgingsgebied") \
            .size() \
            .reset_index(name="Total Incidents")

        # Merge the counts with the service areas geodataframe
        gdf_service_areas = pd.merge(
            gdf_service_areas,
            incidents_per_area,
            left_on="Verzorgingsgebied",
            right_on="Verzorgingsgebied",
            how="left"
        )

        # Replace NaN values with 0 for service areas without incidents
        gdf_service_areas["Total Incidents"].fillna(0, inplace=True)

        # Remove the now unnecessary 'Service_Area' column if it exists
        if 'Service_Area' in gdf_service_areas.columns:
            gdf_service_areas.drop("Service_Area", axis=1, inplace=True)

        # Get the geojson representation for plotting
        geojson = gdf_service_areas.geometry.__geo_interface__

        # Create a base map using Plotly
        fig = px.choropleth_mapbox(
            gdf_service_areas,
            geojson=geojson,
            locations=gdf_service_areas.index,
            color="Total Incidents",
            color_continuous_scale=px.colors.sequential.Reds,
            opacity=0.40,
            center={"lat": 52.320001, "lon": 4.87278},
            mapbox_style="carto-darkmatter",
            zoom=9.7,
            custom_data=[gdf_service_areas["Verzorgingsgebied"]],
            labels={"Total Incidents": "Aantal incidenten"}  # Set custom label for the legend
        )


        # Adjust the border color for polygons
        fig.update_traces(
            marker_line_width=2,
            marker_line_color="grey",
            hovertemplate="<b>%{customdata[0]}</b><extra></extra>",
        )

        # Hide the color bar
        fig.update_layout(coloraxis_showscale=True)
        # Adjust legend - make it smaller
        fig.update_layout(
            legend=dict(
                font=dict(size=8),  # Smaller font size
                yanchor="top",
                y=0.99,
                xanchor="center",
                x=0.01,  # Position the legend close to the left edge
                itemsizing="constant"  # Ensures symbols in legend remain small
            ),        
            margin={"r": 0, "t": 0, "l": 0, "b": 0}
        )
        
        # Display the figure in Streamlit
        st.plotly_chart(fig, use_container_width=True)

    if selected_view == 'Buurt':
        # Convert GeoDataFrame to GeoJSON format
        geojson = gdf_buurten.geometry.__geo_interface__

        # Groepeer en tel incidenten per buurt
        incident_counts = df_accumulated.groupby("Buurt").size().reset_index(name="Total Incidents")

        # Voeg een rij toe voor elke buurt die geen incidenten heeft
        all_buurten = pd.DataFrame(gdf_buurten['buurtnaam'].unique(), columns=['Buurt'])
        incident_counts = all_buurten.merge(incident_counts, on='Buurt', how='left').fillna(0)

        # Voeg de geografische data toe
        gdf_buurten = pd.merge(
            incident_counts,
            gdf_buurten,
            left_on="Buurt",
            right_on="buurtnaam",
            how="right"
        )
        gdf_buurten.drop("buurtnaam", axis=1, inplace=True)

        fig = px.choropleth_mapbox(
            gdf_buurten,
            geojson=geojson,
            locations=gdf_buurten.index,
            color="Total Incidents",  # using the dummy column here
            color_continuous_scale=px.colors.sequential.Reds,
            opacity=0.40,
            center={"lat": 52.3550001, "lon": 4.8678},
            mapbox_style="carto-darkmatter",
            zoom=10.2,
            custom_data=[gdf_buurten["Buurt"]],
            labels={"Total Incidents": "Aantal incidenten"}  # Set custom label for the legend

        )

        # Adjust the border color for polygons
        fig.update_traces(
            marker_line_width=2,
            marker_line_color="grey",
            hovertemplate="<b>%{customdata[0]}</b><extra></extra>",
        )
        # Hide the color bar
        fig.update_layout(coloraxis_showscale=True)
        # Adjust legend - make it smaller
        fig.update_layout(
            legend=dict(
                font=dict(size=10),  # Smaller font size
                yanchor="top",
                y=0.99,
                xanchor="left",
                x=0.01,  # Position the legend close to the left edge
                itemsizing="constant"  # Ensures symbols in legend remain small
            ),        
            margin={"r": 0, "t": 0, "l": 0, "b": 0}
        )

        # Display the figure in Streamlit
        st.plotly_chart(fig, use_container_width=True)
    
    if selected_view == 'Wijk':
        # Convert GeoDataFrame to GeoJSON format
        geojson = gdf_wijken.geometry.__geo_interface__

        # Groepeer en tel incidenten per buurt
        incident_counts = df_accumulated.groupby("Wijk").size().reset_index(name="Total Incidents")

        # Voeg een rij toe voor elke buurt die geen incidenten heeft
        all_wijken = pd.DataFrame(gdf_wijken['wijknaam'].unique(), columns=['Wijk'])
        incident_counts = all_wijken.merge(incident_counts, on='Wijk', how='left').fillna(0)

        # Voeg de geografische data toe
        gdf_wijken = pd.merge(
            incident_counts,
            gdf_wijken,
            left_on="Wijk",
            right_on="wijknaam",
            how="right"
        )

        fig = px.choropleth_mapbox(
            gdf_wijken,
            geojson=geojson,
            locations=gdf_wijken.index,
            color="Total Incidents",  # using the dummy column here
            color_continuous_scale=px.colors.sequential.Reds,
            opacity=0.40,
            center={"lat": 52.3550001, "lon": 4.8678},
            mapbox_style="carto-darkmatter",
            zoom=10.2,
            custom_data=[gdf_wijken["Wijk"]],
            labels={"Total Incidents": "Aantal incidenten"}  # Set custom label for the legend

        )

        # Adjust the border color for polygons
        fig.update_traces(
            marker_line_width=2,
            marker_line_color="grey",
            hovertemplate="<b>%{customdata[0]}</b><extra></extra>",
        )

        # Hide the color bar
        fig.update_layout(coloraxis_showscale=True)
        # Adjust legend - make it smaller
        fig.update_layout(
            legend=dict(
                font=dict(size=10),  # Smaller font size
                yanchor="top",
                y=0.99,
                xanchor="left",
                x=0.01,  # Position the legend close to the left edge
                itemsizing="constant"  # Ensures symbols in legend remain small
            ),        
            margin={"r": 0, "t": 0, "l": 0, "b": 0}
        )

        # Display the figure in Streamlit
        st.plotly_chart(fig, use_container_width=True)


    if selected_view == 'Incident':
        incidents_per_area = df_accumulated.groupby("Verzorgingsgebied") \
            .size() \
            .reset_index(name="Total Incidents")

        # Merge the counts with the service areas geodataframe
        gdf_service_areas = pd.merge(
            gdf_service_areas,
            incidents_per_area,
            left_on="Verzorgingsgebied",
            right_on="Verzorgingsgebied",
            how="left"
        )

        # Replace NaN values with 0 for service areas without incidents
        gdf_service_areas["Total Incidents"].fillna(0, inplace=True)

        # Remove the now unnecessary 'Service_Area' column if it exists
        if 'Service_Area' in gdf_service_areas.columns:
            gdf_service_areas.drop("Service_Area", axis=1, inplace=True)

        # Get the geojson representation for plotting
        geojson = gdf_service_areas.geometry.__geo_interface__

        gdf_service_areas["dummy"] = 1

        # Create a base map
        fig = px.choropleth_mapbox(
            gdf_service_areas,
            geojson=geojson,
            locations=gdf_service_areas.index,
            color="dummy",  # using the dummy column here
            color_continuous_scale=[
                "rgba(200, 200, 200, 0.2)",
                "rgba(200, 200, 200, 0.2)",
            ],  # px.colors.sequential.Viridis, #["rgba(200, 200, 200, 0.2)", "rgba(0, 0, 0, 0.2)"],  # semi-transparent light
            center={"lat": 52.320001, "lon": 4.87278},
            mapbox_style="carto-darkmatter",
            zoom=9.7,
            custom_data=[gdf_service_areas["Verzorgingsgebied"]],
        )

        # Adjust the border color for polygons
        fig.update_traces(
            marker_line_width=2,
            marker_line_color="grey",
            hovertemplate="<b>%{customdata[0]}</b><extra></extra>",
        )
        # Adjust legend - make it smaller
        fig.update_layout(
            coloraxis_showscale=False,
            legend=dict(
                font=dict(size=10),  # Smaller font size
                yanchor="top",
                y=0.99,
                xanchor="left",
                x=0.01,
                itemsizing="constant",  # Ensures symbols in legend remain small
            ),
        )

        # Collect coordinates for fire stations into lists
        latitudes = gdf_service_areas["LAT"].tolist()
        longitudes = gdf_service_areas["LON"].tolist()

        # Add a single trace for all fire stations using go.Scattermapbox
        fig.add_trace(
            go.Scattermapbox(
                lat=latitudes,  # Latitude data for all fire stations
                lon=longitudes,  # Longitude data for all fire stations
                mode="markers",  # Use markers to represent the fire stations
                marker=go.scattermapbox.Marker(
                    size=5,  # Adjust size as needed
                    color="silver",  # Set marker color
                ),
                text=["{}".format(name) for name in gdf_service_areas["Verzorgingsgebied"]],
                hoverinfo="text+name",  # Optionally, disable hover information or customize as needed
                name="Kazerne",  # Name of the trace, appearing in the legend and hover
                hoverlabel=dict(bgcolor="black", font=dict(color="white")),
            )
        )

        # Define a color map for damage types
        color_map = {
            "Bomen": "#06d6a0",
            "Gebouwen": "#118ab2",
            "Hekwerk, Verkeersborden, Steigers": "#ffd166",
            "Onbekend": "#ef476f"
        }

        # Only proceed if there are damage types specified
        for d_type in damage_type:  # Loop through each damage type
            # Filter the dataframe for the current damage type
            df_filtered = df_accumulated[(df_accumulated["Damage_Type"] == d_type)]

            hover_text = []
            for row in df_filtered.itertuples():
                hover_parts = ["Categorie: {}".format(row.Damage_Type)]

                # Voeg 'Prioriteit' toe als het bestaat in de dataframe
                if "Incident_Priority" in df_filtered.columns:
                    hover_parts.append("Prioriteit: {}".format(row.Incident_Priority))

                # Voeg 'Duur' toe als het bestaat in de dataframe
                if "Incident_Duration" in df_filtered.columns:
                    hover_parts.append("Duur: {}".format(row.Incident_Duration))

                # Voeg coördinaten toe
                hover_parts.append("Coördinaten: ({}, {})".format(row.LAT, row.LON))

                # Combineer de delen tot één hovertekst
                hover_text.append("<br>".join(hover_parts))

            # Get the color for the current damage type
            color = color_map.get(d_type)  # Default to black if not found

            # Create a trace for the current damage type
            fig.add_trace(
                go.Scattermapbox(
                    lat=df_filtered["LAT"],
                    lon=df_filtered["LON"],
                    mode="markers",
                    marker=go.scattermapbox.Marker(
                        size=5,  # Adjust size as needed
                        color=color,  # Set color based on damage type
                    ),
                    text=hover_text,
                    hoverinfo="text",
                    name=d_type,  # Set the name of the trace to the damage type
                    hoverlabel=dict(bgcolor="black", font=dict(color=color)),
                )
            )

        # Update layout as needed
        fig.update_layout(margin={"r": 0, "t": 0, "l": 0, "b": 0})

        # Display the figure in Streamlit
        st.plotly_chart(fig, use_container_width=True)


col1, col2 = st.columns([3, 1])  # Adjust the ratio as needed for your layout
with col1:
    # Display the map here (your existing map code)
    display_map_plotly_streamlit(gdf_service_areas, gdf_wijken, gdf_buurten, df_accumulated, damage_type)


# Define colors for each damage type
color_dict = {
    "Bomen": "#06d6a0",
    "Gebouwen": "#118ab2",
    "Hekwerk, Verkeersborden, Steigers": "#ffd166",
    "Onbekend": "#ef476f",
}

if df_filtered.empty:
    st.write("Geen informatie beschikbaar voor de geselecteerde datum.")
else:  
    if selected_view == 'Incident' or selected_view == 'Verzorgingsgebied':
         # Group by hour and damage type, then count incidents
        hourly_damage = (
            df_filtered.groupby(["Hour", "Damage_Type"])
            .size()
            .reset_index(name="Incidents")
        )

        # Group by date and damage type for the donut pie chart
        daily_damage = (
            df_filtered.groupby(["Date", "Damage_Type"])
            .size()
            .reset_index(name="Incidents")
        )

        # Filter the data for the specific date
        specific_date_data = daily_damage[
            daily_damage["Date"] == daily_damage["Date"].max()
        ]

        # Total incidents for annotation
        total_incidents = specific_date_data["Incidents"].sum()  
        # Group by date and service area for the top list
        daily_damage_area = (
            df_filtered.groupby(["Date", "Verzorgingsgebied"])
            .size()
            .reset_index(name="Incidents")
            .sort_values(by="Incidents", ascending=True)
            .head(10)
        )

        fig_df = px.bar(
            daily_damage_area,
            x="Incidents",
            y="Verzorgingsgebied",
            orientation="h",
            text="Incidents",
            color_discrete_sequence=["#073b4c"],
        )

        fig_df.update_traces(texttemplate="%{text}", textposition="outside")
        fig_df.update_layout(
            xaxis={"visible": False, "showticklabels": False},
            yaxis_title=None,
            title_text="Aantal incidenten per verzorgingsgebied", 
            title_x=0.5,  
            title_xanchor='center',
            width=400,
            height=520,
            margin=dict(l=50, r=15, t=20, b=70),  # Adjust margins if needed
        )
        with col2:
            st.plotly_chart(fig_df, use_container_width=True)

        # Prepare the donut pie chart
        fig_donut = go.Figure()

        # Add a pie chart to the figure
        fig_donut.add_trace(
            go.Pie(
                labels=specific_date_data["Damage_Type"],
                values=specific_date_data["Incidents"],
                hoverinfo="percent+label",
                textinfo="value",
                textposition="outside",
                hole=0.6,
                marker_colors=[
                    color_dict.get(damage_type, "#000000")
                    for damage_type in specific_date_data["Damage_Type"]
                ],
            )
        )

        # Update the layout of the donut pie chart
        fig_donut.update_layout(
            title_text="Aantal incidenten per categorie",
            title_x=0.22,
            annotations=[
                dict(
                    text=f"Totaal: {total_incidents}",
                    x=0.5,
                    y=0.5,
                    font_size=20,
                    showarrow=False,
                )
            ],
            showlegend=False,
            width=410,
            height=430,
            margin=dict(l=70, r=90, t=94, b=20),
        )

        # Prepare the histogram
        fig_histogram = go.Figure()

        # Iterate through each damage type and hour to ensure all are represented
        for damage_type in hourly_damage["Damage_Type"].unique():
            for hour in range(24):
                if not (
                    (hourly_damage["Damage_Type"] == damage_type)
                    & (hourly_damage["Hour"] == hour)
                ).any():
                    # Create a DataFrame for the missing hour and concatenate
                    missing_hour_df = pd.DataFrame(
                        {"Damage_Type": [damage_type], "Hour": [hour], "Incidents": [0]}
                    )
                    hourly_damage = pd.concat(
                        [hourly_damage, missing_hour_df], ignore_index=True
                    )

        # Now proceed with your existing plotting logic
        for damage_type in hourly_damage["Damage_Type"].unique():
            df_damage = hourly_damage[hourly_damage["Damage_Type"] == damage_type]
            fig_histogram.add_trace(
                go.Bar(
                    x=df_damage["Hour"],
                    y=df_damage["Incidents"],
                    name=damage_type,
                    marker_color=color_dict.get(
                        damage_type, "#000000"
                    ),  # default color if not found
                    hoverinfo="y+name",
                )
            )

        # Update the layout of the histogram
        fig_histogram.update_layout(
            title="Aantal incidenten per uur",
            title_x=0.35,
            xaxis=dict(
                title="Uur",
                tickmode="array",
                tickvals=list(range(24)),
                ticktext=[f"{hour}:00" for hour in range(24)],
            ),
            yaxis=dict(title="Aantal incidenten"),
            plot_bgcolor="rgba(0,0,0,0)",
            paper_bgcolor="rgba(0,0,0,0)",
            bargap=0.1,
            barmode="stack"
        )

        # Arrange the histogram and donut chart side by side in Streamlit
        col3, col4 = st.columns([3, 1])  # adjust the ratio as needed for your layout
        with col4:
            st.plotly_chart(fig_donut, use_container_width=True)
        with col3:
            st.plotly_chart(fig_histogram, use_container_width=True)
    

    if selected_view == 'Buurt':
        df_filtered = df_filtered[df_filtered['Wijk'] != 'Onbekend']
         # Group by hour and damage type, then count incidents
        hourly_damage = (
            df_filtered.groupby(["Hour", "Damage_Type"])
            .size()
            .reset_index(name="Incidents")
        )

        # Group by date and damage type for the donut pie chart
        daily_damage = (
            df_filtered.groupby(["Date", "Damage_Type"])
            .size()
            .reset_index(name="Incidents")
        )

        # Filter the data for the specific date
        specific_date_data = daily_damage[
            daily_damage["Date"] == daily_damage["Date"].max()
        ]

        # Total incidents for annotation
        total_incidents = specific_date_data["Incidents"].sum()  

        daily_damage_area = (
            df_filtered.groupby(["Date", "Buurt"])
            .size()
            .reset_index(name="Incidents")
            .sort_values(by="Incidents", ascending=False)
            .head(10)
            .sort_values(by="Incidents", ascending=True)
        )

        fig_df = px.bar(
            daily_damage_area,
            x="Incidents",
            y="Buurt",
            orientation="h",
            text="Incidents",
            title="Aantal incidenten per buurt",
            color_discrete_sequence=["#073b4c"],
        )

        fig_df.update_traces(texttemplate="%{text}", textposition="outside")
        fig_df.update_layout(
            xaxis={"visible": False, "showticklabels": False},
            yaxis_title=None,
            title_x=0.30,
            width=400,
            height=520,
            margin=dict(l=50, r=15, t=20, b=70),  # Adjust margins if needed
        )
        with col2:
            st.plotly_chart(fig_df, use_container_width=True)
 
        # Prepare the donut pie chart
        fig_donut = go.Figure()

        # Add a pie chart to the figure
        fig_donut.add_trace(
            go.Pie(
                labels=specific_date_data["Damage_Type"],
                values=specific_date_data["Incidents"],
                hoverinfo="percent+label",
                textinfo="value",
                textposition="outside",
                hole=0.6,
                marker_colors=[
                    color_dict.get(damage_type, "#000000")
                    for damage_type in specific_date_data["Damage_Type"]
                ],
            )
        )

        # Update the layout of the donut pie chart
        fig_donut.update_layout(
            title_text="Aantal incidenten per categorie",
            title_x=0.22,
            annotations=[
                dict(
                    text=f"Totaal: {total_incidents}",
                    x=0.5,
                    y=0.5,
                    font_size=20,
                    showarrow=False,
                )
            ],
            showlegend=False,
            width=410,
            height=430,
            margin=dict(l=70, r=90, t=94, b=20),
        )

        # Prepare the histogram
        fig_histogram = go.Figure()

        # Iterate through each damage type and hour to ensure all are represented
        for damage_type in hourly_damage["Damage_Type"].unique():
            for hour in range(24):
                if not (
                    (hourly_damage["Damage_Type"] == damage_type)
                    & (hourly_damage["Hour"] == hour)
                ).any():
                    # Create a DataFrame for the missing hour and concatenate
                    missing_hour_df = pd.DataFrame(
                        {"Damage_Type": [damage_type], "Hour": [hour], "Incidents": [0]}
                    )
                    hourly_damage = pd.concat(
                        [hourly_damage, missing_hour_df], ignore_index=True
                    )

        # Now proceed with your existing plotting logic
        for damage_type in hourly_damage["Damage_Type"].unique():
            df_damage = hourly_damage[hourly_damage["Damage_Type"] == damage_type]
            fig_histogram.add_trace(
                go.Bar(
                    x=df_damage["Hour"],
                    y=df_damage["Incidents"],
                    name=damage_type,
                    marker_color=color_dict.get(
                        damage_type, "#000000"
                    ),  # default color if not found
                    hoverinfo="y+name",
                )
            )

        # Update the layout of the histogram
        fig_histogram.update_layout(
            title="Aantal incidenten per uur",
            title_x=0.35,
            xaxis=dict(
                title="Uur",
                tickmode="array",
                tickvals=list(range(24)),
                ticktext=[f"{hour}:00" for hour in range(24)],
            ),
            yaxis=dict(title="Aantal incidenten"),
            plot_bgcolor="rgba(0,0,0,0)",
            paper_bgcolor="rgba(0,0,0,0)",
            bargap=0.1,
            barmode="stack",
        )

        # Arrange the histogram and donut chart side by side in Streamlit
        col3, col4 = st.columns([3, 1])  # adjust the ratio as needed for your layout
        with col4:
            st.plotly_chart(fig_donut, use_container_width=True)
        with col3:
            st.plotly_chart(fig_histogram, use_container_width=True)


    if selected_view == 'Wijk':
        df_filtered = df_filtered[df_filtered['Wijk'] != 'Onbekend']
             # Group by hour and damage type, then count incidents
        hourly_damage = (
            df_filtered.groupby(["Hour", "Damage_Type"])
            .size()
            .reset_index(name="Incidents")
        )

        # Group by date and damage type for the donut pie chart
        daily_damage = (
            df_filtered.groupby(["Date", "Damage_Type"])
            .size()
            .reset_index(name="Incidents")
        )

        # Filter the data for the specific date
        specific_date_data = daily_damage[
            daily_damage["Date"] == daily_damage["Date"].max()
        ]

        # Total incidents for annotation
        total_incidents = specific_date_data["Incidents"].sum()  

        daily_damage_area = (
            df_filtered.groupby(["Date", "Wijk"])
            .size()
            .reset_index(name="Incidents")
            .sort_values(by="Incidents", ascending=False)
            .head(10)
            .sort_values(by="Incidents", ascending=True)
        )

        fig_df = px.bar(
            daily_damage_area,
            x="Incidents",
            y="Wijk",
            orientation="h",
            text="Incidents",
            title="Aantal incidenten per wijk",
            color_discrete_sequence=["#073b4c"],
        )

        fig_df.update_traces(texttemplate="%{text}", textposition="outside")
        fig_df.update_layout(
            xaxis={"visible": False, "showticklabels": False},
            yaxis_title=None,
            title_x=0.30,
            width=400,
            height=520,
            margin=dict(l=50, r=15, t=20, b=70),  # Adjust margins if needed
        )
        with col2:
            st.plotly_chart(fig_df, use_container_width=True)
                # Prepare the donut pie chart
        fig_donut = go.Figure()

        # Add a pie chart to the figure
        fig_donut.add_trace(
            go.Pie(
                labels=specific_date_data["Damage_Type"],
                values=specific_date_data["Incidents"],
                hoverinfo="percent+label",
                textinfo="value",
                textposition="outside",
                hole=0.6,
                marker_colors=[
                    color_dict.get(damage_type, "#000000")
                    for damage_type in specific_date_data["Damage_Type"]
                ],
            )
        )

        # Update the layout of the donut pie chart
        fig_donut.update_layout(
            title_text="Aantal incidenten per categorie",
            title_x=0.22,
            annotations=[
                dict(
                    text=f"Totaal: {total_incidents}",
                    x=0.5,
                    y=0.5,
                    font_size=20,
                    showarrow=False,
                )
            ],
            showlegend=False,
            width=410,
            height=430,
            margin=dict(l=70, r=90, t=94, b=20),
        )

        # Prepare the histogram
        fig_histogram = go.Figure()

        # Iterate through each damage type and hour to ensure all are represented
        for damage_type in hourly_damage["Damage_Type"].unique():
            for hour in range(24):
                if not (
                    (hourly_damage["Damage_Type"] == damage_type)
                    & (hourly_damage["Hour"] == hour)
                ).any():
                    # Create a DataFrame for the missing hour and concatenate
                    missing_hour_df = pd.DataFrame(
                        {"Damage_Type": [damage_type], "Hour": [hour], "Incidents": [0]}
                    )
                    hourly_damage = pd.concat(
                        [hourly_damage, missing_hour_df], ignore_index=True
                    )

        # Now proceed with your existing plotting logic
        for damage_type in hourly_damage["Damage_Type"].unique():
            df_damage = hourly_damage[hourly_damage["Damage_Type"] == damage_type]
            fig_histogram.add_trace(
                go.Bar(
                    x=df_damage["Hour"],
                    y=df_damage["Incidents"],
                    name=damage_type,
                    marker_color=color_dict.get(
                        damage_type, "#000000"
                    ),  # default color if not found
                    hoverinfo="y+name",
                )
            )

        # Update the layout of the histogram
        fig_histogram.update_layout(
            title="Aantal incidenten per uur",
            title_x=0.35,
            xaxis=dict(
                title="Uur",
                tickmode="array",
                tickvals=list(range(24)),
                ticktext=[f"{hour}:00" for hour in range(24)],
            ),
            yaxis=dict(title="Aantal incidenten"),
            plot_bgcolor="rgba(0,0,0,0)",
            paper_bgcolor="rgba(0,0,0,0)",
            bargap=0.1,
            barmode="stack",
        )

        # Arrange the histogram and donut chart side by side in Streamlit
        col3, col4 = st.columns([3, 1])  # adjust the ratio as needed for your layout
        with col4:
            st.plotly_chart(fig_donut, use_container_width=True)
        with col3:
            st.plotly_chart(fig_histogram, use_container_width=True)


    # Adjust KNMI dataset
    df_knmi_selected["Hour"] = df_knmi_selected["Hour"].apply(
        lambda x: f"{(x-1):02d}:00"
    )
    df_knmi_selected["Ff"] = df_knmi_selected["Ff"] * 0.36  # in km/h
    df_knmi_selected["Rh"] = df_knmi_selected["Rh"] / 0.1  # in mm

    # Add Force
    beaufort_windscale = {
        "Windkracht": [0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12],
        "Min": [0, 1, 6, 12, 20, 29, 39, 50, 62, 75, 89, 103, 117],
        "Max": [1, 5, 11, 19, 28, 38, 49, 61, 74, 88, 102, 117, 200],
    }
    beaufort_df = pd.DataFrame(beaufort_windscale)

    def find_kracht(ff_value):
        rounded_ff = round(ff_value)
        for _, row in beaufort_df.iterrows():
            if row["Min"] <= rounded_ff <= row["Max"]:
                return row["Windkracht"]
        return None

    df_knmi_selected["Windkracht"] = df_knmi_selected["Ff"].apply(find_kracht)

    # Function to convert degrees to cardinal directions
    def degrees_to_direction(deg):
        if 348.75 <= deg or deg <= 11.25:
            return "N"
        elif 11.25 < deg <= 33.75:
            return "NNE"
        elif 33.75 < deg <= 56.25:
            return "NE"
        elif 56.25 < deg <= 78.75:
            return "ENE"
        elif 78.75 < deg <= 101.25:
            return "E"
        elif 101.25 < deg <= 123.75:
            return "ESE"
        elif 123.75 < deg <= 146.25:
            return "SE"
        elif 146.25 < deg <= 168.75:
            return "SSE"
        elif 168.75 < deg <= 191.25:
            return "S"
        elif 191.25 < deg <= 213.75:
            return "SSW"
        elif 213.75 < deg <= 236.25:
            return "SW"
        elif 236.25 < deg <= 258.75:
            return "WSW"
        elif 258.75 < deg <= 281.25:
            return "W"
        elif 281.25 < deg <= 303.75:
            return "WNW"
        elif 303.75 < deg <= 326.25:
            return "NW"
        elif 326.25 < deg <= 348.75:
            return "NNW"

    # Apply the function to the DataFrame
    df_knmi_selected["Direction"] = df_knmi_selected["Dd"].apply(degrees_to_direction)

    # Windspeed and Precipitation graph
    fig_weather_graph = go.Figure()

    fig_weather_graph.add_trace(
        go.Scatter(
            x=df_knmi_selected["Hour"],
            y=df_knmi_selected["Ff"],
            mode="lines",
            name="Windsnelheid",
        )
    )

    # Add the precipitation plot (secondary y-axis)
    fig_weather_graph.add_trace(
        go.Scatter(
            x=df_knmi_selected["Hour"],
            y=df_knmi_selected["Rh"],
            mode="lines",
            name="Neerslag",
            yaxis="y2",
        )
    )

    # Update the layout
    fig_weather_graph.update_layout(
        title="Gemiddelde windsnelheid en neerslag per uur",
        xaxis_title="Uur",
        title_x=0.34,
        yaxis=dict(title="Windsnelheid (in km/u)"),
        yaxis2=dict(title="Neerslag (in mm)", overlaying="y", side="right"),
        xaxis=dict(
            tickmode="array",
            tickvals=[f"{hour:02d}:00" for hour in range(24)],
            range=["00:00", "23:00"],
        ),
        width=1025,
        legend=dict(
            x=1.025
            # # orientation="h"
        ),
    )

    fig_windrose = px.bar_polar(
        df_knmi_selected,
        r="Ff",
        theta="Direction",
        color="Windkracht",
        template="plotly_dark",
        color_discrete_sequence=px.colors.sequential.swatches_continuous(),
        category_orders={
            "Direction": [
                "N",  # North
                "NNE",  # North-Northeast
                "NE",  # Northeast
                "ENE",  # East-Northeast
                "E",  # East
                "ESE",  # East-Southeast
                "SE",  # Southeast
                "SSE",  # South-Southeast
                "S",  # South
                "SSW",  # South-Southwest
                "SW",  # Southwest
                "WSW",  # West-Southwest
                "W",  # West
                "WNW",  # West-Northwest
                "NW",  # Northwest
                "NNW",  # North-Northwest
            ]
        },
        barnorm="fraction",
        custom_data=["Ff", "Windkracht"]
    )

    # Customize hover labels
    fig_windrose.update_traces(
        hovertemplate="<br>".join(
            ["Windsnelheid: %{customdata[0]:.1f} km/u", "Windkracht: %{customdata[1]}"]
        )
    )

    fig_windrose.update_layout(
        title_text="Windrichting en windkracht",
        title_x=0.25,
        width=420,
        height=450,
        margin=dict(l=50, r=0, t=80, b=50),
    )

    # Display in Streamlit
    col5, col6 = st.columns([3, 1])
    with col5:
        st.plotly_chart(fig_weather_graph, use_container_width=True)
    with col6:
        st.plotly_chart(fig_windrose, use_container_width=True)

    # Create the figure
    fig_weather_condition = go.Figure()

    # Add line plots for each weather condition
    fig_weather_condition.add_trace(
        go.Scatter(
            x=df_knmi_selected["Hour"],
            y=df_knmi_selected["M"],
            mode="lines",
            name="Mist",
        )
    )
    fig_weather_condition.add_trace(
        go.Scatter(
            x=df_knmi_selected["Hour"],
            y=df_knmi_selected["R"],
            mode="lines",
            name="Regen",
        )
    )
    fig_weather_condition.add_trace(
        go.Scatter(
            x=df_knmi_selected["Hour"],
            y=df_knmi_selected["S"],
            mode="lines",
            name="Sneeuw",
        )
    )
    fig_weather_condition.add_trace(
        go.Scatter(
            x=df_knmi_selected["Hour"],
            y=df_knmi_selected["O"],
            mode="lines",
            name="Onweer",
        )
    )
    fig_weather_condition.add_trace(
        go.Scatter(
            x=df_knmi_selected["Hour"],
            y=df_knmi_selected["Y"],
            mode="lines",
            name="Ijzel",
        )
    )

    # Update the layout
    fig_weather_condition.update_layout(
        title="Weerconditie per uur",
        xaxis_title="Uur",
        # yaxis_title='Condition Occurrence (0 or 1)',
        yaxis=dict(tickmode="array", tickvals=[0, 1], ticktext=["Nee", "Ja"]),
        title_x=0.38,
        width=1025,
        height=400,
        margin=dict(l=65),
    )

    # Create the dataframe
    data_featureimportance = {
        "Variable": [
            "Longitude",
            "Windsnelheid (1 uur)",
            "Hoogste Windstoot",
            "Latitude",
            "Windsnelheid (10 min)",
            "Temperatuur",
            "Globale Straling",
            "Dauwpuntstemperatuur",
            "Luchtdruk",
            "Windrichting",
            "Relatieve Vochtigheid",
            "Jaar van aanleg",
            "Horizontaal Zicht",
            "Boomhoogte Klasse",

        ],
        "Feature Importance": [
            0.14244, 0.11205, 0.1085, 0.08981, 0.07304, 0.05829,
            0.05804, 0.05499, 0.05173, 0.04112, 0.04217, 0.03311, 0.03146, 0.02629      
        ]
    }

    df_features = pd.DataFrame(data_featureimportance)
    df_features = df_features.sort_values(by="Feature Importance", ascending=True)

    fig_features = px.bar(
        df_features,
        x="Feature Importance",
        y="Variable",
        orientation="h",
        text="Feature Importance",
        title="Feature Importance",
        color_discrete_sequence=["#073b4c"],
    )

    fig_features.update_traces(texttemplate="%{text:.2f}", textposition="outside")
    fig_features.update_layout(
        xaxis={"visible": False, "showticklabels": True},
        yaxis_title=None,
        title_x=0.32,
        width=380,
        height=400,
        margin=dict(l=10, r=10, t=70, b=20),  # Adjust margins if needed
    )
        

    # Display in Streamlit
    col7, col8 = st.columns([3, 1])
    with col7:
        st.plotly_chart(fig_weather_condition, use_container_width=True)
    with col8:
        st.plotly_chart(fig_features, use_container_width=True)



# # Check if the DataFrame is not empty and 'Dd' column exists
# if df_knmi_selected is not None and "Dd" in df_knmi_selected.columns:
#     # Count occurrences where Dd is equal to 230
#     wind_direction_count = (df_knmi_selected["Dd"] == 230).sum()

#     # Display the count using Streamlit
#     st.write(
#         f"Number of occurrences where wind direction is 230: {wind_direction_count}"
#     )
# else:
#     st.write("Error: DataFrame is empty or 'Dd' column is missing.")

# st.sidebar.image('brandweerlogozwart.png', use_column_width=True)
