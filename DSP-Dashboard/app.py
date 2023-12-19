import streamlit as st
import numpy as np
import pandas as pd
import geopandas as gpd
from datetime import date
import matplotlib.pyplot as plt
import plotly.express as px
import plotly.graph_objs as go
import folium
import json
from streamlit_folium import st_folium

df = pd.read_csv('Storm_Data_Incidents.csv')

# Sample Data
areas = ["Area 1", "Area 2", "Area 3", "Area 4"]
incident_counts = df['Date'].value_counts()
dates_with_20_or_more_incidents = incident_counts[incident_counts >= 20].index
times = [f"{hour}:00" for hour in range(24)]

# Title
st.title("Storm Damage Prediction Dashboard")

# Storm Selection
st.sidebar.header("Previous Storms")
selected_storm = st.sidebar.selectbox("Select a Previous Storm", dates_with_20_or_more_incidents)
df_selected_storm = df[(df['Date'] == selected_storm)]
#st.write(df_selected_storm)


# Time Slider
# st.sidebar.header("Time Slider")
# selected_time = st.sidebar.selectbox("Select Time", times)
 
# Date Selection 
st.sidebar.header("Date and Damage Type Selection")
selected_date = st.sidebar.date_input("Select a Date", date.today())
print(selected_date)

# if len(selected_storm) > 0:
#     selected_date = selected_storm
# else:
#     selected_date = selected_date

# Map and Area Information
col1, col2 = st.columns(2)

#Map 
#col1.header("Amsterdam-Amstelland")

available_damage_types = df['Damage_Type'].unique()
damage_type = st.sidebar.multiselect("Select Damage Types", available_damage_types)
print(damage_type)
# # Sidebar to get user input
# st.sidebar.header("Select Options")
# selected_date = st.sidebar.date_input("Select Date", value=pd.to_datetime('today'))


def display_map(df, date, damage_type):
    map = folium.Map(location=[52.360001, 4.885278], tiles='CartoDB positron', scrollWheelZoom = False)

    if len(damage_type) < 2:
        df2 = df[(df['Date'] == date) & (df['Damage_Type'] == damage_type[0])]
        for i,j in df2.iterrows():
            lon = j['LON']
            lat = j['LAT']
            folium.CircleMarker(location=[lat, lon], radius=2, weight=5).add_to(map)
    else:
        for damage in damage_type:
            df_filtered = df[(df['Date'] == date) & (df['Damage_Type'] == damage)]
            for i,j in df_filtered.iterrows():
                lon = j['LON']
                lat = j['LAT']
                folium.CircleMarker(location=[lat, lon], radius=2, weight=5).add_to(map)
    #map.fit_bounds(map.get_bounds())
    st_map = st_folium(map, width=700, height=450)
    #st.write(df2.head())
    #st.write(df2.shape)
if len(damage_type) > 0:
    display_map(df, selected_date.strftime('%Y-%m-%d'), damage_type)
else: 
    df3 = df[(df['Date'] == selected_date.strftime('%Y-%m-%d'))]
    # Load the GeoJSON file
    gdf = gpd.read_file('converted_serviceareas.geojson')

# Convert GeoDataFrame to a GeoJSON format
    geojson_data = gdf.to_json()

    # Convert GeoDataFrame back to a GeoJSON format

    # Create the Folium map
    map = folium.Map(location=[52.360001, 4.885278], tiles='CartoDB positron', scrollWheelZoom = False)
    # Create the Choropleth layer with additional styling options
    choropleth = folium.Choropleth(
        geo_data=geojson_data,
        line_opacity=0.9,
        fill_opacity=0.9,   # Adjust the opacity
        fill_color='OrRd',  # Adjust the fill color
        highlight=True
    )

    # Add the Choropleth layer to the map
    choropleth.geojson.add_to(map)
    
    #map = folium.Map(location=[52.360001, 4.885278], zoom_start=11, tiles='CartoDB positron' , scrollWheelZoom = False)
    for i,j in df3.iterrows():
        lon = j['LON']
        lat = j['LAT']
        folium.CircleMarker(location=[lat, lon], radius=2, weight=5).add_to(map)
    st_map = st_folium(map, width=700, height=450)
    #st.write(df2.head())
    #st.write(df2.shape)

# Filter the DataFrame for the selected date
df_filtered = df[df['Date'] == selected_date.strftime('%Y-%m-%d')]

# Check if df_filtered is empty
if df_filtered.empty:
    st.write("No data available for the selected date.")
else:
    # Extract hour from Incident_Starttime
    df_filtered['Hour'] = pd.to_datetime(df_filtered['Incident_Starttime']).dt.hour
    
    # Create a summary DataFrame with counts of each damage type per hour
    hourly_damage = df_filtered.groupby(['Hour', 'Damage_Type']).size().reset_index(name='Incidents')

    # Create a DataFrame for total incidents per hour
    total_incidents = hourly_damage.groupby('Hour')['Incidents'].sum().reset_index()

    # Ensure all hours (0-23) are present
    all_hours = pd.DataFrame({'Hour': range(24)})
    total_incidents = all_hours.merge(total_incidents, on='Hour', how='left').fillna(0)

    # Prepare hover text which excludes damage types with 0 incidents
    hover_text = []
    for hour in all_hours['Hour']:
        hour_data = hourly_damage[hourly_damage['Hour'] == hour]
        hover_info = "<br>".join([f"- {row['Damage_Type']}: {int(row['Incidents'])}" for i, row in hour_data.iterrows() if row['Incidents'] > 0])
        hover_text.append(f"Damage type(s):<br>{hover_info}" if hover_info else "No incidents")

    # Create the bar plot
    fig = go.Figure(data=[
        go.Bar(
            x=total_incidents['Hour'],
            y=total_incidents['Incidents'],
            hovertext=hover_text,
            hoverinfo='text',
            marker=dict(color='blue'),
        )
    ])

    # Update the layout to match your dashboard style and display all hours
    fig.update_layout(
        title='Number of Incidents per Hour',
        xaxis=dict(title='Hour of the Day', tickmode='linear', tick0=0, dtick=1),
        yaxis=dict(title='Number of Incidents'),
        plot_bgcolor='#F5F2F0',
        paper_bgcolor='#F5F2F0',
        bargap=0.1,  # Adjust the gap between bars if needed
    )

    # Show plot in Streamlit
    st.plotly_chart(fig, use_container_width=True)

# map_data = pd.DataFrame({'lat': [52.349876235310184], 'lon': [4.914844775990842]})
# st.map(map_data)

# st.write(df.shape)
# st.write(df.head())
# st.write(df.columns)
# st.write(len(df))
