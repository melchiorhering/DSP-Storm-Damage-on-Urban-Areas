import streamlit as st
import numpy as np
import pandas as pd
from datetime import date
import matplotlib.pyplot as plt
import plotly.express as px
from shapely import wkt
import plotly.graph_objs as go
import folium
from streamlit_folium import st_folium
from shapely.geometry import Polygon
from PIL import Image
import geopandas as gpd
import geodatasets
# Data
df = pd.read_csv('Storm_Data_Incidents.csv') 
df_serviceareas = pd.read_csv('serviceareas.csv', sep  = ",")

# Sample Data
areas = ["Area 1", "Area 2", "Area 3", "Area 4"]
incident_counts = df['Date'].value_counts()
dates_with_20_or_more_incidents = incident_counts[incident_counts >= 20].index
times = [f"{hour}:00" for hour in range(24)]


# Title
st.title("Storm Damage Simulator")

# Load and display the logo
#st.sidebar.image('82436.png', use_column_width=True)

# Storm Selection
st.sidebar.header("Storm")
selected_storm = st.sidebar.selectbox("Select Previous Storm", dates_with_20_or_more_incidents)
selected_future_storm = st.sidebar.selectbox("Select Future Storm", dates_with_20_or_more_incidents)

df_selected_storm = df[(df['Date'] == selected_storm)]
#st.write(df_selected_storm)


# Time Slider
# st.sidebar.header("Time Slider")
# selected_time = st.sidebar.selectbox("Select Time", times)
 
# Date Selection 
st.sidebar.header("Date")
selected_date = st.sidebar.date_input("Select Date", date.today())
print(selected_date)

# if len(selected_storm) > 0:
#     selected_date = selected_storm
# else:
#     selected_date = selected_date

# Map and Area Information
col1, col2 = st.columns(2)

#Map 
#col1.header("Amsterdam-Amstelland")

st.sidebar.header("Incident Type")
available_damage_types = df['Damage_Type'].unique()
damage_type = st.sidebar.multiselect("Select Type(s)", available_damage_types)
print(damage_type)
# # Sidebar to get user input
# st.sidebar.header("Select Options")
# selected_date = st.sidebar.date_input("Select Date", value=pd.to_datetime('today'))


# Convert df_serviceareas into gpd
df_serviceareas['geometry'] = df_serviceareas['geomtext'].apply(wkt.loads)
gdf = gpd.GeoDataFrame(df_serviceareas, geometry='geometry')

# Set the CRS to EPSG:28992 for RD New (Rijksdriehoekstelsel)
gdf.set_crs("EPSG:28992", inplace=True)

# Convert it to EPSG:4326 
gdf = gdf.to_crs(epsg=4326)

def display_map(df, date, damage_type):
    map = folium.Map(location=[52.360001, 4.885278], tiles='CartoDB positron', scrollWheelZoom = True)

    # Loop through the rows of the dataframe to add service areas and fire stations
    for _, row in gdf.iterrows():
        sim_geo = gpd.GeoSeries(row["geometry"]).simplify(tolerance=0.001)
        geo_j = sim_geo.to_json()
        geo_j = folium.GeoJson(data=geo_j, 
                               style_function=lambda x: {
                                   "fillColor": "green",
                                   "color" : "#BC9B53",
                                   "weight" : 2})
        geo_j.add_child(folium.Tooltip(row['Verzorgingsgebied']))
        geo_j.add_to(map)
        

        icon = folium.CustomIcon('brandweericon.png', icon_size=(15, 15))  # Adjust icon_size as needed
        folium.Marker([row['LAT'], row['LON']], icon=icon, tooltip=row['Verzorgingsgebied']).add_to(map)

    
    # choropleth = folium.Choropleth(
    # geo_data = 'INDELING_STADSDEEL.csv'
    # )
    # choropleth.geojson.add_to(st_map)
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
                folium.CircleMarker(location=[lat, lon], radius=2, weight=5, color = '#f60000').add_to(map)
    #map.fit_bounds(map.get_bounds())
    st_map = st_folium(map, width=800, height=550)
    #st.write(df2.head())
    #st.write(df2.shape)


if len(damage_type) > 0:
    display_map(df, selected_date.strftime('%Y-%m-%d'), damage_type)
else: 
    df3 = df[(df['Date'] == selected_date.strftime('%Y-%m-%d'))]
    #map = folium.Map(location=[52.360001, 4.885278], zoom_start=11, tiles='CartoDB positron' , scrollWheelZoom = False)
    map = folium.Map(location=[52.360001, 4.885278], tiles='CartoDB positron', scrollWheelZoom = True)

    # Loop through the rows of the dataframe to add service areas and fire stations
    for _, row in gdf.iterrows():
        sim_geo = gpd.GeoSeries(row["geometry"]).simplify(tolerance=0.001)
        geo_j = sim_geo.to_json()
        geo_j = folium.GeoJson(data=geo_j, 
                               style_function=lambda x: {
                                   "fillColor": "green",
                                   "color" : "#BC9B53",
                                   "weight" : 2})
        geo_j.add_child(folium.Tooltip(row['Verzorgingsgebied']))
        geo_j.add_to(map)
        

        icon = folium.CustomIcon('brandweericon.png', icon_size=(15, 15))  # Adjust icon_size as needed
        folium.Marker([row['LAT'], row['LON']], icon=icon, tooltip=row['Verzorgingsgebied']).add_to(map)
    
    for i,j in df3.iterrows():
        lon = j['LON']
        lat = j['LAT']
        folium.CircleMarker(location=[lat, lon], radius=2, weight=5, color = '#f60000').add_to(map)
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
            marker=dict(color='#f60000'),
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


