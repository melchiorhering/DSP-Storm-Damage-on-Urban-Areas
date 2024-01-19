import streamlit as st
import numpy as np
import pandas as pd
from datetime import date
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
import time 
from custom.database import connect_to_duckdb, get_table_as_dataframe


# Set the page layout to wide
st.set_page_config(layout="wide")
# st.sidebar.image('brandweerlogorond.png', use_column_width=True)

# Data
db_file_path = "data_systems_project.duckdb"
conn = connect_to_duckdb(db_file_path)

df_knmi = get_table_as_dataframe(
    conn=conn, schema="cleaned", table_name="cleaned_knmi_weather_data"
)

df_service_areas = get_table_as_dataframe(
    conn=conn, schema="public", table_name="service_areas"
)

df_incidents = get_table_as_dataframe(
    conn=conn, schema="cleaned", table_name="cleaned_storm_incidents"
)   


incident_counts = df_incidents['Date'].value_counts()
dates_with_20_or_more_incidents = incident_counts[incident_counts >= 20].index
# times = [f"{hour}:00" for hour in range(24)]

# Title
st.title("Storm Damage Amsterdam-Amstelland")

# Storm Selection
st.sidebar.header("Storm")
selected_storm = st.sidebar.selectbox("Select Previous Storm", dates_with_20_or_more_incidents)
# selected_future_storm = st.sidebar.selectbox("Select Future Storm", dates_with_20_or_more_incidents)

df_selected_storm = df_incidents[(df_incidents['Date'] == selected_storm)]

# Date Selection 
st.sidebar.header("Date")
selected_date = st.sidebar.date_input("Select Date", date.today())
print(selected_date)

# Time Slider
selected_hour = st.sidebar.slider("Select Hour", 0, 23, value=23)  # Values will be 0 to 23

# Continue with your operations using the selected_hour
df_incidents['Hour'] = pd.to_datetime(df_incidents['Incident_Starttime'], format='%H:%M:%S').dt.hour
df_accumulated = df_incidents[(df_incidents['Date'] == selected_date.strftime('%Y-%m-%d')) & (df_incidents['Hour'] <= selected_hour)]


damage_type = df_incidents['Damage_Type'].unique()
df_knmi_selected = df_knmi[df_knmi['Date'] == selected_date.strftime('%Y-%m-%d')]

# Convert df_serviceareas into gpd
df_service_areas['geometry'] = df_service_areas['geometry'].apply(wkt.loads)
gdf = gpd.GeoDataFrame(df_service_areas, geometry='geometry')

# Set the CRS to EPSG:28992 for RD New (Rijksdriehoekstelsel)
gdf.set_crs("EPSG:28992", inplace=True)

# Convert it to EPSG:4326 
gdf = gdf.to_crs(epsg=4326)

# Filter the DataFrame for the selected date
df_filtered = df_incidents[df_incidents['Date'] == selected_date.strftime('%Y-%m-%d')]


# Function to display the map using Plotly in Streamlit
def display_map_plotly_streamlit(gdf, df_accumulated, damage_type):
    show_intensity = st.checkbox('Show Intensity of Service Areas')

    if show_intensity == 1:
        # Convert GeoDataFrame to GeoJSON format
        geojson = gdf.geometry.__geo_interface__
        gdf = pd.merge(df_accumulated.groupby('Service_Area').size().reset_index(name='Total Incidents'), gdf, left_on='Service_Area', right_on='Verzorgingsgebied', how='right')
        gdf.drop('Service_Area', axis=1, inplace=True)

        # Create a base map
        fig = px.choropleth_mapbox(gdf,
                                geojson=geojson,
                                locations= gdf.index,
                                color='Total Incidents',  # using the dummy column here
                                color_continuous_scale= ["rgba(54, 77, 122, 0.2)", "rgba(255, 140, 0, 0.2)"], #px.colors.sequential.Viridis, #["rgba(200, 200, 200, 0.2)", "rgba(0, 0, 0, 0.2)"],  # semi-transparent light
                                center={"lat": 52.320001, "lon": 4.87278},
                                mapbox_style="carto-darkmatter",
                                zoom=9.7,
                                custom_data=[gdf['Verzorgingsgebied']]
                                )
        
        # Adjust the border color for polygons
        fig.update_traces(marker_line_width=2, marker_line_color="grey",  hovertemplate="<b>%{customdata[0]}</b><extra></extra>")
        # Hide the color bar
        fig.update_layout(coloraxis_showscale=True)
        # Adjust legend - make it smaller
        fig.update_layout(
            legend=dict(
                font=dict(size=10),  # Smaller font size
                yanchor="top",
                y=0.99,
                xanchor="left",
                x=0.01,
                itemsizing='constant'  # Ensures symbols in legend remain small
            )
        )

        # Update layout as needed
        fig.update_layout(margin={"r":0,"t":0,"l":0,"b":0})

        # Display the figure in Streamlit
        st.plotly_chart(fig, use_container_width=True)
    
    if show_intensity == 0:
        # Convert GeoDataFrame to GeoJSON format
        geojson = gdf.geometry.__geo_interface__
        
        gdf['dummy'] = 1

        # Create a base map
        fig = px.choropleth_mapbox(gdf,
                                geojson=geojson,
                                locations= gdf.index,
                                color='dummy',  # using the dummy column here
                                color_continuous_scale= ["rgba(200, 200, 200, 0.2)", "rgba(200, 200, 200, 0.2)"], #px.colors.sequential.Viridis, #["rgba(200, 200, 200, 0.2)", "rgba(0, 0, 0, 0.2)"],  # semi-transparent light
                                center={"lat": 52.320001, "lon": 4.87278},
                                mapbox_style="carto-darkmatter",
                                zoom=9.7,
                                custom_data=[gdf['Verzorgingsgebied']]
                                )
        
        # Adjust the border color for polygons
        fig.update_traces(marker_line_width=2, marker_line_color="grey",  hovertemplate="<b>%{customdata[0]}</b><extra></extra>")
        # Adjust legend - make it smaller
        fig.update_layout(
            coloraxis_showscale=False,
            legend=dict(
                font=dict(size=10),  # Smaller font size
                yanchor="top",
                y=0.99,
                xanchor="left",
                x=0.01,
                itemsizing='constant'  # Ensures symbols in legend remain small
            )
        )

    # Collect coordinates for fire stations into lists
        latitudes = gdf['LAT'].tolist()
        longitudes = gdf['LON'].tolist()

        # Add a single trace for all fire stations using go.Scattermapbox
        fig.add_trace(go.Scattermapbox(
            lat=latitudes,  # Latitude data for all fire stations
            lon=longitudes,  # Longitude data for all fire stations
            mode='markers',  # Use markers to represent the fire stations
            marker=go.scattermapbox.Marker(
                size=5,  # Adjust size as needed
                color='silver',  # Set marker color
            ),
            text= ["{}".format(name) for name in gdf['Verzorgingsgebied']],
            hoverinfo='text+name',  # Optionally, disable hover information or customize as needed
            name='Fire Station',  # Name of the trace, appearing in the legend and hover
            hoverlabel=dict(bgcolor='black',font=dict(color='white'))
        ))

        # Define a color map for damage types
        color_map = {
            "Tree": "#06d6a0",
            "Building": "#118ab2",
            "Fence, Road signs, Scaffolding": "#ffd166",
            "Unknown": "#ef476f"
            # Ensure you have a color for each damage type
        }

        # Only proceed if there are damage types specified
        for d_type in damage_type:  # Loop through each damage type
            # Filter the dataframe for the current damage type
            df_filtered = df_accumulated[(df_accumulated['Damage_Type'] == d_type)]
            
            # Create a hover text series
            hover_text = ["Type: {}<br>Priority: {}<br>Duration: {}<br>Coordinates: ({}, {})".format(type, priority, duration, lat, lon) 
                            for type, priority, duration, lat, lon in zip(df_filtered['Damage_Type'], df_filtered['Incident_Priority'], df_filtered['Incident_Duration'], df_filtered['LAT'], df_filtered['LON'], )]

            # Get the color for the current damage type
            color = color_map.get(d_type)  # Default to black if not found
            
            # Create a trace for the current damage type
            fig.add_trace(go.Scattermapbox(
                lat=df_filtered['LAT'],
                lon=df_filtered['LON'],
                mode='markers',
                marker=go.scattermapbox.Marker(
                    size=5,  # Adjust size as needed
                    color=color  # Set color based on damage type
                ),
                text = hover_text,
                hoverinfo = 'text',
                name=d_type,  # Set the name of the trace to the damage type
                hoverlabel=dict(bgcolor='black',font=dict(color=color))   
            ))
            
        # Update layout as needed
        fig.update_layout(margin={"r":0,"t":0,"l":0,"b":0})

        # Display the figure in Streamlit
        st.plotly_chart(fig, use_container_width=True)

col1, col2 = st.columns([3, 1])  # Adjust the ratio as needed for your layout
with col1:
    # Display the map here (your existing map code)
    display_map_plotly_streamlit(gdf, df_accumulated, damage_type)


# Define colors for each damage type
color_dict = {
    "Tree": "#06d6a0",
    "Building": "#118ab2",
    "Fence, Road signs, Scaffolding": "#ffd166",
    "Unknown": "#ef476f"
}

if df_filtered.empty:
    st.write("No data available for the selected date.")
else:
    # Extract hour from Incident_Starttime
    # df_filtered['Hour'] = pd.to_datetime(df_filtered['Incident_Starttime']).dt.hour
    
    # Group by hour and damage type, then count incidents
    hourly_damage = df_filtered.groupby(['Hour', 'Damage_Type']).size().reset_index(name='Incidents')

    # Group by date and damage type for the donut pie chart
    daily_damage = df_filtered.groupby(['Date', 'Damage_Type']).size().reset_index(name='Incidents')

    # Group by date and service area for the top list
    daily_damage_area = df_filtered.groupby(['Date', 'Service_Area']).size().reset_index(name='Incidents').sort_values(by="Incidents", ascending=True)

    # Filter the data for the specific date
    specific_date_data = daily_damage[daily_damage['Date'] == daily_damage['Date'].max()]

    # Total incidents for annotation
    total_incidents = specific_date_data['Incidents'].sum()

    # Prepare the donut pie chart
    fig_donut = go.Figure()

    # Add a pie chart to the figure
    fig_donut.add_trace(go.Pie(
        labels=specific_date_data['Damage_Type'],
        values=specific_date_data['Incidents'],
        hoverinfo='percent+label',
        textinfo='value',  
        textposition='outside', 
        hole=.6,
        marker_colors=[color_dict.get(damage_type, '#000000') for damage_type in specific_date_data['Damage_Type']]
    ))

    # Update the layout of the donut pie chart
    fig_donut.update_layout(
        title_text='Number of Incidents per Type',
        title_x=0.22,
        annotations=[dict(text=f'Total: {total_incidents}', x=0.5, y=0.5, font_size=20, showarrow=False)],
        showlegend=False,
        width = 410, height = 430,
        margin=dict(l=70, r=90, t=94, b=20)
    )

    # Prepare the histogram
    fig_histogram = go.Figure()

    # Iterate through each damage type and hour to ensure all are represented
    for damage_type in hourly_damage['Damage_Type'].unique():
        for hour in range(24):
            if not ((hourly_damage['Damage_Type'] == damage_type) & (hourly_damage['Hour'] == hour)).any():
                # Create a DataFrame for the missing hour and concatenate
                missing_hour_df = pd.DataFrame({'Damage_Type': [damage_type], 'Hour': [hour], 'Incidents': [0]})
                hourly_damage = pd.concat([hourly_damage, missing_hour_df], ignore_index=True)

    # Now proceed with your existing plotting logic
    for damage_type in hourly_damage['Damage_Type'].unique():
        df_damage = hourly_damage[hourly_damage['Damage_Type'] == damage_type]
        fig_histogram.add_trace(go.Bar(
            x=df_damage['Hour'],
            y=df_damage['Incidents'],
            name=damage_type,
            marker_color=color_dict.get(damage_type, '#000000'),  # default color if not found
            hoverinfo='y+name'
        ))

    # Update the layout of the histogram
    fig_histogram.update_layout(
        title='Number of Incidents per Hour',
        title_x=0.35,
        xaxis=dict(
            title='Hour',
            tickmode='array',
            tickvals=list(range(24)),
            ticktext=[f"{hour}:00" for hour in range(24)],
        ),
        yaxis=dict(title='Number of Incidents'),
        plot_bgcolor='rgba(0,0,0,0)',
        paper_bgcolor='rgba(0,0,0,0)',
        bargap=0.1,
        barmode='stack'
    )    

    fig_df = px.bar(daily_damage_area, x='Incidents', y='Service_Area', orientation='h', text='Incidents', title='Total Incidents per Area',  color_discrete_sequence=['#073b4c'])
    fig_df.update_traces(texttemplate='%{text}', textposition='outside')
    fig_df.update_layout(xaxis={'visible': False, 'showticklabels': False}
                         , yaxis_title=None
                         , title_x=0.30
                         , width = 400
                         , height = 510
                         , margin=dict(l=50, r=15, t=40, b=20)  # Adjust margins if needed
    )
    with col2:
        st.plotly_chart(fig_df, use_container_width=False)

    # Arrange the histogram and donut chart side by side in Streamlit
    col3, col4 = st.columns([3,1])  # adjust the ratio as needed for your layout
    with col4:
        st.plotly_chart(fig_donut, use_container_width=False)
    with col3:
        st.plotly_chart(fig_histogram, use_container_width=True)

    # Adjust KNMI dataset 
    df_knmi_selected['Hour'] = df_knmi_selected['Hour'].apply(lambda x: f"{(x-1):02d}:00")
    df_knmi_selected['Ff'] = df_knmi_selected['Ff'] * 0.36 # in km/h
    df_knmi_selected['Rh'] = df_knmi_selected['Rh'] / 0.1 # in mm

    # Add Force
    beaufort_windscale = {
    'Force': [ 0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12],
    'Min': [0, 1, 6, 12, 20, 29, 39, 50, 62, 75, 89, 103, 117],
    'Max': [1, 5, 11, 19, 28, 38, 49, 61, 74, 88, 102, 117, 200]
    }
    beaufort_df = pd.DataFrame(beaufort_windscale)

    def find_kracht(ff_value):
        rounded_ff = round(ff_value)
        for _, row in beaufort_df.iterrows():
            if row['Min'] <= rounded_ff <= row['Max']:
                return row['Force']
        return None

    df_knmi_selected['Force'] = df_knmi_selected['Ff'].apply(find_kracht)


    # Function to convert degrees to cardinal directions
    def degrees_to_direction(deg):
        if 348.75 <= deg or deg <= 11.25:
            return 'N'
        elif 11.25 < deg <= 33.75:
            return 'NNE'
        elif 33.75 < deg <= 56.25:
            return 'NE'
        elif 56.25 < deg <= 78.75:
            return 'ENE'
        elif 78.75 < deg <= 101.25:
            return 'E'
        elif 101.25 < deg <= 123.75:
            return 'ESE'
        elif 123.75 < deg <= 146.25:
            return 'SE'
        elif 146.25 < deg <= 168.75:
            return 'SSE'
        elif 168.75 < deg <= 191.25:
            return 'S'
        elif 191.25 < deg <= 213.75:
            return 'SSW'
        elif 213.75 < deg <= 236.25:
            return 'SW'
        elif 236.25 < deg <= 258.75:
            return 'WSW'
        elif 258.75 < deg <= 281.25:
            return 'W'
        elif 281.25 < deg <= 303.75:
            return 'WNW'
        elif 303.75 < deg <= 326.25:
            return 'NW'
        elif 326.25 < deg <= 348.75:
            return 'NNW'
        
    # Apply the function to the DataFrame
    df_knmi_selected['Direction'] = df_knmi_selected['Dd'].apply(degrees_to_direction)

    # Ensure all directions are represented
    all_directions = ['N', 'NE', 'E', 'SE', 'S', 'SW', 'W', 'NW']
    missing_directions = set(all_directions) - set(df_knmi_selected['Direction'])
    missing_data = pd.DataFrame({
        'Direction': list(missing_directions),
        'Ff': [0] * len(missing_directions),
        'Force' : [0] * len(missing_directions)
    })

    # Concatenate with original DataFrame
    df_windrose = pd.concat([df_knmi_selected, missing_data]).sort_values(by='Force')
    
        
    # Windspeed and Precipitation graph
    fig_weather_graph = go.Figure()

    fig_weather_graph.add_trace(go.Scatter(
        x=df_knmi_selected['Hour'], 
        y=df_knmi_selected['Ff'], 
        mode='lines',
        name='Windspeed'
    ))

    # Add the precipitation plot (secondary y-axis)
    fig_weather_graph.add_trace(go.Scatter(
        x=df_knmi_selected['Hour'], 
        y=df_knmi_selected['Rh'],
        mode='lines',
        name='Precipitation',
        yaxis='y2'
    ))

    # Update the layout
    fig_weather_graph.update_layout(
        title='Average Windspeed and Precipitation per Hour',
        xaxis_title='Hour',
        title_x=0.34,
        yaxis=dict(
            title='Windspeed (in km/h)'
        ),
        yaxis2=dict(
            title='Precipitation (in mm)',
            overlaying='y',
            side='right'
        ),
        xaxis=dict(
            tickmode='array',
            tickvals=[f'{hour:02d}:00' for hour in range(24)],
            range=['00:00', '23:00']
        ),
        width = 1025,
        legend=dict(
        x=1.025
        # # orientation="h"
         ) 
    )

    fig_windrose = px.bar_polar(df_windrose,
                                r="Ff",
                                theta="Direction",
                                color="Force",
                                template="plotly_dark",
                                color_discrete_sequence=px.colors.sequential.swatches_continuous(),
                                category_orders={'Direction': ["N",   # North
                                    "NNE", # North-Northeast
                                    "NE",  # Northeast
                                    "ENE", # East-Northeast
                                    "E",   # East
                                    "ESE", # East-Southeast
                                    "SE",  # Southeast
                                    "SSE", # South-Southeast
                                    "S",   # South
                                    "SSW", # South-Southwest
                                    "SW",  # Southwest
                                    "WSW", # West-Southwest
                                    "W",   # West
                                    "WNW", # West-Northwest
                                    "NW",  # Northwest
                                    "NNW"  # North-Northwest
                                ]},
                                barnorm = 'percent',
                                custom_data=['Ff','Force']
    )  
    
    # Customize hover labels

    fig_windrose.update_traces(
        hovertemplate="<br>".join([
            "Wind Speed: %{customdata[0]:.1f} km/h",
            "Force: %{customdata[1]}"
        ])
    )

    fig_windrose.update_layout(
        title_text='Wind Direction and Force',
        title_x=0.25,
        width=410,
        height=440,
        legend=dict(x=1, y=1),
        margin=dict(l=55, r=40, t=90, b=50),
    )
    

    # Display in Streamlit
    col5,col6 = st.columns([3,1])
    with col5:
        st.plotly_chart(fig_weather_graph, use_container_width=False)
    with col6:
        st.plotly_chart(fig_windrose, use_container_width=False)


    
    # Create the figure
    fig_weather_condition = go.Figure()

        # Add line plots for each weather condition
    fig_weather_condition.add_trace(go.Scatter(x=df_knmi_selected['Hour'], y=df_knmi_selected['M'], mode='lines', name='Mist'))
    fig_weather_condition.add_trace(go.Scatter(x=df_knmi_selected['Hour'], y=df_knmi_selected['R'], mode='lines', name='Rain'))
    fig_weather_condition.add_trace(go.Scatter(x=df_knmi_selected['Hour'], y=df_knmi_selected['S'], mode='lines', name='Snow'))
    fig_weather_condition.add_trace(go.Scatter(x=df_knmi_selected['Hour'], y=df_knmi_selected['O'], mode='lines', name='Thunderstorm'))
    fig_weather_condition.add_trace(go.Scatter(x=df_knmi_selected['Hour'], y=df_knmi_selected['Y'], mode='lines', name='Ice Formation'))

    # Update the layout
    fig_weather_condition.update_layout(
        title='Weather Conditions per Hour',
        xaxis_title='Hour',
        # yaxis_title='Condition Occurrence (0 or 1)',
        yaxis=dict(
            tickmode='array',
            tickvals=[0, 1],
            ticktext=['No', 'Yes']
        )
        , title_x=0.38
        , width = 1025
        , height = 400
        , margin=dict(l=65)
    )
    
    # Display in Streamlit
    col7,col8 = st.columns([3,1])
    with col7:
        st.plotly_chart(fig_weather_condition, use_container_width=False)



# st.sidebar.image('brandweerlogozwart.png', use_column_width=True)
