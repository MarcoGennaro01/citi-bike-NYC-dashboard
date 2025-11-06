import pandas as pd
import streamlit as st
import plotly.express as px
from geopy.distance import geodesic

weather_data = pd.read_parquet("./data_parquet/weather.parq")
rides = pd.read_parquet("./data_parquet/rides.parq")
# Compute and group rides, selecting only from 8am to 8pm
rides = rides.groupby("starting_date_hour").size().reset_index(name="count")
rides = rides[
    (rides["starting_date_hour"].dt.year >= 2023)
    & (rides["starting_date_hour"].dt.hour.between(7, 20))
]

# Compute temp data and scatter plot
rides["hour_before"] = rides["starting_date_hour"] - pd.Timedelta(hours=1)
temp_data = pd.merge(
    rides, weather_data, left_on="hour_before", right_on=weather_data.index
)
temp_data["temp"] = round(temp_data["temp"], 1)
temp_data = temp_data.groupby(temp_data["temp"])["count"].mean().reset_index()
temp_data["count"] = round(temp_data["count"])
scatter_temp = px.scatter(
    temp_data,
    x=temp_data["temp"],
    y=temp_data["count"],
    labels={"temp": "Temperature (in C°)", "count": "Avg rides per hour"},
    color_discrete_sequence=["hsv(11.53, 84.58%, 94.12%)"],
).update_layout(font_family="JetBrains Mono", hovermode="x")


# Compute precipitation data and scatter plot
prcp_data = pd.merge(
    rides, weather_data, left_on="hour_before", right_on=weather_data.index
)
prcp_data["prcp"] = round(prcp_data["prcp"], 1)
prcp_data = prcp_data.groupby(prcp_data["prcp"])["count"].mean().reset_index()
prcp_data["count"] = round(prcp_data["count"])
scatter_prcp = px.scatter(
    prcp_data,
    labels={"count": "Avg rides per hour", "prcp": "Precipitations (in mm)"},
    y=prcp_data["count"],
    x=prcp_data["prcp"],
    color_discrete_sequence=["hsv(213.48, 100%, 70.98%)"],
).update_layout(font_family="JetBrains Mono", hovermode="x")

# Compute wind speed data and scatter plot
wspd_data = pd.merge(
    rides, weather_data, left_on="starting_date_hour", right_on=weather_data.index
)
wspd_data["wspd"] = round(wspd_data["wspd"], 1)
wspd_data = wspd_data.groupby(wspd_data["wspd"])["count"].mean().reset_index()
wspd_data["count"] = round(wspd_data["count"])
scatter_wspd = px.scatter(
    wspd_data,
    labels={"count": "Avg rides per hour", "wspd": "Peak Wind Speed (in km/h)"},
    y=wspd_data["count"],
    x=wspd_data["wspd"],
    color_discrete_sequence=["hsv(213.48, 100%, 70.98%)"],
).update_layout(font_family="JetBrains Mono", hovermode="x")
