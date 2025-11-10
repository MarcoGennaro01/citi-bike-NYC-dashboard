import streamlit as st
import pandas as pd
import plotly.express as px
from geopy.distance import geodesic
import numpy as np
import plotly.graph_objects as go


@st.cache_data
def rides_map():
    stations = pd.read_parquet("./data_parquet/stations.parq")
    rides = pd.read_parquet("./data_parquet/rides.parq")
    rides = rides.groupby("start_station_id").size().reset_index(name="count")
    rides = pd.merge(
        rides, stations, left_on="start_station_id", right_on=stations.index
    )
    rides_map = px.scatter_map(
        rides,
        lat="station_lat",
        lon="station_lng",
        color="count",
        size="count",
        hover_name="station_name",
        hover_data={"station_lat": False, "station_lng": False},
        labels={"count": "No. of Rides"},
        size_max=15,
        color_continuous_scale=px.colors.cyclical.IceFire,
        zoom=10,
    )
    return rides_map


def distancer(row):
    coords_1 = (row["start_lat"], row["start_lng"])
    coords_2 = (row["end_lat"], row["end_lng"])
    return geodesic(coords_1, coords_2)


@st.cache_data
def avg_distance():
    rides = (
        pd.read_parquet("./data_parquet/rides.parq")
        .groupby(["start_station_id", "end_station_id"])
        .count()
        .loc[:, "ride_id"]
        .reset_index()
        .rename(columns={"ride_id": "count"})
    )
    stations = pd.read_parquet("./data_parquet/stations.parq")

    rides = pd.merge(
        rides, stations, left_on="start_station_id", right_on=stations.index
    ).rename(columns={"station_lat": "start_lat", "station_lng": "start_lng"})

    rides = pd.merge(
        rides, stations, left_on="end_station_id", right_on=stations.index
    ).rename(columns={"station_lat": "end_lat", "station_lng": "end_lng"})

    rides = rides.loc[:, ["start_lat", "end_lat", "start_lng", "end_lng", "count"]]

    rides["distance"] = rides.apply(distancer, axis=1)
    rides = rides[rides["distance"] > 0]
    rides["distance"] = rides["distance"] * rides["count"]
    rides = rides["distance"].sum() / rides["count"].sum()
    rides = round(rides.km, 2)

    avg_distance = go.Figure()
    avg_distance.add_trace(
        go.Indicator(
            value=rides,
            gauge={"axis": {"visible": False}},
            title="Avg distance (in km)",
        )
    )
    return avg_distance


@st.cache_data
def duration_plot():
    rides = pd.read_parquet("./data_parquet/rides.parq")
    rides = rides.duration
    rides = rides[rides < 60]
    rides, bins = np.histogram(rides, bins=20)
    plot_hist_duration = px.histogram(
        x=[
            f"{round(bins[i], 2)} - {round(bins[i + 1], 2)}"
            for i in range(len(bins) - 1)
        ],
        y=rides,
        labels={"x": "Duration", "y": "Absolute frequency"},
        title="Rides duration distribution (in minutes)",
    ).update_layout(
        font_family="JetBrains Mono",
        xaxis=dict(
            showticklabels=False,  # Hide tick labels
        ),
        yaxis_title="Absolute frequency",
    )
    return plot_hist_duration


@st.cache_data
def freq_hour_plot():
    rides = pd.read_parquet("/home/mark/git/bike-data-newyork/data_parquet/rides.parq")
    # Computing rides distribution through months and hours
    rides_reduced = rides.groupby("starting_date_hour").count().reset_index(drop=False)
    rides_reduced["month"] = rides_reduced["starting_date_hour"].apply(
        lambda x: x.month
    )
    rides_reduced["hour"] = rides_reduced["starting_date_hour"].apply(lambda x: x.hour)
    rides_reduced = rides_reduced.loc[:, ["month", "hour", "is_electric"]]
    rides_reduced = rides_reduced.rename(columns={"is_electric": "count"})

    # Plotting average hourly rides
    mean_hour = (
        rides_reduced.groupby("hour")
        .mean()
        .reset_index(drop=False)
        .rename(columns={"count": "mean"})
        .loc[:, ["mean", "hour"]]
    )
    mean_hour_bar = px.bar(
        mean_hour,
        x="hour",
        y="mean",
        color_discrete_sequence=["hsv(11.53, 84.58%, 94.12%)"],
        labels={"hour": "Hour", "mean": "Avg rides"},
        title="Avg number of rides per hour",
    ).update_layout(
        font_family="JetBrains Mono",
        xaxis=dict(
            tickmode="array",
            tickvals=list(range(24)),
            ticktext=list([str(x) for x in range(1, 25)]),
        ),
    )
    return mean_hour_bar


@st.cache_data
def weather_plots():
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
    return (scatter_prcp, scatter_temp, scatter_wspd)


hide_st_style = """
            <style>
            #MainMenu {visibility: hidden;}
            footer {visibility: hidden;}
            header {visibility: hidden;}
            </style>
            """
st.markdown(hide_st_style, unsafe_allow_html=True)

st.markdown(
    """<style>
header.stAppHeader {
    background-color: transparent;
}
section.stMain .block-container {
    padding-top: 0rem;
    z-index: 1;
}
</style>""",
    unsafe_allow_html=True,
)


top_left_column, top_right_column = st.columns([5, 3])
down_left_column, down_right_columns = st.columns([5, 3])
st.set_page_config(layout="wide")

with top_left_column:
    st.plotly_chart(rides_map())
with top_right_column:
    col_1, col_2 = st.columns([1, 1])
    with col_1:
        st.plotly_chart(duration_plot())
    with col_2:
        st.plotly_chart(freq_hour_plot())
with down_left_column:
    weather_plots = weather_plots()
    option = st.selectbox(
        "Choose weather variable",
        ("Precipitations (in mm)", "Temperature (in C°)", "Wind top speed (in km/h)"),
    )
    if option == "Precipitations (in mm)":
        st.plotly_chart(weather_plots[0])
    elif option == "Temperature (in C°)":
        st.plotly_chart(weather_plots[1])
    else:
        st.plotly_chart(weather_plots[2])
