import streamlit as st
import pandas as pd
import plotly.express as px
from geopy.distance import geodesic
import numpy as np
import plotly.graph_objects as go


def distancer(row):
    """Calculate geodesic distance between two coordinate pairs."""
    coords_1 = (row["start_lat"], row["start_lng"])
    coords_2 = (row["end_lat"], row["end_lng"])
    return geodesic(coords_1, coords_2)


@st.cache_data
def rides_map():
    """Map of ride counts by starting station."""
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
        color_continuous_scale="Reds",
        zoom=10,
        map_style="dark",
    ).update_layout(
        showlegend=False,
        coloraxis_showscale=False,
        margin=dict(l=0, r=0, t=0, b=0),
        height=600,
    )
    return rides_map


@st.cache_data
def avg_distance():
    """Calculate and display average ride distance."""
    rides = (
        pd.read_parquet("./data_parquet/rides.parq")
        .groupby(["start_station_id", "end_station_id"])
        .count()
        .loc[:, "ride_id"]
        .reset_index()
        .rename(columns={"ride_id": "count"})
    )
    stations = pd.read_parquet("./data_parquet/stations.parq")

    # Merge start station data
    rides = pd.merge(
        rides, stations, left_on="start_station_id", right_on=stations.index
    ).rename(columns={"station_lat": "start_lat", "station_lng": "start_lng"})

    # Merge end station data
    rides = pd.merge(
        rides, stations, left_on="end_station_id", right_on=stations.index
    ).rename(columns={"station_lat": "end_lat", "station_lng": "end_lng"})

    rides = rides.loc[:, ["start_lat", "end_lat", "start_lng", "end_lng", "count"]]

    # Calculate weighted average distance
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
    ).update_layout(height=250)
    return avg_distance


@st.cache_data
def duration_plot():
    """Histogram of ride duration distribution (under 60 minutes)."""
    rides = pd.read_parquet("./data_parquet/rides.parq")
    rides = rides.loc[rides.duration < 60, "duration"]
    rides, bins = np.histogram(rides, bins=20)

    plot_hist_duration = px.histogram(
        x=[
            f"{round(bins[i], 1)}-{round(bins[i + 1], 1)}" for i in range(len(bins) - 1)
        ],
        y=rides,
        labels={"x": "Duration (min)", "y": "Count"},
        title="Ride duration",
    ).update_layout(
        font_family="JetBrains Mono",
        xaxis_showticklabels=False,
        yaxis_title="Count",
    )
    return plot_hist_duration


@st.cache_data
def freq_hour_plot():
    """Bar chart of average rides per hour."""
    rides = pd.read_parquet("./data_parquet/rides.parq")
    rides["hour"] = rides["starting_date_hour"].dt.hour

    mean_hour = rides.groupby("hour").size().reset_index(name="count")
    mean_hour_bar = px.bar(
        mean_hour,
        x="hour",
        y="count",
        color_discrete_sequence=["hsv(11.53, 84.58%, 94.12%)"],
        labels={"hour": "Hour", "count": "Rides"},
        title="Rides per hour",
    ).update_layout(
        font_family="JetBrains Mono",
        xaxis=dict(tickvals=list(range(24)), ticktext=[str(x) for x in range(24)]),
    )
    return mean_hour_bar


@st.cache_data
def freq_month_plot():
    """Bar chart of average rides per month."""
    rides = pd.read_parquet("./data_parquet/rides.parq")
    rides["month"] = rides["starting_date_hour"].dt.month

    mean_month = rides.groupby("month").size().reset_index(name="count")

    months = [
        "Jan",
        "Feb",
        "Mar",
        "Apr",
        "May",
        "Jun",
        "Jul",
        "Aug",
        "Sep",
        "Oct",
        "Nov",
        "Dec",
    ]

    mean_month_bar = px.bar(
        mean_month,
        x="month",
        y="count",
        color_discrete_sequence=["hsv(11.53, 84.58%, 94.12%)"],
        labels={"count": "Rides", "month": ""},
        title="Rides per month",
    ).update_layout(
        font_family="JetBrains Mono",
        xaxis=dict(tickvals=list(range(1, 13)), ticktext=months),
    )
    return mean_month_bar


@st.cache_data
def weather_plots():
    """Generate scatter plots for weather impact on ride counts."""
    weather_data = pd.read_parquet("./data_parquet/weather.parq")
    rides = pd.read_parquet("./data_parquet/rides.parq")
    # Filter rides from 7am-8pm in 2023+
    rides = rides.groupby("starting_date_hour").size().reset_index(name="count")
    rides = rides[
        (rides["starting_date_hour"].dt.year >= 2023)
        & (rides["starting_date_hour"].dt.hour.between(7, 20))
    ]

    # Temperature impact
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


@st.cache_data
def pies_plot():
    """Pie charts for member and electric bike distribution."""
    rides = pd.read_parquet("./data_parquet/rides.parq")

    member_data = (
        rides.groupby("is_m")
        .size()
        .reset_index(name="count")
        .replace({"is_m": {False: "Not Member", True: "Member"}})
    )
    member_pie = px.pie(
        member_data,
        names="is_m",
        values="count",
        color="is_m",
        color_discrete_map={
            "Not Member": "hsv(63, 18.43%, 85.1%)",
            "Member": "hsv(11.53, 84.58%, 94.12%)",
        },
        title="Member",
    ).update_layout(showlegend=False)

    electric_data = (
        rides.groupby("is_electric")
        .size()
        .reset_index(name="count")
        .replace({"is_electric": {False: "Not Electric", True: "Electric"}})
    )
    electric_pie = px.pie(
        electric_data,
        names="is_electric",
        values="count",
        color="is_electric",
        color_discrete_map={
            "Not Electric": "hsv(197.18, 100%, 86.27%)",
            "Electric": "hsv(241.58, 77.55%, 19.22%)",
        },
        title="Electric",
    ).update_layout(showlegend=False)
    return (member_pie, electric_pie)


st.set_page_config(layout="wide")
st.markdown(
    """<style>
#MainMenu {visibility: hidden;} footer {visibility: hidden;} header {visibility: hidden;}
header.stAppHeader {background-color: transparent;}
section.stMain .block-container {padding-top: 0; z-index: 1;}
</style>""",
    unsafe_allow_html=True,
)

top_left_column, top_right_column = st.columns([2, 1], border=True)

with top_left_column:
    st.plotly_chart(rides_map(), use_container_width=True)

with top_right_column:
    weather_data = weather_plots()
    idx = st.selectbox(
        "Weather:",
        (0, 1, 2),
        format_func=lambda x: ["Precipitation", "Temperature", "Wind Speed"][x],
    )
    st.plotly_chart(weather_data[idx])

down_left_column, down_right_column = st.columns([2, 1], border=True)

with down_left_column:
    tab1, tab2, tab3 = st.tabs(["Duration", "Hourly", "Monthly"])
    with tab1:
        st.plotly_chart(duration_plot())
    with tab2:
        st.plotly_chart(freq_hour_plot())
    with tab3:
        st.plotly_chart(freq_month_plot())

with down_right_column:
    pies = pies_plot()
    tab1p, tab2p = st.tabs(["Electric", "Member"])
    with tab1p:
        st.plotly_chart(pies[0])
    with tab2p:
        st.plotly_chart(pies[1])
