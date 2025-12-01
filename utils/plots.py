import streamlit as st
import pandas as pd
import plotly.express as px
import numpy as np


@st.cache_data
def rides_map(search_term=None):
    """Map of ride counts by starting station, with optional search filter."""
    stations = pd.read_parquet("./data_parquet/stations.parq")
    rides = pd.read_parquet("./data_parquet/rides.parq")
    rides = rides.groupby("start_station_id").size().reset_index(name="count")
    rides = pd.merge(
        rides,
        stations,
        left_on="start_station_id",
        right_on=stations.index,
    )
    if search_term:
        rides = rides[
            rides["station_name"].str.contains(search_term, case=False, na=False)
        ]
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


def stations_freq_table():
    stations = pd.read_parquet("./data_parquet/stations.parq")
    rides = pd.read_parquet("./data_parquet/rides.parq")
    rides = rides.groupby("start_station_id").size().reset_index(name="count")
    rides = pd.merge(
        rides,
        stations,
        left_on="start_station_id",
        right_on=stations.index,
        how="right",
    )
    rides = rides.loc[:, ["station_name", "count"]].rename(
        columns={"count": "N. of Rides", "station_name": "Station"}
    )

    rides = rides.sort_values("N. of Rides", ascending=False)
    rides = rides.reset_index(drop=True)
    rides.index += 1
    rides.loc[:, "N. of Rides"] = np.nan_to_num(rides.loc[:, "N. of Rides"])
    table_rides = st.dataframe(rides)
    return table_rides


@st.cache_data
def duration_plot(station_arg=None):
    """Histogram of ride duration distribution (under 60 minutes)."""
    rides = pd.read_parquet("./data_parquet/rides.parq")
    rides = rides.loc[rides.duration < 60, ["duration", "start_station_id"]]
    plot_title = "Ride duration"
    if station_arg:
        stations = pd.read_parquet("./data_parquet/stations.parq")
        rides = pd.merge(
            rides,
            stations,
            left_on="start_station_id",
            right_on=stations.index,
        )
        rides = rides[rides["station_name"] == station_arg]
        plot_title = f"Ride Duration from {station_arg}"

    counts, bins = np.histogram(rides["duration"], bins=21)
    plot_hist_duration = px.histogram(
        x=[
            f"{round(bins[i], 1)}-{round(bins[i + 1], 1)}" for i in range(len(bins) - 1)
        ],
        y=counts,
        labels={"x": "Duration (min)", "y": "Count"},
        title=plot_title,
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
    member_pie = (
        px.pie(
            member_data,
            names="is_m",
            values="count",
            labels={"count": "Total"},
            hover_name="is_m",
            color_discrete_sequence=[
                "hsv(11.53, 84.58%, 94.12%)",
                "hsv(63, 18.43%, 85.1%)",
            ],
            title="Percentage of members vs non-members",
            hover_data={"is_m": False},
        )
        .update_traces(marker=dict(line=dict(color="white", width=0)))
        .update_layout(
            showlegend=False,
            font_family="JetBrains Mono",
        )
    )

    electric_data = (
        rides.groupby("is_electric")
        .size()
        .reset_index(name="count")
        .replace({"is_electric": {False: "Not Electric", True: "Electric"}})
    )
    electric_pie = (
        px.pie(
            electric_data,
            names="is_electric",
            values="count",
            labels={"count": "Total"},
            hover_name="is_electric",
            hover_data={"is_electric": False},
            color_discrete_sequence=[
                "hsv(241.58, 77.55%, 19.22%)",
                "hsv(197.18, 100%, 86.27%)",
            ],
            title="Percentage of rides with electric bikes and classic bikes",
        )
        .update_traces(marker=dict(line=dict(color="white", width=0)))
        .update_layout(
            showlegend=False,
            font_family="JetBrains Mono",
        )
    )
    return (member_pie, electric_pie)


def get_unique_stations():
    stations = pd.read_parquet("./data_parquet/stations.parq")["station_name"]
    return stations


@st.cache_data
def time_series_rides(station_selected=None):
    stations = pd.read_parquet("./data_parquet/stations.parq")
    station_filtered = stations[stations["station_name"] == station_selected]
    rides = pd.read_parquet("./data_parquet/rides.parq")
    rides = rides[rides["start_station_id"].isin(station_filtered.index)]
    rides["month"] = rides["starting_date_hour"].dt.month

    # Count rides per month
    ride_counts = rides.groupby("month").size().reset_index(name="count")

    # Create a full range of months (1-12)
    all_months = pd.DataFrame({"month": range(1, 13)})

    # Merge to ensure all months are present, filling missing counts with 0
    merged_counts = pd.merge(all_months, ride_counts, on="month", how="left").fillna(0)

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

    time_series_chart = px.line(
        merged_counts,
        x="month",
        y="count",
        title=f"Monthly Rides for {station_selected}",
        labels={"count": "Rides", "month": ""},
        hover_data={"month": False, "count": True},
    ).update_layout(
        font_family="JetBrains Mono",
        hovermode="x",
        xaxis=dict(tickvals=list(range(1, 13)), ticktext=months),
    )
    return time_series_chart
