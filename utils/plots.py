import streamlit as st
import pandas as pd
import plotly.express as px
import numpy as np


MONTH_LABELS = [
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


def load_rides():
    return pd.read_parquet("./data_parquet/rides.parq")


def load_stations():
    return pd.read_parquet("./data_parquet/stations.parq")


def search_term_filter(rides, search_term):
    if search_term:
        rides = rides[
            rides["station_name"].str.contains(search_term, case=False, na=False)
        ]
    return rides


def merge_stations_names(rides, stations):
    rides = pd.merge(
        rides, stations, left_on="start_station_id", right_on=stations.index
    )
    return rides


def add_split_column(rides, split_by=None):
    if split_by == "Membership":
        rides["split"] = rides["is_m"].map({True: "Member", False: "Not Member"})
    elif split_by == "Electric":
        rides["split"] = rides["is_electric"].map({True: "Electric", False: "Classic"})
    elif split_by == "Both":
        membership = rides["is_m"].map({True: "Member", False: "Not Member"})
        electric = rides["is_electric"].map({True: "Electric", False: "Classic"})
        rides["split"] = membership + " / " + electric
    return rides


def split_plot_config(split_by=None):
    if split_by == "Membership":
        return {
            "category_orders": {"split": ["Member", "Not Member"]},
            "color_discrete_map": {
                "Member": "hsv(11.53, 84.58%, 94.12%)",
                "Not Member": "hsv(63, 18.43%, 85.1%)",
            },
        }
    if split_by == "Electric":
        return {
            "category_orders": {"split": ["Electric", "Classic"]},
            "color_discrete_map": {
                "Electric": "hsv(241.58, 77.55%, 19.22%)",
                "Classic": "hsv(197.18, 100%, 86.27%)",
            },
        }
    if split_by == "Both":
        categories = [
            "Member / Electric",
            "Member / Classic",
            "Not Member / Electric",
            "Not Member / Classic",
        ]
        return {
            "category_orders": {"split": categories},
            "color_discrete_map": {
                "Member / Electric": "hsv(197.18, 100%, 86.27%)",
                "Member / Classic": "hsv(63, 18.43%, 85.1%)",
                "Not Member / Electric": "hsv(213.48, 100%, 70.98%)",
                "Not Member / Classic": "hsv(11.53, 84.58%, 94.12%)",
            },
        }
    return {}


def monthly_ride_counts(rides, split_by=None):
    rides = rides.copy()
    rides["month"] = rides["starting_date_hour"].dt.month
    all_months = pd.DataFrame({"month": range(1, 13)})
    if not split_by:
        counts = rides.groupby("month").size().reset_index(name="count")
        return pd.merge(all_months, counts, on="month", how="left").fillna(0)

    rides = add_split_column(rides, split_by)
    counts = rides.groupby(["month", "split"]).size().reset_index(name="count")
    split_order = split_plot_config(split_by)["category_orders"]["split"]
    full_index = pd.MultiIndex.from_product(
        [all_months["month"], split_order], names=["month", "split"]
    )
    counts = (
        counts.set_index(["month", "split"])
        .reindex(full_index, fill_value=0)
        .reset_index()
    )
    return counts


@st.cache_data
def rides_map(search_term=None):
    """Map of ride counts by starting station, with optional search filter."""
    rides, stations = load_rides(), load_stations()
    rides = rides.groupby("start_station_id").size().reset_index(name="count")
    rides = merge_stations_names(rides, stations)
    rides = search_term_filter(rides, search_term)
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
def stations_freq_table(search_term=None):
    rides, stations = load_rides(), load_stations()
    rides = rides.groupby("start_station_id").size().reset_index(name="count")
    rides = merge_stations_names(rides, stations)
    rides = search_term_filter(rides, search_term)
    rides = rides.loc[:, ["station_name", "count"]].rename(
        columns={"count": "N. of Rides", "station_name": "Station"}
    )
    rides = rides.sort_values("N. of Rides", ascending=False)
    rides = rides.reset_index(drop=True)
    rides.index += 1
    rides.loc[:, "N. of Rides"] = np.nan_to_num(rides.loc[:, "N. of Rides"])
    return rides


@st.cache_data
def duration_plot(search_term=None, split_by=None):
    """Histogram of ride duration distribution (under 60 minutes)."""
    rides, stations = load_rides(), load_stations()
    rides = merge_stations_names(rides, stations)
    rides = search_term_filter(rides, search_term)
    rides = rides.loc[rides.duration < 60, ["duration", "is_m", "is_electric"]].copy()
    rides = add_split_column(rides, split_by)
    plot_kwargs = split_plot_config(split_by)
    plot_hist_duration = px.histogram(
        rides,
        x="duration",
        color="split" if split_by else None,
        nbins=21,
        barmode="stack",
        labels={"duration": "Duration (min)", "count": "Rides", "split": split_by},
        title="Ride Duration",
        **plot_kwargs,
    ).update_layout(
        font_family="JetBrains Mono",
        yaxis_title="Count",
    )
    return plot_hist_duration


@st.cache_data
def freq_hour_plot(search_term=None, split_by=None):
    """Bar chart of average rides per hour."""
    rides, stations = load_rides(), load_stations()
    rides = merge_stations_names(rides, stations)
    rides = search_term_filter(rides, search_term)
    rides = add_split_column(rides, split_by)
    rides["hour"] = rides["starting_date_hour"].dt.hour
    group_columns = ["hour", "split"] if split_by else ["hour"]
    mean_hour = rides.groupby(group_columns).size().reset_index(name="count")
    plot_kwargs = split_plot_config(split_by)
    mean_hour_bar = px.bar(
        mean_hour,
        x="hour",
        y="count",
        color="split" if split_by else None,
        barmode="stack",
        labels={"hour": "Hour", "count": "Rides"},
        title="Rides per hour",
        **plot_kwargs,
    ).update_layout(
        font_family="JetBrains Mono",
        xaxis=dict(tickvals=list(range(24)), ticktext=[str(x) for x in range(24)]),
    )
    return mean_hour_bar


@st.cache_data
def freq_month_plot(search_term=None, split_by=None):
    """Bar chart of average rides per month."""
    rides, stations = load_rides(), load_stations()
    rides = merge_stations_names(rides, stations)
    rides = search_term_filter(rides, search_term)
    mean_month = monthly_ride_counts(rides, split_by)
    plot_kwargs = split_plot_config(split_by)
    mean_month_bar = px.bar(
        mean_month,
        x="month",
        y="count",
        color="split" if split_by else None,
        barmode="stack",
        labels={"count": "Rides", "month": ""},
        title="Rides per month",
        **plot_kwargs,
    ).update_layout(
        font_family="JetBrains Mono",
        xaxis=dict(tickvals=list(range(1, 13)), ticktext=MONTH_LABELS),
    )
    return mean_month_bar


@st.cache_data
def weather_plots(search_term=None):
    """Generate scatter plots for weather impact on ride counts."""
    weather_data = pd.read_parquet("./data_parquet/weather.parq")
    rides, stations = load_rides(), load_stations()
    rides = merge_stations_names(rides, stations)
    rides = search_term_filter(rides, search_term)
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
    ).update_layout(
        font_family="JetBrains Mono", hovermode="x"
    )  # Compute precipitation data and scatter plot
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
def pies_plot(search_term=None):
    """Pie charts for member and electric bike distribution."""
    rides, stations = load_rides(), load_stations()
    rides = merge_stations_names(rides, stations)
    rides = search_term_filter(rides, search_term)
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
def time_series_rides(station_selected=None, split_by=None):
    rides, stations = load_rides(), load_stations()
    stations = stations[stations["station_name"] == station_selected]
    rides = rides[rides["start_station_id"].isin(stations.index)]
    merged_counts = monthly_ride_counts(rides, split_by)
    plot_kwargs = split_plot_config(split_by)
    time_series_chart = px.line(
        merged_counts,
        x="month",
        y="count",
        color="split" if split_by else None,
        title=f"Monthly Rides for {station_selected}",
        labels={"count": "Rides", "month": "", "split": split_by},
        hover_data={"month": False, "count": True},
        **plot_kwargs,
    ).update_layout(
        font_family="JetBrains Mono",
        hovermode="x",
        xaxis=dict(tickvals=list(range(1, 13)), ticktext=MONTH_LABELS),
    )
    return time_series_chart
