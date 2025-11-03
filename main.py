import pandas as pd
import streamlit as st
import plotly.express as px
import numpy as np

rides = pd.read_parquet("/home/mark/git/bike-data-newyork/data_parquet/rides.parq")
# Computing average duration distribution
rides = rides[rides["duration"] < 120]
avg_duration = px.histogram(
    rides,
    x="duration",
    color="is_m",
    color_discrete_sequence=["deepskyblue", "darkblue"],
    histnorm="percent",
    facet_row="is_electric",
    nbins=40,
)
# Computing rides distribution through months and hours
rides_reduced = (
    rides.groupby("starting_date_hour")
    .count()
    .loc["starting_date_hour", "start_station_id"]
)
rides_reduced["month"] = rides_reduced["starting_date_hour"].apply(lambda x: x.month)
rides_reduced["hour"] = rides_reduced["starting_date_hour"].apply(lambda x: x.hour)
rides_reduced = rides_reduced.loc[:, ["month", "hour", "is_electric"]]
rides_reduced = rides_reduced.rename(columns={"is_electric": "count"})
mean_hour = rides_reduced.groupby("hour").mean().reset_index(drop=True).loc[:, "count"]

mean_hour_bar = px.bar(mean_hour, x=mean_hour.index, y="count")
