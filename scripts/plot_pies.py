import pandas as pd
import streamlit as st
import plotly.express as px
from geopy.distance import geodesic

rides = pd.read_parquet("../data_parquet/rides.parq")
stations = pd.read_parquet("../data_parquet/stations.parq")


member_pie = px.pie(
    rides.groupby("is_m")
    .size()
    .reset_index(name="count")
    .loc[:, ["is_m", "count"]]
    .replace({False: "Not Member", True: "Member"}),
    color="is_m",
    values="count",
    hover_name="is_m",
    color_discrete_map={
        "Member": "hsv(11.53, 84.58%, 94.12%)",
        "Not Member": "hsv(63, 18.43%, 85.1%)",
    },
)

electric_pie = px.pie(
    rides.groupby("is_electric")
    .size()
    .reset_index(name="count")
    .loc[:, ["is_electric", "count"]]
    .replace({False: "Not Electric", True: "Electric"}),
    color="is_electric",
    values="count",
    hover_name="is_electric",
    color_discrete_map={
        "Electric": "hsv(241.58, 77.55%, 19.22%)",
        "Not Electric": "hsv(197.18, 100%, 86.27%)",
    },
)
member_pie.write_html("../html/plot_pies_member_pie.html")
electric_pie.write_html("../html/plot_pies_electric_pie.html")
