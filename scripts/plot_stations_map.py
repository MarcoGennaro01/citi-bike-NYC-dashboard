import plotly.express as px
import pandas as pd

stations = pd.read_parquet("../data_parquet/stations.parq")
rides = pd.read_parquet("../data_parquet/rides.parq")
rides = rides.groupby("start_station_id").size().reset_index(name="count")
rides = pd.merge(rides, stations, left_on="start_station_id", right_on=stations.index)
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

rides_map.write_html("../html/plot_stations_map.html")
