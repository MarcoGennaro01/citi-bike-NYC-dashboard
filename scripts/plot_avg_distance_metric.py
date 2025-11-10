import pandas as pd
from geopy.distance import geodesic
import plotly.graph_objects as go
import plotly.express as px
import pandas as pd


def distancer(row):
    coords_1 = (row["start_lat"], row["start_lng"])
    coords_2 = (row["end_lat"], row["end_lng"])
    return geodesic(coords_1, coords_2)


rides = (
    pd.read_parquet("../data_parquet/rides.parq")
    .groupby(["start_station_id", "end_station_id"])
    .count()
    .loc[:, "ride_id"]
    .reset_index()
    .rename(columns={"ride_id": "count"})
)
stations = pd.read_parquet("../data_parquet/stations.parq")

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
        value=rides, gauge={"axis": {"visible": False}}, title="Avg distance (in km)"
    )
)
avg_distance.write_html("../html/plot_avg_distance_metric.html")
