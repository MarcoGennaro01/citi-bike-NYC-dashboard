import dask.dataframe as dd

stations = dd.read_csv(
    "./data/csvs/*.csv",
    usecols=[
        "start_station_name",
        "end_station_name",
        "start_lat",
        "start_lng",
        "end_lat",
        "end_lng",
    ],
    low_memory=True,
)


# Properly combine station data with aligned coordinates
start_stations = stations[["start_station_name", "start_lat", "start_lng"]].rename(
    columns={
        "start_station_name": "station_name",
        "start_lat": "station_lat",
        "start_lng": "station_lng",
    }
)

end_stations = stations[["end_station_name", "end_lat", "end_lng"]].rename(
    columns={
        "end_station_name": "station_name",
        "end_lat": "station_lat",
        "end_lng": "station_lng",
    }
)

# Concatenate start and end stations
stacked_stations = dd.concat([start_stations, end_stations], axis=0)

# Remove duplicates and clean data
stacked_stations = stacked_stations.dropna()
stacked_stations = stacked_stations.drop_duplicates(subset="station_name")
stacked_stations = stacked_stations.sort_values("station_name")
stacked_stations = stacked_stations.reset_index(drop=True)
stacked_stations.index.name = "station_id"

stacked_stations = stacked_stations.compute()
stacked_stations = stacked_stations.reset_index(drop=True)

stacked_stations.to_parquet("./data_parquet/stations.parq")
