import dask.dataframe as dd  # Import Dask for distributed processing

stations = dd.read_csv(
    "./data/csvs/*.csv",  # Read multiple CSV files with station data
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

station_names = dd.concat(
    [
        stations["start_station_name"],  # Combine start/end station names
        stations["end_station_name"],
    ],
    axis=0,
)
latitudes = dd.concat(
    [stations["start_lat"], stations["end_lat"]], axis=0
)  # Combine start/end latitudes
longitudes = dd.concat(
    [stations["start_lng"], stations["end_lng"]], axis=0
)  # Combine start/end longitudes

stacked_stations = dd.concat(
    [station_names, latitudes, longitudes], axis=1
)  # Merge all columns into one DataFrame

stacked_stations.columns = [
    "station_name",
    "station_lat",
    "station_lng",
]  # Rename columns for clarity

stacked_stations = stacked_stations.dropna()  # Remove rows with missing values

stacked_stations = stacked_stations.drop_duplicates()  # Remove duplicate stations

stacked_stations = stacked_stations.sort_values(
    "station_name"
)  # Sort alphabetically by station name

stacked_stations = stacked_stations.reset_index(
    drop=True
)  # Reset index to sequential numbers
stacked_stations.index.name = "station_id"  # Name index as station_id

stacked_stations = stacked_stations.compute()  # Execute all operations
stacked_stations = stacked_stations.reset_index(drop=True)

stacked_stations.to_parquet("./cleaned_data/stations.parquet")  # Save to parquet format
