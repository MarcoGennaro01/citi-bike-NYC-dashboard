import os
import pandas as pd
import dask.dataframe as dd


# Create dictionary mapping station names to their IDs
stations = pd.read_parquet("./data_parquet/stations.parq")
stations = pd.Series(stations.index, index=stations.station_name.values).to_dict()

folder = "./data/csvs"

# Process each CSV file in the folder
for filename in os.listdir(folder):
    file_path = os.path.join(folder, filename)
    # Read only needed columns from CSV
    data = pd.read_csv(
        file_path,
        low_memory=True,
        usecols=[
            "started_at",
            "ended_at",
            "is_m",
            "is_electric",
            "start_station_name",
            "end_station_name",
        ],
    )

    # Convert timestamps to datetime, compute duration column, dropping started_at and ended_at columns, dropping negative durations
    data["started_at"] = pd.to_datetime(data["started_at"])
    data["ended_at"] = pd.to_datetime(data["ended_at"])

    data["duration"] = data["ended_at"] - data["started_at"]
    data["duration"] = [x.total_seconds() / 60 for x in data["duration"]]

    data["starting_date_hour"] = data["started_at"].apply(
        lambda x: x.replace(minute=0, second=0, microsecond=0)
    )

    data.drop(["started_at", "ended_at"], inplace=True, axis=1)

    data = data.loc[data["duration"] > 0]

    # Replace station names with their IDs
    data["start_station_name"] = data["start_station_name"].replace(to_replace=stations)
    data["end_station_name"] = data["end_station_name"].replace(to_replace=stations)

    # Rename columns to reflect they now contain IDs
    data = data.rename(
        columns={
            "start_station_name": "start_station_id",
            "end_station_name": "end_station_id",
        }
    )
    data.index.name = "ride_id"

    # Convert to boolean type
    data["is_electric"] = data["is_electric"].astype("bool")
    data["is_m"] = data["is_m"].astype("bool")

    # Save processed file as parquet
    path = os.path.join("./data/parq", filename.replace(".csv", ".parq"))
    data.to_parquet(path)

# Combine all parquet files and save as single file
rides = dd.read_parquet("./data/parq/*.parq", index=False, engine="pyarrow")
rides = rides.compute()
rides = rides.reset_index(drop=True)
rides.to_parquet("./data_parquet/rides.parq")
