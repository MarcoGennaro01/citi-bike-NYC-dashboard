from pathlib import Path
import pandas as pd
import dask.dataframe as dd


def load_stations(stations_path: Path) -> dict:
    """Create dictionary mapping station names to their IDs."""
    df = pd.read_parquet(stations_path)
    return pd.Series(df.index, index=df.station_name.values).to_dict()


def process_file(file_path: Path, stations_map: dict, out_dir: Path) -> None:
    """Read a single CSV, transform it, and save as parquet."""
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
        parse_dates=["started_at", "ended_at"],
    )

    # Vectorized datetime calculations
    data["duration"] = (data["ended_at"] - data["started_at"]).dt.total_seconds() / 60
    data["starting_date_hour"] = data["started_at"].dt.floor("h")
    data = data.drop(columns=["started_at", "ended_at"])
    data = data.loc[data["duration"] > 0]

    # Map station names to IDs
    data["start_station_id"] = data["start_station_name"].map(stations_map)
    data["end_station_id"] = data["end_station_name"].map(stations_map)
    data = data.drop(columns=["start_station_name", "end_station_name"])

    # Safe boolean coercion
    data["is_electric"] = data["is_electric"].fillna(False).astype(bool)
    data["is_m"] = data["is_m"].fillna(False).astype(bool)

    out_dir.mkdir(parents=True, exist_ok=True)
    data.to_parquet(out_dir / (file_path.stem + ".parq"))


def main() -> None:
    repo_root = Path(__file__).resolve().parent.parent
    csv_folder = repo_root / "data" / "csvs"
    per_file_out = repo_root / "data" / "parq"
    final_out = repo_root / "data_parquet"

    stations_map = load_stations(repo_root / "data_parquet" / "stations.parq")

    for fp in sorted(csv_folder.glob("*.csv")):
        process_file(fp, stations_map, per_file_out)

    # Combine all parquet files and save as single file
    rides = dd.read_parquet(str(per_file_out / "*.parq"), index=False, engine="pyarrow")
    rides = rides.compute()
    rides = rides.reset_index(drop=True)
    final_out.mkdir(parents=True, exist_ok=True)
    rides.to_parquet(final_out / "rides.parq")


if __name__ == "__main__":
    main()
