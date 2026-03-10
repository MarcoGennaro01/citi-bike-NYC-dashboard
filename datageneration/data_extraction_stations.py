from pathlib import Path
import pandas as pd
import dask.dataframe as dd


RENAME_START = {
    "start_station_name": "station_name",
    "start_lat": "station_lat",
    "start_lng": "station_lng",
}
RENAME_END = {
    "end_station_name": "station_name",
    "end_lat": "station_lat",
    "end_lng": "station_lng",
}


def extract_stations(csv_pattern: str) -> pd.DataFrame:
    """Read all CSVs, stack start/end stations, deduplicate, and return a clean DataFrame."""
    raw = dd.read_csv(
        csv_pattern,
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

    start = raw[list(RENAME_START)].rename(columns=RENAME_START)
    end = raw[list(RENAME_END)].rename(columns=RENAME_END)

    stacked = dd.concat([start, end], axis=0).dropna()

    result = (
        stacked.drop_duplicates(subset="station_name")
        .compute()
        .sort_values("station_name")
        .reset_index(drop=True)
    )
    result.index.name = "station_id"
    return result


def main() -> None:
    repo_root = Path(__file__).resolve().parent.parent
    csv_pattern = str(repo_root / "data" / "csvs" / "*.csv")
    out_path = repo_root / "data_parquet" / "stations.parq"

    out_path.parent.mkdir(parents=True, exist_ok=True)

    stations = extract_stations(csv_pattern)
    stations.to_parquet(out_path)


if __name__ == "__main__":
    main()
