from datetime import datetime
from meteostat import hourly
from pathlib import Path
import pandas as pd


def download_data() -> pd.DataFrame:
    start = datetime(2024, 1, 1)
    end = datetime(2024, 12, 31, 23, 59)

    weather_data = hourly("KJRB0", start, end)

    weather_data = weather_data.fetch()

    return weather_data


def process_export_data(weather_data: pd.DataFrame, out_path: Path) -> None:
    weather_data["hour"] = weather_data.index.hour
    weather_data.to_parquet(out_path / "weather.parq")


def main() -> None:
    repo_root = Path(__file__).resolve().parent.parent
    out_path = repo_root / "data_parquet"
    data = download_data()
    process_export_data(data, out_path)


if __name__ == "__main__":
    main()
