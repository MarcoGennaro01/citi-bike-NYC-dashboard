from datetime import datetime
from meteostat import Hourly

start = datetime(2024, 1, 1)
end = datetime(2024, 12, 31, 23, 59)
weather_data = Hourly("KJRB0", start, end)
weather_data = weather_data.fetch()
weather_data["hour"] = [x.hour for x in weather_data.index]
weather_data.to_parquet("./data_parquet/weather.parquet")
