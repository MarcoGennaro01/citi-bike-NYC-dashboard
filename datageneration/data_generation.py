import subprocess

# Calling bash commands for extracting and first step of transformation
subprocess.call("bashcommands")

# Creating stations.parq
import data_extraction_stations

# Creating rides.parq
import data_extraction_rides

# Creating weather.parq
import data_extraction_weather
