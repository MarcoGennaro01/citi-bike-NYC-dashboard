# NYC Citi Bike Analytics Dashboard

Interactive Streamlit dashboard for exploring NYC Citi Bike operations in 2024.

## TL;DR

This project provides a single-page analytics dashboard built with Streamlit and Plotly. It combines system-wide metrics with station-specific drill-downs, so you can move from a citywide overview to one selected station without leaving the main page.

## Overview

The dashboard is designed for operational analysis of Citi Bike activity across 2024. It supports quick exploration of ride volume, seasonality, weather sensitivity, station-level demand, and rider or bike-type composition using preprocessed Parquet datasets.

## Key Features

### Unified Dashboard
- Station selector in the header for drilling into a single station without navigating to a separate page.
- Split selector for comparing ride behavior by Membership, Electric, or Both across multiple charts.
- Station map and frequency table for identifying the busiest origins.
- Weather analysis for precipitation, temperature, and wind speed against ride counts.
- Pie charts for member mix and electric-bike usage.

### Temporal Analysis
- Monthly station time series shown directly in the header when a station is selected.
- Ride duration histogram with stacked splits by membership, electric usage, or both.
- Hourly and monthly ride charts with the same split logic reused across views.

## Application Structure

- `datageneration/`: Scripts to download, clean, and transform the raw Citi Bike and weather data into Parquet files.
- `data_parquet/`: Optimized analytical datasets for rides, stations, and weather.
- `Home_Page.py`: The main Streamlit app, including both global and station-level analysis.
- `utils/plots.py`: Shared data-loading, filtering, aggregation, and Plotly chart helpers.

## Technical Stack

- Data Processing: Pandas, NumPy
- Analytics and Visualization: Plotly, Streamlit
- Storage: Parquet

## Data Sources

- NYC Citi Bike official trip history datasets for 2024
- Open-Meteo API for historical weather data
