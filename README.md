# 🚴 NYC Citi Bike Analytics Dashboard

**Interactive business intelligence platform for NYC Citi Bike operations in 2024**

## TL;DR

An interactive, multi-page Streamlit dashboard for analyzing NYC Citi Bike data. The main page provides a high-level operational overview with a searchable map, while a second page allows for detailed drill-down analysis of individual stations.

## 📊 Overview

A real-time analytical dashboard providing stakeholders with actionable insights into NYC Citi Bike service performance. It enables data-driven decision-making for operations, capacity planning, and member engagement strategies.

## 🎯 Key Features

### 🏙️ General Dashboard
- **Live Station Map**: Geographic heatmap of station traffic with a search function to quickly locate stations.
- **Temporal Trends**: Analysis of hourly and monthly ride patterns to optimize scheduling.
- **Weather Impact**: Correlation between temperature, precipitation, and ridership to forecast demand.
- **User & Fleet Breakdown**: Insights into member vs. casual users and electric vs. classic bike usage.

### 🚉 Single Station Analysis
- A dedicated page to analyze a single station's performance, including:
  - Monthly ride trends over the year.
  - A map of the most frequent inbound/outbound routes.
  - Distribution of ride durations originating from that station.

## ⚙️ Application Structure

- **`datageneration/`**: Scripts to download, process, and transform raw CSV data into an optimized Parquet data lake (`data_extraction_*.py`).
- **`data_parquet/`**: The analytical data store containing cleaned `rides`, `stations`, and `weather` data.
- **`Home_Page.py`**: The main Streamlit dashboard page, providing a high-level overview.
- **`pages/Single_Station_Analysis.py`**: The secondary page for detailed, station-specific analytics.
- **`utils/plots.py`**: A utility module containing all data processing and Plotly visualization functions used by the app.

## 🛠️ Technical Stack
- **Data Processing**: Pandas
- **Analytics & Visualization**: Plotly, Streamlit
- **Storage**: Parquet

## 📡 Data Source
- NYC Citi Bike official trip history datasets (2024)
- Open-Meteo API for historical weather data
