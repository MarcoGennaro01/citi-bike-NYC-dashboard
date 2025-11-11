# NYC Citi Bike Analytics Dashboard

**Interactive business intelligence platform for NYC Citi Bike operations in 2024**

## Overview

Real-time analytical dashboard providing stakeholders with actionable insights into NYC Citi Bike service performance. Enables data-driven decision-making for operations, capacity planning, and member engagement strategies.

## Key Metrics & Features

### Operational Intelligence
- **Station Performance**: Geographic heatmap identifying high-traffic stations and demand patterns
- **Trip Duration Analysis**: Distribution of ride lengths to optimize bike fleet allocation
- **Temporal Trends**: Hourly and monthly ride patterns for workforce scheduling and maintenance windows

### Environmental & Market Insights
- **Weather Impact Analysis**: Correlation between precipitation, temperature, and ridership to forecast demand
- **Fleet Utilization**: Electric vs. standard bike adoption rates and member vs. casual user breakdown

### Business Value
- Identify peak demand periods for targeted marketing and dynamic pricing
- Optimize maintenance schedules during low-traffic windows
- Plan capacity expansion based on geographic demand clustering
- Track electric bike adoption as key growth metric

## Data Pipeline

```
Raw CSV Data (Monthly Citi Bike feeds)
    ↓
[rides.py] - Transforms transaction records (station mapping, duration calculation)
    ↓
[stations.py] - Builds geographic database (deduplication, geocoding)
    ↓
[weather.py] - Integrates weather data (hourly correlation analysis)
    ↓
Parquet Lake (optimized for analytics)
    ↓
[main.py] - Interactive Streamlit dashboard
```

## Technical Stack
- **Data Processing**: Pandas, Dask (distributed)
- **Analytics**: Plotly, NumPy
- **Visualization**: Streamlit
- **Storage**: Parquet (columnar format for query efficiency)

## Data Source
NYC Citi Bike official trip history datasets (2024) + NOAA weather data
