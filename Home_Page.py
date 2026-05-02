import streamlit as st
from utils.plots import (
    duration_plot,
    freq_hour_plot,
    freq_month_plot,
    get_unique_stations,
    pies_plot,
    rides_map,
    stations_freq_table,
    time_series_rides,
    weather_plots,
)


st.set_page_config(page_title="Home page", layout="wide")
st.markdown(
    """<style>
header.stAppHeader {background-color: transparent;}
section.stMain .block-container {padding-top: 3; z-index: 1;}
</style>""",
    unsafe_allow_html=True,
)
header = st.columns([1], border=True)[0]
with header:
    station_column, split_column = st.columns([2, 1])
    with station_column:
        station = st.selectbox(
            "Station:",
            get_unique_stations(),
            index=None,
            placeholder="Select a station...",
        )
    with split_column:
        split = st.selectbox(
            "Split by",
            ["Membership", "Electric", "Both"],
            index=None,
            placeholder="Split by...",
            key="split_by",
        )
    if station:
        st.plotly_chart(time_series_rides(station, split))
top_left_column, top_right_column = st.columns([2, 1], border=True)
with top_left_column:
    tab1m, tab2m = st.tabs(["Map", "Table"])
    with tab1m:
        if station:
            st.plotly_chart(rides_map(station))
        else:
            st.plotly_chart(rides_map())
    with tab2m:
        if station:
            st.dataframe(stations_freq_table(station))
        else:
            st.dataframe(stations_freq_table())
with top_right_column:
    if station:
        weather_data = weather_plots(station)
    else:
        weather_data = weather_plots()
    ptw = st.selectbox(
        "Weather:",
        (0, 1, 2),
        format_func=lambda x: ["Precipitation", "Temperature", "Wind Speed"][x],
    )
    st.plotly_chart(weather_data[ptw])
down_left_column, down_right_column = st.columns([2, 1], border=True)

with down_left_column:
    tab1, tab2, tab3 = st.tabs(["Duration", "Hourly", "Monthly"])
    with tab1:
        if station:
            st.plotly_chart(duration_plot(station, split))
        else:
            st.plotly_chart(duration_plot(split_by=split))
    with tab2:
        if station:
            st.plotly_chart(freq_hour_plot(station, split))
        else:
            st.plotly_chart(freq_hour_plot(split_by=split))
    with tab3:
        if station:
            st.plotly_chart(freq_month_plot(station, split))
        else:
            st.plotly_chart(freq_month_plot(split_by=split))

with down_right_column:
    if station:
        pies = pies_plot(station)
    else:
        pies = pies_plot()
    tab1p, tab2p = st.tabs(["Member", "Electric"])
    with tab1p:
        st.plotly_chart(pies[0])
    with tab2p:
        st.plotly_chart(pies[1])
