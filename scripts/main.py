import streamlit as st
import streamlit.components.v1 as components

top_left_column, top_right_column = st.columns([3, 2])

with top_left_column:
    file = open("./html/plot_stations_map.html")
    html_content = file.read()
    components.html(html_content)

with top_right_column:
    col_1, col_2 = st.columns([1, 1])
    with col_1:
        file = open("./html/plot_rides_hour_bar.html")
        html_content = file.read()
        components.html(html_content)
    with col_2:
        file = open("./html/plot_rides_bar.html")
        html_content = file.read()
        components.html(html_content)
