import pandas as pd
import plotly.express as px
import numpy as np

rides = pd.read_parquet("../data_parquet/rides.parq")
rides = rides.duration
rides = rides[rides < 60]
rides, bins = np.histogram(rides, bins=20)
plot_hist_duration = px.histogram(
    x=[f"{round(bins[i], 2)} - {round(bins[i + 1], 2)}" for i in range(len(bins) - 1)],
    y=rides,
    labels={"x": "Duration", "y": "Absolute frequency"},
    title="Rides duration distribution (in minutes)",
).update_layout(
    font_family="JetBrains Mono",
    xaxis=dict(
        showticklabels=False,  # Hide tick labels
    ),
    yaxis_title="Absolute frequency",
)
