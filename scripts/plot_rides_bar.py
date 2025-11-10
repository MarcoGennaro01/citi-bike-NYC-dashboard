import pandas as pd
import plotly.express as px

rides = pd.read_parquet("/home/mark/git/bike-data-newyork/data_parquet/rides.parq")
# Computing rides distribution through months and hours
rides_reduced = rides.groupby("starting_date_hour").count().reset_index(drop=False)
rides_reduced["month"] = rides_reduced["starting_date_hour"].apply(lambda x: x.month)
rides_reduced["hour"] = rides_reduced["starting_date_hour"].apply(lambda x: x.hour)
rides_reduced = rides_reduced.loc[:, ["month", "hour", "is_electric"]]
rides_reduced = rides_reduced.rename(columns={"is_electric": "count"})

# Plotting average hourly rides
mean_hour = (
    rides_reduced.groupby("hour")
    .mean()
    .reset_index(drop=False)
    .rename(columns={"count": "mean"})
    .loc[:, ["mean", "hour"]]
)
mean_hour_bar = px.bar(
    mean_hour,
    x="hour",
    y="mean",
    color_discrete_sequence=["hsv(11.53, 84.58%, 94.12%)"],
    labels={"hour": "Hour", "mean": "Avg rides"},
    title="Avg number of rides per hour",
).update_layout(
    font_family="JetBrains Mono",
    xaxis=dict(
        tickmode="array",
        tickvals=list(range(24)),
        ticktext=list([str(x) for x in range(1, 25)]),
    ),
)


mean_month = (
    rides_reduced.groupby("month")
    .mean()
    .reset_index(drop=False)
    .rename(columns={"count": "mean"})
    .loc[:, ["mean", "month"]]
)

mean_month_bar = px.bar(
    mean_month,
    x="month",
    y="mean",
    color_discrete_sequence=["hsv(11.53, 84.58%, 94.12%)"],
    labels={"mean": "Average number of rides per hour", "month": ""},
    title="Avg number of rides per hour",
).update_layout(
    font_family="JetBrains Mono",
    xaxis=dict(
        tickmode="array",
        tickvals=list(range(1, 13)),
        ticktext=[
            "Jan",
            "Feb",
            "Mar",
            "Apr",
            "May",
            "Jun",
            "Jul",
            "Aug",
            "Sep",
            "Oct",
            "Nov",
            "Dec",
        ],
    ),
)
mean_hour_bar.write_html("../html/plot_rides_hour_bar.html")
mean_month_bar.write_html("../html/plot_rides_month_bar.html")
