import numpy as np
import polars as pl
from matplotlib import pyplot as plt
import matplotlib.dates as mdates
import plotly.graph_objects as go
from mango_blog.constants import DATES, DATES2FORMATTED

FS = 14


def plot_gini_annot(df: pl.DataFrame, x_selected: int, smooth: bool = False):
    fig, ax = plt.subplots(figsize=(12, 3.5), layout="constrained")

    y = df.select(pl.col("gini")).to_numpy()
    x = df.select(pl.col("timewindow_start")).to_numpy()

    date_format = mdates.DateFormatter("%b %Y")

    ax.plot(x, y)

    if smooth:
        y2 = df.select(pl.col("gini_smooth")).to_numpy()
        ax.plot(x, y2, color="tab:orange", alpha=0.5)

    ymin, ymax = ax.get_ylim()

    XYTEXTS = [(-70, 10), (-80, 30), (5, 50), (80, 25)]

    for i, event in enumerate(DATES):
        # Check if we have a label for this event
        if event in DATES2FORMATTED:
            label = DATES2FORMATTED[event]
            date = DATES[event]

            # Draw vertical line at the date
            ax.vlines(
                x=date,
                ymin=ymin,
                ymax=ymax,
                linestyle="--",
                color="gray",
                alpha=0.7,
                linewidth=1,
            )

            # Calculate a y-position in the middle of the plot for the annotation
            annotation_y = ymax

            # Add annotation with custom arc and arms
            ax.annotate(
                label,
                xy=(date, annotation_y),
                xycoords="data",
                xytext=XYTEXTS[i],
                textcoords="offset points",
                arrowprops=dict(
                    arrowstyle="-",
                    lw=0.5,
                    connectionstyle="arc,angleA=0,armA=0,angleB=90,armB=20,rad=0",
                ),
                fontname="Verdana",
                fontsize=FS,
                ha="right",
                va="bottom",
            )

    selected_date = x[x_selected]
    ax.vlines(x=selected_date, ymin=ymin, ymax=ymax, ls="--", lw=1.5, color="tab:red")

    ax.set_ylabel("Gini coefficient", fontsize=FS + 4)
    ax.set_xlabel("Time", fontsize=FS + 4)
    # ax.set_title(f"Concentration of hashtags over time ({day_selector.value} day window)", fontsize=FS+4)

    ax.spines["top"].set_visible(False)
    ax.spines["right"].set_visible(False)
    ax.spines["left"].set_visible(False)
    ax.spines["bottom"].set_visible(False)
    # ax.set_facecolor("gray")
    ax.grid(visible=True, lw=0.5, alpha=0.5)
    ax.xaxis.set_major_formatter(date_format)
    ax.tick_params(labelsize=FS)

    return fig


def plot_bar(data_frame, ax):
    if ax is None:
        fig, ax = plt.subplots(figsize=(5.5, 5.5))
    else:
        fig = None

    x3 = np.arange(len(data_frame["hashtags"].to_numpy()))
    y3 = data_frame["hashtag_perc"].to_numpy()[::-1]

    ax.barh(x3, y3, color="#93c47d")
    ax.set_xlim(0, 40)
    for i, el in enumerate(data_frame["hashtags"][::-1]):
        c, fw = "tab:gray", "normal"
        if i == (len(data_frame["hashtags"]) - 1):
            c, fw = "tab:orange", "semibold"
        ax.text(x=y3[i], y=i - 0.15, s=el, color=c, fontsize=12, fontweight=fw)

    ax.tick_params(labelsize=FS)
    ax.set_xlabel("% all hashtags in the selected time period", fontsize=FS)
    ax.set_title("Most frequent hashtags", fontsize=FS + 2)
    ax.spines["top"].set_visible(False)
    ax.spines["right"].set_visible(False)
    ax.get_yaxis().set_visible(False)

    return fig, ax


def plot_gini_plotly(
    df: pl.DataFrame, x_selected, annotate: bool = False, smooth: bool = False
):
    """Create a plotly line plot with white theme"""

    y = df.select(pl.col("gini")).to_numpy().flatten()
    x = df.select(pl.col("timewindow_start")).to_numpy().flatten()

    fig = go.Figure()

    # Add main line
    fig.add_trace(
        go.Scatter(
            x=x,
            y=y,
            mode="lines",
            name="Gini coefficient",
            line=dict(color="black", width=1.5),
        )
    )

    # Add smooth line if requested
    if smooth:
        y2 = df.select(pl.col("gini_smooth")).to_numpy().flatten()
        fig.add_trace(
            go.Scatter(
                x=x,
                y=y2,
                mode="lines",
                name="Smoothed",
                line=dict(color="orange", width=2),
                opacity=0.8,
            )
        )

    # Add vertical line for selected date (x_selected is now the datetime value directly)
    fig.add_vline(x=x_selected, line_dash="dash", line_color="red", line_width=2)

    # Add event annotations
    if annotate:
        for i, event in enumerate(DATES):
            if event in DATES2FORMATTED:
                label = DATES2FORMATTED[event]
                date = DATES[event]

                fig.add_vline(
                    x=date,
                    line_dash="dash",
                    line_color="gray",
                    opacity=0.7,
                    line_width=1,
                )

                fig.add_annotation(
                    x=date,
                    y=max(y),
                    text=label,
                    showarrow=True,
                    arrowhead=2,
                    arrowsize=1,
                    arrowwidth=1,
                    arrowcolor="gray",
                    ax=0,
                    ay=-30,
                    font=dict(size=12),
                )

    # Update layout with white theme
    fig.update_layout(
        template="plotly_white",
        title="Concentration of hashtags over time",
        xaxis_title="Time",
        yaxis_title="Gini coefficient",
        showlegend=False,
        height=300,
        margin=dict(l=50, r=50, t=50, b=50),
    )

    return fig


def plot_bar_plotly(data_frame, selected_date=None, show_title=True):
    """Create an interactive plotly bar plot"""

    if len(data_frame) == 0:
        fig = go.Figure()
        fig.add_annotation(
            x=0.5,
            y=0.5,
            text="No data for selected date",
            showarrow=False,
            font=dict(size=16),
            xref="paper",
            yref="paper",
        )
        fig.update_layout(
            template="plotly_white",
            xaxis=dict(range=[0, 1]),
            yaxis=dict(range=[0, 1]),
            # height=400
        )
        return fig

    # Use all data, no threshold filtering
    df_sorted = data_frame.sort("hashtag_perc", descending=False)

    # Get data (lowest to highest for plotly to display highest at top)
    hashtags = df_sorted["hashtags"].to_list()
    percentages = df_sorted["hashtag_perc"].to_list()

    # Create horizontal bar chart with fixed bar width
    fig = go.Figure(
        go.Bar(
            x=percentages,
            y=hashtags,
            orientation="h",
            marker_color="#609949",
            hovertemplate="<b>%{y}</b><br>%{x:.1f}% of all hashtags<extra></extra>",
            width=0.8,  # Fixed bar width
            text=hashtags,  # Add text labels on bars
            textposition="outside",
            textfont=dict(color="black", size=12),
        )
    )

    # Format title with date (optional)
    if show_title:
        if selected_date:
            formatted_date = selected_date.strftime("%B %d, %Y")
            title = f"Most frequent hashtags - {formatted_date}"
        else:
            title = "Most frequent hashtags"
    else:
        title = None

    # Use fixed height and let container handle scrolling
    # Calculate dynamic height based on number of hashtags (fixed spacing per bar)
    bar_height = 30  # Fixed height per bar
    dynamic_height = len(hashtags) * bar_height + 100  # +100 for margins

    # Update layout
    fig.update_layout(
        template="plotly_white",
        title=title,
        xaxis_title="% all hashtags in the selected time period",
        yaxis_title="",
        height=dynamic_height,
        margin=dict(l=0, r=100, t=10, b=50),
        showlegend=False,
    )

    # Update axes
    fig.update_xaxes(
        range=[0, max(percentages) * 1.5], side="top"
    )  # Extra space for text, x-axis on top
    fig.update_yaxes(
        categoryorder="array", categoryarray=hashtags, showticklabels=False
    )

    return fig


def plot_users_plotly(users_data, selected_hashtag=None):
    """Create an interactive plotly bar plot for user distribution"""

    if len(users_data) == 0:
        fig = go.Figure()
        fig.add_annotation(
            x=0.5,
            y=0.5,
            text="No users found for this hashtag",
            showarrow=False,
            font=dict(size=16),
            xref="paper",
            yref="paper",
        )
        fig.update_layout(
            template="plotly_white",
            xaxis=dict(range=[0, 1]),
            yaxis=dict(range=[0, 1]),
            height=400,
        )
        return fig

    # Sort by count (ascending for plotly display with highest at top)
    df_sorted = users_data.sort("count", descending=False)

    # Get data
    users = df_sorted["users_all"].to_list()
    counts = df_sorted["count"].to_list()

    # Create horizontal bar chart with fixed bar width
    fig = go.Figure(
        go.Bar(
            x=counts,
            y=users,
            orientation="h",
            marker_color="#609949",
            hovertemplate="<b>%{y}</b><br>%{x} posts<extra></extra>",
            width=0.8,  # Fixed bar width
            text=users,  # Add text labels on bars
            textposition="outside",
            textfont=dict(color="black", size=12),
        )
    )

    # Calculate dynamic height based on number of users
    bar_height = 30  # Fixed height per bar
    dynamic_height = len(users) * bar_height + 100  # +100 for margins

    # Update layout
    fig.update_layout(
        template="plotly_white",
        title=None,
        xaxis_title="Number of posts",
        yaxis_title="",
        height=dynamic_height,
        margin=dict(l=0, r=50, t=0, b=10),
        showlegend=False,
    )

    # Update axes
    fig.update_xaxes(
        range=[0, max(counts) * 1.5], side="top"
    )  # Extra space for text, x-axis on top
    fig.update_yaxes(categoryorder="array", categoryarray=users, showticklabels=False)

    return fig
