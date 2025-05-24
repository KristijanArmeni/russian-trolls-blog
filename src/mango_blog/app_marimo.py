# /// script
# requires-python = ">=3.12"
# dependencies = [
#     "great-tables==0.17.0",
#     "marimo",
#     "matplotlib==3.10.1",
#     "numpy==2.2.5",
#     "polars==1.29.0",
#     "mango-blog==0.1.0",
# ]
#
# [tool.uv.sources]
# mango-blog = { path = "../../", editable = true }
# ///

import marimo

__generated_with = "0.13.11"
app = marimo.App()


@app.cell
def _():
    import marimo as mo
    from matplotlib import pyplot as plt
    import matplotlib.dates as mdates
    from datetime import timedelta
    import numpy as np
    import polars as pl
    from great_tables import GT, md

    from mango_blog.constants import DATA_PATH
    from mango_blog.plots import plot_gini_annot
    from mango_blog.hashtags import (
        hashtag_analysis,
        secondary_analyzer,
        COL_AUTHOR_ID,
        COL_TIME,
        COL_POST,
        OUTPUT_COL_HASHTAGS,
    )

    return (
        COL_AUTHOR_ID,
        COL_POST,
        COL_TIME,
        DATA_PATH,
        GT,
        OUTPUT_COL_HASHTAGS,
        hashtag_analysis,
        md,
        mdates,
        mo,
        np,
        pl,
        plot_gini_annot,
        plt,
        secondary_analyzer,
        timedelta,
    )


@app.cell
def _(DATA_PATH):
    DATA_PATH
    return


@app.cell
def _():
    # constants
    FS = 12
    return (FS,)


@app.cell
def _(mo):
    mo.md("""# Hashtag analysis dashboard (prototype)""")
    return


@app.cell
def _(mo):
    mo.md(
        r"""
    ## Dataset
    ---
    """
    )
    return


@app.cell
def _(mo):
    file_browser = mo.ui.file_browser(
        label="Select .csv dataset to load",
        initial_path="../../data/inputs",
        multiple=False,
    )
    file_browser
    return (file_browser,)


@app.cell
def _(file_browser, pl):
    filename = file_browser.name(index=0)
    filepath = file_browser.path(index=0)
    lf = pl.scan_csv(
        source=filepath,
        skip_rows=3,
    )
    return filename, lf


@app.cell
def _(COL_AUTHOR_ID, COL_POST, COL_TIME, lf, pl):
    df = (
        lf.rename(
            {
                "Twitter screenname": COL_AUTHOR_ID,
                "Date tweet sent": COL_TIME,
                "Tweet text": COL_POST,
            }
        )
        .select(pl.col(COL_AUTHOR_ID), pl.col(COL_TIME), pl.col(COL_POST))
        .with_columns(pl.col(COL_TIME).str.to_datetime("%m/%d/%Y %H:%M"))
    ).collect()
    return (df,)


@app.cell
def _(COL_AUTHOR_ID, COL_POST, df, pl):
    df_sum = df.select(
        unique_users=pl.col(COL_AUTHOR_ID).unique().len(),
        total_posts=pl.col(COL_AUTHOR_ID).len(),
        nulls=pl.col(COL_POST).is_null().sum(),
        has_hashtags=pl.col(COL_POST).str.contains("#").sum(),
        has_hashtags_perc=(
            (pl.col(COL_POST).str.contains("#").sum() / pl.col(COL_POST).len()) * 100
        ).round(1),
    )
    return (df_sum,)


@app.cell
def _(GT, df_sum, md):
    df_sum_ = df_sum.rename(
        {
            "unique_users": "N users",
            "total_posts": "N posts",
            "has_hashtags": "N posts with hashtag",
            "has_hashtags_perc": "% posts with hashtag",
        }
    )
    (
        GT(df_sum_)
        .tab_spanner(
            label="Posts with hashtags",
            columns=["N posts with hashtag", "% posts with hashtag"],
        )
        .cols_label({"N posts with hashtag": "N", "% posts with hashtag": "%"})
        .fmt_number(
            columns=["N users", "N posts", "N posts with hashtag"],
            compact=True,
            decimals=1,
        )
        .fmt_integer(columns=["N users", "nulls"])
        .tab_header(
            title="Russian trolls dataset (NBC News)", subtitle="Summary information"
        )
        .tab_source_note(
            source_note=md(
                "**Source:** _nodeassets.nbcnews.com/russian-twitter-trolls/streamlined_tweets.csv_"
            )
        )
    )
    return


@app.cell
def _(mo):
    mo.md(
        """
    ## Hashtag analysis
    ---
    """
    )
    return


@app.cell
def _(mo):
    interval_selector = mo.ui.number(
        start=1, stop=20, label="Time window interval (nr. days):"
    )
    duration_selector = mo.ui.number(
        start=1, stop=20, label="Time window duration (nr. days):"
    )
    return duration_selector, interval_selector


@app.cell
def _(duration_selector, interval_selector, mo):
    mo.hstack([interval_selector, duration_selector])
    return


@app.cell
def _(duration_selector, interval_selector, mo):
    mo.md(
        f"""### {duration_selector.value} day window every {interval_selector.value} days interval"""
    )
    return


@app.cell
def _(df, duration_selector, hashtag_analysis, interval_selector, pl):
    df_out = hashtag_analysis(
        data_frame=df,
        every=f"{interval_selector.value}d",
        period=f"{duration_selector.value}d",
    )
    df_out = df_out.with_columns(pl.col("timewindow_start").str.to_datetime())
    return (df_out,)


@app.cell
def _(df_out, mo, pl):
    series = range(1, len(df_out.select(pl.col("timewindow_start")).to_series()) + 1)
    date_selector = mo.ui.slider(steps=series, full_width=True, show_value=True)
    return (date_selector,)


@app.cell
def _(date_selector, df_out, plot_gini_annot):
    fig = plot_gini_annot(df=df_out, x_selected=date_selector.value)
    fig
    return


@app.cell
def _(date_selector):
    date_selector
    return


@app.cell
def _(filename, mo):
    mo.md(f"""Source: {filename}""")
    return


@app.cell
def _(date_selector, df_out, pl):
    selected_date_formatted = (
        df_out.select(pl.col("timewindow_start"))[date_selector.value]
        .item()
        .strftime("%B %d, %Y")
    )
    return (selected_date_formatted,)


@app.cell
def _(mo):
    mo.md(
        """
    ## Single time-window analysis
    ---
    """
    )
    return


@app.cell
def _(date_selector, df_out, pl, secondary_analyzer):
    x = df_out.select(pl.col("timewindow_start")).to_numpy()
    df_out2 = secondary_analyzer(df_out, x[date_selector.value])
    return (df_out2,)


@app.cell
def _(FS, df_out2, np, pl, plt, selected_date_formatted):
    FREQ_THRESHOLD = 0.5
    df_out3 = df_out2.filter(pl.col("hashtag_perc") > FREQ_THRESHOLD)

    fig3, ax3 = plt.subplots(figsize=(8, 6), layout="constrained")

    x3 = np.arange(len(df_out3["hashtags"].to_numpy()))
    y3 = df_out3["hashtag_perc"].to_numpy()[::-1]

    ax3.barh(x3, y3, color="tab:blue")

    for i, el in enumerate(df_out3["hashtags"][::-1]):
        ax3.text(x=y3[i], y=i - 0.15, s=el, color="tab:gray", fontsize=12)

    ax3.tick_params(labelsize=FS)
    ax3.set_xlabel("% all hashtags", fontsize=FS)
    ax3.set_title(
        f"Most frequent hashtags on {selected_date_formatted}", fontsize=FS + 4
    )
    ax3.spines["top"].set_visible(False)
    ax3.spines["right"].set_visible(False)
    ax3.get_yaxis().set_visible(False)

    return df_out3, fig3


@app.cell
def _(fig3):
    fig3
    return


@app.cell
def _(df_out3, mo):
    options = df_out3["hashtags"].to_list()
    hashtag_selector = mo.ui.dropdown(
        options=options,
        label="Display users for hashtag:",
        searchable=True,
    )
    return (hashtag_selector,)


@app.cell
def _(hashtag_selector):
    hashtag_selector
    return


@app.cell
def _(OUTPUT_COL_HASHTAGS, df_out3, hashtag_selector, mo, pl):
    mo.stop(hashtag_selector.value is None, mo.md(""))

    users = (
        df_out3.filter(pl.col(OUTPUT_COL_HASHTAGS) == hashtag_selector.value)[
            "users_all"
        ]
        .explode()
        .value_counts(sort=True)
    )

    users = users.rename({"users_all": "User name", "count": "Hashtag count"})
    user_selector = mo.ui.table(
        users,
        label="Users using this hashtag (select user to display posts):",
        selection="single",
        initial_selection=[0],
    )

    user_selector
    return (user_selector,)


@app.cell
def _(user_selector):
    selected_user = user_selector.value["User name"].to_list()[0]
    return (selected_user,)


@app.cell
def _(COL_AUTHOR_ID, COL_POST, df, hashtag_selector, pl, selected_user):
    posts = df.filter(
        pl.col(COL_AUTHOR_ID) == selected_user,
        pl.col(COL_POST).str.contains(hashtag_selector.value),
    ).sort(by="time")
    return (posts,)


@app.cell
def _(pl, posts):
    n_posts_per_time = posts.select(
        pl.col("time"), n_posts=pl.col("text").count().over("time")
    )["n_posts"].to_list()
    return (n_posts_per_time,)


@app.cell
def _(mdates, timedelta):
    def get_axis_formatting(axis, dates):
        timerange = max(dates) - min(dates)

        if timerange <= timedelta(hours=2):
            # For ranges under 2 hours, show minutes
            axis.xaxis.set_major_formatter(mdates.DateFormatter("%H:%M"))
            axis.xaxis.set_major_locator(mdates.MinuteLocator(interval=10))
            axis.xaxis.set_minor_locator(mdates.MinuteLocator(interval=1))

            xlabel = "Post time (HH:MM)"

        elif timerange <= timedelta(days=1):
            # For ranges under a day, show hours
            axis.xaxis.set_major_formatter(mdates.DateFormatter("%H:%M"))
            axis.xaxis.set_major_locator(mdates.HourLocator(interval=1))
            axis.xaxis.set_minor_locator(mdates.MinuteLocator(interval=15))
            xlabel = "Post time (HH:MM)"

        elif timerange <= timedelta(days=7):
            # For ranges under a week, show days and hours
            axis.xaxis.set_major_formatter(mdates.DateFormatter("%m-%d %H:%M"))
            axis.xaxis.set_major_locator(mdates.DayLocator(interval=1))
            axis.xaxis.set_minor_locator(mdates.HourLocator(interval=6))
            xlabel = "Post date and time"

        elif timerange <= timedelta(days=60):
            # For ranges under 2 months, show days
            axis.xaxis.set_major_formatter(mdates.DateFormatter("%m-%d"))

            # Calculate a reasonable day interval (aim for ~5-10 major ticks)
            days_total = timerange.days
            day_interval = max(1, days_total // 8)
            axis.xaxis.set_major_locator(mdates.DayLocator(interval=day_interval))
            axis.xaxis.set_minor_locator(mdates.DayLocator(interval=1))
            xlabel = "Post date (MM-DD)"

        else:
            # For ranges over 2 months, show months
            axis.xaxis.set_major_formatter(mdates.DateFormatter("%Y-%m"))
            axis.xaxis.set_major_locator(mdates.MonthLocator())
            axis.xaxis.set_minor_locator(mdates.DayLocator(interval=5))
            xlabel = "Post date (YYYY-MM)"

        return axis, xlabel

    return (get_axis_formatting,)


@app.cell
def _(
    FS,
    get_axis_formatting,
    hashtag_selector,
    n_posts_per_time,
    plt,
    posts,
    selected_user,
):
    fig4, ax4 = plt.subplots(figsize=(12, 4))

    x4 = posts["time"].to_list()

    ax4, xlabel = get_axis_formatting(axis=ax4, dates=x4)

    unique_counts = [0] + list(set(n_posts_per_time))

    ax4.vlines(
        x=x4, ymin=0, ymax=n_posts_per_time, color="tab:purple", ls="--", alpha=0.5
    )
    ax4.set_yticks(ticks=unique_counts, labels=unique_counts)

    ax4.spines["top"].set_visible(False)
    ax4.spines["right"].set_visible(False)
    ax4.spines.left.set_bounds(min(unique_counts), max(unique_counts))
    ax4.grid(visible=True, ls="--")
    ax4.set_xlabel(xlabel, fontsize=FS)
    ax4.set_ylabel("Number of posts", fontsize=FS)
    ax4.tick_params(labelsize=FS)
    ax4.set_title(
        f"Overview of posts timestamps for hashtag {hashtag_selector.value}, user {selected_user}"
    )
    return


@app.cell
def _(hashtag_selector, mo, posts, selected_user):
    mo.md(
        f"""### Showing **{len(posts)} {"post" if len(posts) == 1 else "posts"}** by **{selected_user}** containing hashtag **{hashtag_selector.value}**:"""
    )
    return


@app.cell
def _(mo, posts, selected_user):
    mo.vstack(
        [
            mo.ui.text_area(
                value=row["text"],
                label=f"{selected_user} | {str(row['time'])}",
                full_width=True,
            )
            for row in posts.rows(named=True)
        ]
    )
    return


@app.cell
def _():
    return


@app.cell
def _():
    return


if __name__ == "__main__":
    app.run()
