from pathlib import Path
from datetime import datetime, timedelta
import polars as pl
from great_tables import GT, md
import numpy as np
from matplotlib import pyplot as plt

from .hashtags import (
    hashtag_analysis,
    secondary_analyzer,
    make_table1,
    COL_AUTHOR_ID,
    COL_TIME,
    COL_POST,
    OUTPUT_COL_HASHTAGS,
    OUTPUT_COL_TIMESPAN,
)
from .plots import plot_gini_annot, plot_bar, FS

if __name__ == "__main__":
    import argparse

    parser = argparse.ArgumentParser()

    parser.add_argument(
        "input_csv", type=str, help="Path to the russian trolls dataset"
    )
    parser.add_argument("output_path", type=str, help="Folder to store the data to")

    args = parser.parse_args()

    lf = pl.scan_csv(
        source=args.input_csv,
        skip_rows=3,  # we know this in advance
    )

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

    df_out = hashtag_analysis(
        data_frame=df,
        every="6d",
        period="6d",
    )

    df_out = df_out.with_columns(pl.col(OUTPUT_COL_TIMESPAN).str.to_datetime())

    parquet_fn = "primary_output.parquet"
    print(f"Saving {parquet_fn}")
    df_out.write_parquet(Path(args.output_path, parquet_fn))

    json_fn = "primary_output.json"
    print(f"Saving {json_fn}")
    df_out.write_json(Path(args.output_path, json_fn))

    # select March 22
    TIMEPOINT_STR = datetime(2016, 3, 22, 6, 59, 00)

    x_selected = df_out.with_columns(
        sel=pl.col(OUTPUT_COL_TIMESPAN) == TIMEPOINT_STR
    ).select(pl.col("sel"))

    idx = np.where(x_selected)[0].item()

    # FIGURE 1
    fig = plot_gini_annot(df=df_out, x_selected=idx)

    fn_fig1_png = Path(args.output_path, "fig1_gini_time.png")
    print(f"Saving {fn_fig1_png}")
    fig.savefig(fn_fig1_png, dpi=300)

    fn_fig1_svg = Path(args.output_path, "fig1_gini_time.svg")
    print(f"Saving {fn_fig1_svg}")
    fig.savefig(fn_fig1_svg, dpi=300)

    # ===== TABLE 1 ===== #

    table1 = make_table1(russ_trol_df=df)

    fn_table1_png = Path(args.output_path, "dataset_summary_table.png")
    fn_table1_pdf = Path(args.output_path, "dataset_summary_table.pdf")
    print(f"Saving {fn_table1_png}")
    table1.save(fn_table1_png, web_driver="firefox", scale=2)

    print(f"Saving {fn_table1_pdf}")
    table1.save(fn_table1_pdf, web_driver="firefox", scale=2)

    # ===== BAR PLOT ===== #
    selected_date = df_out.select(pl.col(OUTPUT_COL_TIMESPAN))[idx].item()

    start_date_formatted = selected_date.strftime("%B %d")
    end_date = selected_date + timedelta(days=6)
    end_date_formatted = end_date.strftime("%d, %Y")

    x = df_out.select(pl.col(OUTPUT_COL_TIMESPAN)).to_numpy()[idx].item()
    df_out2 = secondary_analyzer(primary_output=df_out, timewindow=x)

    FREQ_THRESHOLD = 0.5
    df_out_filtered = df_out2.filter(pl.col("hashtag_perc") > FREQ_THRESHOLD)

    # Select users for selected hashtags
    SELHASHTAG = "#IslamKills"
    USER_N_POSTS_THRESHOLD = 5
    users = (
        df_out_filtered.filter(
            pl.col(OUTPUT_COL_HASHTAGS) == SELHASHTAG,
        )["users_all"]
        .explode()
        .value_counts(sort=True)
    ).filter(pl.col("count") > USER_N_POSTS_THRESHOLD)

    # ===== BAR PLOT ===== #
    x4 = np.arange(len(users["users_all"].to_numpy()))
    y4 = users["count"].to_numpy()[::-1]
    fig, axes = plt.subplots(1, 2, figsize=(13, 5.5))

    _ = plot_bar(df_out_filtered, ax=axes[0])

    axes[1].barh(x4, y4, color="tab:orange")

    axes[1].tick_params(labelsize=FS)
    axes[1].set_xlabel("Number of tweets", fontsize=FS)
    axes[1].set_title(
        f"Accounts using {SELHASHTAG}",
        fontsize=FS + 2,
    )
    axes[1].spines["top"].set_visible(False)
    axes[1].spines["right"].set_visible(False)
    axes[1].get_yaxis().set_visible(False)
    for i, el in enumerate(users["users_all"][::-1]):
        axes[1].text(
            x=y4[i] - 0.5,
            y=i - 0.2,
            s=el,
            ha="right",
            color="white",
            fontsize=10,
            fontweight="semibold",
        )

    for a in axes:
        a.grid(visible=True, axis="x", alpha=0.4, ls="--")

    fig.suptitle(
        f"Zooming in: {start_date_formatted} to {end_date_formatted}",
        fontsize=FS + 3,
        fontweight="semibold",
    )

    fn_barplot_png = Path(args.output_path, "fig2_barplots.png")
    fn_barplot_svg = Path(args.output_path, "fig2_barplots.svg")

    print(f"Saving {fn_barplot_png}")
    fig.savefig(fn_barplot_png, dpi=300)

    print(f"Saving {fn_barplot_svg}")
    fig.savefig(fn_barplot_svg, dpi=300)

    # TWEETS TABLE
    SEL_USER = "lazykstafford"
    df_user = (
        df.filter(
            pl.col("user_id") == SEL_USER,
            pl.col("time").is_between(selected_date, end_date),
            pl.col("text").str.contains(SELHASHTAG),
        )
        .select(pl.col("time", "text"))
        .sort(by=pl.col("time"))
    )

    table_tweets = (
        GT(df_user.slice(10, 10))
        .cols_label({"time": md("**Tweet time stamp**"), "text": md("**Tweet text**")})
        .tab_header(md(f"**Tweets by account {SEL_USER}**"), subtitle="10 samples")
        .fmt_datetime(columns="time", date_style="m_day_year", time_style="h_m_p")
    )

    fn_table_tweets_png = Path(args.output_path, "tweets_table.png")
    print(f"Saving {fn_table_tweets_png}")
    table_tweets.save(fn_table_tweets_png, web_driver="firefox", scale=2)

    fn_table_tweets_pdf = Path(args.output_path, "tweets_table.pdf")
    print(f"Saving {fn_table_tweets_pdf}")
    table_tweets.save(fn_table_tweets_pdf, web_driver="firefox", scale=2)
