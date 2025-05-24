from itertools import accumulate

import polars as pl
from great_tables import GT, md

# input dataframe should have these columns
COL_AUTHOR_ID = "user_id"
COL_TIME = "time"
COL_POST = "text"

OUTPUT_GINI = "hashtag_analysis"

OUTPUT_COL_USERS = "users"
OUTPUT_COL_TIMESPAN = "timewindow_start"
OUTPUT_COL_GINI = "gini"
OUTPUT_COL_COUNT = "count"
OUTPUT_COL_HASHTAGS = "hashtags"


def gini(x: pl.Series) -> float:
    """
    Parameters
    ----------
    x : pl.Series
        polars Series containing values for which to compute the Gini coefficient

    Returns
    -------
    float
        Gini coefficient (between 0.0 and 1.0)
    """
    sorted_x = x.value_counts().sort(by="count", descending=False)[:, 1].to_list()

    n = len(sorted_x)
    cumx = list(accumulate(sorted_x))

    return (n + 1 - 2 * sum(cumx) / cumx[-1]) / n


def hashtag_analysis(data_frame: pl.DataFrame, every="1h", period="1h") -> pl.DataFrame:
    if not isinstance(data_frame.schema[COL_TIME], pl.Datetime):
        data_frame = data_frame.with_columns(
            pl.col(COL_TIME).str.to_datetime().alias(COL_TIME)
        )

    # define the expressions
    has_hashtag_symbols = pl.col(COL_POST).str.contains("#").any()
    extract_hashtags = pl.col(COL_POST).str.extract_all(r"(#\S+)")

    # if hashtag symbol is detected, extract with regex
    if data_frame.select(has_hashtag_symbols).item():
        df_input = data_frame.with_columns(extract_hashtags).filter(
            pl.col(COL_POST) != []
        )

    else:  # otherwise, we assume str: "['hashtag1', 'hashtag2', ...]"
        raise ValueError(f"The data in {COL_POST} column appear to have no hashtags.")

    # select columns and sort
    df_input = df_input.select(pl.col([COL_AUTHOR_ID, COL_TIME, COL_POST])).sort(
        pl.col(COL_TIME)
    )

    # compute gini per timewindow
    df_out = (
        df_input.explode(pl.col(COL_POST))
        .group_by_dynamic(
            pl.col(COL_TIME), every=every, period=period, start_by="datapoint"
        )
        .agg(
            pl.col(COL_AUTHOR_ID).alias(OUTPUT_COL_USERS),
            pl.col(COL_POST).alias(OUTPUT_COL_HASHTAGS),
            pl.col(COL_POST).count().alias(OUTPUT_COL_COUNT),
            pl.col(COL_POST)
            .map_batches(gini, returns_scalar=True, return_dtype=pl.Float64)
            .alias(OUTPUT_COL_GINI),
        )
        .with_columns(
            pl.col(OUTPUT_COL_GINI)
            .rolling_mean(window_size=3, center=True)
            .alias(OUTPUT_COL_GINI + "_smooth")
        )
        .rename({COL_TIME: OUTPUT_COL_TIMESPAN})
    )

    # convert datetime back to string
    df_out = df_out.with_columns(
        pl.col(OUTPUT_COL_TIMESPAN).dt.to_string("%Y-%m-%d %H:%M:%S")
    )

    return df_out


def secondary_analyzer(primary_output, timewindow):
    dataframe_single_timewindow = primary_output.filter(
        pl.col("timewindow_start") == timewindow
    )

    secondary_output = (
        dataframe_single_timewindow.explode(
            [OUTPUT_COL_HASHTAGS, OUTPUT_COL_USERS]
        )  # make eash hashtag and user a separate row
        .with_columns(
            n_hashtags=pl.col(OUTPUT_COL_HASHTAGS).len()
        )  # column with number of hashtags
        .group_by(
            pl.col(OUTPUT_COL_HASHTAGS)
        )  # for each hashtag, compute the folllowing
        .agg(
            users_all=pl.col(OUTPUT_COL_USERS),
            users_unique=pl.col(OUTPUT_COL_USERS).unique(),
            hashtag_perc=(
                pl.col(OUTPUT_COL_HASHTAGS).count() / pl.col("n_hashtags").first()
            )
            * 100,
            user_ratio=pl.col(OUTPUT_COL_USERS).unique().len()
            / pl.col(OUTPUT_COL_USERS).len(),
            common_user=pl.col(OUTPUT_COL_USERS).value_counts(sort=True),
        )
        .sort(by="hashtag_perc", descending=True)
        .with_columns(
            pl.col("hashtag_perc").round(2),
        )
    )

    return secondary_output


def make_table1(russ_trol_df: pl.DataFrame) -> GT:
    df_sum = russ_trol_df.select(
        unique_users=pl.col(COL_AUTHOR_ID).unique().len(),
        total_posts=pl.col(COL_AUTHOR_ID).len(),
        has_hashtags=pl.col(COL_POST).str.contains("#").sum(),
        has_hashtags_perc=(
            (pl.col(COL_POST).str.contains("#").sum() / pl.col(COL_POST).len()) * 100
        ).round(1),
    )

    df_sum_ = df_sum.rename(
        {
            "unique_users": "N users",
            "total_posts": "N tweets",
            "has_hashtags": "N tweets with hashtag",
            "has_hashtags_perc": "% Tweets with hashtag",
        }
    )
    table = (
        GT(df_sum_)
        .tab_spanner(
            label="Tweets with hashtags",
            columns=["N tweets with hashtag", "% Tweets with hashtag"],
        )
        .cols_label({"N tweets with hashtag": "N", "% Tweets with hashtag": "%"})
        .fmt_number(
            columns=["N users", "N tweets", "N tweets with hashtag"],
            compact=True,
            decimals=1,
        )
        .fmt_integer(columns=["N users"])
        .tab_header(
            title=md("**Russian Trolls Twitter Dataset**"),
            subtitle="Summary information",
        )
        .tab_source_note(
            source_note=md(
                "**Source:** _nodeassets.nbcnews.com/russian-twitter-trolls/streamlined_tweets.csv_"
            )
        )
    )

    return table
