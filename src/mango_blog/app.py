from plots import plot_gini_plotly, plot_bar_plotly, plot_users_plotly, FS
from hashtags import secondary_analyzer, COL_AUTHOR_ID, COL_TIME, COL_POST
from matplotlib import pyplot as plt
import polars as pl
import numpy as np
from dotenv import dotenv_values
from pathlib import Path

from shiny import App, ui, render, reactive
from shinywidgets import render_widget, output_widget

data_dir = dotenv_values()["DATA_PATH"]
DATA_RAW = Path(data_dir, "inputs", "confirmed_russia_troll_tweets.parquet")
DATA = Path(data_dir, "outputs", "primary_output.parquet")

# https://icons.getbootstrap.com/icons/question-circle-fill/
question_circle_fill = ui.HTML(
    '<svg xmlns="http://www.w3.org/2000/svg" width="16" height="16" fill="currentColor" class="bi bi-question-circle-fill mb-1" viewBox="0 0 16 16"><path d="M16 8A8 8 0 1 1 0 8a8 8 0 0 1 16 0zM5.496 6.033h.825c.138 0 .248-.113.266-.25.09-.656.54-1.134 1.342-1.134.686 0 1.314.343 1.314 1.168 0 .635-.374.927-.965 1.371-.673.489-1.206 1.06-1.168 1.987l.003.217a.25.25 0 0 0 .25.246h.811a.25.25 0 0 0 .25-.25v-.105c0-.718.273-.927 1.01-1.486.609-.463 1.244-.977 1.244-2.056 0-1.511-1.276-2.241-2.673-2.241-1.267 0-2.655.59-2.75 2.286a.237.237 0 0 0 .241.247zm2.325 6.443c.61 0 1.029-.394 1.029-.927 0-.552-.42-.94-1.029-.94-.584 0-1.009.388-1.009.94 0 .533.425.927 1.01.927z"/></svg>'
)


def load_raw_data_subset(time_start, time_end, user_id, hashtag):
    lf = pl.scan_parquet(source=DATA_RAW)

    df_ = (
        lf.rename(
            {
                "Twitter screenname": COL_AUTHOR_ID,
                "Date tweet sent": COL_TIME,
                "Tweet text": COL_POST,
            }
        )
        .select(pl.col(COL_AUTHOR_ID), pl.col(COL_TIME), pl.col(COL_POST))
        .with_columns(pl.col(COL_TIME).str.to_datetime("%m/%d/%Y %H:%M"))
        .with_columns(pl.col(COL_TIME).dt.replace_time_zone("UTC"))
        .filter(
            pl.col(COL_AUTHOR_ID) == user_id,
            pl.col(COL_TIME).is_between(lower_bound=time_start, upper_bound=time_end),
            pl.col(COL_POST).str.contains(hashtag),
        )
        .sort(by=COL_TIME)
    ).collect()

    return df_


def load_primary_output():
    df = pl.read_parquet(DATA)

    df = df.with_columns(pl.col("timewindow_start").dt.replace_time_zone("UTC"))

    return df


df = load_primary_output()

# Calculate step size from the data
time_step = df["timewindow_start"][1] - df["timewindow_start"][0]


def select_users(secondary_output, selected_hashtag):
    users_df = (
        secondary_output.filter(pl.col("hashtags") == selected_hashtag)["users_all"]
        .explode()
        .value_counts(sort=True)
    )

    return users_df


def plot_bar(data_frame):
    fig3, ax3 = plt.subplots(figsize=(8, 6), layout="constrained")

    if len(data_frame) == 0:
        ax3.text(
            0.5,
            0.5,
            "No data for selected date",
            ha="center",
            va="center",
            transform=ax3.transAxes,
            fontsize=14,
        )
        ax3.set_xlim(0, 1)
        ax3.set_ylim(0, 1)
    else:
        FREQ_THRESHOLD = 0.5
        df_out3 = data_frame.filter(pl.col("hashtag_perc") > FREQ_THRESHOLD)

        if len(df_out3) == 0:
            ax3.text(
                0.5,
                0.5,
                "No hashtags above threshold",
                ha="center",
                va="center",
                transform=ax3.transAxes,
                fontsize=14,
            )
            ax3.set_xlim(0, 1)
            ax3.set_ylim(0, 1)
        else:
            x3 = np.arange(len(df_out3["hashtags"].to_numpy()))
            y3 = df_out3["hashtag_perc"].to_numpy()[::-1]

            ax3.barh(x3, y3, color="tab:blue")

            for i, el in enumerate(df_out3["hashtags"][::-1]):
                ax3.text(x=y3[i], y=i - 0.15, s=el, color="tab:gray", fontsize=12)

    ax3.tick_params(labelsize=FS)
    ax3.set_xlabel("% all hashtags", fontsize=FS)
    ax3.spines["top"].set_visible(False)
    ax3.spines["right"].set_visible(False)
    ax3.get_yaxis().set_visible(False)

    return fig3


MANGO_ORANGE2 = "#f3921e"

page_dependencies = ui.tags.head(
    ui.tags.style(".card-header { color:white; background:#f3921e !important; }"),
    ui.tags.link(
        rel="stylesheet", href="https://fonts.googleapis.com/css?family=Roboto"
    ),
    ui.tags.style("body { font-family: 'Roboto', sans-serif; }"),
)

app_ui = ui.page_fluid(
    page_dependencies,
    ui.panel_title(ui.h4("Hashtag analysis dashboard")),
    ui.accordion(
        ui.accordion_panel(
            "",
            [
                ui.card(
                    ui.card_header(
                        "Full time scale analysis ",
                        ui.tooltip(
                            ui.tags.span(
                                question_circle_fill,
                                style="cursor: help; font-size: 14px;",
                            ),
                            "This analysis shows the gini coefficient over the entire dataset. Select specific timepoints below to explore narrow time windows",
                            placement="top",
                        ),
                    ),
                    ui.input_checkbox(
                        "smooth_checkbox", "Show smoothed line", value=False
                    ),
                    output_widget("line_plot", height="300px"),
                )
            ],
        )
    ),
    ui.hr(),
    ui.layout_columns(
        ui.card(
            ui.card_header("Most frequent hashtags"),
            ui.input_selectize(
                id="date_picker",
                label="Show hashtags for date:",
                choices=[
                    dt.strftime("%B %d, %Y") for dt in df["timewindow_start"].to_list()
                ],
                selected=df["timewindow_start"].first().strftime("%B %d, %Y"),
                width="100%",
            ),
            output_widget("bar_plot", height="1500px"),
            max_height="500px",
            full_screen=True,
        ),
        ui.card(
            ui.card_header("Users"),
            ui.input_selectize(
                id="hashtag_picker",
                label="Show users for hashtag:",
                choices=[],
                width="100%",
            ),
            output_widget("user_plot", height="800px"),
            max_height="500px",
            full_screen=True,
        ),
    ),
    ui.hr(),
    ui.card(
        ui.card_header("Tweets"),
        ui.input_selectize(
            id="user_picker",
            label="Show tweets for user:",
            choices=[],
            width="100%",
        ),
        ui.output_text(id="tweets_title"),
        ui.output_data_frame("tweets"),
    ),
    title="Hashtag analysis dashboard",
)


def server(input, output, session):
    def get_selected_datetime():
        """Convert selected formatted date back to datetime"""
        selected_formatted = input.date_picker()
        # Find the datetime that matches the formatted string
        for dt in df["timewindow_start"].to_list():
            if dt.strftime("%B %d, %Y") == selected_formatted:
                return dt
        return df["timewindow_start"].first()  # fallback

    @reactive.calc
    def selected_date():
        x_selected = df.with_columns(
            sel=pl.col("timewindow_start") == input.date_picker()
        ).select(pl.col("sel"))

        return np.where(x_selected)[0].item()

    @reactive.calc
    def secondary_analysis():
        timewindow = get_selected_datetime()
        df_out2 = secondary_analyzer(df, timewindow)
        return df_out2

    @reactive.effect
    def update_hashtag_choices():
        hashtags = secondary_analysis()["hashtags"].to_list()
        ui.update_selectize(
            "hashtag_picker",
            choices=hashtags,
            selected=hashtags[0] if hashtags else None,
            session=session,
        )

    @reactive.effect
    def update_user_choices():
        df_users = select_users(
            secondary_analysis(), selected_hashtag=input.hashtag_picker()
        ).sort("count", descending=True)

        users = df_users["users_all"].to_list()

        ui.update_selectize(
            "user_picker",
            choices=users,
            selected=users[0] if users else None,
            session=session,
        )

    @render_widget
    def line_plot():
        selected_date = get_selected_datetime()
        smooth_enabled = input.smooth_checkbox()
        return plot_gini_plotly(df=df, x_selected=selected_date, smooth=smooth_enabled)

    @render_widget
    def bar_plot():
        selected_date = get_selected_datetime()
        return plot_bar_plotly(
            data_frame=secondary_analysis(),
            selected_date=selected_date,
            show_title=False,
        )

    @render_widget
    def user_plot():
        selected_hashtag = input.hashtag_picker()
        if selected_hashtag:
            users_data = select_users(secondary_analysis(), selected_hashtag)
            return plot_users_plotly(users_data, selected_hashtag)
        else:
            # Return empty plot if no hashtag selected
            import plotly.graph_objects as go

            fig = go.Figure()
            fig.add_annotation(
                x=0.5,
                y=0.5,
                text="Select a hashtag to see user distribution",
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

    @render.text
    def tweets_title():
        timewindow = get_selected_datetime()
        timewindow_end = timewindow + time_step
        format_code = "%B %d, %Y"
        dates_formatted = f"{timewindow.strftime(format_code)} - {timewindow_end.strftime(format_code)}"

        return "Showing posts in time window: " + dates_formatted

    @render.data_frame
    def tweets():
        timewindow = get_selected_datetime()

        df_posts = load_raw_data_subset(
            time_start=timewindow,
            time_end=timewindow + time_step,
            user_id=input.user_picker(),
            hashtag=input.hashtag_picker(),
        )

        # format strings
        df_posts = df_posts.with_columns(
            pl.col(COL_TIME).dt.strftime("%B %d, %Y %I:%M %p")
        )

        df_posts = df_posts.drop(pl.col(COL_AUTHOR_ID))

        return render.DataGrid(df_posts, width="100%")


app = App(app_ui, server)
