from db_tables import *
import streamlit as st

st.set_page_config(layout="wide")
app_context = app.app_context()
app_context.push()


class FutureFixtures:
    def __init__(self, df):
        self.all = df.copy().loc[
            (df["gameweek_id"] >= sel_start_gw) & (df["gameweek_id"] <= sel_end_gw)
        ]

        # home = pd.DataFrame()
        home_games = pd.DataFrame(self.all[["Home Team"]].value_counts())
        home_avg_diff = pd.DataFrame(
            self.all[["Home Team", "Home Difficulty"]].groupby("Home Team").mean()
        )
        home_games = home_games.join(home_avg_diff)
        self.home = home_games

        self.away = self.all[["Away Team"]].value_counts()


def advantage_colours(val):
    if val > 0:
        color = "green"
    elif val < 0:
        color = "red"
    else:
        return None
    return f"background-color: {color}"


def map_team_names(x):
    teams = object_as_df(Teams().query.all())
    teams = dict(zip(teams["team_id"], teams["team_name"]))
    return teams.get(x)


@st.cache
def load_gameweeks():
    df_gameweeks = object_as_df(Gameweeks().query.all())
    return df_gameweeks


@st.cache
def load_fixtures():
    rename_fields = {
        "team_a_difficulty": "Away Difficulty",
        "team_h_difficulty": "Home Difficulty",
    }
    drop_fields = [
        "pulse_id",
        "location",
        "minutes",
        "provisional_start_time",
        "finished_provisional",
        "code",
    ]
    fixtures = Fixtures().query.all()
    df_fixtures = object_as_df(fixtures)
    df_fixtures = df_fixtures.drop(drop_fields, axis="columns").rename(
        columns=rename_fields
    )
    df_fixtures["Away Team"] = df_fixtures["team_a"].apply(map_team_names)
    df_fixtures["Home Team"] = df_fixtures["team_h"].apply(map_team_names)
    df_fixtures["Kickoff Day"] = pd.to_datetime(
        df_fixtures["kickoff_time"]
    ).dt.day_name()
    df_fixtures["Home Difficulty Advantage"] = (
        df_fixtures["Away Difficulty"] - df_fixtures["Home Difficulty"]
    )
    # df_fixtures.style.applymap(color_survived, subset=["Home Difficulty"])
    return df_fixtures


@st.cache
def load_future_fixtures(df):
    df_future_fixtures = df.copy().loc[
        (df["gameweek_id"] >= sel_start_gw) & (df["gameweek_id"] <= sel_end_gw)
    ]
    return df_future_fixtures


@st.cache
def load_current_fixtures(df):
    df_current_fixtures = df.copy().loc[df["gameweek_id"] == current_gw]
    return df_current_fixtures


@st.cache
def load_next_fixtures(df):
    keep_fields = [
        "Home Team",
        "Away Team",
        "Home Difficulty",
        "Away Difficulty",
        "kickoff_time",
        "Home Difficulty Advantage",
    ]
    df_next_fixtures = df[keep_fields].copy().loc[df["gameweek_id"] == next_gw]
    return df_next_fixtures


df_gameweeks = load_gameweeks()
current_gw = df_gameweeks["gameweek_id"].max()
next_gw = df_gameweeks["gameweek_id"].max() + 1

df_fixtures = load_fixtures()
df_current_fixtures = load_current_fixtures(df_fixtures)
df_next_fixtures = load_next_fixtures(df_fixtures)


# df_pivot = df_fixtures.pivot(
#     index="Home Team", columns="Away Team", values="kickoff_time"
# )

current_tab, next_tab, future_tab, past_tab = st.tabs(
    ["Current Fixtures", "Next Fixtures", "Future Fixtures", "Past Fixtures"]
)
with current_tab:
    st.title("Current Gameweek Fixtures")
    st.dataframe(
        data=df_current_fixtures.style.applymap(
            advantage_colours, subset=["Home Difficulty Advantage"]
        )
    )
with next_tab:
    st.title("Next Gameweek Fixtures")
    st.write("*higher advantage is better for home team")
    st.dataframe(data=df_next_fixtures)
with future_tab:
    sel_start_gw, sel_end_gw = st.select_slider(
        "Select a Gameweek Range:",
        options=[item for item in range(next_gw, 38 + 1)],
        value=(next_gw, next_gw + 5),
    )
    df_future_fixtures = load_future_fixtures(df_fixtures)
    st.dataframe(df_future_fixtures)
    st.dataframe(
        df_future_fixtures[["Home Team", "Home Difficulty Advantage"]]
        .groupby("Home Team")
        .agg(["mean", "count"])
    )
