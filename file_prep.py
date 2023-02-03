from db_tables import *
import pyarrow.parquet as pq
import pyarrow as pa


def map_team_names(x):
    teams = object_as_df(Teams().query.all())
    teams = dict(zip(teams["team_id"], teams["team_name"]))
    return teams.get(x)


app_context = app.app_context()
app_context.push()

data = "data/streamlit/fixtures/"

# Team Games

file_name = "teamgames"

teamgames = TeamFixtureResults().query.all()
df = object_as_df(teamgames)
df["team_name"] = df["team_id"].apply(map_team_names)
df["opponent_name"] = df["opponent_id"].apply(map_team_names)
df = df[
    [
        "fixture_id",
        "team_name",
        "opponent_name",
        "fixture_difficulty",
        "finished",
        "score",
        "opponent_score",
        "win",
        "home",
        "gameweek_id",
        "kickoff_time",
    ]
]  # set column order

pq.write_table(pa.Table.from_pandas(df), f"{data}{file_name}.parquet")

# Team Blanks

file_name = "potential_dgw"

field_rename = {"gameweek_id": "Gameweek", "kickoff_time": "Kickoff Time"}
fixtures = Fixtures().query.all()
df = object_as_df(fixtures)
df = df.loc[(df["gameweek_id"] == 0)]
df["Home Team"] = df["team_h"].apply(map_team_names)
df["Away Team"] = df["team_a"].apply(map_team_names)
df = df.rename(columns=field_rename).sort_values(["Gameweek", "Home Team"])
df = df[["Home Team", "Away Team"]]

pq.write_table(pa.Table.from_pandas(df), f"{data}{file_name}.parquet")

# Fixtures

file_name = "fixtures"
field_rename = {"gameweek_id": "Gameweek", "kickoff_time": "Kickoff Time"}

fixtures = Fixtures().query.all()
df = object_as_df(fixtures)

df["Home Team"] = df["team_h"].apply(map_team_names)
df["Away Team"] = df["team_a"].apply(map_team_names)

df["Home Score"] = df.apply(
    lambda x: x["team_h_score"] if x["finished"] == 1 else "-", axis=1
).astype(str)
df["Away Score"] = df.apply(
    lambda x: x["team_a_score"] if x["finished"] == 1 else "-", axis=1
).astype(str)

df = df.rename(columns=field_rename).sort_values(["Gameweek", "Home Team"])
df = df[["Gameweek", "Home Team", "Home Score", "Away Score", "Away Team", "finished"]]

pq.write_table(pa.Table.from_pandas(df), f"{data}{file_name}.parquet")


# Team Performance

data = "data/streamlit/team_performance/"

# Fixture Stats

file_name = "fixture_stats"

player_fixtures = PlayerFixtureHistory().query.all()
df_players = object_as_df(player_fixtures)  #
df_players = df_players.loc[df_players["minutes"] > 60]
df_players["team_name"] = df_players["team_id"].apply(map_team_names)
df_players["opponent_name"] = df_players["opponent_team"].apply(map_team_names)
mean_fields = [
    "team_name",
    "fixture_id",
    "bps",
    "influence",
    "creativity",
    "threat",
    "ict_index",
    "expected_goals",
    "expected_assists",
    "expected_goal_involvements",
    "expected_goals_conceded",
    "value",
]
mean_rename = {
    "bps": "avg_bps",
    "influence": "avg_influence",
    "creativity": "avg_creativity",
    "threat": "avg_threat",
    "ict_index": "avg_ict_index",
    "expected_goals": "avg_expected_goals",
    "expected_assists": "avg_expected_assists",
    "expected_goal_involvements": "avg_expected_goal_involvements",
    "expected_goals_conceded": "avg_expected_goals_conceded",
    "value": "avg_value",
}
sum_fields = [
    "team_name",
    "fixture_id",
    "assists",
    "own_goals",
    "clean_sheets",
    "saves",
    "yellow_cards",
    "red_cards",
    "bonus",
    "penalties_saved",
    "penalties_missed",
    "total_points",
]
sum_rename = {
    "assists": "total_assists",
    "own_goals": "total_own_goals",
    "clean_sheets": "total_clean_sheets",
    "saves": "total_saves",
    "yellow_cards": "total_yellow_cards",
    "red_cards": "total_red_cards",
    "bonus": "total_bonus",
    "penalties_saved": "total_penalties_saved",
    "penalties_missed": "total_penalties_missed",
}
df_players_mean = df_players[mean_fields].rename(columns=mean_rename)
df_players_sum = df_players[sum_fields].rename(columns=sum_rename)
df_players_mean = (
    df_players_mean.groupby(["team_name", "fixture_id"]).mean().reset_index()
)
df_players_sum = df_players_sum.groupby(["team_name", "fixture_id"]).sum().reset_index()

df = pq.read_table(f"data/streamlit/fixtures/teamgames.parquet").to_pandas()
df = df.loc[df["finished"] == 1]
df = df[
    [
        "fixture_id",
        "team_name",
        "opponent_name",
        "score",
        "opponent_score",
        "home",
        "gameweek_id",
    ]
]
df = df.merge(df_players_sum, on=["team_name", "fixture_id"])
df = df.merge(df_players_mean, on=["team_name", "fixture_id"])

pq.write_table(pa.Table.from_pandas(df), f"{data}{file_name}.parquet")

# Players
data = "data/streamlit/player_performance/"
file_name = "player_fixtures"

players = object_as_df(Players().query.all())
players = players[["player_id", "position", "web_name"]]

teams = object_as_df(Teams().query.all())
teams = dict(zip(teams["team_id"], teams["team_name"]))


player_fixtures = PlayerFixtureHistory().query.all()
df = object_as_df(player_fixtures)

df = pd.merge(df, players, on="player_id")
df["team_name"] = df["team_id"].map(teams)
df["opponent_name"] = df["opponent_team"].map(teams)
df["value"] = df["value"].div(10).round(1)
df["position"] = df["position"].astype("category")
df["goals_assists"] = df["goals_scored"] + df["assists"]
df = df.drop(["team_a_score", "team_h_score"], axis=1)
df = df.rename(columns={"total_points": "gameweek_points"})

pq.write_table(pa.Table.from_pandas(df), f"{data}{file_name}.parquet")
