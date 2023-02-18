from db_tables import *
import pyarrow.parquet as pq
import pyarrow as pa
import numpy as np


app_context = app.app_context()
app_context.push()

# manager_players


def assign_active(starting, sub_in, sub_out):
    if (starting == 1 and sub_out == 0) or (starting == 0 and sub_in == 1):
        return True
    elif starting == 0 and sub_in == 0:
        return False


player_info = pq.read_table(f"data/streamlit/all/player_info.parquet").to_pandas()

teams = pq.read_table(f"data/streamlit/all/teams.parquet").to_pandas()
teams = teams[["team_id", "team_name"]]

player_fixture_history = PlayerFixtureHistory().query.all()
player_fixture_history = object_as_df(player_fixture_history)

player_teams = player_fixture_history[["player_id", "team_id", "gameweek_id"]]
player_teams = player_teams.merge(teams, on=["team_id"], how="left")

player_gw_sum = [
    "player_id",
    "gameweek_id",
    "total_points",
    "goals_scored",
    "clean_sheets",
    "own_goals",
    "penalties_saved",
    "penalties_missed",
    "yellow_cards",
    "red_cards",
    "saves",
    "bonus",
    "bps",
    "minutes",
    "goals_conceded",
    "expected_goals",
    "expected_assists",
    "expected_goal_involvements",
    "expected_goals_conceded",
]

no_of_fixtures = (
    player_fixture_history[["player_id", "gameweek_id", "team_id"]]
    .groupby(["player_id", "gameweek_id"])
    .count()
    .reset_index()
    .rename(columns={"team_id": "fixture_count"})
)


player_gw = player_fixture_history[player_gw_sum]
player_gw = player_gw.groupby(["player_id", "gameweek_id"]).mean().reset_index()
player_gw = player_gw.merge(no_of_fixtures, on=["player_id", "gameweek_id"], how="left")

gw_picks = GameweekPicks().query.all()
gw_picks = object_as_df(gw_picks)
gw_picks_rename = {"position": "field_position", "starting": "picked"}

gw_picks = gw_picks.rename(columns=gw_picks_rename)

manager_info = pq.read_table(
    f"data/streamlit/manager_performance/manager_info.parquet"
).to_pandas()

gw_picks = gw_picks.merge(
    manager_info[["manager_id", "full_name", "squad_name"]],
    on=["manager_id"],
    how="left",
)

gw_picks = gw_picks.merge(
    player_teams,
    on=["player_id", "gameweek_id"],
    how="left",
)

gw_picks = gw_picks.merge(player_gw, on=["player_id", "gameweek_id"], how="left")

gw_picks = gw_picks.merge(player_info, on=["player_id"], how="left")

gw_picks = gw_picks.drop(["team_id"], axis=1)

gw_subs = GameweekSubs().query.all()
gw_subs = object_as_df(gw_subs)

subbed_out = gw_subs[["manager_id", "gameweek_id", "player_out"]].rename(
    columns={"player_out": "player_id"}
)
subbed_out["subbed_out"] = 1

subbed_in = gw_subs[["manager_id", "gameweek_id", "player_in"]].rename(
    columns={"player_in": "player_id"}
)
subbed_in["subbed_in"] = 1

gw_picks = gw_picks.merge(
    subbed_in, on=["manager_id", "gameweek_id", "player_id"], how="left"
)
gw_picks = gw_picks.merge(
    subbed_out, on=["manager_id", "gameweek_id", "player_id"], how="left"
)


testing = gw_picks.loc[gw_picks["player_id"] == 13]

testing = testing.loc[testing["manager_id"] == 10_299_002]

print(testing)
