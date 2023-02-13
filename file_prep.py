from db_tables import *
import pyarrow.parquet as pq
import pyarrow as pa
import numpy as np


def map_team_names(x):
    teams = pq.read_table(f"data/streamlit/all/teams.parquet").to_pandas()
    teams = dict(zip(teams["team_id"], teams["team_name"]))
    return teams.get(x)


app_context = app.app_context()
app_context.push()


# All

data = "data/streamlit/all/"
file_name = "teams"
teams = Teams().query.all()
df = object_as_df(teams)
pq.write_table(pa.Table.from_pandas(df), f"{data}{file_name}.parquet")


# Team Games
data = "data/streamlit/fixtures/"
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
    "expected_goals",
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
    "expected_goals": "total_expected_goals",
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
df["avg_value"] = df["avg_value"].div(10)
df = df.sort_values(["gameweek_id", "fixture_id"])
df["fixture_number"] = (
    df.groupby("team_name")["gameweek_id"].rank(method="first").astype("int")
)
df["clean_sheet"] = np.where(df["total_clean_sheets"] > 0, True, False)

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

# Managers
data = "data/streamlit/manager_performance/"

file_name = "manager_info"

manager_info = ManagerInfo().query.all()
manager_info = object_as_df(manager_info)
manager_info["full_name"] = (
    manager_info["manager_first_name"].str.title()
    + " "
    + manager_info["manager_last_name"].str.title()
)
manager_info["squad_name"] = manager_info["squad_name"].str.title()
pq.write_table(pa.Table.from_pandas(manager_info), f"{data}{file_name}.parquet")

file_name = "manager_chips"

manager_chips = ManagerChips().query.all()
manager_chips = object_as_df(manager_chips)
manager_chips["value"] = 1
manager_chips = pd.pivot(
    manager_chips, index=["manager_id", "gameweek_id"], columns="name", values="value"
)
manager_chips = manager_chips.reset_index()
chips = ["3xc", "bboost", "freehit", "wildcard"]
manager_chips[chips] = manager_chips[chips].fillna(0).astype("bool")
pq.write_table(pa.Table.from_pandas(manager_chips), f"{data}{file_name}.parquet")


file_name = "manager_gameweeks"
manager_gw_rename = {"total_points": "points_to_gameweek"}

manager_gws = ManagerGameweeks().query.all()
manager_gws = object_as_df(manager_gws)
manager_gws["value"] = manager_gws["value"].div(10)
manager_gws["bank"] = manager_gws["bank"].div(10)

manager_gws = manager_gws.merge(
    manager_info[["manager_id", "full_name", "squad_name"]],
    on=["manager_id"],
    how="left",
)
manager_gws = manager_gws.merge(
    manager_chips, on=["manager_id", "gameweek_id"], how="left"
)
chips = ["3xc", "bboost", "freehit", "wildcard"]
manager_gws[chips] = manager_gws[chips].fillna(0).astype("bool")
manager_gws = manager_gws.rename(columns=manager_gw_rename)
pq.write_table(pa.Table.from_pandas(manager_gws), f"{data}{file_name}.parquet")


# Player maps
data = "data/streamlit/all/"
file_name = "player_info"

player_info = Players().query.all()
player_info = object_as_df(player_info)
player_info_rename = {"web_name": "player_name"}
player_info = player_info[["player_id", "position", "web_name"]].rename(
    columns=player_info_rename
)
pq.write_table(pa.Table.from_pandas(player_info), f"{data}{file_name}.parquet")

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
    player_fixture_history[["player_id", "gameweek_id"]]
    .groupby(["player_id", "gameweek_id"])
    .value_counts()
    .reset_index()
    .rename(columns={0: "fixture_count"})
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

gw_picks[["subbed_in", "subbed_out"]] = gw_picks[["subbed_in", "subbed_out"]].fillna(0)
gw_picks["active"] = gw_picks.apply(
    lambda x: assign_active(x.picked, x.subbed_in, x.subbed_out), axis=1
)

int8_list = [
    "saves",
    "bonus",
    "bps",
    "minutes",
    "fixture_count",
    "goals_conceded",
    "field_position",
    "multiplier",
    "total_points",
    "goals_scored",
    "clean_sheets",
    "own_goals",
    "penalties_saved",
    "penalties_missed",
    "yellow_cards",
    "red_cards",
]
int16_list = ["gameweek_id", "player_id"]
float32_list = [
    "expected_goals",
    "expected_assists",
    "expected_goal_involvements",
    "expected_goals_conceded",
]
bool_list = ["subbed_in", "subbed_out", "active"]

gw_picks[int8_list] = gw_picks[int8_list].fillna(0).astype("int8")
gw_picks[int16_list] = gw_picks[int16_list].astype("int16")
gw_picks[float32_list] = gw_picks[float32_list].astype("float32")
gw_picks[bool_list] = gw_picks[bool_list].astype("bool")

data = "data/streamlit/manager_performance/"
file_name = "manager_players"

pq.write_table(pa.Table.from_pandas(gw_picks), f"{data}{file_name}.parquet")
