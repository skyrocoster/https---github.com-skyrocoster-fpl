import requests
import json
import pandas as pd
from db_tables import *
import os
from pathlib import Path

fpl_api = "https://fantasy.premierleague.com/api/"
raw_extract = "data/raw_extract/"
app_context = app.app_context()
app_context.push()

# Bootstrap-static
api_url = f"{fpl_api}bootstrap-static/"
root_folder = f"{raw_extract}bootstrap-static/"

result = requests.get(api_url).json()
with open(f"{root_folder}bootstrap-static.json", "w", encoding="utf-8") as f:
    json.dump(result, f, ensure_ascii=False, indent=4)

bootstrap = json.load(open(f"{root_folder}bootstrap-static.json", encoding="utf8"))
bootstrap_events = pd.DataFrame(bootstrap["events"])
bootstrap_events = bootstrap_events.loc[
    bootstrap_events["most_captained"].notna()
].rename(columns={"id": "gameweek_id"})

# top_element
bootstrap_top_element_temp = list(
    zip(bootstrap_events["gameweek_id"], bootstrap_events["top_element_info"])
)
bootstrap_top_element = []
for week in bootstrap_top_element_temp:
    week[1]["gameweek_id"] = week[0]
    bootstrap_top_element.append(week[1])
bootstrap_top_element = (
    pd.DataFrame(bootstrap_top_element)
    .rename(columns={"id": "player_id", "points": "gameweek_points"})
    .to_dict(orient="records")
)
for week in bootstrap_top_element:
    db.session.merge(TopPlayers(**week))
db.session.commit()

# chip_plays
bootstrap_chips_temp = list(
    zip(bootstrap_events["gameweek_id"], bootstrap_events["chip_plays"])
)
bootstrap_chips = []
for week in bootstrap_chips_temp:
    week_dict = {}
    if len(week[-1]) > 0:
        for i in week[-1]:
            week_dict.update({i.get("chip_name"): i.get("num_played")})
            week_dict.update({"gameweek_id": week[0]})
        bootstrap_chips.append(week_dict)
bootstrap_chips = (
    pd.DataFrame(bootstrap_chips)
    .fillna(0)
    .astype(int)
    .rename(columns={"bboost": "bench_boost", "3xc": "triple_captain"})
    .to_dict(orient="records")
)
for week in bootstrap_chips:
    db.session.merge(ChipsPlayed(**week))
db.session.commit()

# events
float_to_int = [
    "highest_scoring_entry",
    "highest_score",
    "most_selected",
    "most_transferred_in",
    "most_captained",
    "most_vice_captained",
    "top_element",
]
bootstrap_events = bootstrap_events.drop(["top_element_info", "chip_plays"], axis=1)
bootstrap_events[float_to_int] = bootstrap_events[float_to_int].astype(int)
bootstrap_events["deadline_time"] = pd.to_datetime(bootstrap_events["deadline_time"])
bootstrap_events = bootstrap_events.rename(
    columns={"top_element": "top_player"}
).to_dict(orient="records")
for week in bootstrap_events:
    db.session.merge(Gameweeks(**week))
db.session.commit()

# teams

bootstrap_teams = (
    pd.DataFrame(bootstrap["teams"])
    .rename(columns={"id": "team_id", "name": "team_name"})
    .to_dict(orient="records")
)
for team in bootstrap_teams:
    db.session.merge(Teams(**team))
db.session.commit()

# players
map_position = {}
for i in bootstrap["element_types"]:
    map_position.update({i.get("id"): i.get("singular_name_short")})

bootstrap_players = pd.DataFrame(bootstrap["elements"]).rename(
    columns={"id": "player_id", "team": "team_id", "element_type": "position"}
)
bootstrap_players = bootstrap_players.drop("news_added", axis=1)
bootstrap_players["position"] = bootstrap_players["position"].map(map_position)
# float
obj_to_float = [
    "ep_next",
    "ep_this",
    "form",
    "points_per_game",
    "selected_by_percent",
    "value_form",
    "value_season",
    "influence",
    "creativity",
    "threat",
    "ict_index",
    "expected_goals",
    "expected_assists",
    "expected_goal_involvements",
    "expected_goals_conceded",
]
bootstrap_players[obj_to_float] = bootstrap_players[obj_to_float].astype("float")
bootstrap_players = bootstrap_players.fillna(0).to_dict(orient="records")
for player in bootstrap_players:
    db.session.merge(Players(**player))
db.session.commit()

# Fixtures
api_url = f"{fpl_api}fixtures/"
root_folder = f"{raw_extract}fixtures/"

result = requests.get(api_url).json()
with open(f"{root_folder}fixtures.json", "w", encoding="utf-8") as f:
    json.dump(result, f, ensure_ascii=False, indent=4)

fixtures = json.load(open(f"{root_folder}fixtures.json", encoding="utf8"))
fixtures = pd.DataFrame(fixtures).rename(
    columns={"event": "gameweek_id", "id": "fixture_id"}
)

fixture_stats = fixtures[["fixture_id", "stats", "team_a", "team_h"]][
    fixtures["stats"].map(lambda d: len(d)) > 0
].to_dict(orient="records")

goals = []
assists = []
owngoals = []
pensaves = []
penmisses = []
yellows = []
reds = []
saves = []
bonuses = []
bps = []
for i in fixture_stats:
    fixture_id = i.get("fixture_id")
    away_team = i.get("team_a")
    home_team = i.get("team_h")

    fixture_goals = i.get("stats")[0]
    away_goals = fixture_goals.get("a")
    home_goals = fixture_goals.get("h")
    if len(away_goals) > 0:
        for goal in away_goals:
            goal["fixture_id"] = fixture_id
            goal["team_id"] = away_team
            goal["location"] = "away"
            goals.append(goal)
    if len(home_goals) > 0:
        for goal in home_goals:
            goal["fixture_id"] = fixture_id
            goal["team_id"] = home_team
            goal["location"] = "home"
            goals.append(goal)

    fixture_assists = i.get("stats")[1]
    away_assists = fixture_assists.get("a")
    home_assists = fixture_assists.get("h")
    if len(away_assists) > 0:
        for goal in away_assists:
            goal["fixture_id"] = fixture_id
            goal["team_id"] = away_team
            goal["location"] = "away"
            assists.append(goal)
    if len(home_assists) > 0:
        for goal in home_assists:
            goal["fixture_id"] = fixture_id
            goal["team_id"] = home_team
            goal["location"] = "home"
            assists.append(goal)

    fixture_owngoals = i.get("stats")[2]
    away_owngoals = fixture_owngoals.get("a")
    home_owngoals = fixture_owngoals.get("h")
    if len(away_owngoals) > 0:
        for goal in away_owngoals:
            goal["fixture_id"] = fixture_id
            goal["team_id"] = away_team
            goal["location"] = "away"
            owngoals.append(goal)
    if len(home_owngoals) > 0:
        for goal in home_owngoals:
            goal["fixture_id"] = fixture_id
            goal["team_id"] = home_team
            goal["location"] = "home"
            owngoals.append(goal)

    fixture_pensaves = i.get("stats")[3]
    away_pensaves = fixture_pensaves.get("a")
    home_pensaves = fixture_pensaves.get("h")
    if len(away_pensaves) > 0:
        for goal in away_pensaves:
            goal["fixture_id"] = fixture_id
            goal["team_id"] = away_team
            goal["location"] = "away"
            pensaves.append(goal)
    if len(home_pensaves) > 0:
        for goal in home_pensaves:
            goal["fixture_id"] = fixture_id
            goal["team_id"] = home_team
            goal["location"] = "home"
            pensaves.append(goal)

    fixture_penmisses = i.get("stats")[4]
    away_penmisses = fixture_penmisses.get("a")
    home_penmisses = fixture_penmisses.get("h")
    if len(away_penmisses) > 0:
        for goal in away_penmisses:
            goal["fixture_id"] = fixture_id
            goal["team_id"] = away_team
            goal["location"] = "away"
            penmisses.append(goal)
    if len(home_penmisses) > 0:
        for goal in home_penmisses:
            goal["fixture_id"] = fixture_id
            goal["team_id"] = home_team
            goal["location"] = "home"
            penmisses.append(goal)

    fixture_yellows = i.get("stats")[5]
    away_yellows = fixture_yellows.get("a")
    home_yellows = fixture_yellows.get("h")
    if len(away_yellows) > 0:
        for goal in away_yellows:
            goal["fixture_id"] = fixture_id
            goal["team_id"] = away_team
            goal["location"] = "away"
            yellows.append(goal)
    if len(home_yellows) > 0:
        for goal in home_yellows:
            goal["fixture_id"] = fixture_id
            goal["team_id"] = home_team
            goal["location"] = "home"
            yellows.append(goal)

    fixture_reds = i.get("stats")[6]
    away_reds = fixture_reds.get("a")
    home_reds = fixture_reds.get("h")
    if len(away_reds) > 0:
        for goal in away_reds:
            goal["fixture_id"] = fixture_id
            goal["team_id"] = away_team
            goal["location"] = "away"
            reds.append(goal)
    if len(home_reds) > 0:
        for goal in home_reds:
            goal["fixture_id"] = fixture_id
            goal["team_id"] = home_team
            goal["location"] = "home"
            reds.append(goal)

    fixture_saves = i.get("stats")[7]
    away_saves = fixture_saves.get("a")
    home_saves = fixture_saves.get("h")
    if len(away_saves) > 0:
        for goal in away_saves:
            goal["fixture_id"] = fixture_id
            goal["team_id"] = away_team
            goal["location"] = "away"
            saves.append(goal)
    if len(home_saves) > 0:
        for goal in home_saves:
            goal["fixture_id"] = fixture_id
            goal["team_id"] = home_team
            goal["location"] = "home"
            saves.append(goal)

    fixture_bonuses = i.get("stats")[8]
    away_bonuses = fixture_bonuses.get("a")
    home_bonuses = fixture_bonuses.get("h")
    if len(away_bonuses) > 0:
        for goal in away_bonuses:
            goal["fixture_id"] = fixture_id
            goal["team_id"] = away_team
            goal["location"] = "away"
            bonuses.append(goal)
    if len(home_bonuses) > 0:
        for goal in home_bonuses:
            goal["fixture_id"] = fixture_id
            goal["team_id"] = home_team
            goal["location"] = "home"
            bonuses.append(goal)

    fixture_bps = i.get("stats")[9]
    away_bps = fixture_bps.get("a")
    home_bps = fixture_bps.get("h")
    if len(away_bps) > 0:
        for goal in away_bps:
            goal["fixture_id"] = fixture_id
            goal["team_id"] = away_team
            goal["location"] = "away"
            bps.append(goal)
    if len(home_bps) > 0:
        for goal in home_bps:
            goal["fixture_id"] = fixture_id
            goal["team_id"] = home_team
            goal["location"] = "home"
            bps.append(goal)

stats_rename = {"element": "player_id"}

goals = pd.DataFrame(goals).rename(columns=stats_rename).to_dict(orient="records")
assists = pd.DataFrame(assists).rename(columns=stats_rename).to_dict(orient="records")
owngoals = pd.DataFrame(owngoals).rename(columns=stats_rename).to_dict(orient="records")
pensaves = pd.DataFrame(pensaves).rename(columns=stats_rename).to_dict(orient="records")
penmisses = (
    pd.DataFrame(penmisses).rename(columns=stats_rename).to_dict(orient="records")
)
yellows = pd.DataFrame(yellows).rename(columns=stats_rename).to_dict(orient="records")
reds = pd.DataFrame(reds).rename(columns=stats_rename).to_dict(orient="records")
saves = pd.DataFrame(saves).rename(columns=stats_rename).to_dict(orient="records")
bonuses = pd.DataFrame(bonuses).rename(columns=stats_rename).to_dict(orient="records")
bps = pd.DataFrame(bps).rename(columns=stats_rename).to_dict(orient="records")

for goal in goals:
    db.session.merge(FixtureGoals(**goal))
db.session.commit()

for assist in assists:
    db.session.merge(FixtureAssists(**assist))
db.session.commit()

for owngoal in owngoals:
    db.session.merge(FixtureOwnGoals(**owngoal))
db.session.commit()

for pensave in pensaves:
    db.session.merge(FixturePenSaves(**pensave))
db.session.commit()

for penmiss in penmisses:
    db.session.merge(FixturePenMisses(**penmiss))
db.session.commit()

for yellow in yellows:
    db.session.merge(FixtureYellows(**yellow))
db.session.commit()

for red in reds:
    db.session.merge(FixtureReds(**red))
db.session.commit()

for save in saves:
    db.session.merge(FixtureSaves(**save))
db.session.commit()

for bonus in bonuses:
    db.session.merge(FixtureBonuses(**bonus))
db.session.commit()

for bp in bps:
    db.session.merge(FixtureBPS(**bp))
db.session.commit()

fixtures["kickoff_time"] = pd.to_datetime(fixtures["kickoff_time"].fillna(0))
fixtures = fixtures.drop(["stats"], axis=1).fillna(0).to_dict(orient="records")
for fixture in fixtures:
    db.session.merge(Fixtures(**fixture))
db.session.commit()

# Players
api_url = f"{fpl_api}element-summary/"
root_folder = f"{raw_extract}element-summary/"

player_list = Players().query.all()
player_list = [player.player_id for player in player_list]

for player in player_list:
    result = requests.get(f'{api_url}{player}/').json()
    with open(f"{root_folder}{player}.json", "w", encoding="utf-8") as f:
        json.dump(result, f, ensure_ascii=False, indent=4)

player_remaining_fixtures = pd.DataFrame()
remaining_rename = {"id": "fixture_id", "event": "gameweek_id"}

player_fixture_history = pd.DataFrame()
history_rename = {
    "element": "player_id",
    "fixture": "fixture_id",
    "round": "gameweek_id",
}

player_fixture_historypast = pd.DataFrame()


for player in player_list:
    player_id = player
    player = json.load(open(f"{root_folder}{player}.json", encoding="utf8"))

    # player_remaining_fixtures_temp = pd.DataFrame(player["fixtures"]).drop(
    #     ["team_h_score", "team_a_score", "minutes"], axis=1
    # )
    # player_remaining_fixtures_temp["player_id"] = player_id
    # player_remaining_fixtures = pd.concat(
    #     [player_remaining_fixtures, player_remaining_fixtures_temp]
    # )

    player_fixture_history_temp = pd.DataFrame(player["history"])
    player_fixture_history = pd.concat(
        [player_fixture_history, player_fixture_history_temp]
    )

    player_fixture_historypast_temp = pd.DataFrame(player["history_past"])
    player_fixture_historypast_temp["player_id"] = player_id
    player_fixture_historypast = pd.concat(
        [player_fixture_historypast, player_fixture_historypast_temp]
    )


# player_remaining_fixtures["kickoff_time"] = pd.to_datetime(
#     player_remaining_fixtures["kickoff_time"].fillna(0)
# )
# player_remaining_fixtures = player_remaining_fixtures.rename(
#     columns=remaining_rename
# ).to_dict(orient="records")
# for fixture in player_remaining_fixtures:
#     db.session.merge(PlayerRemainingFixtures(**fixture))
# db.session.commit()

player_fixture_history["kickoff_time"] = pd.to_datetime(
    player_fixture_history["kickoff_time"]
)
player_fixture_history = player_fixture_history.rename(columns=history_rename).to_dict(
    orient="records"
)
for fixture in player_fixture_history:
    db.session.merge(PlayerFixtureHistory(**fixture))
db.session.commit()

player_fixture_historypast = player_fixture_historypast.convert_dtypes().to_dict(
    orient="records"
)
for season in player_fixture_historypast:
    db.session.merge(PlayerPreviousSeasons(**season))
db.session.commit()

# Leagues
api_url = f"{fpl_api}leagues-classic/"
root_folder = f"{raw_extract}leagues-classic/"

league_list = [2257667, 1567329]

for league in league_list:
    result = requests.get(f"{api_url}{league}/standings").json()
    with open(f"{root_folder}{league}.json", "w", encoding="utf-8") as f:
        json.dump(result, f, ensure_ascii=False, indent=4)

league_info = pd.DataFrame()
league_info_rename = {"id": "league_id"}

league_managers = pd.DataFrame()
league_managers_rename = {"entry": "manager_id"}
league_managers_cols = ["manager_id", "league_id"]

for league in league_list:
    league_id = league
    league = json.load(open(f"{root_folder}{league}.json", encoding="utf8"))

    league_info_temp = pd.DataFrame(league["league"], index=[0]).rename(
        columns=league_info_rename
    )
    league_info = pd.concat([league_info, league_info_temp])

    league_managers_temp = pd.DataFrame(league["standings"]["results"]).rename(
        columns=league_managers_rename
    )
    league_managers_temp["league_id"] = league_id
    league_managers = pd.concat(
        [league_managers, league_managers_temp[league_managers_cols]]
    )


league_info["created"] = pd.to_datetime(league_info["created"])
league_info = (
    league_info.reset_index(drop=True)
    .convert_dtypes()
    .drop(["max_entries", "cup_league", "rank"], axis=1)
    .to_dict(orient="records")
)
for league in league_info:
    db.session.merge(LeagueInfo(**league))
db.session.commit()

league_managers = league_managers.convert_dtypes().to_dict(orient="records")
for manager in league_managers:
    db.session.merge(ManagerLeagues(**manager))
db.session.commit()

# Managers/Leagues
api_url = f"{fpl_api}entry/"
root_folder = f"{raw_extract}entry/"

managers = ManagerLeagues().query.all()
manager_list = set([manager.manager_id for manager in managers])

for manager in manager_list:
    result = requests.get(f"{api_url}{manager}/").json()
    with open(f"{root_folder}{manager}.json", "w", encoding="utf-8") as f:
        json.dump(result, f, ensure_ascii=False, indent=4)

manager_leagues = pd.DataFrame()
manager_classic_rename = {"id": "league_id", "cup_league": "cup_league_id"}
manager_classic_cols = [
    "league_id",
    "name",
    "short_name",
    "created",
    "league_type",
    "scoring",
    "start_event",
    "has_cup",
    "cup_league_id",
    "type",
]

manager_h2h_rename = {"id": "league_id"}
manager_h2h_cols = [
    "league_id",
    "name",
    "short_name",
    "created",
    "league_type",
    "scoring",
    "has_cup",
    "type",
]

manager_info = pd.DataFrame()
manager_info_rename = {
    "id": "manager_id",
    "player_first_name": "manager_first_name",
    "player_last_name": "manager_last_name",
    "player_region_id": "manager_region_id",
    "player_region_name": "manager_region_name",
    "player_region_iso_code_short": "manager_region_iso_code_short",
    "player_region_iso_code_long": "manager_region_iso_code_long",
    "name": "squad_name",
    "started_event": "started_gameweek",
}
manager_info_cols = [
    "manager_id",
    "joined_time",
    "started_gameweek",
    "favourite_team",
    "manager_first_name",
    "manager_last_name",
    "manager_region_id",
    "manager_region_name",
    "manager_region_iso_code_short",
    "manager_region_iso_code_long",
    "squad_name",
]

for manager in manager_list:
    manager_id = manager
    manager = json.load(open(f"{root_folder}{manager}.json", encoding="utf8"))

    manager_leagues_temp = pd.DataFrame(manager["leagues"]["classic"]).rename(
        columns=manager_classic_rename
    )
    manager_leagues_temp["type"] = "classic"
    manager_leagues = pd.concat(
        [manager_leagues, manager_leagues_temp[manager_classic_cols]]
    )

    if len(manager["leagues"]["h2h"]) > 0:
        manager_leagues_temp = pd.DataFrame(manager["leagues"]["h2h"]).rename(
            columns=manager_h2h_rename
        )
        manager_leagues_temp["type"] = "h2h"
        manager_leagues = pd.concat(
            [manager_leagues, manager_leagues_temp[manager_h2h_cols]]
        )

    manager_info_temp = pd.DataFrame(manager).rename(columns=manager_info_rename)
    manager_info = pd.concat([manager_info, manager_info_temp[manager_info_cols]])


manager_leagues["created"] = pd.to_datetime(manager_leagues["created"])
manager_leagues = (
    manager_leagues.reset_index(drop=True)
    .fillna(0)
    .convert_dtypes()
    .drop_duplicates()
    .to_dict(orient="records")
)

for league in manager_leagues:
    db.session.merge(LeagueInfo(**league))
db.session.commit()

manager_info["joined_time"] = pd.to_datetime(manager_info["joined_time"])
manager_info = manager_info.convert_dtypes().fillna(0).to_dict(orient="records")

for manager in manager_info:
    db.session.merge(ManagerInfo(**manager))
db.session.commit()


# Manager History
api_url = f"{fpl_api}entry/"
root_folder = f"{raw_extract}entry/history/"

managers = ManagerInfo().query.all()
manager_list = set([manager.manager_id for manager in managers])

for manager in manager_list:
    result = requests.get(f"{api_url}{manager}/history/").json()
    with open(f"{root_folder}{manager}.json", "w", encoding="utf-8") as f:
        json.dump(result, f, ensure_ascii=False, indent=4)

manager_gameweek = pd.DataFrame()
manager_gameweek_rename = {
    "event": "gameweek_id",
    "rank": "gameweek_rank",
    "event_transfers": "gameweek_transfers",
    "event_transfers_cost": "gameweek_transfers_cost",
}

manager_seasons = pd.DataFrame()

manager_chips = pd.DataFrame()
manager_chips_rename = {"event": "gameweek_id"}

for manager in manager_list:
    manager_id = manager
    manager = json.load(open(f"{root_folder}{manager}.json", encoding="utf8"))

    manager_gameweek_temp = pd.DataFrame(manager["current"]).rename(
        columns=manager_gameweek_rename
    )
    manager_gameweek_temp["manager_id"] = manager_id
    manager_gameweek = pd.concat([manager_gameweek, manager_gameweek_temp])

    if len(manager["past"]) > 0:
        manager_seasons_temp = pd.DataFrame(manager["past"])
        manager_seasons_temp["manager_id"] = manager_id
        manager_seasons = pd.concat([manager_seasons, manager_seasons_temp])

    if len(manager["chips"]) > 0:
        manager_chips_temp = pd.DataFrame(manager["chips"]).rename(
            columns=manager_chips_rename
        )
        manager_chips_temp["manager_id"] = manager_id
        manager_chips = pd.concat([manager_chips, manager_chips_temp])

manager_gameweek = manager_gameweek.fillna(0).convert_dtypes().to_dict(orient="records")
for manager in manager_gameweek:
    db.session.merge(ManagerGameweeks(**manager))
db.session.commit()

manager_seasons = manager_seasons.convert_dtypes().to_dict(orient="records")
for season in manager_seasons:
    db.session.merge(ManagerSeasons(**season))
db.session.commit()

manager_chips["time"] = pd.to_datetime(manager_chips["time"])
manager_chips = manager_chips.convert_dtypes().to_dict(orient="records")
for chip in manager_chips:
    db.session.merge(ManagerChips(**chip))
db.session.commit()

# Manager Pick/Subs
api_url = f"{fpl_api}entry/"
root_folder = f"{raw_extract}entry/gameweeks/"

manager_gameweeks = ManagerGameweeks().query.all()
manager_list = set([manager.manager_id for manager in manager_gameweeks])
gameweek_list = set([gameweek.gameweek_id for gameweek in manager_gameweeks])

for gameweek in gameweek_list:
    gameweek_id = gameweek
    try:
        os.makedirs(f"{root_folder}{gameweek_id}")
    except:
        pass

    for manager in manager_list:
        result = requests.get(f"{api_url}{manager}/event/{gameweek}/picks/").json()
        if len(result) > 1:
            with open(
                f"{root_folder}{gameweek}/{manager}.json", "w", encoding="utf-8"
            ) as f:
                json.dump(result, f, ensure_ascii=False, indent=4)

gameweek_subs = pd.DataFrame()
gameweek_subs_rename = {
    "entry": "manager_id",
    "element_in": "player_in",
    "element_out": "player_out",
    "event": "gameweek_id",
}

gameweek_picks = pd.DataFrame()
gameweek_picks_rename = {"element": "player_id"}


for gameweek in gameweek_list:
    gameweek_id = gameweek

    for manager in manager_list:
        manager_id = manager
        if not Path(f"{root_folder}{gameweek}/{manager}.json").is_file():
            continue

        manager = json.load(
            open(f"{root_folder}{gameweek}/{manager}.json", encoding="utf8")
        )

        gameweek_subs_temp = pd.DataFrame(manager["automatic_subs"]).rename(
            columns=gameweek_subs_rename
        )
        gameweek_subs = pd.concat([gameweek_subs, gameweek_subs_temp])

        gameweek_picks_temp = pd.DataFrame(manager["picks"]).rename(
            columns=gameweek_picks_rename
        )
        gameweek_picks_temp["gameweek_id"] = gameweek_id
        gameweek_picks_temp["manager_id"] = manager_id
        gameweek_picks = pd.concat([gameweek_picks, gameweek_picks_temp])


gameweek_subs = gameweek_subs.convert_dtypes().to_dict(orient="records")
for sub in gameweek_subs:
    db.session.merge(GameweekSubs(**sub))
db.session.commit()

gameweek_picks.loc[gameweek_picks["position"] <= 11, "starting"] = True
gameweek_picks["starting"] = gameweek_picks["starting"].fillna(0)
gameweek_picks = gameweek_picks.convert_dtypes().to_dict(orient="records")
for pick in gameweek_picks:
    db.session.merge(GameweekPicks(**pick))
db.session.commit()

# Team Results

fixtures = Fixtures().query.all()

team_fixtures = []
for fixture in fixtures:
    home_team = {}
    away_team = {}

    home_team["team_id"] = fixture.team_h
    away_team["team_id"] = fixture.team_a
    home_team["opponent_id"] = fixture.team_a
    away_team["opponent_id"] = fixture.team_h
    home_team["home"] = 1
    away_team["home"] = 0
    home_team["fixture_id"] = fixture.fixture_id
    away_team["fixture_id"] = fixture.fixture_id
    home_team["score"] = fixture.team_h_score
    away_team["score"] = fixture.team_a_score
    home_team["opponent_score"] = fixture.team_a_score
    away_team["opponent_score"] = fixture.team_h_score
    home_team["finished"] = fixture.finished
    away_team["finished"] = fixture.finished
    home_team["gameweek_id"] = fixture.gameweek_id
    away_team["gameweek_id"] = fixture.gameweek_id
    home_team["fixture_difficulty"] = fixture.team_h_difficulty
    away_team["fixture_difficulty"] = fixture.team_a_difficulty
    home_team["kickoff_time"] = fixture.kickoff_time
    away_team["kickoff_time"] = fixture.kickoff_time
    if fixture.team_h_score > fixture.team_a_score:
        home_team["win"] = "win"
        away_team["win"] = "loss"
    elif fixture.team_h_score < fixture.team_a_score:
        home_team["win"] = "loss"
        away_team["win"] = "win"
    else:
        home_team["win"] = "draw"
        away_team["win"] = "draw"

    team_fixtures.append(home_team)
    team_fixtures.append(away_team)

for fixture in team_fixtures:
    db.session.merge(TeamFixtureResults(**fixture))
db.session.commit()
