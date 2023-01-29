from flask import Flask
from flask_sqlalchemy import SQLAlchemy
import pandas as pd
from sqlalchemy import inspect
from time import strptime


def object_as_df(results):
    dict_result = []

    for result in results:
        new_row = {
            c.key: getattr(result, c.key) for c in inspect(result).mapper.column_attrs
        }
        dict_result.append(new_row)

    df = pd.DataFrame(dict_result)
    return df


app = Flask(__name__)
app.config["SQLALCHEMY_DATABASE_URI"] = "sqlite:///fpl.db"
db = SQLAlchemy(app)


class TopPlayers(db.Model):

    __tablename__ = "top_players"

    player_id = db.Column(
        db.Integer, db.ForeignKey("players.player_id"), primary_key=True
    )
    gameweek_id = db.Column(
        db.Integer, db.ForeignKey("gameweeks.gameweek_id"), primary_key=True
    )
    gameweek_points = db.Column(db.Integer)


class ChipsPlayed(db.Model):

    __tablename__ = "chips_played"

    gameweek_id = db.Column(
        db.Integer, db.ForeignKey("gameweeks.gameweek_id"), primary_key=True
    )
    bench_boost = db.Column(db.Integer)
    triple_captain = db.Column(db.Integer)
    freehit = db.Column(db.Integer)
    wildcard = db.Column(db.Integer)


class Gameweeks(db.Model):

    __tablename__ = "gameweeks"

    gameweek_id = db.Column(db.Integer, primary_key=True)

    top_player = db.Column(db.Integer, db.ForeignKey("players.player_id"))
    most_selected = db.Column(db.Integer, db.ForeignKey("players.player_id"))
    most_transferred_in = db.Column(db.Integer, db.ForeignKey("players.player_id"))
    most_captained = db.Column(db.Integer, db.ForeignKey("players.player_id"))
    most_vice_captained = db.Column(db.Integer, db.ForeignKey("players.player_id"))

    name = db.Column(db.String(100))
    deadline_time = db.Column(db.DateTime)
    average_entry_score = db.Column(db.Integer)
    finished = db.Column(db.Boolean)
    data_checked = db.Column(db.Boolean)
    highest_scoring_entry = db.Column(db.Integer)
    deadline_time_epoch = db.Column(db.Integer)
    deadline_time_game_offset = db.Column(db.Integer)
    highest_score = db.Column(db.Integer)
    is_previous = db.Column(db.Boolean)
    is_current = db.Column(db.Boolean)
    is_next = db.Column(db.Boolean)
    cup_leagues_created = db.Column(db.Boolean)
    h2h_ko_matches_created = db.Column(db.Boolean)
    transfers_made = db.Column(db.Integer)

    top_players = db.relationship("TopPlayers", backref="gameweeks", lazy="select")
    chips_played = db.relationship("ChipsPlayed", backref="gameweeks", lazy="select")
    fixtures = db.relationship("Fixtures", backref="gameweeks", lazy="select")
    player_fixture_history = db.relationship(
        "PlayerFixtureHistory", backref="gameweeks", lazy="select"
    )
    manager_info = db.relationship("ManagerInfo", backref="gameweeks", lazy="select")
    manager_gameweeks = db.relationship(
        "ManagerGameweeks", backref="gameweeks", lazy="select"
    )
    manager_chips = db.relationship("ManagerChips", backref="gameweeks", lazy="select")
    gameweek_subs = db.relationship("GameweekSubs", backref="gameweeks", lazy="select")
    gameweek_picks = db.relationship(
        "GameweekPicks", backref="gameweeks", lazy="select"
    )
    team_fixture_results = db.relationship(
        "TeamFixtureResults", backref="gameweeks", lazy="select"
    )


class Teams(db.Model):
    __tablename__ = "teams"

    team_id = db.Column(db.Integer, primary_key=True)
    team_name = db.Column(db.String(50))
    code = db.Column(db.Integer)
    draw = db.Column(db.Integer)
    form = db.Column(db.Integer)
    loss = db.Column(db.Integer)
    played = db.Column(db.Integer)
    points = db.Column(db.Integer)
    position = db.Column(db.Integer)
    short_name = db.Column(db.String(4))
    strength = db.Column(db.Integer)
    team_division = db.Column(db.Integer)
    unavailable = db.Column(db.Boolean)
    win = db.Column(db.Integer)
    strength_overall_home = db.Column(db.Integer)
    strength_overall_away = db.Column(db.Integer)
    strength_attack_home = db.Column(db.Integer)
    strength_attack_away = db.Column(db.Integer)
    strength_defence_home = db.Column(db.Integer)
    strength_defence_away = db.Column(db.Integer)
    pulse_id = db.Column(db.Integer)

    players = db.relationship("Players", backref="teams", lazy="select")

    fixtures_team_a = db.relationship(
        "Fixtures",
        backref="teams_team_a",
        lazy="select",
        foreign_keys="Fixtures.team_a",
    )
    fixtures_team_h = db.relationship(
        "Fixtures",
        backref="teams_team_h",
        lazy="select",
        foreign_keys="Fixtures.team_h",
    )
    teams_fixtures_results_team = db.relationship(
        "TeamFixtureResults",
        backref="teams_team_id",
        lazy="select",
        foreign_keys="TeamFixtureResults.team_id",
    )
    teams_fixtures_results_team = db.relationship(
        "TeamFixtureResults",
        backref="teams_opponent_id",
        lazy="select",
        foreign_keys="TeamFixtureResults.opponent_id",
    )

    fixture_goals = db.relationship("FixtureGoals", backref="teams", lazy="select")
    fixture_assists = db.relationship("FixtureAssists", backref="teams", lazy="select")
    fixture_owngoals = db.relationship(
        "FixtureOwnGoals", backref="teams", lazy="select"
    )
    fixture_pensaves = db.relationship(
        "FixturePenSaves", backref="teams", lazy="select"
    )
    fixture_penmisses = db.relationship(
        "FixturePenMisses", backref="teams", lazy="select"
    )
    fixture_yellows = db.relationship("FixtureYellows", backref="teams", lazy="select")
    fixture_reds = db.relationship("FixtureReds", backref="teams", lazy="select")
    fixture_saves = db.relationship("FixtureSaves", backref="teams", lazy="select")
    fixture_bonuses = db.relationship("FixtureBonuses", backref="teams", lazy="select")
    fixture_bps = db.relationship("FixtureBPS", backref="teams", lazy="select")
    player_fixture_history = db.relationship(
        "PlayerFixtureHistory", backref="teams", lazy="select"
    )
    manager_info = db.relationship("ManagerInfo", backref="teams", lazy="select")


class Players(db.Model):
    __tablename__ = "players"

    player_id = db.Column(db.Integer, primary_key=True)

    team_id = db.Column(db.Integer, db.ForeignKey("teams.team_id"))

    chance_of_playing_next_round = db.Column(db.Float)
    chance_of_playing_this_round = db.Column(db.Float)
    code = db.Column(db.Integer)
    cost_change_event = db.Column(db.Integer)
    cost_change_event_fall = db.Column(db.Integer)
    cost_change_start = db.Column(db.Integer)
    cost_change_start_fall = db.Column(db.Integer)
    dreamteam_count = db.Column(db.Integer)
    position = db.Column(db.String(3))
    ep_next = db.Column(db.Float)
    ep_this = db.Column(db.Float)
    event_points = db.Column(db.Integer)
    first_name = db.Column(db.String(100))
    form = db.Column(db.Float)
    in_dreamteam = db.Column(db.Boolean)
    news = db.Column(db.String(250))
    now_cost = db.Column(db.Integer)
    photo = db.Column(db.String(100))
    points_per_game = db.Column(db.Float)
    second_name = db.Column(db.String(100))
    selected_by_percent = db.Column(db.Float)
    special = db.Column(db.Boolean)
    squad_number = db.Column(db.Integer)
    status = db.Column(db.String(3))
    team_code = db.Column(db.Integer)
    total_points = db.Column(db.Integer)
    transfers_in = db.Column(db.Integer)
    transfers_in_event = db.Column(db.Integer)
    transfers_out = db.Column(db.Integer)
    transfers_out_event = db.Column(db.Integer)
    value_form = db.Column(db.Float)
    value_season = db.Column(db.Float)
    web_name = db.Column(db.String(100))
    minutes = db.Column(db.Integer)
    goals_scored = db.Column(db.Integer)
    assists = db.Column(db.Integer)
    clean_sheets = db.Column(db.Integer)
    goals_conceded = db.Column(db.Integer)
    own_goals = db.Column(db.Integer)
    penalties_saved = db.Column(db.Integer)
    penalties_missed = db.Column(db.Integer)
    yellow_cards = db.Column(db.Integer)
    red_cards = db.Column(db.Integer)
    saves = db.Column(db.Integer)
    bonus = db.Column(db.Integer)
    bps = db.Column(db.Integer)
    influence = db.Column(db.Float)
    creativity = db.Column(db.Float)
    threat = db.Column(db.Float)
    ict_index = db.Column(db.Float)
    starts = db.Column(db.Integer)
    expected_goals = db.Column(db.Float)
    expected_assists = db.Column(db.Float)
    expected_goal_involvements = db.Column(db.Float)
    expected_goals_conceded = db.Column(db.Float)
    influence_rank = db.Column(db.Integer)
    influence_rank_type = db.Column(db.Integer)
    creativity_rank = db.Column(db.Integer)
    creativity_rank_type = db.Column(db.Integer)
    threat_rank = db.Column(db.Integer)
    threat_rank_type = db.Column(db.Integer)
    ict_index_rank = db.Column(db.Integer)
    ict_index_rank_type = db.Column(db.Integer)
    corners_and_indirect_freekicks_order = db.Column(db.Float)
    corners_and_indirect_freekicks_text = db.Column(db.String(100))
    direct_freekicks_order = db.Column(db.Float)
    direct_freekicks_text = db.Column(db.String(100))
    penalties_order = db.Column(db.Float)
    penalties_text = db.Column(db.String(100))
    expected_goals_per_90 = db.Column(db.Float)
    saves_per_90 = db.Column(db.Float)
    expected_assists_per_90 = db.Column(db.Float)
    expected_goal_involvements_per_90 = db.Column(db.Float)
    expected_goals_conceded_per_90 = db.Column(db.Float)
    goals_conceded_per_90 = db.Column(db.Float)
    now_cost_rank = db.Column(db.Integer)
    now_cost_rank_type = db.Column(db.Integer)
    form_rank = db.Column(db.Integer)
    form_rank_type = db.Column(db.Integer)
    points_per_game_rank = db.Column(db.Integer)
    points_per_game_rank_type = db.Column(db.Integer)
    selected_rank = db.Column(db.Integer)
    selected_rank_type = db.Column(db.Integer)
    starts_per_90 = db.Column(db.Float)
    clean_sheets_per_90 = db.Column(db.Float)

    top_players = db.relationship("TopPlayers", backref="players", lazy="select")
    gameweeks_top_player = db.relationship(
        "Gameweeks",
        backref="players_top_player",
        lazy="select",
        foreign_keys="Gameweeks.top_player",
    )
    gameweeks_most_selected = db.relationship(
        "Gameweeks",
        backref="players_most_selected",
        lazy="select",
        foreign_keys="Gameweeks.most_selected",
    )
    gameweeks_most_transferred_in = db.relationship(
        "Gameweeks",
        backref="players_most_transferred_in",
        lazy="select",
        foreign_keys="Gameweeks.most_transferred_in",
    )
    gameweeks_most_captained = db.relationship(
        "Gameweeks",
        backref="players_most_captained",
        lazy="select",
        foreign_keys="Gameweeks.most_captained",
    )
    gameweeks_most_vice_captained = db.relationship(
        "Gameweeks",
        backref="players_most_vice_captained",
        lazy="select",
        foreign_keys="Gameweeks.most_vice_captained",
    )

    fixture_goals = db.relationship(
        "FixtureGoals",
        backref="players",
        lazy="select",
    )
    fixture_assists = db.relationship(
        "FixtureAssists",
        backref="players",
        lazy="select",
    )
    fixture_owngoals = db.relationship(
        "FixtureOwnGoals",
        backref="players",
        lazy="select",
    )
    fixture_pensaves = db.relationship(
        "FixturePenSaves",
        backref="players",
        lazy="select",
    )
    fixture_penmisses = db.relationship(
        "FixturePenMisses",
        backref="players",
        lazy="select",
    )
    fixture_yellows = db.relationship(
        "FixtureYellows",
        backref="players",
        lazy="select",
    )
    fixture_reds = db.relationship(
        "FixtureReds",
        backref="players",
        lazy="select",
    )
    fixture_saves = db.relationship(
        "FixtureSaves",
        backref="players",
        lazy="select",
    )
    fixture_bonuses = db.relationship(
        "FixtureBonuses",
        backref="players",
        lazy="select",
    )
    fixture_bps = db.relationship(
        "FixtureBPS",
        backref="players",
        lazy="select",
    )
    player_fixture_history = db.relationship(
        "PlayerFixtureHistory",
        backref="players",
        lazy="select",
    )
    player_previous_seasons = db.relationship(
        "PlayerPreviousSeasons",
        backref="players",
        lazy="select",
    )
    gameweek_picks = db.relationship(
        "GameweekPicks",
        backref="players",
        lazy="select",
    )


class Fixtures(db.Model):
    __tablename__ = "fixtures"

    fixture_id = db.Column(db.Integer, primary_key=True)

    gameweek_id = db.Column(db.Integer, db.ForeignKey("gameweeks.gameweek_id"))
    team_a = db.Column(db.Integer, db.ForeignKey("teams.team_id"))
    team_h = db.Column(db.Integer, db.ForeignKey("teams.team_id"))

    code = db.Column(db.Integer)
    finished = db.Column(db.Boolean)
    finished_provisional = db.Column(db.Boolean)
    kickoff_time = db.Column(db.DateTime)
    minutes = db.Column(db.Integer)
    provisional_start_time = db.Column(db.Boolean)
    started = db.Column(db.Boolean)
    team_a_score = db.Column(db.Integer)
    team_h_score = db.Column(db.Integer)
    team_h_difficulty = db.Column(db.Integer)
    team_a_difficulty = db.Column(db.Integer)
    pulse_id = db.Column(db.Integer)
    location = db.Column(db.String(10))

    fixture_goals = db.relationship(
        "FixtureGoals",
        backref="fixtures",
        lazy="select",
    )
    fixture_assists = db.relationship(
        "FixtureAssists",
        backref="fixtures",
        lazy="select",
    )
    fixture_owngoals = db.relationship(
        "FixtureOwnGoals",
        backref="fixtures",
        lazy="select",
    )
    fixture_pensaves = db.relationship(
        "FixturePenSaves",
        backref="fixtures",
        lazy="select",
    )
    fixture_penmisses = db.relationship(
        "FixturePenMisses",
        backref="fixtures",
        lazy="select",
    )
    fixture_yellows = db.relationship(
        "FixtureYellows",
        backref="fixtures",
        lazy="select",
    )
    fixture_reds = db.relationship(
        "FixtureReds",
        backref="fixtures",
        lazy="select",
    )
    fixture_saves = db.relationship(
        "FixtureSaves",
        backref="fixtures",
        lazy="select",
    )
    fixture_bonuses = db.relationship(
        "FixtureBonuses",
        backref="fixtures",
        lazy="select",
    )
    fixture_bps = db.relationship(
        "FixtureBPS",
        backref="fixtures",
        lazy="select",
    )
    team_fixture_results = db.relationship(
        "TeamFixtureResults",
        backref="fixtures",
        lazy="select",
    )
    team_fixture_results = db.relationship(
        "PlayerFixtureHistory",
        backref="fixtures",
        lazy="select",
    )


class FixtureGoals(db.Model):
    __tablename__ = "fixture_goals"

    player_id = db.Column(
        db.Integer, db.ForeignKey("players.player_id"), primary_key=True
    )
    fixture_id = db.Column(
        db.Integer, db.ForeignKey("fixtures.fixture_id"), primary_key=True
    )
    team_id = db.Column(db.Integer, db.ForeignKey("teams.team_id"))
    value = db.Column(db.Integer)
    location = db.Column(db.String(10))


class FixtureAssists(db.Model):
    __tablename__ = "fixture_assists"

    player_id = db.Column(
        db.Integer, db.ForeignKey("players.player_id"), primary_key=True
    )
    fixture_id = db.Column(
        db.Integer, db.ForeignKey("fixtures.fixture_id"), primary_key=True
    )
    team_id = db.Column(db.Integer, db.ForeignKey("teams.team_id"))
    value = db.Column(db.Integer)
    location = db.Column(db.String(10))


class FixtureOwnGoals(db.Model):
    __tablename__ = "fixture_owngoals"

    player_id = db.Column(
        db.Integer, db.ForeignKey("players.player_id"), primary_key=True
    )
    fixture_id = db.Column(
        db.Integer, db.ForeignKey("fixtures.fixture_id"), primary_key=True
    )
    team_id = db.Column(db.Integer, db.ForeignKey("teams.team_id"))
    value = db.Column(db.Integer)
    location = db.Column(db.String(10))


class FixturePenSaves(db.Model):
    __tablename__ = "fixture_pensaves"

    player_id = db.Column(
        db.Integer, db.ForeignKey("players.player_id"), primary_key=True
    )
    fixture_id = db.Column(
        db.Integer, db.ForeignKey("fixtures.fixture_id"), primary_key=True
    )
    team_id = db.Column(db.Integer, db.ForeignKey("teams.team_id"))
    value = db.Column(db.Integer)
    location = db.Column(db.String(10))


class FixturePenMisses(db.Model):
    __tablename__ = "fixture_penmisses"

    player_id = db.Column(
        db.Integer, db.ForeignKey("players.player_id"), primary_key=True
    )
    fixture_id = db.Column(
        db.Integer, db.ForeignKey("fixtures.fixture_id"), primary_key=True
    )
    team_id = db.Column(db.Integer, db.ForeignKey("teams.team_id"))
    value = db.Column(db.Integer)
    location = db.Column(db.String(10))


class FixtureYellows(db.Model):
    __tablename__ = "fixture_yellows"

    player_id = db.Column(
        db.Integer, db.ForeignKey("players.player_id"), primary_key=True
    )
    fixture_id = db.Column(
        db.Integer, db.ForeignKey("fixtures.fixture_id"), primary_key=True
    )
    team_id = db.Column(db.Integer, db.ForeignKey("teams.team_id"))
    value = db.Column(db.Integer)
    location = db.Column(db.String(10))


class FixtureReds(db.Model):
    __tablename__ = "fixture_reds"

    player_id = db.Column(
        db.Integer, db.ForeignKey("players.player_id"), primary_key=True
    )
    fixture_id = db.Column(
        db.Integer, db.ForeignKey("fixtures.fixture_id"), primary_key=True
    )
    team_id = db.Column(db.Integer, db.ForeignKey("teams.team_id"))
    value = db.Column(db.Integer)
    location = db.Column(db.String(10))


class FixtureSaves(db.Model):
    __tablename__ = "fixture_saves"

    player_id = db.Column(
        db.Integer, db.ForeignKey("players.player_id"), primary_key=True
    )
    fixture_id = db.Column(
        db.Integer, db.ForeignKey("fixtures.fixture_id"), primary_key=True
    )
    team_id = db.Column(db.Integer, db.ForeignKey("teams.team_id"))
    value = db.Column(db.Integer)
    location = db.Column(db.String(10))


class FixtureBonuses(db.Model):
    __tablename__ = "fixture_bonuses"

    player_id = db.Column(
        db.Integer, db.ForeignKey("players.player_id"), primary_key=True
    )
    fixture_id = db.Column(
        db.Integer, db.ForeignKey("fixtures.fixture_id"), primary_key=True
    )
    team_id = db.Column(db.Integer, db.ForeignKey("teams.team_id"))
    value = db.Column(db.Integer)
    location = db.Column(db.String(10))


class FixtureBPS(db.Model):
    __tablename__ = "fixture_bps"

    player_id = db.Column(
        db.Integer, db.ForeignKey("players.player_id"), primary_key=True
    )
    fixture_id = db.Column(
        db.Integer, db.ForeignKey("fixtures.fixture_id"), primary_key=True
    )
    team_id = db.Column(db.Integer, db.ForeignKey("teams.team_id"))
    value = db.Column(db.Integer)
    location = db.Column(db.String(10))


class TeamFixtureResults(db.Model):
    __tablename__ = "team_fixture_results"

    fixture_id = db.Column(
        db.Integer, db.ForeignKey("fixtures.fixture_id"), primary_key=True
    )
    team_id = db.Column(db.Integer, db.ForeignKey("teams.team_id"), primary_key=True)
    opponent_id = db.Column(db.Integer, db.ForeignKey("teams.team_id"))
    home = db.Column(db.Boolean)
    score = db.Column(db.Integer)
    opponent_score = db.Column(db.Integer)
    finished = db.Column(db.Boolean)
    gameweek_id = db.Column(db.Integer, db.ForeignKey("gameweeks.gameweek_id"))
    fixture_difficulty = db.Column(db.Integer)
    win = db.Column(db.String(4))


class PlayerFixtureHistory(db.Model):
    __tablename__ = "player_fixture_history"

    player_id = db.Column(
        db.Integer, db.ForeignKey("players.player_id"), primary_key=True
    )
    fixture_id = db.Column(
        db.Integer, db.ForeignKey("fixtures.fixture_id"), primary_key=True
    )

    gameweek_id = db.Column(db.Integer, db.ForeignKey("gameweeks.gameweek_id"))
    opponent_team = db.Column(db.Integer, db.ForeignKey("teams.team_id"))

    total_points = db.Column(db.Integer)
    was_home = db.Column(db.Boolean)
    kickoff_time = db.Column(db.DateTime)
    team_h_score = db.Column(db.Integer)
    team_a_score = db.Column(db.Integer)
    minutes = db.Column(db.Integer)
    goals_scored = db.Column(db.Integer)
    assists = db.Column(db.Integer)
    clean_sheets = db.Column(db.Integer)
    goals_conceded = db.Column(db.Integer)
    own_goals = db.Column(db.Integer)
    penalties_saved = db.Column(db.Integer)
    penalties_missed = db.Column(db.Integer)
    yellow_cards = db.Column(db.Integer)
    red_cards = db.Column(db.Integer)
    saves = db.Column(db.Integer)
    bonus = db.Column(db.Integer)
    bps = db.Column(db.Integer)
    influence = db.Column(db.Float)
    creativity = db.Column(db.Float)
    threat = db.Column(db.Float)
    ict_index = db.Column(db.Float)
    starts = db.Column(db.Integer)
    expected_goals = db.Column(db.Float)
    expected_assists = db.Column(db.Float)
    expected_goal_involvements = db.Column(db.Float)
    expected_goals_conceded = db.Column(db.Float)
    value = db.Column(db.Integer)
    transfers_balance = db.Column(db.Integer)
    selected = db.Column(db.Integer)
    transfers_in = db.Column(db.Integer)
    transfers_out = db.Column(db.Integer)


# class PlayerRemainingFixtures(db.Model):
#     # rework table
#     __tablename__ = "player_remaining_fixtures"

#     fixture_id = db.Column(db.Integer, primary_key=True)
#     player_id = db.Column(db.Integer, primary_key=True)
#     code = db.Column(db.Integer)
#     team_h = db.Column(db.Integer)
#     team_a = db.Column(db.Integer)
#     gameweek_id = db.Column(db.Integer)
#     finished = db.Column(db.Boolean)
#     provisional_start_time = db.Column(db.Boolean)
#     kickoff_time = db.Column(db.DateTime)
#     event_name = db.Column(db.String(50))
#     is_home = db.Column(db.Boolean)
#     difficulty = db.Column(db.Integer)


class PlayerPreviousSeasons(db.Model):
    __tablename__ = "player_previous_seasons"

    player_id = db.Column(
        db.Integer, db.ForeignKey("players.player_id"), primary_key=True
    )
    season_name = db.Column(db.String(50), primary_key=True)

    element_code = db.Column(db.Integer)
    start_cost = db.Column(db.Integer)
    end_cost = db.Column(db.Integer)
    total_points = db.Column(db.Integer)
    minutes = db.Column(db.Integer)
    goals_scored = db.Column(db.Integer)
    assists = db.Column(db.Integer)
    clean_sheets = db.Column(db.Integer)
    goals_conceded = db.Column(db.Integer)
    own_goals = db.Column(db.Integer)
    penalties_saved = db.Column(db.Integer)
    penalties_missed = db.Column(db.Integer)
    yellow_cards = db.Column(db.Integer)
    red_cards = db.Column(db.Integer)
    saves = db.Column(db.Integer)
    bonus = db.Column(db.Integer)
    bps = db.Column(db.Integer)
    influence = db.Column(db.Float)
    creativity = db.Column(db.Float)
    threat = db.Column(db.Float)
    ict_index = db.Column(db.Float)
    starts = db.Column(db.Integer)
    expected_goals = db.Column(db.Float)
    expected_assists = db.Column(db.Float)
    expected_goal_involvements = db.Column(db.Float)
    expected_goals_conceded = db.Column(db.Float)


class LeagueInfo(db.Model):
    __tablename__ = "league_info"

    league_id = db.Column(db.Integer, primary_key=True)

    name = db.Column(db.String(50))
    created = db.Column(db.DateTime)
    closed = db.Column(db.Boolean)
    league_type = db.Column(db.String(50))
    scoring = db.Column(db.String(50))
    admin_entry = db.Column(db.Integer)
    start_event = db.Column(db.Integer)
    code_privacy = db.Column(db.String(50))
    has_cup = db.Column(db.Boolean)
    short_name = db.Column(db.String(50))
    cup_league_id = db.Column(db.Integer)
    type = db.Column(db.String(50))

    manager_leagues = db.relationship(
        "ManagerLeagues",
        backref="leagues",
        lazy="select",
    )


class ManagerLeagues(db.Model):
    __tablename__ = "manager_leagues"

    manager_id = db.Column(
        db.Integer, db.ForeignKey("manager_info.manager_id"), primary_key=True
    )
    league_id = db.Column(
        db.Integer, db.ForeignKey("league_info.league_id"), primary_key=True
    )


class ManagerInfo(db.Model):
    __tablename__ = "manager_info"

    manager_id = db.Column(db.Integer, primary_key=True)
    favourite_team = db.Column(db.Integer, db.ForeignKey("teams.team_id"))
    started_gameweek = db.Column(db.Integer, db.ForeignKey("gameweeks.gameweek_id"))

    joined_time = db.Column(db.DateTime)
    manager_first_name = db.Column(db.String(50))
    manager_last_name = db.Column(db.String(50))
    manager_region_id = db.Column(db.Integer)
    manager_region_name = db.Column(db.String(50))
    manager_region_iso_code_short = db.Column(db.String(50))
    manager_region_iso_code_long = db.Column(db.String(50))
    squad_name = db.Column(db.String(50))

    manager_leagues = db.relationship(
        "ManagerLeagues",
        backref="managers",
        lazy="select",
    )
    manager_gameweeks = db.relationship(
        "ManagerGameweeks",
        backref="managers",
        lazy="select",
    )
    manager_seasons = db.relationship(
        "ManagerSeasons",
        backref="managers",
        lazy="select",
    )
    manager_chips = db.relationship(
        "ManagerChips",
        backref="managers",
        lazy="select",
    )
    gameweek_subs = db.relationship(
        "GameweekSubs",
        backref="managers",
        lazy="select",
    )
    gameweek_picks = db.relationship(
        "GameweekPicks",
        backref="managers",
        lazy="select",
    )


class ManagerGameweeks(db.Model):
    __tablename__ = "manager_gameweeks"

    gameweek_id = db.Column(
        db.Integer, db.ForeignKey("gameweeks.gameweek_id"), primary_key=True
    )
    manager_id = db.Column(
        db.Integer, db.ForeignKey("manager_info.manager_id"), primary_key=True
    )
    points = db.Column(db.Integer)
    total_points = db.Column(db.Integer)
    gameweek_rank = db.Column(db.Integer)
    rank_sort = db.Column(db.Integer)
    overall_rank = db.Column(db.Integer)
    bank = db.Column(db.Integer)
    value = db.Column(db.Integer)
    gameweek_transfers = db.Column(db.Integer)
    gameweek_transfers_cost = db.Column(db.Integer)
    points_on_bench = db.Column(db.Integer)


class ManagerSeasons(db.Model):
    __tablename__ = "manager_seasons"

    manager_id = db.Column(
        db.Integer, db.ForeignKey("manager_info.manager_id"), primary_key=True
    )
    season_name = db.Column(db.String(30), primary_key=True)
    total_points = db.Column(db.Integer)
    rank = db.Column(db.Integer)


class ManagerChips(db.Model):
    __tablename__ = "manager_chips"

    manager_id = db.Column(
        db.Integer, db.ForeignKey("manager_info.manager_id"), primary_key=True
    )
    gameweek_id = db.Column(db.Integer, db.ForeignKey("gameweeks.gameweek_id"))
    name = db.Column(db.String(30), primary_key=True)
    time = db.Column(db.DateTime)


class GameweekSubs(db.Model):
    __tablename__ = "gameweek_subs"

    manager_id = db.Column(
        db.Integer, db.ForeignKey("manager_info.manager_id"), primary_key=True
    )
    gameweek_id = db.Column(
        db.Integer, db.ForeignKey("gameweeks.gameweek_id"), primary_key=True
    )
    player_in = db.Column(db.Integer)
    player_out = db.Column(db.Integer)


class GameweekPicks(db.Model):
    __tablename__ = "gameweek_picks"

    manager_id = db.Column(
        db.Integer, db.ForeignKey("manager_info.manager_id"), primary_key=True
    )
    gameweek_id = db.Column(
        db.Integer, db.ForeignKey("gameweeks.gameweek_id"), primary_key=True
    )
    player_id = db.Column(
        db.Integer, db.ForeignKey("players.player_id"), primary_key=True
    )
    position = db.Column(db.Integer)
    multiplier = db.Column(db.Integer)
    is_captain = db.Column(db.Boolean)
    is_vice_captain = db.Column(db.Boolean)
    starting = db.Column(db.Boolean)


if __name__ == "__main__":
    app_context = app.app_context()
    app_context.push()
    db.create_all()
