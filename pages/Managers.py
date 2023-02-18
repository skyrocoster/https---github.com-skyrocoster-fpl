import streamlit as st
import plotly.express as px
import pyarrow.parquet as pq
import pandas as pd
import plotly.graph_objects as go

st.set_page_config(layout="wide")
sidebar = st.sidebar


@st.cache_data
def load_manager_list():
    df = pq.read_table(
        f"data/streamlit/manager_performance/manager_gameweeks.parquet"
    ).to_pandas()
    manager_list = df["full_name"].sort_values().unique()
    return manager_list


def load_manager_gws():
    df = pq.read_table(
        f"data/streamlit/manager_performance/manager_gameweeks.parquet"
    ).to_pandas()
    df = df.loc[(df["gameweek_id"] >= sel_gw_start) & (df["gameweek_id"] <= sel_gw_end)]
    if len(sel_manager) > 0:
        df = df.loc[df["full_name"].isin(sel_manager)]
    return df


def load_manager_avgs(df):
    sum_cols = {
        "full_name": "full_name",
        "manager_id": "manager_id",
        "points": "sum_points",
        "gameweek_transfers": "sum_gameweek_transfers",
        "gameweek_transfers_cost": "sum_gameweek_transfers_cost",
        "points_on_bench": "sum_points_on_bench",
        "bank": "sum_bank",
    }
    avg_cols = {
        "full_name": "full_name",
        "manager_id": "manager_id",
        "points": "avg_points",
        "gameweek_rank": "avg_gameweek_rank",
        "overall_rank": "avg_overall_rank",
        "points_on_bench": "avg_points_on_bench",
        "bank": "avg_bank",
        "value": "avg_value",
        "3xc": "3xc",
        "bboost": "bboost",
        "freehit": "freehit",
    }
    df_sum = (
        df[sum_cols.keys()]
        .groupby(["manager_id", "full_name"])
        .sum(numeric_only=True)
        .rename(columns=sum_cols)
    )
    df_avg = (
        df[avg_cols.keys()]
        .groupby(["manager_id", "full_name"])
        .mean(numeric_only=True)
        .rename(columns=avg_cols)
    )
    df = df_sum.merge(df_avg, on=["manager_id", "full_name"])
    df["3xc"] = df["3xc"].apply(lambda x: True if x > 0 else False)
    df["bboost"] = df["bboost"].apply(lambda x: True if x > 0 else False)
    df["freehit"] = df["freehit"].apply(lambda x: True if x > 0 else False)
    df = df.reset_index()
    return df


def load_player_performances():
    df = pq.read_table(
        f"data/streamlit/manager_performance/manager_players.parquet"
    ).to_pandas()
    df = df.loc[(df["gameweek_id"] >= sel_gw_start) & (df["gameweek_id"] <= sel_gw_end)]
    if active_players == True:
        df = df.loc[df["active"] == True]
    if len(sel_manager) > 0:
        df = df.loc[df["full_name"].isin(sel_manager)]
    if len(sel_teams) > 0:
        df = df.loc[df["team_name"].isin(sel_teams)]
    if len(sel_positions) > 0:
        df = df.loc[df["position"].isin(sel_positions)]
    if len(sel_player) > 0:
        df = df.loc[df["player_name"].isin(sel_player)]
    return df


def load_player_avg(df):
    # fixture_fields = []
    # df_fixtures = 

    # df_gws = 

    # possible thrown out by dgws
    sum_cols = {
        "manager_id": "manager_id",
        "player_id": "player_id",
        "player_name": "player_name",
        "goals_conceded": "sum_goals_conceded",
        "active": "count_active",
        "subbed_in": "count_subbed_in",
        "subbed_out": "count_subbed_out",
        "fixture_count": "count_fixtures",
    }
    avg_cols = {
        "manager_id": "manager_id",
        "player_id": "player_id",
        "player_name": "player_name",
        "goals_conceded": "avg_goals_conceded",
    }
    count_cols = {"gameweek_id": "count_gw"}
    df_sum = (
        df[sum_cols.keys()]
        .groupby(["manager_id", "player_id", "player_name"])
        .sum(numeric_only=True)
        .rename(columns=sum_cols)
    )
    df_avg = (
        df[avg_cols.keys()]
        .groupby(["manager_id", "player_id", "player_name"])
        .mean(numeric_only=True)
        .rename(columns=avg_cols)
    )
    # Gameweek count
    # Active count
    # Bench count
    df = df_sum.merge(df_avg, on=["manager_id", "player_id", "player_name"])
    return df


@st.cache_data
def load_team_list():
    df = pq.read_table(f"data/streamlit/all/teams.parquet").to_pandas()
    team_list = df["team_name"].values.tolist()
    return team_list


def load_player_list():
    df = pq.read_table(f"data/streamlit/player_performance/players.parquet").to_pandas()
    if len(sel_teams) > 0:
        df = df.loc[df["team_name"].isin(sel_teams)]
    if len(sel_positions) > 0:
        df = df.loc[df["position"].isin(sel_positions)]
    player_list = sorted(df[["web_name"]].squeeze().tolist())
    return player_list


class SingleMangerStats:
    def __init__(self, df_gws, df_avg, df_pp):
        for k, v in df_avg.to_dict(orient="records")[0].items():
            setattr(self, k, v)

        self.points_kpi = go.Figure(
            go.Indicator(
                mode="number",
                value=self.sum_points,
                title={"text": "Points to Gameweek"},
            )
        )

        self.chips_used = df_avg[["3xc", "freehit", "bboost"]]

        self.min_gw = df_gws["gameweek_id"].min()
        self.max_gw = df_gws["gameweek_id"].max()


with sidebar:
    sel_gw_start, sel_gw_end = st.select_slider(
        "Select a Gameweek Range:",
        options=[item for item in range(1, 38 + 1)],
        value=(1, 38),
    )

    manager_list = load_manager_list()
    sel_manager = st.multiselect("Select Managers", manager_list)

    positions = ["GKP", "FWD", "MID", "DEF"]
    sel_positions = st.multiselect("Select Positions to Include", positions, positions)

    team_names = load_team_list()
    sel_teams = st.multiselect("Select Teams to Include", team_names, [])

    player_names = load_player_list()
    sel_player = st.multiselect("Select a Player", player_names, [])

# Load Dataframe
manager_gws = load_manager_gws()
manager_avgs = load_manager_avgs(manager_gws)


# Load Fieldlists
manager_gws_fields = list(manager_gws.columns)
manager_avgs_fields = list(manager_avgs.columns)

gw_tab, overall_tab, players_tab, one_manager = st.tabs(
    ["By Gameweek", "Overall", "Player Performances", "Single Manager Performance"]
)


with gw_tab:
    st.dataframe(manager_gws)

    fig_gw_line = px.line(
        manager_gws,
        x="gameweek_id",
        y="points_to_gameweek",
        color="manager_id",
        width=1600,
    )

    fig_gw_line.update_layout(
        xaxis=dict(tickmode="linear", tick0=1, dtick=1),
        yaxis=dict(tickmode="linear", tick0=0, dtick=100),
    )

    st.plotly_chart(fig_gw_line)

with overall_tab:
    st.dataframe(manager_avgs)

    col1, col2 = st.columns(2)

    with col1:
        sel_mngr_avgs_scatter_x = st.selectbox(
            "Select X Axis", manager_avgs_fields, key="sel_mngr_avgs_scatter_x"
        )
    with col2:
        sel_mngr_avgs_scatter_y = st.selectbox(
            "Select Y Axis", manager_avgs_fields, key="sel_mngr_avgs_scatter_y"
        )
        sel_mngr_avgs_scatter_size = st.selectbox(
            "Select Trace Size", manager_avgs_fields, key="sel_mngr_avgs_scatter_size"
        )

    manager_avgs_scatter = px.scatter(
        manager_avgs,
        x=sel_mngr_avgs_scatter_x,
        y=sel_mngr_avgs_scatter_y,
        size=None,
        color=None,
        width=1600,
        trendline="ols",
    )

    st.plotly_chart(manager_avgs_scatter)

    manager_avgs_bar = px.bar(
        manager_avgs.sort_values("sum_points"),
        x="sum_points",
        y="manager_id",
        color="sum_gameweek_transfers_cost",
        width=1600,
    )

    st.plotly_chart(manager_avgs_bar)


with players_tab:
    active_players = st.checkbox("Active Players Only")  # This will affect other tabs
    player_performances = load_player_performances()
    player_avgs = load_player_avg(player_performances)
    st.title("Gameweek Performances")
    st.dataframe(player_performances)

    st.markdown("---")

    st.title("Averages")
    st.dataframe(player_avgs)

with one_manager:
    if len(sel_manager) != 1:
        st.write("Please select one manager")
    else:
        manager_stats = SingleMangerStats(
            manager_gws, manager_avgs, player_performances
        )

        st.title("Gameweeks")
        st.dataframe(manager_gws)

        st.markdown("---")

        st.title("Manager Overall")
        st.write(f"Points to Gameweek: {manager_stats.manager_id}")
        st.plotly_chart(manager_stats.points_kpi)
        st.dataframe(manager_stats.chips_used)
        st.dataframe(manager_avgs)

        st.markdown("---")

        st.title("Player Performances")


# fig.show()
