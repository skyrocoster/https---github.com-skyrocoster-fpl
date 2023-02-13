import streamlit as st
import plotly.express as px
import pyarrow.parquet as pq
import pandas as pd

st.set_page_config(layout="wide")
sidebar = st.sidebar


@st.cache_data
def load_manager_list():
    df = pq.read_table(
        f"data/streamlit/manager_performance/manager_gameweeks.parquet"
    ).to_pandas()
    manager_list = df["full_name"].sort_values().unique()
    return manager_list


@st.cache_data
def load_manager_gws():
    df = pq.read_table(
        f"data/streamlit/manager_performance/manager_gameweeks.parquet"
    ).to_pandas()
    df = df.loc[(df["gameweek_id"] >= sel_gw_start) & (df["gameweek_id"] <= sel_gw_end)]
    if len(sel_manager) > 0:
        df = df.loc[df["full_name"].isin(sel_manager)]
    return df


@st.cache_data
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
    df = df.reset_index()
    return df


@st.cache_data
def load_h2h(df):
    return df


class MinsMaxes:
    # @st.cache_data
    def __init__(self):
        df = pq.read_table(
            f"data/streamlit/manager_performance/manager_gameweeks.parquet"
        ).to_pandas()

        self.last_gw = sel_gw_end

        df_gameweek = df.loc[df["gameweek_id"] == self.last_gw]
        self.max_total_points = df_gameweek["points_to_gameweek"].max()
        self.min_total_points = df_gameweek["points_to_gameweek"].min()

        df_to_gw = df.loc[df["gameweek_id"] <= self.last_gw]
        df_sums = (
            df_to_gw[["manager_id", "gameweek_transfers"]].groupby("manager_id").sum()
        )
        self.max_gw_transfers = df_sums["gameweek_transfers"].max()
        self.min_gw_transfers = df_sums["gameweek_transfers"].min()


manager_list = load_manager_list()
with sidebar:
    sel_gw_start, sel_gw_end = st.select_slider(
        "Select a Gameweek Range:",
        options=[item for item in range(1, 38 + 1)],
        value=(1, 38),
    )
    sel_manager = st.multiselect("Select Managers", manager_list)

# Load Dataframe
manager_gws = load_manager_gws()
manager_avgs = load_manager_avgs(manager_gws)
minsmaxes = MinsMaxes()

# Load Fieldlists
manager_gws_fields = list(manager_gws.columns)
manager_avgs_fields = list(manager_avgs)

gw_tab, overall_tab, h2h_tab = st.tabs(["By Gameweek", "Overall", "Head to Head"])


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

with h2h_tab:
    if len(sel_manager) != 2:
        st.write("Please Select Two Managers")
        h2h = pd.DataFrame()
        h2h_bar = px.bar()
        h2h_bar2 = px.bar()
    else:
        h2h = load_h2h(manager_avgs)
        h2h_bar = px.bar(
            h2h,
            x="full_name",
            y="sum_points",
            color="sum_points",
            range_color=(minsmaxes.min_total_points, minsmaxes.max_total_points),
            title="Total Points",
        )
        h2h_bar2 = px.bar(
            h2h,
            x="full_name",
            y="sum_gameweek_transfers",
            color="sum_gameweek_transfers",
            range_color=(minsmaxes.min_gw_transfers, minsmaxes.max_gw_transfers),
            title="Total Transfers",
        )
    st.dataframe(h2h)

    col1, col2 = st.columns(2)
    with col1:
        st.plotly_chart(h2h_bar)
    with col2:
        st.plotly_chart(h2h_bar2)
