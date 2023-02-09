from db_tables import *
import streamlit as st
import plotly.express as px
import pyarrow.parquet as pq
import pyarrow as pa
import plotly.graph_objs as go
from io import BytesIO
from pyxlsb import open_workbook as open_xlsb

st.set_page_config(layout="wide")
sidebar = st.sidebar

app_context = app.app_context()
app_context.push()


@st.cache
def load_manager_list():
    df = pq.read_table(
        f"data/streamlit/manager_performance/manager_gameweeks.parquet"
    ).to_pandas()
    manager_list = df["full_name"].sort_values().unique()
    return manager_list


@st.cache
def load_manager_gws():
    df = pq.read_table(
        f"data/streamlit/manager_performance/manager_gameweeks.parquet"
    ).to_pandas()
    df = df.loc[(df["gameweek_id"] >= sel_gw_start) & (df["gameweek_id"] <= sel_gw_end)]
    if len(sel_manager) > 0:
        df = df.loc[df["full_name"].isin(sel_manager)]
    return df


@st.cache
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


# @st.cache
# def load_h2h(df):
#     return df
class HeadToHead:
    def __init__(self, df):
        df = df.to_dict(orient="records")
        p1 = df[0]
        p2 = df[1]

        if p1["sum_points"] > p2["sum_points"]:
            self.sum_points = p1["full_name"]


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
    # manager_avgs_scatter.update_layout(
    #     xaxis=dict(tickmode="linear", tick0=1, dtick=1),
    # )
    # y_mean = manager_avgs[sel_mngr_avgs_scatter_y].mean()
    # x_mean = manager_avgs[sel_mngr_avgs_scatter_x].mean()
    # y_max = manager_avgs[sel_mngr_avgs_scatter_y].max()
    # y_min = manager_avgs[sel_mngr_avgs_scatter_y].min()
    # x_min = manager_avgs[sel_mngr_avgs_scatter_x].min()
    # x_max = manager_avgs[sel_mngr_avgs_scatter_x].max()
    # manager_avgs_scatter.add_shape(
    #     type="line", y0=y_mean, y1=y_mean, x0=x_min, x1=x_max
    # )
    # manager_avgs_scatter.add_shape(
    #     type="line", x0=x_mean, x1=x_mean, y0=y_min, y1=y_max
    # )

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
        h2h = None
    else:
        h2h = HeadToHead(manager_avgs)
    col1, col2 = st.columns(2)
    # st.write(h2h)
    with col1:
        st.write(h2h.sum_points)
        st.write("🟢")
    with col2:
        st.write(h2h)
