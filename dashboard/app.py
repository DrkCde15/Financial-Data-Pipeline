"""Dashboard Streamlit — serving visual da Gold (BQ ou parquet local).

Run:
    DATA_SOURCE=bq  streamlit run dashboard/app.py   # default, via ADC
    DATA_SOURCE=local streamlit run dashboard/app.py # offline (data/gold/)

Sem cliques manuais: filtros e gráficos são código, versionados no repo.
"""

from __future__ import annotations

import os
import sys
from pathlib import Path

import streamlit as st

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "src"))
sys.path.insert(0, str(ROOT))

from dashboard.data import filter_period, kpis, load_bq, load_local  # noqa: E402

from financial_pipeline.config import get_project_root  # noqa: E402

st.set_page_config(page_title="Data Fin Pipeline", layout="wide")
st.title("Cooperativa — volume e ticket")

SOURCE = os.getenv("DATA_SOURCE", "bq")
PROJECT = os.getenv("GCP_PROJECT", "engdta")


@st.cache_data(ttl=300)
def _load(source: str, project: str, use_secrets: bool):
    root = get_project_root()
    if source == "local":
        return load_local(root / "data" / "gold")
    creds = None
    if use_secrets:  # Streamlit Cloud: [gcp_service_account] nos Secrets
        from google.oauth2 import service_account  # noqa: E402

        try:
            info = dict(st.secrets["gcp_service_account"])
        except (KeyError, FileNotFoundError):
            info = None
        if info is not None:
            creds = service_account.Credentials.from_service_account_info(info)
    return load_bq(project, creds)


try:
    use_secrets = "gcp_service_account" in st.secrets
except Exception:
    use_secrets = False
try:
    frames = _load(SOURCE, PROJECT, use_secrets)
except Exception as exc:
    st.error(f"Falha ao carregar fonte `{SOURCE}`: {exc}")
    st.stop()

st.caption(f"fonte: {SOURCE} · projeto: {PROJECT if SOURCE == 'bq' else '—'} · refresh 5min")
fact = frames["fact_daily_volume"]

dmin, dmax = fact["transaction_date"].min(), fact["transaction_date"].max()
start, end = st.slider(
    "Período",
    min_value=dmin.date(), max_value=dmax.date(), value=(dmin.date(), dmax.date()),
)
fact_f = filter_period(fact, start, end)

k = kpis(fact_f)
c1, c2, c3, c4 = st.columns(4)
c1.metric("Dias", k["n_days"])
c2.metric("Transações (completed)", k["total_transactions"])
c3.metric("Volume total", f"R$ {k['total_volume']:,.2f}")
c4.metric("Outliers", k["total_outliers"])

st.subheader("Volume por dia (completed, sem outliers)")
st.line_chart(fact_f.set_index("transaction_date")["total_completed"])

col_a, col_b = st.columns(2)
with col_a:
    st.subheader("Ranking por agência")
    by_branch = frames["agg_branch"].sort_values("total_volume", ascending=False)
    by_branch["agencia"] = by_branch["city"] + " · " + by_branch["branch_id"]
    st.bar_chart(by_branch.set_index("agencia")["total_volume"])
with col_b:
    st.subheader("Ticket por tipo")
    by_type = frames["agg_transaction_type"].sort_values("total", ascending=False)
    st.bar_chart(by_type.set_index("transaction_type")["avg_ticket"])

st.subheader("Outliers p/ investigação")
st.dataframe(frames["outliers"], use_container_width=True)
