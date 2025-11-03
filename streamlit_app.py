import streamlit as st
import pandas as pd
from decimal import Decimal
from datetime import datetime

# importe suas funções do módulo data (mesmo nomes usados no Flask)
from data import (
    dbInsertTrade, dbInsertPnl, dbInsertPos,
    dbFetchMtM, dbFetchTrade,
    dbLoadPnl, dbLoadPos, dbLoadMtm, dbLoadTrade,
    dbLoadGraphPnl
)

# optional: if you have a pxLoadGraph in graphs.py
try:
    from graphs import pxLoadGraph
    HAS_PX = True
except Exception:
    HAS_PX = False

st.set_page_config(page_title="PNL Dashboard", layout="wide")

# --- helpers ---
PRODUCTS = ["SoyBean", "SoyMeal", "YelCorn"]
CATEGORIES = ["FOB Vessel", "FOB Paper", "C&F Vessel"]
# ship codes are 3 chars in your DB; choose defaults (you can adjust)
SHIPMENTS = ["VSL", "PPR", "CNF"]

def get_conversion_value(prod: str) -> Decimal:
    if prod == "SoyBean":
        return Decimal("36.7454")
    elif prod == "SoyMeal":
        return Decimal("1.1023")
    elif prod == "YelCorn":
        return Decimal("39.3678")
    else:
        return Decimal("1")

# --- UI ---
st.title("PNL System — Streamlit")

with st.sidebar:
    st.header("Controls")
    current_year = st.number_input("Year", min_value=2000, max_value=2100, value=datetime.now().year)
    prod_sidebar = st.selectbox("Product (quick select)", PRODUCTS)

    # garante que a chave exista
    if "refresh_count" not in st.session_state:
        st.session_state["refresh_count"] = 0

    if st.button("Refresh / Rerun"):
        st.session_state["refresh_count"] += 1
        # tenta rerun — captura erro para evitar crash no Cloud
        try:
            st.experimental_rerun()
        except Exception as e:
            # não deixa o app quebrar — mostra aviso amigável
            st.warning("Não foi possível forçar um rerun automaticamente. Atualize a página manualmente.")
            # para debug local você pode descomentar a linha abaixo (não deixe em produção)
            # st.write(f"Detalhe técnico: {e}")


tabs = st.tabs(["Overview", "Insert Trade", "Insert MTM", "Trade Log", "Graphs"])

# --- Overview tab: show tables for each product ---
with tabs[0]:
    st.header("Overview")
    cols = st.columns(3)
    for i, prod in enumerate(PRODUCTS):
        with cols[i]:
            st.subheader(prod)
            with st.spinner("Loading MTM / PNL / POS..."):
                try:
                    mtm_df = dbLoadMtm(prod, int(current_year))
                    pnl_df = dbLoadPnl(prod, int(current_year))
                    pos_df = dbLoadPos(prod, int(current_year))
                except Exception as e:
                    st.error(f"Erro ao carregar dados: {e}")
                    mtm_df = pd.DataFrame()
                    pnl_df = pd.DataFrame()
                    pos_df = pd.DataFrame()

            st.markdown("**MTM (latest per ship)**")
            st.dataframe(mtm_df)

            st.markdown("**PNL (monthly)**")
            st.dataframe(pnl_df)

            st.markdown("**POS (latest per ship)**")
            st.dataframe(pos_df)

# --- Insert Trade tab ---
with tabs[1]:
    st.header("Insert Trade")
    with st.form("trade_form"):
        prod = st.selectbox("Product", PRODUCTS, index=PRODUCTS.index(prod_sidebar) if prod_sidebar in PRODUCTS else 0)
        op = st.selectbox("Operation", ["Purchase", "Sale"])
        year = st.number_input("Year", min_value=2000, max_value=2100, value=current_year)
        ton = st.number_input("Tons", min_value=0.0, step=1.0, value=1.0)
        lvl_pct = st.number_input("Level (%)", min_value=0.0, max_value=100.0, value=100.0)
        categories = st.multiselect("Categories", CATEGORIES, default=CATEGORIES)
        shipments = st.multiselect("Shipments (3-char codes)", SHIPMENTS, default=SHIPMENTS)
        submit_trade = st.form_submit_button("Insert Trade")

    if submit_trade:
        try:
            lvl = Decimal(str(lvl_pct)) / Decimal("100")
            ton_dec = Decimal(str(ton))
            conV = get_conversion_value(prod)
            inserted = 0
            for cat in categories:
                for ship in shipments:
                    notion = conV * lvl * ton_dec
                    oldPos = dbFetchMtM  # noop to avoid lint; actual pos fetched below using dbFetchTrade/dbFetchMtM/dbFetchPos
                    # get current pos (dbFetchPos exists in your data.py)
                    try:
                        from data import dbFetchPos
                        old_pos_val = dbFetchPos(prod, cat, ship, int(year))
                    except Exception:
                        old_pos_val = 0
                    if op == "Purchase":
                        curPos = old_pos_val + int(ton_dec)
                    else:
                        curPos = old_pos_val - int(ton_dec)
                    dbInsertTrade(prod, cat, ship, int(year), op, int(ton_dec), lvl, notion)
                    dbInsertPos(prod, cat, ship, int(year), curPos)
                    inserted += 1
            st.success(f"Inserted {inserted} trade(s) and updated positions.")
        except Exception as e:
            st.error(f"Erro insertTrade: {e}")

# --- Insert MTM tab ---
with tabs[2]:
    st.header("Insert MTM (mark-to-market)")
    with st.form("mtm_form"):
        prod_mtm = st.selectbox("Product", PRODUCTS, index=PRODUCTS.index(prod_sidebar) if prod_sidebar in PRODUCTS else 0)
        year_mtm = st.number_input("Year", min_value=2000, max_value=2100, value=current_year, key="year_mtm")
        mtm_pct = st.number_input("MTM Level (%)", min_value=-100.0, max_value=1000.0, value=0.0)
        categories_mtm = st.multiselect("Categories", CATEGORIES, default=CATEGORIES, key="cat_mtm")
        shipments_mtm = st.multiselect("Shipments (3-char codes)", SHIPMENTS, default=SHIPMENTS, key="ship_mtm")
        submit_mtm = st.form_submit_button("Insert MTM")

    if submit_mtm:
        try:
            mtm = Decimal(str(mtm_pct)) / Decimal("100")
            updated = 0
            for cat in categories_mtm:
                for ship in shipments_mtm:
                    trading = dbFetchTrade(prod_mtm, cat, ship, int(year_mtm))
                    # trading rows are (id, op, ton, lvl)
                    for trade in trading:
                        try:
                            id_trade, op_trade, ton_trade, lvl_trade = trade
                        except Exception:
                            # If your dbFetchTrade returns different ordering, try to adapt
                            st.warning("Unexpected trade tuple shape; skipping one trade row.")
                            continue
                        mtmOld = dbFetchMtM(id_trade)
                        if mtmOld is None:
                            if op_trade == "Sale":
                                diff = lvl_trade - mtm
                            else:  # Purchase
                                diff = mtm - lvl_trade
                        else:
                            if op_trade == "Sale":
                                diff = mtmOld - mtm
                            else:
                                diff = mtm - mtmOld

                        conV = get_conversion_value(prod_mtm)
                        # ensure numeric types: ton_trade may be int or Decimal
                        ton_dec = Decimal(str(ton_trade))
                        pnl = diff * conV * ton_dec

                        dbInsertPnl(id_trade, prod_mtm, cat, ship, int(year_mtm), mtm, pnl)
                        updated += 1
            st.success(f"Inserted/updated MTM for {updated} trade-entries.")
        except Exception as e:
            st.error(f"Erro insertMTM: {e}")

# --- Trade Log tab ---
with tabs[3]:
    st.header("Trade Log")
    with st.spinner("Loading trade log..."):
        try:
            df_trades = dbLoadTrade()
            st.dataframe(df_trades)
        except Exception as e:
            st.error(f"Erro ao carregar trade log: {e}")

# --- Graphs tab ---
with tabs[4]:
    st.header("PNL Graphs")
    prod_for_graph = st.selectbox("Product for graph", PRODUCTS, index=0)
    with st.spinner("Loading PNL timeseries..."):
        try:
            # Prefer pxLoadGraph if available (your existing function)
            if HAS_PX:
                fig = pxLoadGraph(prod_for_graph)
                st.plotly_chart(fig, use_container_width=True)
            else:
                df_graph = dbLoadGraphPnl(prod_for_graph)
                if df_graph is None or df_graph.empty:
                    st.info("No PNL timeseries available.")
                else:
                    # ensure date is datetime
                    if 'date' in df_graph.columns:
                        df_graph['date'] = pd.to_datetime(df_graph['date'])
                        df_graph = df_graph.sort_values('date')
                        import plotly.express as px
                        fig = px.line(df_graph, x='date', y='pnl', color='cat', markers=True,
                                      title=f"PNL timeseries — {prod_for_graph}")
                        st.plotly_chart(fig, use_container_width=True)
                    else:
                        st.write(df_graph)
        except Exception as e:
            st.error(f"Erro ao gerar gráfico: {e}")

# small footer
st.markdown("---")
st.caption("App convertido from Flask → Streamlit. Use the sidebar to change year and refresh.")
