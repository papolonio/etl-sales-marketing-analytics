"""
Dashboard Executivo Lead-to-Cash
=================================
Consome a DaaS API (FastAPI) rodando em http://api:8001/api/v1/dashboard.
"""

import os

import pandas as pd
import plotly.graph_objects as go
import requests
import streamlit as st
from datetime import date, timedelta

# ── Configuracao ───────────────────────────────────────────────────────────────

st.set_page_config(
    page_title="Lead-to-Cash",
    layout="wide",
    initial_sidebar_state="collapsed",
)

API_BASE = os.environ.get("API_BASE", "http://localhost:8001/api/v1/dashboard")


# ── Helpers ────────────────────────────────────────────────────────────────────

def _get(path: str, params: dict | None = None) -> list | dict | None:
    try:
        r = requests.get(f"{API_BASE}{path}", params=params, timeout=10)
        r.raise_for_status()
        return r.json()
    except requests.exceptions.ConnectionError:
        st.error("**API indisponivel.** Certifique-se de que o container `api` esta rodando.")
        return None
    except requests.exceptions.HTTPError as exc:
        st.error(f"Erro da API ({exc.response.status_code}): {exc.response.text}")
        return None


def _brl(v: float | None) -> str:
    """Formata numero como moeda brasileira: R$ 1.500,00"""
    if v is None:
        return "—"
    return f"R$ {v:,.2f}".replace(",", "X").replace(".", ",").replace("X", ".")


# ── Cabecalho com filtros de data integrados ───────────────────────────────────

hoje = date.today()

col_titulo, col_dt1, col_dt2, col_vazia = st.columns([3, 1, 1, 1])

with col_titulo:
    st.title("Lead-to-Cash · Visao Executiva")

with col_dt1:
    data_inicio = st.date_input(
        "Data inicial",
        value=hoje - timedelta(days=89),
        max_value=hoje,
    )

with col_dt2:
    data_fim = st.date_input(
        "Data final",
        value=hoje,
        min_value=data_inicio,
        max_value=hoje,
    )

filtro = {"data_inicio": data_inicio.isoformat(), "data_fim": data_fim.isoformat()}

# ── Tabs ───────────────────────────────────────────────────────────────────────

tab1, tab2, tab3 = st.tabs([
    "📈 Visao Executiva",
    "📣 Marketing & Funil",
    "🎯 Metas & Run Rate",
])


# =============================================================================
# TAB 1 — Visao Executiva
# =============================================================================

with tab1:

    kpis = _get("/kpis", params=filtro)

    # ── Grid de KPIs 4 x 2 ────────────────────────────────────────────────────

    st.subheader("Indicadores do Periodo")

    # Calcula Taxa de Conversao a partir dos KPIs
    taxa_conv = None
    if kpis:
        vendas = kpis.get("total_vendas") or 0
        leads  = kpis.get("total_leads")  or 0
        if leads > 0:
            taxa_conv = round(vendas / leads * 100, 1)

    col1, col2, col3, col4 = st.columns(4)

    # Linha 1 — Receita
    if kpis:
        col1.metric(
            label="Faturamento Total",
            value=_brl(kpis.get("faturamento_total")),
            help="Soma da receita de todas as vendas ganhas no periodo",
        )
        col2.metric(
            label="Vendas Fechadas",
            value=int(kpis.get("total_vendas") or 0),
            help="Numero de negociacoes marcadas como ganhas",
        )
        col3.metric(
            label="Ticket Medio",
            value=_brl(kpis.get("ticket_medio")),
            help="Faturamento Total / Vendas Fechadas",
        )
        col4.metric(
            label="Taxa de Conversao",
            value=f"{taxa_conv:.1f}%" if taxa_conv is not None else "—",
            help="Vendas Fechadas / Total de Leads x 100",
        )

    # Linha 2 — Custos (reutiliza as mesmas 4 colunas)
    if kpis:
        col1.metric(
            label="Total Investido",
            value=_brl(kpis.get("total_spend")),
            help="Total gasto em Meta Ads no periodo",
        )
        col2.metric(
            label="Leads Gerados",
            value=int(kpis.get("total_leads") or 0),
            help="Total de leads criados no CRM no periodo",
        )
        col3.metric(
            label="CAC",
            value=_brl(kpis.get("cac")),
            help="Custo de Aquisicao de Cliente = Investimento / Vendas",
        )
        col4.metric(
            label="CPL",
            value=_brl(kpis.get("cpl")),
            help="Custo por Lead = Investimento / Leads",
        )

    # ── Combo Chart: Receita (linha) + Investimento (barras) — ponta a ponta ───

    st.divider()
    st.subheader("Evolucao Diaria: Receita vs. Investimento")

    evo = _get("/evolucao-marketing", params=filtro)

    if evo:
        df_evo = pd.DataFrame(evo)
        df_evo["data"] = pd.to_datetime(df_evo["data"])

        fig_evo = go.Figure()

        # Barras de investimento — cor suave ao fundo
        fig_evo.add_trace(go.Bar(
            x=df_evo["data"],
            y=df_evo["total_spend"],
            name="Investimento (R$)",
            marker_color="rgba(239,68,68,0.30)",
            hovertemplate="<b>%{x|%d/%m/%Y}</b><br>Investimento: R$ %{y:,.2f}<extra></extra>",
        ))

        # Linha de receita — destaque em verde com marcadores
        fig_evo.add_trace(go.Scatter(
            x=df_evo["data"],
            y=df_evo["receita"],
            name="Receita (R$)",
            mode="lines+markers",
            line=dict(color="#10B981", width=2.5),
            marker=dict(size=5, color="#10B981"),
            hovertemplate="<b>%{x|%d/%m/%Y}</b><br>Receita: R$ %{y:,.2f}<extra></extra>",
        ))

        fig_evo.update_layout(
            height=300,
            barmode="overlay",
            margin=dict(l=0, r=0, t=10, b=0),
            legend=dict(orientation="h", yanchor="bottom", y=1.02, x=0),
            xaxis=dict(showgrid=False, showline=False, zeroline=False, title=None),
            yaxis=dict(showgrid=False, showline=False, zeroline=False,
                       tickprefix="R$ ", tickformat=",.0f", title=None),
        )

        st.plotly_chart(fig_evo, use_container_width=True, theme="streamlit")
    else:
        st.info("Sem dados de evolucao para o periodo selecionado.")

    # ── Rankings lado a lado (50% / 50%) ──────────────────────────────────────

    st.divider()

    col_esq, col_dir = st.columns(2)

    # ── Esquerda: Ranking de Vendedores (tabela com data bars) ───────────────

    with col_esq:
        st.subheader("Ranking de Vendedores")
        st.caption("Por receita total · periodo selecionado")

        rank = _get("/ranking-vendedores", params={"limit": 10, **filtro})

        if rank:
            df_rank = pd.DataFrame(rank)
            df_rank["salesperson_name"] = df_rank["salesperson_name"].fillna("(sem nome)")
            df_rank = df_rank.sort_values("receita_total", ascending=False).reset_index(drop=True)

            total_receita = df_rank["receita_total"].sum()
            df_rank["share_pct"] = (
                df_rank["receita_total"] / total_receita * 100
                if total_receita > 0 else 0
            ).round(1)

            df_tbl = df_rank[["salesperson_name", "receita_total",
                               "total_vendas", "share_pct"]].rename(columns={
                "salesperson_name": "Vendedor",
                "receita_total":    "Receita (R$)",
                "total_vendas":     "Vendas",
                "share_pct":        "Share Receita (%)",
            })

            styled = (
                df_tbl.style
                .format({
                    "Receita (R$)":     "R$ {:,.2f}",
                    "Share Receita (%)": "{:.1f}%",
                })
                .bar(subset=["Receita (R$)"],     color="#dbeafe", vmin=0, align="left")
                .bar(subset=["Share Receita (%)"], color="#dcfce7", vmin=0, align="left")
            )

            st.dataframe(styled, use_container_width=True, hide_index=True)

    # ── Direita: Ranking de Produtos (mock com data bars) ────────────────────

    with col_dir:
        st.subheader("Ranking de Produtos / Ofertas")
        st.caption("Simulacao · endpoint /ranking-produtos previsto para Sprint 6")

        _dias  = max((data_fim - data_inicio).days + 1, 1)
        _fator = round(_dias / 90, 4)

        df_produtos = pd.DataFrame({
            "Produto": [
                "Consultoria Premium",
                "Treinamento Online",
                "Licenca SaaS Anual",
                "Implementacao Express",
                "Suporte Dedicado",
            ],
            "Receita (R$)": [round(v * _fator, 2)
                             for v in [142000.0, 98500.0, 76200.0, 54800.0, 31400.0]],
            "Unidades":     [max(1, round(v * _fator))
                             for v in [12, 34, 9, 18, 22]],
            "Taxa Conv. (%)": [68.5, 45.2, 72.1, 38.9, 51.3],
        })

        styled_prod = (
            df_produtos.style
            .format({
                "Receita (R$)":   "R$ {:,.2f}",
                "Taxa Conv. (%)": "{:.1f}%",
            })
            .bar(subset=["Receita (R$)"],   color="#dbeafe", vmin=0, align="left")
            .bar(subset=["Taxa Conv. (%)"], color="#dcfce7", vmin=0, align="left")
        )

        st.dataframe(styled_prod, use_container_width=True, hide_index=True)


# =============================================================================
# TAB 2 — Marketing & Funil (placeholder)
# =============================================================================

with tab2:
    st.info(
        "**Em construcao.** "
        "Esta aba trara: Sankey de conversao de leads, "
        "Matriz de campanhas (Magic Quadrant) e "
        "distribuicao por ciclo de vida do lead.",
        icon="🚧",
    )


# =============================================================================
# TAB 3 — Metas & Run Rate (placeholder)
# =============================================================================

with tab3:
    st.info(
        "**Em construcao.** "
        "Esta aba trara: Bullet Charts de atingimento de metas por vendedor "
        "e editor interativo de metas mensais com write-back na API.",
        icon="🚧",
    )
