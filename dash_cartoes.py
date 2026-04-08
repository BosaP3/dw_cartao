import os

import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
from plotly.subplots import make_subplots
import streamlit as st
from sqlalchemy import create_engine, text


DB_CONFIG = {
    "host":     os.getenv("DB_HOST",     "localhost"),
    "port":     int(os.getenv("DB_PORT", "5434")),
    "database": os.getenv("DB_NAME",     "dw_cartao"),
    "user":     os.getenv("DB_USER",     "postgres"),
    "password": os.getenv("DB_PASSWORD", "postgres"),
}

# Paleta de cores coerente
PALETTE = px.colors.qualitative.Set2
COLOR_PRIMARY = "#2196F3"
COLOR_ACCENT  = "#FF6F00"
COLOR_NEG     = "#E53935"
COLOR_POS     = "#43A047"


# Conexão (cacheada)
@st.cache_resource
def get_engine():
    url = (
        f"postgresql+psycopg2://{DB_CONFIG['user']}:{DB_CONFIG['password']}"
        f"@{DB_CONFIG['host']}:{DB_CONFIG['port']}/{DB_CONFIG['database']}"
    )
    return create_engine(url, echo=False)


@st.cache_data(ttl=300)
def query(_engine, sql: str, params: dict = None) -> pd.DataFrame:
    """Executa uma query e retorna DataFrame (cacheado por 5 min)."""
    with _engine.connect() as conn:
        return pd.read_sql(text(sql), conn, params=params)


# Queries
def load_kpis(engine, titulares, categorias, ano_inicio, ano_fim):
    filtros = _build_filtros(titulares, categorias, ano_inicio, ano_fim)
    sql = f"""
        SELECT
            COUNT(*)                                             AS total_transacoes,
            COUNT(DISTINCT f.id_titular)                         AS total_titulares,
            SUM(f.valor_brl) FILTER (WHERE f.valor_brl > 0)     AS receita_bruta,
            SUM(ABS(f.valor_brl)) FILTER (WHERE f.valor_brl < 0) AS total_estornos,
            SUM(f.valor_brl)                                     AS saldo_liquido,
            AVG(f.valor_brl) FILTER (WHERE f.valor_brl > 0)     AS ticket_medio,
            MAX(f.valor_brl)                                     AS maior_compra,
            COUNT(DISTINCT f.id_categoria)                       AS categorias_ativas
        FROM fato_transacao f
        JOIN dim_titular   t ON f.id_titular   = t.id_titular
        JOIN dim_categoria c ON f.id_categoria = c.id_categoria
        JOIN dim_data      d ON f.id_data      = d.id_data
        {filtros}
    """
    return query(engine, sql)


def load_evolucao_mensal(engine, titulares, categorias, ano_inicio, ano_fim):
    filtros = _build_filtros(titulares, categorias, ano_inicio, ano_fim)
    sql = f"""
        SELECT
            MAKE_DATE(d.ano, d.mes, 1)      AS periodo,
            d.ano, d.mes, d.nome_mes,
            COUNT(*)                        AS total_transacoes,
            SUM(f.valor_brl)                AS gasto_total,
            SUM(f.valor_brl) FILTER (WHERE f.valor_brl > 0) AS compras,
            SUM(ABS(f.valor_brl)) FILTER (WHERE f.valor_brl < 0) AS estornos,
            AVG(f.valor_brl) FILTER (WHERE f.valor_brl > 0) AS ticket_medio
        FROM fato_transacao f
        JOIN dim_titular   t ON f.id_titular   = t.id_titular
        JOIN dim_categoria c ON f.id_categoria = c.id_categoria
        JOIN dim_data      d ON f.id_data      = d.id_data
        {filtros}
        GROUP BY d.ano, d.mes, d.nome_mes
        ORDER BY d.ano, d.mes
    """
    return query(engine, sql)


def load_categorias(engine, titulares, categorias, ano_inicio, ano_fim):
    filtros = _build_filtros(titulares, categorias, ano_inicio, ano_fim)
    sql = f"""
        SELECT
            c.nome_categoria,
            COUNT(*)             AS total_transacoes,
            SUM(f.valor_brl)     AS gasto_total
        FROM fato_transacao f
        JOIN dim_titular   t ON f.id_titular   = t.id_titular
        JOIN dim_categoria c ON f.id_categoria = c.id_categoria
        JOIN dim_data      d ON f.id_data      = d.id_data
        WHERE f.valor_brl > 0 {_build_filtros(titulares, categorias, ano_inicio, ano_fim, prefix='AND')}
        GROUP BY c.nome_categoria
        ORDER BY gasto_total DESC
    """
    return query(engine, sql)


def load_titulares(engine, titulares, categorias, ano_inicio, ano_fim):
    filtros = _build_filtros(titulares, categorias, ano_inicio, ano_fim)
    sql = f"""
        SELECT
            t.nome_titular,
            COUNT(*)              AS total_transacoes,
            SUM(f.valor_brl)      AS gasto_total,
            AVG(f.valor_brl)      AS ticket_medio
        FROM fato_transacao f
        JOIN dim_titular   t ON f.id_titular   = t.id_titular
        JOIN dim_categoria c ON f.id_categoria = c.id_categoria
        JOIN dim_data      d ON f.id_data      = d.id_data
        WHERE f.valor_brl > 0 {_build_filtros(titulares, categorias, ano_inicio, ano_fim, prefix='AND')}
        GROUP BY t.nome_titular
        ORDER BY gasto_total DESC
    """
    return query(engine, sql)


def load_dia_semana(engine, titulares, categorias, ano_inicio, ano_fim):
    filtros_extra = _build_filtros(titulares, categorias, ano_inicio, ano_fim, prefix="AND")
    sql = f"""
        SELECT
            d.dia_semana,
            COUNT(*)          AS total_transacoes,
            SUM(f.valor_brl)  AS gasto_total
        FROM fato_transacao f
        JOIN dim_titular   t ON f.id_titular   = t.id_titular
        JOIN dim_categoria c ON f.id_categoria = c.id_categoria
        JOIN dim_data      d ON f.id_data      = d.id_data
        WHERE f.valor_brl > 0 {filtros_extra}
        GROUP BY d.dia_semana
        ORDER BY
            CASE d.dia_semana
                WHEN 'Segunda-feira' THEN 1 WHEN 'Terça-feira'  THEN 2
                WHEN 'Quarta-feira'  THEN 3 WHEN 'Quinta-feira' THEN 4
                WHEN 'Sexta-feira'   THEN 5 WHEN 'Sábado'       THEN 6
                WHEN 'Domingo'       THEN 7
            END
    """
    return query(engine, sql)


def load_top_estabelecimentos(engine, titulares, categorias, ano_inicio, ano_fim, n=10):
    filtros_extra = _build_filtros(titulares, categorias, ano_inicio, ano_fim, prefix="AND")
    sql = f"""
        SELECT
            e.nome_estabelecimento,
            COUNT(*)          AS total_transacoes,
            SUM(f.valor_brl)  AS gasto_total
        FROM fato_transacao f
        JOIN dim_titular         t ON f.id_titular         = t.id_titular
        JOIN dim_categoria       c ON f.id_categoria       = c.id_categoria
        JOIN dim_estabelecimento e ON f.id_estabelecimento = e.id_estabelecimento
        JOIN dim_data            d ON f.id_data            = d.id_data
        WHERE f.valor_brl > 0 {filtros_extra}
        GROUP BY e.nome_estabelecimento
        ORDER BY gasto_total DESC
        LIMIT {n}
    """
    return query(engine, sql)


def load_parcelamento(engine, titulares, categorias, ano_inicio, ano_fim):
    filtros_extra = _build_filtros(titulares, categorias, ano_inicio, ano_fim, prefix="AND")
    sql = f"""
        SELECT
            CASE
                WHEN total_parcelas = 1   THEN '1x (À Vista)'
                WHEN total_parcelas <= 3  THEN '2-3x'
                WHEN total_parcelas <= 6  THEN '4-6x'
                WHEN total_parcelas <= 12 THEN '7-12x'
                ELSE '13x+'
            END AS faixa,
            COUNT(*)          AS total_transacoes,
            SUM(f.valor_brl)  AS gasto_total
        FROM fato_transacao f
        JOIN dim_titular   t ON f.id_titular   = t.id_titular
        JOIN dim_categoria c ON f.id_categoria = c.id_categoria
        JOIN dim_data      d ON f.id_data      = d.id_data
        WHERE f.valor_brl > 0 AND f.total_parcelas IS NOT NULL {filtros_extra}
        GROUP BY faixa
        ORDER BY MIN(total_parcelas)
    """
    return query(engine, sql)


def load_filtros_disponiveis(engine):
    """Carrega valores únicos para os filtros da sidebar."""
    titulares = query(engine, "SELECT nome_titular FROM dim_titular ORDER BY nome_titular")["nome_titular"].tolist()
    categorias = query(engine, "SELECT nome_categoria FROM dim_categoria ORDER BY nome_categoria")["nome_categoria"].tolist()
    anos = query(engine, "SELECT DISTINCT ano FROM dim_data ORDER BY ano")["ano"].tolist()
    return titulares, categorias, anos


def _build_filtros(titulares, categorias, ano_inicio, ano_fim, prefix="WHERE"):
    """Constrói cláusula WHERE/AND com os filtros selecionados."""
    parts = []

    if titulares:
        lista = ", ".join(f"'{t.replace(chr(39), chr(39)*2)}'" for t in titulares)
        parts.append(f"t.nome_titular IN ({lista})")

    if categorias:
        lista = ", ".join(f"'{c.replace(chr(39), chr(39)*2)}'" for c in categorias)
        parts.append(f"c.nome_categoria IN ({lista})")

    parts.append(f"d.ano BETWEEN {ano_inicio} AND {ano_fim}")

    if not parts:
        return ""

    connector = prefix + " " if prefix == "AND" else "WHERE "
    return connector + " AND ".join(parts)


# Helpers de formatação
def fmt_brl(v):
    if v is None or (isinstance(v, float) and pd.isna(v)):
        return "R$ —"
    return f"R$ {v:,.2f}".replace(",", "X").replace(".", ",").replace("X", ".")


def fmt_num(v):
    if v is None or (isinstance(v, float) and pd.isna(v)):
        return "—"
    return f"{int(v):,}".replace(",", ".")


# Layout
def render_kpi_cards(kpis: pd.DataFrame):
    row = kpis.iloc[0]
    cols = st.columns(4)

    with cols[0]:
        st.metric("💳 Total de Transações",  fmt_num(row["total_transacoes"]))
    with cols[1]:
        st.metric("💰 Receita Bruta",        fmt_brl(row["receita_bruta"]))
    with cols[2]:
        st.metric("🧾 Ticket Médio",         fmt_brl(row["ticket_medio"]))
    with cols[3]:
        st.metric("↩️ Total de Estornos",    fmt_brl(row["total_estornos"]),
                  delta=f"Saldo líquido: {fmt_brl(row['saldo_liquido'])}",
                  delta_color="inverse")

    cols2 = st.columns(4)
    with cols2[0]:
        st.metric("👥 Titulares Ativos",     fmt_num(row["total_titulares"]))
    with cols2[1]:
        st.metric("🏷️ Categorias Ativas",    fmt_num(row["categorias_ativas"]))
    with cols2[2]:
        st.metric("🏆 Maior Compra",         fmt_brl(row["maior_compra"]))
    with cols2[3]:
        saldo = row["saldo_liquido"]
        cor   = "normal" if (saldo or 0) >= 0 else "inverse"
        st.metric("📊 Saldo Líquido",        fmt_brl(saldo))


def render_evolucao_mensal(df: pd.DataFrame):
    if df.empty:
        st.info("Sem dados para o período selecionado.")
        return

    df["periodo"] = pd.to_datetime(df["periodo"])
    df["label_mes"] = df["nome_mes"].str[:3] + "/" + df["ano"].astype(str).str[-2:]

    fig = make_subplots(rows=2, cols=1, shared_xaxes=True,
                        subplot_titles=("Gasto Total Mensal (R$)", "Quantidade de Transações"),
                        vertical_spacing=0.12)

    fig.add_trace(go.Bar(
        x=df["label_mes"], y=df["compras"],
        name="Compras", marker_color=COLOR_PRIMARY, opacity=0.85,
    ), row=1, col=1)

    fig.add_trace(go.Bar(
        x=df["label_mes"], y=-df["estornos"].fillna(0),
        name="Estornos", marker_color=COLOR_NEG, opacity=0.7,
    ), row=1, col=1)

    fig.add_trace(go.Scatter(
        x=df["label_mes"], y=df["ticket_medio"],
        name="Ticket Médio", mode="lines+markers",
        line=dict(color=COLOR_ACCENT, width=2), yaxis="y3",
    ), row=1, col=1)

    fig.add_trace(go.Bar(
        x=df["label_mes"], y=df["total_transacoes"],
        name="Qtd Transações", marker_color="#7986CB",
    ), row=2, col=1)

    fig.update_layout(
        height=520, barmode="relative",
        legend=dict(orientation="h", yanchor="bottom", y=1.02, xanchor="right", x=1),
        margin=dict(t=60, b=20),
    )
    st.plotly_chart(fig, use_container_width=True)


def render_categorias(df: pd.DataFrame):
    if df.empty:
        return

    col1, col2 = st.columns([1.2, 1])

    with col1:
        fig = px.bar(
            df.head(10), x="gasto_total", y="nome_categoria",
            orientation="h", color="gasto_total",
            color_continuous_scale="Blues_r",
            labels={"gasto_total": "Gasto Total (R$)", "nome_categoria": "Categoria"},
            title="Top 10 Categorias por Gasto",
        )
        fig.update_layout(height=420, showlegend=False, coloraxis_showscale=False,
                          yaxis=dict(autorange="reversed"))
        st.plotly_chart(fig, use_container_width=True)

    with col2:
        fig2 = px.pie(
            df.head(8), values="gasto_total", names="nome_categoria",
            title="Distribuição por Categoria (Top 8)",
            color_discrete_sequence=PALETTE,
            hole=0.45,
        )
        fig2.update_traces(textposition="inside", textinfo="percent+label")
        fig2.update_layout(height=420, showlegend=False)
        st.plotly_chart(fig2, use_container_width=True)


def render_titulares(df: pd.DataFrame):
    if df.empty:
        return

    fig = px.bar(
        df, x="nome_titular", y="gasto_total",
        color="nome_titular", color_discrete_sequence=PALETTE,
        text_auto=".2s",
        labels={"nome_titular": "Titular", "gasto_total": "Gasto Total (R$)"},
        title="Gasto Total por Titular",
    )
    fig.update_traces(textposition="outside")
    fig.update_layout(showlegend=False, height=380, margin=dict(t=50))
    st.plotly_chart(fig, use_container_width=True)


def render_dia_semana(df: pd.DataFrame):
    if df.empty:
        return

    fig = px.bar(
        df, x="dia_semana", y="total_transacoes",
        color="gasto_total", color_continuous_scale="Oranges",
        labels={"dia_semana": "Dia da Semana", "total_transacoes": "Qtd Transações"},
        title="Transações por Dia da Semana",
        text="total_transacoes",
    )
    fig.update_traces(textposition="outside")
    fig.update_layout(height=360, coloraxis_showscale=False, margin=dict(t=50))
    st.plotly_chart(fig, use_container_width=True)


def render_parcelamento(df: pd.DataFrame):
    if df.empty:
        return

    fig = px.bar(
        df, x="faixa", y="total_transacoes",
        color="faixa", color_discrete_sequence=PALETTE,
        text_auto=True,
        labels={"faixa": "Faixa de Parcelamento", "total_transacoes": "Qtd Transações"},
        title="Comportamento de Parcelamento",
    )
    fig.update_traces(textposition="outside")
    fig.update_layout(showlegend=False, height=360, margin=dict(t=50))
    st.plotly_chart(fig, use_container_width=True)


def render_top_estabelecimentos(df: pd.DataFrame):
    if df.empty:
        return

    fig = px.bar(
        df, x="gasto_total", y="nome_estabelecimento",
        orientation="h", color="gasto_total",
        color_continuous_scale="Teal",
        labels={"gasto_total": "Gasto Total (R$)", "nome_estabelecimento": "Estabelecimento"},
        title="Top Estabelecimentos por Gasto",
        text_auto=".2s",
    )
    fig.update_layout(height=420, showlegend=False, coloraxis_showscale=False,
                      yaxis=dict(autorange="reversed"), margin=dict(t=50))
    st.plotly_chart(fig, use_container_width=True)


def render_tabela_detalhe(df_titulares: pd.DataFrame, df_categorias: pd.DataFrame):
    tab1, tab2 = st.tabs(["📋 Por Titular", "🏷️ Por Categoria"])

    with tab1:
        if not df_titulares.empty:
            df_show = df_titulares.copy()
            df_show["gasto_total"]  = df_show["gasto_total"].apply(fmt_brl)
            df_show["ticket_medio"] = df_show["ticket_medio"].apply(fmt_brl)
            df_show.columns = ["Titular", "Qtd Transações", "Gasto Total", "Ticket Médio"]
            st.dataframe(df_show, use_container_width=True, hide_index=True)

    with tab2:
        if not df_categorias.empty:
            df_show = df_categorias.copy()
            df_show["gasto_total"] = df_show["gasto_total"].apply(fmt_brl)
            df_show.columns = ["Categoria", "Qtd Transações", "Gasto Total"]
            st.dataframe(df_show, use_container_width=True, hide_index=True)


# APP PRINCIPAL
def main():
    st.set_page_config(
        page_title="BI — Transações de Cartão",
        page_icon="💳",
        layout="wide",
        initial_sidebar_state="expanded",
    )

    # CSS personalizado
    st.markdown("""
    <style>
        .block-container { padding-top: 1.5rem; padding-bottom: 1rem; }
        [data-testid="metric-container"] {
            background: #f8f9fb;
            border: 1px solid #e3e6ef;
            border-radius: 10px;
            padding: 12px 18px;
        }
        .section-title {
            font-size: 1.1rem;
            font-weight: 700;
            color: #1a237e;
            border-left: 4px solid #2196F3;
            padding-left: 10px;
            margin: 20px 0 10px 0;
        }
    </style>
    """, unsafe_allow_html=True)

    # Header
    st.title("💳 Dashboard BI — Transações de Cartão de Crédito")
    st.caption("Projeto Data Warehouse · Análise e Desenvolvimento de Sistemas")
    st.divider()

    # Conexão
    try:
        engine = get_engine()
        engine.connect()
    except Exception as e:
        st.error(f"Não foi possível conectar ao banco de dados: {e}")
        st.info("Verifique as variáveis de ambiente DB_HOST, DB_PORT, DB_NAME, DB_USER, DB_PASSWORD.")
        st.stop()

    # Sidebar — Filtros
    with st.sidebar:
        st.header("🔧 Filtros")

        try:
            todos_titulares, todas_categorias, anos_disp = load_filtros_disponiveis(engine)
        except Exception as e:
            st.error(f"Erro ao carregar filtros: {e}")
            st.stop()

        ano_min = min(anos_disp) if anos_disp else 2025
        ano_max = max(anos_disp) if anos_disp else 2026

        col_a, col_b = st.columns(2)
        with col_a:
            ano_inicio = st.selectbox("Ano Início", anos_disp, index=0)
        with col_b:
            ano_fim = st.selectbox("Ano Fim", anos_disp, index=len(anos_disp)-1)

        if ano_inicio > ano_fim:
            st.warning("Ano início maior que ano fim.")

        titulares_sel = st.multiselect(
            "Titular(es)", todos_titulares,
            placeholder="Todos os titulares",
        )

        categorias_sel = st.multiselect(
            "Categoria(s)", todas_categorias,
            placeholder="Todas as categorias",
        )

        top_n = st.slider("Top N estabelecimentos", 5, 20, 10)

        st.divider()
        st.caption("Dados atualizados a cada 5 min (cache)")

        if st.button("🔄 Atualizar dados", use_container_width=True):
            st.cache_data.clear()
            st.rerun()

    # Carregamento de dados
    args = (engine, titulares_sel or None, categorias_sel or None, ano_inicio, ano_fim)

    # Ajusta para listas vazias → None (sem filtro)
    t_sel = titulares_sel  if titulares_sel  else []
    c_sel = categorias_sel if categorias_sel else []

    with st.spinner("Carregando dados..."):
        try:
            df_kpis      = load_kpis(engine, t_sel, c_sel, ano_inicio, ano_fim)
            df_evolucao  = load_evolucao_mensal(engine, t_sel, c_sel, ano_inicio, ano_fim)
            df_cats      = load_categorias(engine, t_sel, c_sel, ano_inicio, ano_fim)
            df_titulares = load_titulares(engine, t_sel, c_sel, ano_inicio, ano_fim)
            df_diasem    = load_dia_semana(engine, t_sel, c_sel, ano_inicio, ano_fim)
            df_parcelas  = load_parcelamento(engine, t_sel, c_sel, ano_inicio, ano_fim)
            df_estabs    = load_top_estabelecimentos(engine, t_sel, c_sel, ano_inicio, ano_fim, top_n)
        except Exception as e:
            st.error(f"Erro ao carregar dados: {e}")
            st.stop()

    # KPIs
    st.markdown('<div class="section-title">📊 Indicadores Gerais</div>', unsafe_allow_html=True)
    render_kpi_cards(df_kpis)

    st.divider()

    #  Evolução Mensal ─
    st.markdown('<div class="section-title">📈 Evolução Mensal</div>', unsafe_allow_html=True)
    render_evolucao_mensal(df_evolucao)

    st.divider()

    #  Categorias 
    st.markdown('<div class="section-title">🏷️ Análise por Categoria</div>', unsafe_allow_html=True)
    render_categorias(df_cats)

    st.divider()

    #  Titulares ─
    st.markdown('<div class="section-title">👥 Gasto por Titular</div>', unsafe_allow_html=True)
    render_titulares(df_titulares)

    st.divider()

    #  Dia da semana e Parcelamento ─
    st.markdown('<div class="section-title">📅 Comportamento Temporal e de Parcelamento</div>',
                unsafe_allow_html=True)
    col_l, col_r = st.columns(2)
    with col_l:
        render_dia_semana(df_diasem)
    with col_r:
        render_parcelamento(df_parcelas)

    st.divider()

    #  Top Estabelecimentos ─
    st.markdown('<div class="section-title">🏪 Top Estabelecimentos</div>', unsafe_allow_html=True)
    render_top_estabelecimentos(df_estabs)

    st.divider()

    #  Tabelas detalhadas 
    st.markdown('<div class="section-title">📋 Tabelas Detalhadas</div>', unsafe_allow_html=True)
    render_tabela_detalhe(df_titulares, df_cats)

    #  Rodapé 
    st.divider()
    st.caption("💳 DW Transações de Cartão de Crédito · Projeto BI · Ciências da Computação")


if __name__ == "__main__":
    main()
