import os
import sys
import argparse
from pathlib import Path
from datetime import datetime

import pandas as pd
from sqlalchemy import create_engine, text

try:
    from tabulate import tabulate
    HAS_TABULATE = True
except ImportError:
    HAS_TABULATE = False

# Configuração
DB_CONFIG = {
    "host":     os.getenv("DB_HOST",     "localhost"),
    "port":     int(os.getenv("DB_PORT", "5434")),
    "database": os.getenv("DB_NAME",     "dw_cartao"),
    "user":     os.getenv("DB_USER",     "postgres"),
    "password": os.getenv("DB_PASSWORD", "postgres"),
}

EXPORT_DIR = Path("resultados_analiticos")


def get_engine():
    url = (
        f"postgresql+psycopg2://{DB_CONFIG['user']}:{DB_CONFIG['password']}"
        f"@{DB_CONFIG['host']}:{DB_CONFIG['port']}/{DB_CONFIG['database']}"
    )
    return create_engine(url, echo=False)


# Utilitários
def executar_query(engine, sql: str, params: dict = None) -> pd.DataFrame:
    """Executa uma query e retorna um DataFrame."""
    with engine.connect() as conn:
        return pd.read_sql(text(sql), conn, params=params)


def exibir_df(df: pd.DataFrame, titulo: str, max_rows: int = 20):
    """Exibe um DataFrame formatado no terminal."""
    print(f"\n{'═'*60}")
    print(f"  {titulo}")
    print(f"{'═'*60}")
    if df.empty:
        print("  (sem resultados)")
        return
    df_show = df.head(max_rows)
    if HAS_TABULATE:
        print(tabulate(df_show, headers="keys", tablefmt="rounded_outline",
                       showindex=False, floatfmt=".2f"))
    else:
        print(df_show.to_string(index=False))
    if len(df) > max_rows:
        print(f"  ... e mais {len(df) - max_rows} linhas.")


def exportar_csv(df: pd.DataFrame, nome: str):
    """Exporta um DataFrame para CSV na pasta de resultados."""
    EXPORT_DIR.mkdir(exist_ok=True)
    caminho = EXPORT_DIR / f"{nome}.csv"
    df.to_csv(caminho, index=False, encoding="utf-8-sig", sep=";")
    print("Exportado: %s", caminho)


# CONSULTAS ANALÍTICAS

QUERIES = {}  # Registro das queries disponíveis


def query(nome: str, descricao: str):
    """Decorator para registrar queries analíticas."""
    def decorator(func):
        QUERIES[nome] = {"func": func, "descricao": descricao}
        return func
    return decorator


# 1. Gasto total por titular no período e por mês
@query("gasto_titular", "Gasto total por titular no período completo")
def gasto_por_titular(engine) -> pd.DataFrame:
    sql = """
        SELECT
            t.nome_titular,
            t.final_cartao,
            COUNT(*)                               AS total_transacoes,
            SUM(f.valor_brl)                       AS gasto_total_brl,
            AVG(f.valor_brl)                       AS ticket_medio_brl,
            MIN(d.data)                            AS primeira_compra,
            MAX(d.data)                            AS ultima_compra
        FROM fato_transacao f
        JOIN dim_titular t ON f.id_titular = t.id_titular
        JOIN dim_data    d ON f.id_data    = d.id_data
        WHERE f.valor_brl > 0
        GROUP BY t.nome_titular, t.final_cartao
        ORDER BY gasto_total_brl DESC
    """
    return executar_query(engine, sql)


@query("gasto_titular_mensal", "Gasto por titular e mês (série temporal)")
def gasto_titular_mensal(engine) -> pd.DataFrame:
    sql = """
        SELECT
            t.nome_titular,
            d.ano,
            d.mes,
            d.nome_mes,
            COUNT(*)            AS total_transacoes,
            SUM(f.valor_brl)    AS gasto_brl
        FROM fato_transacao f
        JOIN dim_titular t ON f.id_titular = t.id_titular
        JOIN dim_data    d ON f.id_data    = d.id_data
        WHERE f.valor_brl > 0
        GROUP BY t.nome_titular, d.ano, d.mes, d.nome_mes
        ORDER BY t.nome_titular, d.ano, d.mes
    """
    return executar_query(engine, sql)


# 2. Gasto por categoria (top 10)
@query("top_categorias", "Top 10 categorias por valor total gasto")
def top_categorias(engine) -> pd.DataFrame:
    sql = """
        SELECT
            c.nome_categoria,
            COUNT(*)                                        AS total_transacoes,
            SUM(f.valor_brl)                               AS gasto_total_brl,
            ROUND(100.0 * SUM(f.valor_brl) /
                SUM(SUM(f.valor_brl)) OVER (), 2)          AS pct_total,
            AVG(f.valor_brl)                               AS ticket_medio
        FROM fato_transacao f
        JOIN dim_categoria c ON f.id_categoria = c.id_categoria
        WHERE f.valor_brl > 0
        GROUP BY c.nome_categoria
        ORDER BY gasto_total_brl DESC
        LIMIT 10
    """
    return executar_query(engine, sql)


@query("categorias_por_titular", "Gasto por categoria detalhado por titular")
def categorias_por_titular(engine) -> pd.DataFrame:
    sql = """
        SELECT
            t.nome_titular,
            c.nome_categoria,
            COUNT(*)                AS total_transacoes,
            SUM(f.valor_brl)        AS gasto_total_brl
        FROM fato_transacao f
        JOIN dim_titular  t ON f.id_titular  = t.id_titular
        JOIN dim_categoria c ON f.id_categoria = c.id_categoria
        WHERE f.valor_brl > 0
        GROUP BY t.nome_titular, c.nome_categoria
        ORDER BY t.nome_titular, gasto_total_brl DESC
    """
    return executar_query(engine, sql)


# 3. Evolução mensal do gasto total 
@query("evolucao_mensal", "Evolução mensal do total gasto (série temporal)")
def evolucao_mensal(engine) -> pd.DataFrame:
    sql = """
        SELECT
            d.ano,
            d.mes,
            d.nome_mes,
            MAKE_DATE(d.ano, d.mes, 1)             AS periodo,
            COUNT(*)                               AS total_transacoes,
            SUM(f.valor_brl)                       AS gasto_total_brl,
            AVG(f.valor_brl)                       AS ticket_medio,
            SUM(CASE WHEN f.valor_brl < 0
                     THEN ABS(f.valor_brl) ELSE 0 END) AS total_estornos_brl
        FROM fato_transacao f
        JOIN dim_data d ON f.id_data = d.id_data
        GROUP BY d.ano, d.mes, d.nome_mes
        ORDER BY d.ano, d.mes
    """
    return executar_query(engine, sql)


# 4. Comparativo entre titulares
@query("comparativo_titulares", "Comparativo de métricas entre titulares")
def comparativo_titulares(engine) -> pd.DataFrame:
    sql = """
        SELECT
            t.nome_titular,
            COUNT(*)                                        AS qtd_transacoes,
            SUM(f.valor_brl)                               AS gasto_total_brl,
            AVG(f.valor_brl)                               AS ticket_medio,
            PERCENTILE_CONT(0.5) WITHIN GROUP (
                ORDER BY f.valor_brl)                      AS mediana_brl,
            MAX(f.valor_brl)                               AS maior_compra,
            MIN(f.valor_brl) FILTER (WHERE f.valor_brl>0) AS menor_compra,
            COUNT(DISTINCT d.mes)                          AS meses_ativos,
            COUNT(DISTINCT c.id_categoria)                 AS categorias_distintas,
            COUNT(DISTINCT e.id_estabelecimento)           AS estabelecimentos_distintos
        FROM fato_transacao f
        JOIN dim_titular      t ON f.id_titular      = t.id_titular
        JOIN dim_data         d ON f.id_data         = d.id_data
        JOIN dim_categoria    c ON f.id_categoria    = c.id_categoria
        JOIN dim_estabelecimento e ON f.id_estabelecimento = e.id_estabelecimento
        WHERE f.valor_brl > 0
        GROUP BY t.nome_titular
        ORDER BY gasto_total_brl DESC
    """
    return executar_query(engine, sql)


#  5. Top estabelecimentos por valor 
@query("top_estabelecimentos", "Top 15 estabelecimentos por valor total")
def top_estabelecimentos(engine) -> pd.DataFrame:
    sql = """
        SELECT
            e.nome_estabelecimento,
            COUNT(*)                AS total_transacoes,
            SUM(f.valor_brl)        AS gasto_total_brl,
            AVG(f.valor_brl)        AS ticket_medio,
            COUNT(DISTINCT f.id_titular) AS titulares_distintos
        FROM fato_transacao f
        JOIN dim_estabelecimento e ON f.id_estabelecimento = e.id_estabelecimento
        WHERE f.valor_brl > 0
        GROUP BY e.nome_estabelecimento
        ORDER BY gasto_total_brl DESC
        LIMIT 15
    """
    return executar_query(engine, sql)


# 6. Comportamento de parcelamento
@query("parcelamento", "Análise de parcelamento: à vista vs parcelado")
def analise_parcelamento(engine) -> pd.DataFrame:
    sql = """
        SELECT
            CASE
                WHEN total_parcelas = 1  THEN 'À Vista'
                WHEN total_parcelas <= 3 THEN 'Parcelado 2-3x'
                WHEN total_parcelas <= 6 THEN 'Parcelado 4-6x'
                WHEN total_parcelas <= 12 THEN 'Parcelado 7-12x'
                ELSE 'Parcelado 13x+'
            END                                     AS faixa_parcelamento,
            COUNT(*)                                AS total_transacoes,
            SUM(f.valor_brl)                        AS gasto_total_brl,
            AVG(f.valor_brl)                        AS ticket_medio,
            ROUND(100.0 * COUNT(*) / SUM(COUNT(*)) OVER(), 2) AS pct_transacoes
        FROM fato_transacao f
        WHERE f.valor_brl > 0 AND f.total_parcelas IS NOT NULL
        GROUP BY faixa_parcelamento
        ORDER BY
            CASE faixa_parcelamento
                WHEN 'À Vista'        THEN 1
                WHEN 'Parcelado 2-3x' THEN 2
                WHEN 'Parcelado 4-6x' THEN 3
                WHEN 'Parcelado 7-12x'THEN 4
                ELSE 5
            END
    """
    return executar_query(engine, sql)


#  7. Dia da semana 
@query("dia_semana", "Volume e valor de transações por dia da semana")
def analise_dia_semana(engine) -> pd.DataFrame:
    sql = """
        SELECT
            d.dia_semana,
            COUNT(*)            AS total_transacoes,
            SUM(f.valor_brl)    AS gasto_total_brl,
            AVG(f.valor_brl)    AS ticket_medio
        FROM fato_transacao f
        JOIN dim_data d ON f.id_data = d.id_data
        WHERE f.valor_brl > 0
        GROUP BY d.dia_semana
        ORDER BY
            CASE d.dia_semana
                WHEN 'Segunda-feira' THEN 1
                WHEN 'Terça-feira'   THEN 2
                WHEN 'Quarta-feira'  THEN 3
                WHEN 'Quinta-feira'  THEN 4
                WHEN 'Sexta-feira'   THEN 5
                WHEN 'Sábado'        THEN 6
                WHEN 'Domingo'       THEN 7
            END
    """
    return executar_query(engine, sql)


#  8. Estornos e créditos 
@query("estornos", "Estornos e créditos por titular e categoria")
def analise_estornos(engine) -> pd.DataFrame:
    sql = """
        SELECT
            t.nome_titular,
            c.nome_categoria,
            COUNT(*)                        AS qtd_estornos,
            SUM(ABS(f.valor_brl))           AS total_estornado_brl,
            MIN(d.data)                     AS primeiro_estorno,
            MAX(d.data)                     AS ultimo_estorno
        FROM fato_transacao f
        JOIN dim_titular   t ON f.id_titular  = t.id_titular
        JOIN dim_categoria c ON f.id_categoria = c.id_categoria
        JOIN dim_data      d ON f.id_data      = d.id_data
        WHERE f.valor_brl < 0
        GROUP BY t.nome_titular, c.nome_categoria
        ORDER BY total_estornado_brl DESC
    """
    return executar_query(engine, sql)


# 9. Transações internacionais
@query("internacionais", "Transações internacionais (com valor em USD)")
def transacoes_internacionais(engine) -> pd.DataFrame:
    sql = """
        SELECT
            t.nome_titular,
            c.nome_categoria,
            COUNT(*)                            AS qtd_transacoes,
            SUM(f.valor_usd)                    AS total_usd,
            SUM(f.valor_brl)                    AS total_brl,
            AVG(f.cotacao)                      AS cotacao_media
        FROM fato_transacao f
        JOIN dim_titular   t ON f.id_titular   = t.id_titular
        JOIN dim_categoria c ON f.id_categoria = c.id_categoria
        WHERE f.valor_usd IS NOT NULL AND f.valor_usd > 0
        GROUP BY t.nome_titular, c.nome_categoria
        ORDER BY total_brl DESC
    """
    return executar_query(engine, sql)


#  10. KPIs Gerais 
@query("kpis", "KPIs gerais do período")
def kpis_gerais(engine) -> pd.DataFrame:
    sql = """
        SELECT
            COUNT(*)                                            AS total_transacoes,
            COUNT(DISTINCT f.id_titular)                        AS total_titulares,
            COUNT(DISTINCT f.id_categoria)                      AS total_categorias,
            COUNT(DISTINCT f.id_estabelecimento)                AS total_estabelecimentos,
            COUNT(DISTINCT f.id_data)                           AS dias_com_transacao,
            SUM(f.valor_brl) FILTER (WHERE f.valor_brl > 0)    AS receita_bruta_brl,
            SUM(ABS(f.valor_brl)) FILTER (WHERE f.valor_brl<0) AS total_estornos_brl,
            SUM(f.valor_brl)                                    AS saldo_liquido_brl,
            AVG(f.valor_brl) FILTER (WHERE f.valor_brl > 0)    AS ticket_medio_brl,
            MAX(f.valor_brl)                                    AS maior_compra_brl,
            MIN(d.data)                                         AS inicio_periodo,
            MAX(d.data)                                         AS fim_periodo
        FROM fato_transacao f
        JOIN dim_data d ON f.id_data = d.id_data
    """
    df = executar_query(engine, sql)
    # Transpor para exibição vertical
    return df.T.reset_index().rename(columns={"index": "KPI", 0: "Valor"})


# VALIDAÇÕES DE QUALIDADE

CHECKS = []


def check(descricao: str, esperado=None):
    """Decorator para registrar checks de qualidade."""
    def decorator(func):
        CHECKS.append({"func": func, "descricao": descricao, "esperado": esperado})
        return func
    return decorator


@check("Fato sem id_data (FK órfã)", esperado=0)
def check_fk_data(engine):
    return executar_query(engine, "SELECT COUNT(*) AS n FROM fato_transacao WHERE id_data IS NULL").iloc[0,0]

@check("Fato sem id_titular (FK órfã)", esperado=0)
def check_fk_titular(engine):
    return executar_query(engine, "SELECT COUNT(*) AS n FROM fato_transacao WHERE id_titular IS NULL").iloc[0,0]

@check("Fato sem id_categoria (FK órfã)", esperado=0)
def check_fk_categoria(engine):
    return executar_query(engine, "SELECT COUNT(*) AS n FROM fato_transacao WHERE id_categoria IS NULL").iloc[0,0]

@check("Fato sem id_estabelecimento (FK órfã)", esperado=0)
def check_fk_estab(engine):
    return executar_query(engine, "SELECT COUNT(*) AS n FROM fato_transacao WHERE id_estabelecimento IS NULL").iloc[0,0]

@check("Categoria 'Não Categorizado' presente", esperado=None)
def check_nao_categorizado(engine):
    return executar_query(engine,
        "SELECT COUNT(*) FROM fato_transacao f "
        "JOIN dim_categoria c ON f.id_categoria=c.id_categoria "
        "WHERE c.nome_categoria='Não Categorizado'"
    ).iloc[0,0]

@check("Transações com valor_brl zerado ou nulo", esperado=None)
def check_valor_zero(engine):
    return executar_query(engine,
        "SELECT COUNT(*) FROM fato_transacao WHERE valor_brl IS NULL OR valor_brl = 0"
    ).iloc[0,0]

@check("Datas fora do intervalo esperado (2025-2026)", esperado=0)
def check_datas_fora_range(engine):
    return executar_query(engine,
        "SELECT COUNT(*) FROM dim_data WHERE ano < 2025 OR ano > 2026"
    ).iloc[0,0]

@check("Duplicatas exatas na fato (mesma data+titular+estabelec+valor)", esperado=0)
def check_duplicatas(engine):
    sql = """
        SELECT COUNT(*) FROM (
            SELECT id_data, id_titular, id_estabelecimento, valor_brl,
                   COUNT(*) AS n
            FROM fato_transacao
            GROUP BY id_data, id_titular, id_estabelecimento, valor_brl
            HAVING COUNT(*) > 1
        ) dup
    """
    return executar_query(engine, sql).iloc[0,0]

@check("Arquivos CSV carregados (distintos)", esperado=None)
def check_arquivos(engine):
    return executar_query(engine,
        "SELECT COUNT(DISTINCT arquivo_origem) FROM fato_transacao"
    ).iloc[0,0]

@check("Menor valor_brl em transações positivas (sanidade)", esperado=None)
def check_menor_valor(engine):
    return executar_query(engine,
        "SELECT MIN(valor_brl) FROM fato_transacao WHERE valor_brl > 0"
    ).iloc[0,0]


def executar_validacoes(engine) -> bool:
    """Executa todos os checks e exibe resultados. Retorna True se todos OK."""
    print(f"\n{'═'*65}")
    print("  VALIDAÇÕES DE QUALIDADE DE DADOS")
    print(f"{'═'*65}")

    todos_ok = True
    resultados = []

    for c in CHECKS:
        try:
            valor = c["func"](engine)
            esperado = c["esperado"]
            if esperado is not None:
                ok = valor == esperado
                status = "✓ OK" if ok else f"✗ FALHA (esperado {esperado})"
                if not ok:
                    todos_ok = False
            else:
                status = "ℹ INFO"
            resultados.append({
                "Check": c["descricao"],
                "Resultado": valor,
                "Status": status,
            })
        except Exception as e:
            resultados.append({
                "Check": c["descricao"],
                "Resultado": f"ERRO: {e}",
                "Status": "✗ ERRO",
            })
            todos_ok = False

    df_res = pd.DataFrame(resultados)
    if HAS_TABULATE:
        print(tabulate(df_res, headers="keys", tablefmt="rounded_outline",
                       showindex=False))
    else:
        print(df_res.to_string(index=False))

    conclusao = "✓ Todas as validações passaram!" if todos_ok else "✗ Existem falhas nas validações."
    print(f"\n  {conclusao}")
    print(f"{'═'*65}\n")
    return todos_ok


#  MAIN
def parse_args():
    parser = argparse.ArgumentParser(
        description="Consultas analíticas e validações — DW Cartão de Crédito"
    )
    parser.add_argument(
        "--query", choices=list(QUERIES.keys()) + ["todas"], default="todas",
        help="Query específica para executar (padrão: todas)"
    )
    parser.add_argument(
        "--exportar", action="store_true",
        help="Exporta resultados como CSVs em ./resultados_analiticos/"
    )
    parser.add_argument(
        "--validar-apenas", action="store_true",
        help="Executa apenas os checks de qualidade, sem queries analíticas"
    )
    parser.add_argument(
        "--max-rows", type=int, default=20,
        help="Máximo de linhas exibidas por query (padrão: 20)"
    )
    return parser.parse_args()


def main():
    args = parse_args()
    engine = get_engine()

    # Validações de qualidade sempre executam primeiro
    ok = executar_validacoes(engine)
    if not ok:
        print("Atenção: existem falhas nas validações de qualidade.")

    if args.validar_apenas:
        sys.exit(0 if ok else 1)

    # Queries analíticas
    queries_executar = (
        {args.query: QUERIES[args.query]}
        if args.query != "todas"
        else QUERIES
    )

    print(f"\n{'═'*60}")
    print(f"  ANÁLISES — {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print(f"{'═'*60}")

    for nome, meta in queries_executar.items():
        print("Executando query: %s — %s", nome, meta["descricao"])
        try:
            df = meta["func"](engine)
            exibir_df(df, meta["descricao"], max_rows=args.max_rows)
            if args.exportar:
                exportar_csv(df, nome)
        except Exception as e:
            print("Erro na query '%s': %s", nome, e)

    if args.exportar:
        print("Resultados exportados em: %s/", EXPORT_DIR)


if __name__ == "__main__":
    main()
