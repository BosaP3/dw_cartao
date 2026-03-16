"""
ETL Pipeline — Data Warehouse de Transações de Cartão de Crédito
=================================================================
Autor: Projeto BI — Ciências da Computação
Descrição:
    Lê os arquivos CSV mensais de faturas de cartão de crédito,
    aplica as transformações necessárias e carrega os dados em um
    Data Warehouse PostgreSQL com modelo dimensional (Star Schema).

Estrutura:
    - DIM_DATA
    - DIM_TITULAR
    - DIM_CATEGORIA
    - DIM_ESTABELECIMENTO
    - FATO_TRANSACAO

Requisitos:
    pip install pandas psycopg2-binary sqlalchemy python-dotenv
"""

import os
import re
import glob
import pandas as pd
from sqlalchemy import create_engine, text

# CONFIGURAÇÃO
DB_CONFIG = {
    "host":     "localhost",
    "port":     5433,
    "database": "dw_cartao",
    "user":     "postgres",
    "password": "postgres", 
}
CSV_DIR = os.path.join(os.path.dirname(__file__), "bases")

# CONEXÃO COM O BANCO
def get_engine():
    """Cria e retorna a engine SQLAlchemy para o PostgreSQL."""
    url = (
        f"postgresql+psycopg2://{DB_CONFIG['user']}:{DB_CONFIG['password']}"
        f"@{DB_CONFIG['host']}:{DB_CONFIG['port']}/{DB_CONFIG['database']}"
    )
    engine = create_engine(url, echo=False)
    print("Conexão com o banco estabelecida.")
    return engine


# DDL — CRIAÇÃO DO BANCO (Star Schema)
DDL = """
-- ── Dimensão Data ──────────────────────────────────────────────
CREATE TABLE IF NOT EXISTS dim_data (
    id_data     SERIAL PRIMARY KEY,
    data        DATE        NOT NULL UNIQUE,
    dia         INTEGER     NOT NULL,
    mes         INTEGER     NOT NULL,
    trimestre   INTEGER     NOT NULL,
    ano         INTEGER     NOT NULL,
    dia_semana  VARCHAR(20) NOT NULL,   -- ex.: "Segunda-feira"
    nome_mes    VARCHAR(20) NOT NULL    -- ex.: "Janeiro"
);

-- ── Dimensão Titular ────────────────────────────────────────────
CREATE TABLE IF NOT EXISTS dim_titular (
    id_titular    SERIAL PRIMARY KEY,
    nome_titular  VARCHAR(100) NOT NULL,
    final_cartao  VARCHAR(10)  NOT NULL,
    UNIQUE (nome_titular, final_cartao)
);

-- ── Dimensão Categoria ──────────────────────────────────────────
CREATE TABLE IF NOT EXISTS dim_categoria (
    id_categoria    SERIAL PRIMARY KEY,
    nome_categoria  VARCHAR(100) NOT NULL UNIQUE
);

-- ── Dimensão Estabelecimento ────────────────────────────────────
CREATE TABLE IF NOT EXISTS dim_estabelecimento (
    id_estabelecimento  SERIAL PRIMARY KEY,
    nome_estabelecimento VARCHAR(200) NOT NULL UNIQUE
);

-- ── Fato Transação ──────────────────────────────────────────────
CREATE TABLE IF NOT EXISTS fato_transacao (
    id_transacao        SERIAL PRIMARY KEY,
    id_data             INTEGER REFERENCES dim_data(id_data),
    id_titular          INTEGER REFERENCES dim_titular(id_titular),
    id_categoria        INTEGER REFERENCES dim_categoria(id_categoria),
    id_estabelecimento  INTEGER REFERENCES dim_estabelecimento(id_estabelecimento),
    valor_brl           NUMERIC(15, 2),
    valor_usd           NUMERIC(15, 2),
    cotacao             NUMERIC(10, 4),
    parcela_texto       VARCHAR(20),
    num_parcela         INTEGER,
    total_parcelas      INTEGER,
    arquivo_origem      VARCHAR(50)    -- rastreabilidade: qual CSV originou
);
"""

def criar_schema(engine):
    """Executa o DDL para criar as tabelas (se ainda não existirem)."""
    with engine.begin() as conn:
        conn.execute(text(DDL))
    print("Schema criado / verificado com sucesso.")


# EXTRACT — Leitura dos CSVs
def extrair_csvs(csv_dir: str) -> pd.DataFrame:
    """
    Lê todos os arquivos Fatura_*.csv do diretório informado,
    concatena em um único DataFrame e adiciona a coluna 'arquivo_origem'.
    """
    arquivos = sorted(glob.glob(os.path.join(csv_dir, "Fatura_*.csv")))
    if not arquivos:
        raise FileNotFoundError(f"Nenhum arquivo CSV encontrado em: {csv_dir}")

    frames = []
    for arq in arquivos:
        nome = os.path.basename(arq)
        print(f"Lendo: {nome}")
        try:
            df = pd.read_csv(
                arq,
                sep=";",
                encoding="utf-8",
                dtype=str,          # lê tudo como texto; tipagem feita no Transform
                skip_blank_lines=True,
            )
        except UnicodeDecodeError:
            df = pd.read_csv(arq, sep=";", encoding="latin-1", dtype=str)
            print(f"{nome}: fallback para latin-1.")

        df["arquivo_origem"] = nome
        frames.append(df)

    raw = pd.concat(frames, ignore_index=True)
    print(f"Total de linhas extraídas (raw): {len(raw):,}")
    return raw


# TRANSFORM — Limpeza e Padronização

# Mapeamento flexível de nomes de coluna (case-insensitive)
COLUNAS_MAP = {
    "data de compra":        "data_compra",
    "nome no cartão":        "nome_titular",
    "nome no cartao":        "nome_titular",
    "final do cartão":       "final_cartao",
    "final do cartao":       "final_cartao",
    "categoria":             "categoria",
    "descrição":             "descricao",
    "descricao":             "descricao",
    "parcela":               "parcela",
    "valor (em us$)":        "valor_usd",
    "valor (em us|":         "valor_usd",    # variação de encoding
    "valor em us$":          "valor_usd",
    "cotação (em r$)":       "cotacao",
    "cotacao (em r$)":       "cotacao",
    "cotação":               "cotacao",
    "cotacao":               "cotacao",
    "valor (em r$)":         "valor_brl",
    "valor em r$":           "valor_brl",
    "valor (em r|":          "valor_brl",    # variação de encoding
}

DIAS_SEMANA_PT = {
    0: "Segunda-feira",
    1: "Terça-feira",
    2: "Quarta-feira",
    3: "Quinta-feira",
    4: "Sexta-feira",
    5: "Sábado",
    6: "Domingo",
}

MESES_PT = {
    1: "Janeiro", 2: "Fevereiro", 3: "Março", 4: "Abril",
    5: "Maio", 6: "Junho", 7: "Julho", 8: "Agosto",
    9: "Setembro", 10: "Outubro", 11: "Novembro", 12: "Dezembro",
}


def normalizar_colunas(df: pd.DataFrame) -> pd.DataFrame:
    """Renomeia as colunas para nomes padronizados."""
    df.columns = [c.strip().lower() for c in df.columns]
    renomear = {col: COLUNAS_MAP[col] for col in df.columns if col in COLUNAS_MAP}
    df = df.rename(columns=renomear)
    return df


def converter_numero(serie: pd.Series) -> pd.Series:
    """Converte string com vírgula decimal para float (ex.: '1.234,56' → 1234.56)."""
    return (
        serie
        .str.strip()
        .str.replace(r"\.", "", regex=True)   # remove separador de milhar
        .str.replace(",", ".", regex=False)    # vírgula → ponto
        .pipe(pd.to_numeric, errors="coerce")
    )


def parsear_parcela(serie: pd.Series):
    """
    Extrai num_parcela e total_parcelas da coluna parcela.
    'Única' ou '1/1' → (1, 1)
    '3/10'           → (3, 10)
    """
    num_list, total_list = [], []
    for val in serie:
        val = str(val).strip() if pd.notna(val) else ""
        if val.lower() in ("única", "unica", "1", ""):
            num_list.append(1)
            total_list.append(1)
        else:
            match = re.match(r"(\d+)\s*/\s*(\d+)", val)
            if match:
                num_list.append(int(match.group(1)))
                total_list.append(int(match.group(2)))
            else:
                num_list.append(None)
                total_list.append(None)
    return num_list, total_list


def transformar(raw: pd.DataFrame) -> pd.DataFrame:
    """
    Aplica todas as transformações necessárias ao DataFrame bruto.
    Retorna um DataFrame limpo e pronto para carga.
    """
    df = raw.copy()

    # 1. Normalizar nomes de colunas
    df = normalizar_colunas(df)
    print(f"Colunas após normalização: {list(df.columns)}")

    # 2. Remover linhas completamente vazias
    df = df.dropna(how="all")

    # 3. Tratar coluna data_compra
    df["data_compra"] = pd.to_datetime(
        df["data_compra"].str.strip(),
        format="%d/%m/%Y",
        errors="coerce",
    )
    linhas_sem_data = df["data_compra"].isna().sum()
    if linhas_sem_data:
        print(f"{linhas_sem_data} linhas sem data válida — serão descartadas.")
    df = df.dropna(subset=["data_compra"])

    # 4. Tratar campos de texto
    for col in ["nome_titular", "final_cartao", "categoria", "descricao", "parcela"]:
        if col in df.columns:
            df[col] = df[col].fillna("").str.strip()

    # 5. Padronizar categoria vazia ou "-"
    df["categoria"] = df["categoria"].replace({"": "Não Categorizado", "-": "Não Categorizado"})

    # 6. Padronizar descrição vazia
    df["descricao"] = df["descricao"].replace({"": "Não Informado", "-": "Não Informado"})
    df["descricao"] = df["descricao"].str.upper().str.strip()

    # 7. Converter valores numéricos
    for col in ["valor_brl", "valor_usd", "cotacao"]:
        if col in df.columns:
            df[col] = converter_numero(df[col])
        else:
            df[col] = None

    # 8. Deduplicação de transações internacionais
    #    Regra: quando a mesma transação aparece duas vezes
    #    (uma com valor_usd preenchido e valor_brl=0, outra ao contrário),
    #    manter apenas a linha com valor_brl preenchido.
    mask_usd_sem_brl = (df["valor_usd"].notna()) & (df["valor_brl"].fillna(0) == 0)
    df = df[~mask_usd_sem_brl].copy()
    print(f"Linhas após deduplicação de USD: {len(df):,}")

    # 9. Parsear parcelas
    df["num_parcela"], df["total_parcelas"] = parsear_parcela(df.get("parcela", pd.Series(dtype=str)))

    # 10. Derivações temporais (serão usadas na DIM_DATA)
    df["dia"]         = df["data_compra"].dt.day
    df["mes"]         = df["data_compra"].dt.month
    df["trimestre"]   = df["data_compra"].dt.quarter
    df["ano"]         = df["data_compra"].dt.year
    df["dia_semana"]  = df["data_compra"].dt.dayofweek.map(DIAS_SEMANA_PT)
    df["nome_mes"]    = df["mes"].map(MESES_PT)

    print(f"Total de linhas após transformação: {len(df):,}")
    return df


# LOAD — Carga no Data Warehouse
def upsert_dim_data(conn, df: pd.DataFrame) -> dict:
    """Insere datas únicas e retorna mapa {date: id_data}."""
    datas = df[["data_compra","dia","mes","trimestre","ano","dia_semana","nome_mes"]].drop_duplicates("data_compra")
    mapa = {}
    for _, row in datas.iterrows():
        res = conn.execute(text("""
            INSERT INTO dim_data (data, dia, mes, trimestre, ano, dia_semana, nome_mes)
            VALUES (:data, :dia, :mes, :trimestre, :ano, :dia_semana, :nome_mes)
            ON CONFLICT (data) DO UPDATE SET data = EXCLUDED.data
            RETURNING id_data
        """), {
            "data":       row["data_compra"].date(),
            "dia":        int(row["dia"]),
            "mes":        int(row["mes"]),
            "trimestre":  int(row["trimestre"]),
            "ano":        int(row["ano"]),
            "dia_semana": row["dia_semana"],
            "nome_mes":   row["nome_mes"],
        })
        mapa[row["data_compra"].date()] = res.fetchone()[0]
    print(f"DIM_DATA: {len(mapa)} datas carregadas.")
    return mapa


def upsert_dim_titular(conn, df: pd.DataFrame) -> dict:
    """Insere titulares únicos e retorna mapa {(nome, final): id_titular}."""
    titulares = df[["nome_titular","final_cartao"]].drop_duplicates()
    mapa = {}
    for _, row in titulares.iterrows():
        res = conn.execute(text("""
            INSERT INTO dim_titular (nome_titular, final_cartao)
            VALUES (:nome, :final)
            ON CONFLICT (nome_titular, final_cartao) DO UPDATE SET nome_titular = EXCLUDED.nome_titular
            RETURNING id_titular
        """), {"nome": row["nome_titular"], "final": str(row["final_cartao"])})
        mapa[(row["nome_titular"], str(row["final_cartao"]))] = res.fetchone()[0]
    print(f"DIM_TITULAR: {len(mapa)} titulares carregados.")
    return mapa


def upsert_dim_categoria(conn, df: pd.DataFrame) -> dict:
    """Insere categorias únicas e retorna mapa {nome: id_categoria}."""
    cats = df["categoria"].dropna().unique()
    mapa = {}
    for cat in cats:
        res = conn.execute(text("""
            INSERT INTO dim_categoria (nome_categoria)
            VALUES (:cat)
            ON CONFLICT (nome_categoria) DO UPDATE SET nome_categoria = EXCLUDED.nome_categoria
            RETURNING id_categoria
        """), {"cat": cat})
        mapa[cat] = res.fetchone()[0]
    print(f"DIM_CATEGORIA: {len(mapa)} categorias carregadas.")
    return mapa


def upsert_dim_estabelecimento(conn, df: pd.DataFrame) -> dict:
    """Insere estabelecimentos únicos e retorna mapa {nome: id_estabelecimento}."""
    estabs = df["descricao"].dropna().unique()
    mapa = {}
    for estab in estabs:
        res = conn.execute(text("""
            INSERT INTO dim_estabelecimento (nome_estabelecimento)
            VALUES (:estab)
            ON CONFLICT (nome_estabelecimento) DO UPDATE SET nome_estabelecimento = EXCLUDED.nome_estabelecimento
            RETURNING id_estabelecimento
        """), {"estab": estab})
        mapa[estab] = res.fetchone()[0]
    print(f"DIM_ESTABELECIMENTO: {len(mapa)} estabelecimentos carregados.")
    return mapa


def carregar_fato(conn, df: pd.DataFrame, map_data, map_titular, map_cat, map_estab):
    """Insere todos os registros na tabela fato_transacao."""
    registros = []
    for _, row in df.iterrows():
        registros.append({
            "id_data":            map_data.get(row["data_compra"].date()),
            "id_titular":         map_titular.get((row["nome_titular"], str(row["final_cartao"]))),
            "id_categoria":       map_cat.get(row["categoria"]),
            "id_estabelecimento": map_estab.get(row["descricao"]),
            "valor_brl":          row["valor_brl"] if pd.notna(row["valor_brl"]) else None,
            "valor_usd":          row["valor_usd"] if pd.notna(row.get("valor_usd", None)) else None,
            "cotacao":            row["cotacao"]   if pd.notna(row.get("cotacao", None)) else None,
            "parcela_texto":      row.get("parcela", None),
            "num_parcela":        row["num_parcela"],
            "total_parcelas":     row["total_parcelas"],
            "arquivo_origem":     row.get("arquivo_origem", None),
        })

    conn.execute(text("""
        INSERT INTO fato_transacao
            (id_data, id_titular, id_categoria, id_estabelecimento,
             valor_brl, valor_usd, cotacao,
             parcela_texto, num_parcela, total_parcelas, arquivo_origem)
        VALUES
            (:id_data, :id_titular, :id_categoria, :id_estabelecimento,
             :valor_brl, :valor_usd, :cotacao,
             :parcela_texto, :num_parcela, :total_parcelas, :arquivo_origem)
    """), registros)

    print(f"FATO_TRANSACAO: {len(registros):,} registros inseridos.")


def carregar(engine, df: pd.DataFrame):
    """Orquestra a carga de todas as dimensões e da tabela fato."""
    with engine.begin() as conn:
        map_data    = upsert_dim_data(conn, df)
        map_titular = upsert_dim_titular(conn, df)
        map_cat     = upsert_dim_categoria(conn, df)
        map_estab   = upsert_dim_estabelecimento(conn, df)
        carregar_fato(conn, df, map_data, map_titular, map_cat, map_estab)
    print("Carga concluída com sucesso!")


# VALIDAÇÃO PÓS-CARGA
def validar(engine):
    """Exibe contagens e totais básicos para verificação da carga."""
    queries = {
        "Total de transações":       "SELECT COUNT(*) FROM fato_transacao",
        "Total de datas":            "SELECT COUNT(*) FROM dim_data",
        "Total de titulares":        "SELECT COUNT(*) FROM dim_titular",
        "Total de categorias":       "SELECT COUNT(*) FROM dim_categoria",
        "Total de estabelecimentos": "SELECT COUNT(*) FROM dim_estabelecimento",
        "Soma valor_brl (R$)":       "SELECT ROUND(SUM(valor_brl)::numeric, 2) FROM fato_transacao WHERE valor_brl > 0",
        "Total de estornos (R$)":    "SELECT ROUND(SUM(valor_brl)::numeric, 2) FROM fato_transacao WHERE valor_brl < 0",
    }
    print("─── Validação pós-carga ───────────────────────────")
    with engine.connect() as conn:
        for label, sql in queries.items():
            resultado = conn.execute(text(sql)).scalar()
            print(f"  {label}: {resultado}")
    print("───────────────────────────────────────────────────")


# MAIN
def main():
    print("Iniciando ETL")

    engine = get_engine()

    # 1. Criar schema (idempotente)
    criar_schema(engine)

    # 2. Extract
    raw = extrair_csvs(CSV_DIR)

    # 3. Transform
    df_limpo = transformar(raw)

    # 4. Load
    carregar(engine, df_limpo)

    # 5. Validação
    # validar(engine)

    print(f"ETL finalizado.")


if __name__ == "__main__":
    main()