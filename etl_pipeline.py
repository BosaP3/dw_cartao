import os
import re
import glob
import argparse
from datetime import datetime

import pandas as pd
from sqlalchemy import create_engine, text


# Configuração
DB_CONFIG = {
    "host":     os.getenv("DB_HOST",     "localhost"),
    "port":     int(os.getenv("DB_PORT", "5434")),
    "database": os.getenv("DB_NAME",     "dw_cartao"),
    "user":     os.getenv("DB_USER",     "postgres"),
    "password": os.getenv("DB_PASSWORD", "postgres"),
}

CSV_DIR = os.path.join(os.path.dirname(__file__), "bases")

# Conexão
def get_engine():
    """Cria e retorna a engine SQLAlchemy para o PostgreSQL."""
    url = (
        f"postgresql+psycopg2://{DB_CONFIG['user']}:{DB_CONFIG['password']}"
        f"@{DB_CONFIG['host']}:{DB_CONFIG['port']}/{DB_CONFIG['database']}"
    )
    engine = create_engine(url, echo=False)
    print("Conexão com o banco estabelecida",
             DB_CONFIG["host"], DB_CONFIG["port"], DB_CONFIG["database"])
    return engine


# DDL
DDL = """

DROP TABLE IF EXISTS fato_transacao CASCADE;
DROP TABLE IF EXISTS dim_data CASCADE;
DROP TABLE IF EXISTS dim_titular CASCADE;
DROP TABLE IF EXISTS dim_categoria CASCADE;
DROP TABLE IF EXISTS dim_estabelecimento CASCADE;

-- Dimensão Data
CREATE TABLE IF NOT EXISTS dim_data (
    id_data     SERIAL PRIMARY KEY,
    data        DATE        NOT NULL UNIQUE,
    dia         INTEGER     NOT NULL,
    mes         INTEGER     NOT NULL,
    trimestre   INTEGER     NOT NULL,
    ano         INTEGER     NOT NULL,
    dia_semana  VARCHAR(20) NOT NULL,
    nome_mes    VARCHAR(20) NOT NULL
);

-- Dimensão Titular
CREATE TABLE IF NOT EXISTS dim_titular (
    id_titular    SERIAL PRIMARY KEY,
    nome_titular  VARCHAR(100) NOT NULL,
    final_cartao  VARCHAR(10)  NOT NULL,
    UNIQUE (nome_titular, final_cartao)
);

-- Dimensão Categoria
CREATE TABLE IF NOT EXISTS dim_categoria (
    id_categoria    SERIAL PRIMARY KEY,
    nome_categoria  VARCHAR(100) NOT NULL UNIQUE
);

-- Dimensão Estabelecimento
CREATE TABLE IF NOT EXISTS dim_estabelecimento (
    id_estabelecimento   SERIAL PRIMARY KEY,
    nome_estabelecimento VARCHAR(200) NOT NULL UNIQUE
);

-- Fato Transação
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
    arquivo_origem      VARCHAR(50)
);

-- Índices para performance analítica
CREATE INDEX IF NOT EXISTS idx_fato_data        ON fato_transacao(id_data);
CREATE INDEX IF NOT EXISTS idx_fato_titular     ON fato_transacao(id_titular);
CREATE INDEX IF NOT EXISTS idx_fato_categoria   ON fato_transacao(id_categoria);
CREATE INDEX IF NOT EXISTS idx_fato_estabelec   ON fato_transacao(id_estabelecimento);
CREATE INDEX IF NOT EXISTS idx_dim_data_ano_mes ON dim_data(ano, mes);
"""

def criar_schema(engine):
    """Executa o DDL para criar as tabelas (se ainda não existirem)."""
    with engine.begin() as conn:
        conn.execute(text(DDL))
    print("Schema criado / verificado com sucesso.")


# Mapeamentos
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
    "valor (em us|":         "valor_usd",
    "valor em us$":          "valor_usd",
    "cotação (em r$)":       "cotacao",
    "cotacao (em r$)":       "cotacao",
    "cotação":               "cotacao",
    "cotacao":               "cotacao",
    "valor (em r$)":         "valor_brl",
    "valor em r$":           "valor_brl",
    "valor (em r|":          "valor_brl",
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
    1: "Janeiro", 2: "Fevereiro", 3: "Março",    4: "Abril",
    5: "Maio",    6: "Junho",     7: "Julho",     8: "Agosto",
    9: "Setembro",10: "Outubro", 11: "Novembro", 12: "Dezembro",
}


# EXTRACT
def extrair_csvs(csv_dir: str, modo_incremental: bool = False, engine=None) -> pd.DataFrame:
    """
    Lê todos os arquivos Fatura_*.csv do diretório informado.
    Se modo_incremental=True, ignora arquivos já carregados no DW.
    """
    arquivos = sorted(glob.glob(os.path.join(csv_dir, "Fatura_*.csv")))
    if not arquivos:
        raise FileNotFoundError(f"Nenhum arquivo CSV encontrado em: {csv_dir}")

    # Filtro incremental: descarta arquivos já presentes no DW
    if modo_incremental and engine:
        with engine.connect() as conn:
            ja_carregados = {
                row[0]
                for row in conn.execute(
                    text("SELECT DISTINCT arquivo_origem FROM fato_transacao")
                )
            }
        arquivos = [a for a in arquivos if os.path.basename(a) not in ja_carregados]
        print("Modo incremental: %d arquivo(s) novo(s) para processar.", len(arquivos))
        if not arquivos:
            print("Nenhum arquivo novo. ETL encerrado.")
            return pd.DataFrame()

    frames = []
    for arq in arquivos:
        nome = os.path.basename(arq)
        print("Lendo: %s", nome)
        try:
            df = pd.read_csv(arq, sep=";", encoding="utf-8", dtype=str, skip_blank_lines=True)
        except UnicodeDecodeError:
            df = pd.read_csv(arq, sep=";", encoding="latin-1", dtype=str, skip_blank_lines=True)
            print("%s: fallback para latin-1.", nome)

        df["arquivo_origem"] = nome
        frames.append(df)

    raw = pd.concat(frames, ignore_index=True)
    print("Total de linhas extraídas (raw): %d", len(raw))
    return raw


# TRANSFORM
def normalizar_colunas(df: pd.DataFrame) -> pd.DataFrame:
    """Renomeia as colunas para nomes padronizados (case-insensitive)."""
    df.columns = [c.strip().lower() for c in df.columns]
    renomear = {col: COLUNAS_MAP[col] for col in df.columns if col in COLUNAS_MAP}
    df = df.rename(columns=renomear)
    return df


def converter_numero(serie: pd.Series) -> pd.Series:
    """
    Converte string numérica para float, suportando dois formatos:
      - Padrão BR com vírgula decimal: '1.234,56' → 1234.56
      - Padrão com ponto decimal:      '1234.56'  → 1234.56
    Detecta pelo último separador: se for vírgula, é BR; se for ponto, já é float.
    """
    def _parse(val):
        if pd.isna(val):
            return None
        s = re.sub(r"[^\d.,-]", "", str(val).strip())
        if not s:
            return None
        if s.rfind(",") > s.rfind("."):
            s = s.replace(".", "").replace(",", ".")
        else:
            s = s.replace(",", "")
        try:
            return float(s)
        except ValueError:
            return None
    return serie.apply(_parse)


def parsear_parcela(serie: pd.Series):
    """
    Extrai num_parcela e total_parcelas da coluna parcela.
    'Única' ou '1/1' → (1, 1)  |  '3/10' → (3, 10)  |  vazio → (None, None)
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

def filtrar_pagamentos(df: pd.DataFrame) -> pd.DataFrame:
    """ Remove registros referentes ao pagamento da fatura. """
    termo_filtro = "INCLUSAO DE PAGAMENTO"
    mask_pagamento = df["descricao"].str.contains(termo_filtro, case=False, na=False)
    removidas = mask_pagamento.sum()
    
    if removidas > 0:
        print("Linhas removidas (Inclusão de Pagamento): %d", removidas)
    
    return df[~mask_pagamento].copy()

def transformar(raw: pd.DataFrame) -> pd.DataFrame:
    """
    Aplica todas as transformações ao DataFrame bruto.
    Retorna um DataFrame limpo e pronto para carga.
    """
    df = raw.copy()

    # 1. Normalizar nomes de colunas
    df = normalizar_colunas(df)
    print("Colunas após normalização: %s", list(df.columns))

    # 2. Remover linhas completamente vazias
    before = len(df)
    df = df.dropna(how="all")
    print("Linhas removidas (completamente vazias): %d", before - len(df))

    # 3. Tratar coluna data_compra
    df["data_compra"] = pd.to_datetime(
        df["data_compra"].str.strip(), format="%d/%m/%Y", errors="coerce"
    )
    sem_data = df["data_compra"].isna().sum()
    if sem_data:
        print("%d linhas sem data válida serão descartadas.", sem_data)
    df = df.dropna(subset=["data_compra"])

    # 4. Tratar campos de texto
    for col in ["nome_titular", "final_cartao", "categoria", "descricao", "parcela"]:
        if col in df.columns:
            df[col] = df[col].fillna("").str.strip()

    # 5. Padronizar categoria vazia ou "-"
    df["categoria"] = df["categoria"].replace(
        {"": "Não Categorizado", "-": "Não Categorizado"}
    )

    # 6. Padronizar descrição vazia
    df["descricao"] = df["descricao"].replace(
        {"": "Não Informado", "-": "Não Informado"}
    )
    df["descricao"] = df["descricao"].str.upper().str.strip()

    # 
    df = filtrar_pagamentos(df)

    # 7. Converter valores numéricos
    for col in ["valor_brl", "valor_usd", "cotacao"]:
        if col in df.columns:
            df[col] = converter_numero(df[col])
        else:
            df[col] = None

    # 8. Deduplicação de transações internacionais
    grupos_com_usd = (
        df[df["valor_usd"].fillna(0) > 0]
        .groupby(["data_compra", "descricao"])
        .ngroups
    )
    mask_taxa_iof = (
        df["valor_usd"].fillna(0) == 0
    ) & df.duplicated(subset=["data_compra", "descricao"], keep=False) & (
        df.groupby(["data_compra", "descricao"])["valor_usd"]
        .transform(lambda g: g.fillna(0).gt(0).any())
    )
    removidas_usd = mask_taxa_iof.sum()
    df = df[~mask_taxa_iof].copy()
    print("Linhas removidas por deduplicação USD (taxa/IOF): %d", removidas_usd)
    print("Linhas após deduplicação: %d", len(df))

    # 9. Parsear parcelas
    df["num_parcela"], df["total_parcelas"] = parsear_parcela(
        df.get("parcela", pd.Series(dtype=str))
    )

    # 10. Derivações temporais para DIM_DATA
    df["dia"]        = df["data_compra"].dt.day
    df["mes"]        = df["data_compra"].dt.month
    df["trimestre"]  = df["data_compra"].dt.quarter
    df["ano"]        = df["data_compra"].dt.year
    df["dia_semana"] = df["data_compra"].dt.dayofweek.map(DIAS_SEMANA_PT)
    df["nome_mes"]   = df["mes"].map(MESES_PT)

    print("Total de linhas após transformação: %d", len(df))
    return df


# LOAD
def upsert_dim_data(conn, df: pd.DataFrame) -> dict:
    datas = df[["data_compra","dia","mes","trimestre","ano","dia_semana","nome_mes"]]\
               .drop_duplicates("data_compra")
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
    print("DIM_DATA: %d datas carregadas.", len(mapa))
    return mapa


def upsert_dim_titular(conn, df: pd.DataFrame) -> dict:
    titulares = df[["nome_titular","final_cartao"]].drop_duplicates()
    mapa = {}
    for _, row in titulares.iterrows():
        res = conn.execute(text("""
            INSERT INTO dim_titular (nome_titular, final_cartao)
            VALUES (:nome, :final)
            ON CONFLICT (nome_titular, final_cartao)
            DO UPDATE SET nome_titular = EXCLUDED.nome_titular
            RETURNING id_titular
        """), {"nome": row["nome_titular"], "final": str(row["final_cartao"])})
        mapa[(row["nome_titular"], str(row["final_cartao"]))] = res.fetchone()[0]
    print("DIM_TITULAR: %d titulares carregados.", len(mapa))
    return mapa


def upsert_dim_categoria(conn, df: pd.DataFrame) -> dict:
    cats = df["categoria"].dropna().unique()
    mapa = {}
    for cat in cats:
        res = conn.execute(text("""
            INSERT INTO dim_categoria (nome_categoria)
            VALUES (:cat)
            ON CONFLICT (nome_categoria)
            DO UPDATE SET nome_categoria = EXCLUDED.nome_categoria
            RETURNING id_categoria
        """), {"cat": cat})
        mapa[cat] = res.fetchone()[0]
    print("DIM_CATEGORIA: %d categorias carregadas.", len(mapa))
    return mapa


def upsert_dim_estabelecimento(conn, df: pd.DataFrame) -> dict:
    estabs = df["descricao"].dropna().unique()
    mapa = {}
    for estab in estabs:
        res = conn.execute(text("""
            INSERT INTO dim_estabelecimento (nome_estabelecimento)
            VALUES (:estab)
            ON CONFLICT (nome_estabelecimento)
            DO UPDATE SET nome_estabelecimento = EXCLUDED.nome_estabelecimento
            RETURNING id_estabelecimento
        """), {"estab": estab})
        mapa[estab] = res.fetchone()[0]
    print("DIM_ESTABELECIMENTO: %d estabelecimentos carregados.", len(mapa))
    return mapa


def carregar_fato(conn, df: pd.DataFrame, map_data, map_titular, map_cat, map_estab):
    """Insere todos os registros na tabela fato_transacao via bulk insert."""
    registros = []
    for _, row in df.iterrows():
        registros.append({
            "id_data":            map_data.get(row["data_compra"].date()),
            "id_titular":         map_titular.get((row["nome_titular"], str(row["final_cartao"]))),
            "id_categoria":       map_cat.get(row["categoria"]),
            "id_estabelecimento": map_estab.get(row["descricao"]),
            "valor_brl":          row["valor_brl"]  if pd.notna(row["valor_brl"])  else None,
            "valor_usd":          row["valor_usd"]  if pd.notna(row.get("valor_usd",  pd.NA)) else None,
            "cotacao":            row["cotacao"]    if pd.notna(row.get("cotacao",    pd.NA)) else None,
            "parcela_texto":      row.get("parcela", None),
            "num_parcela":        row["num_parcela"],
            "total_parcelas":     row["total_parcelas"],
            "arquivo_origem":     row.get("arquivo_origem", None),
        })

    if registros:
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

    print("FATO_TRANSACAO: %d registros inseridos.", len(registros))


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
        "Soma valor_brl (R$)":       "SELECT ROUND(SUM(valor_brl)::numeric,2) FROM fato_transacao WHERE valor_brl > 0",
        "Total de estornos (R$)":    "SELECT ROUND(SUM(valor_brl)::numeric,2) FROM fato_transacao WHERE valor_brl < 0",
        "Período (min data)":        "SELECT MIN(data) FROM dim_data",
        "Período (max data)":        "SELECT MAX(data) FROM dim_data",
        "Transações sem id_data":    "SELECT COUNT(*) FROM fato_transacao WHERE id_data IS NULL",
        "Transações sem id_titular": "SELECT COUNT(*) FROM fato_transacao WHERE id_titular IS NULL",
    }
    print("Validação pós-carga")
    with engine.connect() as conn:
        for label, sql in queries.items():
            resultado = conn.execute(text(sql)).scalar()
            print("  %-35s %s", label + ":", resultado)
    print()


# LIMPEZA TOTAL (para re-execução full)
def limpar_dw(engine):
    """Trunca todas as tabelas do DW em ordem correta (respeita FKs)."""
    print("Limpando todas as tabelas do DW para recarga completa...")
    with engine.begin() as conn:
        conn.execute(text("TRUNCATE TABLE fato_transacao RESTART IDENTITY CASCADE"))
        conn.execute(text("TRUNCATE TABLE dim_data RESTART IDENTITY CASCADE"))
        conn.execute(text("TRUNCATE TABLE dim_titular RESTART IDENTITY CASCADE"))
        conn.execute(text("TRUNCATE TABLE dim_categoria RESTART IDENTITY CASCADE"))
        conn.execute(text("TRUNCATE TABLE dim_estabelecimento RESTART IDENTITY CASCADE"))
    print("DW limpo com sucesso.")


# MAIN
def parse_args():
    parser = argparse.ArgumentParser(description="ETL — DW Transações de Cartão de Crédito")
    parser.add_argument("--csv-dir",      default=CSV_DIR,  help="Diretório com os CSVs de fatura")
    parser.add_argument("--incremental",  action="store_true", help="Modo incremental: pula arquivos já carregados")
    parser.add_argument("--full-reload",  action="store_true", help="Limpa o DW antes de recarregar tudo")
    parser.add_argument("--skip-validate",action="store_true", help="Pula a validação pós-carga")
    return parser.parse_args()


def main():
    args = parse_args()

    print("---")
    print("Iniciando ETL")
    print("---")

    # Conexão
    engine = get_engine()

    # 1. Criar schema (idempotente)
    criar_schema(engine)

    # # 2. Limpeza total (opcional)
    # if args.full_reload:
    #     limpar_dw(engine)

    # 3. Extract
    raw = extrair_csvs(args.csv_dir, modo_incremental=args.incremental, engine=engine)
    if raw.empty:
        print("Nenhum dado novo para processar. ETL finalizado.")
        return

    # 4. Transform
    df_limpo = transformar(raw)

    # 5. Load
    carregar(engine, df_limpo)

    # 6. Validação
    if not args.skip_validate:
        validar(engine)


    print("ETL finalizado")
    print("---")


if __name__ == "__main__":
    main()