# ETL — Data Warehouse de Transações de Cartão de Crédito

## Pré-requisitos

- Python 3.9+
- PostgreSQL instalado e rodando
- Banco de dados `dw_cartao` criado no PostgreSQL
- Docker

## 1. Criar o banco de dados no PostgreSQL

```sql
-- Execute o ambiente
docker-compose up -d
```

## 2. Instalar dependências Python

```bash
pip install pandas psycopg2-binary sqlalchemy
```

## 3. Estrutura de pastas esperada

```
PROJETO_BI/
├── bases/
│   ├── Fatura_2025-03-20.csv
│   ├── Fatura_2025-04-20.csv
│   └── ... (demais arquivos)
├── etl_pipeline.py
└── README.md
```

## 4. Configurar as credenciais

Abra o arquivo `etl_pipeline.py` e edite o bloco `DB_CONFIG`:

```python
DB_CONFIG = {
    "host":     "localhost",
    "port":     5432,
    "database": "dw_cartao",
    "user":     "postgres",
    "password": "SUA_SENHA_AQUI",  # <-- altere aqui
}
```

## 5. Executar o ETL

```bash
python etl_pipeline.py
```

## O que o script faz

| Etapa | Descrição |
|-------|-----------|
| **Extract** | Lê todos os `Fatura_*.csv` da pasta `bases/` com separador `;` e encoding UTF-8 |
| **Transform** | Normaliza colunas, converte datas e valores, limpa campos nulos/traço, deduplica transações internacionais, extrai parcelas |
| **Load** | Cria o schema Star Schema no PostgreSQL e insere dimensões + fato |
| **Validação** | Exibe contagens e totais no console para conferência |

## Modelo dimensional (Star Schema)

```
DIM_TITULAR ──────────────────────────────┐
DIM_DATA ──────────────── FATO_TRANSACAO──┤
DIM_CATEGORIA ────────────────────────────┤
DIM_ESTABELECIMENTO ──────────────────────┘
```

## Log

O script gera um arquivo `etl.log` na mesma pasta com todo o histórico de execução.

## Regras de negócio aplicadas

- Linhas com data inválida são descartadas (com aviso no log)
- Categoria vazia ou `-` → `"Não Categorizado"`
- Descrição vazia ou `-` → `"Não Informado"`
- Parcela `"Única"` → `num_parcela=1`, `total_parcelas=1`
- Transações internacionais duplicadas (linha com `valor_brl=0`) são removidas
- O script é **idempotente** para o schema (usa `CREATE TABLE IF NOT EXISTS`)
- Para recarregar os dados do zero, truncate as tabelas antes de rodar novamente:

```sql
TRUNCATE fato_transacao, dim_data, dim_titular, dim_categoria, dim_estabelecimento RESTART IDENTITY CASCADE;
```
