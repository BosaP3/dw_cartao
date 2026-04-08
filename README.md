# DW Transações de Cartão de Crédito — BI Project

## Estrutura do Projeto

```
.
├── etl_pipeline.py        # Pipeline ETL (Extract → Transform → Load)
├── analytics_queries.py   # Consultas analíticas e validações de qualidade
├── dashboard.py           # Dashboard interativo (Streamlit + Plotly)
├── bases/                 # CSVs das faturas (Fatura_AAAA-MM-DD.csv)
├── resultados_analiticos/ # CSVs exportados pelo analytics_queries.py (gerado)
└── etl_pipeline.log       # Log do ETL (gerado)
```

## Instalação

```bash
pip install pandas psycopg2-binary sqlalchemy streamlit plotly tabulate
```

## Variáveis de Ambiente (opcional)

```bash
export DB_HOST=localhost
export DB_PORT=5433
export DB_NAME=dw_cartao
export DB_USER=postgres
export DB_PASSWORD=postgres
```

---

## 1. ETL (`etl_pipeline.py`)

### Executar carga completa
```bash
python etl_pipeline.py
```

### Re-carga total (limpa o DW antes)
```bash
python etl_pipeline.py --full-reload
```

### Carga incremental (só novos arquivos)
```bash
python etl_pipeline.py --incremental
```

### CSVs em outro diretório
```bash
python etl_pipeline.py --csv-dir /caminho/para/faturas
```

---

## 2. Consultas e Validações (`analytics_queries.py`)

### Todas as análises + validações
```bash
python analytics_queries.py
```

### Só uma query específica
```bash
python analytics_queries.py --query top_categorias
python analytics_queries.py --query evolucao_mensal
python analytics_queries.py --query kpis
```

### Queries disponíveis
| Nome                  | Descrição                                      |
|-----------------------|------------------------------------------------|
| `kpis`                | KPIs gerais do período                         |
| `gasto_titular`       | Gasto total por titular                        |
| `gasto_titular_mensal`| Gasto por titular e mês                        |
| `top_categorias`      | Top 10 categorias por valor                    |
| `categorias_por_titular` | Gasto por categoria e titular               |
| `evolucao_mensal`     | Série temporal mensal                          |
| `comparativo_titulares` | Comparativo de métricas entre titulares      |
| `top_estabelecimentos`| Top 15 estabelecimentos por valor              |
| `parcelamento`        | À vista vs parcelado                           |
| `dia_semana`          | Volume por dia da semana                       |
| `estornos`            | Estornos e créditos                            |
| `internacionais`      | Transações em USD                              |

### Exportar resultados para CSV
```bash
python analytics_queries.py --exportar
```

### Só validações de qualidade
```bash
python analytics_queries.py --validar-apenas
```

---

## 3. Dashboard (`dashboard.py`)

```bash
streamlit run dashboard.py
```

Abre em `http://localhost:8501` com:
- Filtros: período (ano), titular(es), categoria(s)
- KPIs gerais (8 indicadores)
- Evolução mensal com gráfico de barras + ticket médio
- Top 10 categorias (barra + pizza)
- Gasto por titular
- Transações por dia da semana
- Comportamento de parcelamento
- Top N estabelecimentos (configurável no sidebar)
- Tabelas detalhadas por titular e categoria
