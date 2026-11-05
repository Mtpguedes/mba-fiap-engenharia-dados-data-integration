# Databricks notebook source
# MAGIC %md
# MAGIC # Bronze — Carga dos Dados Abertos do CNPJ
# MAGIC
# MAGIC **Origem:** arquivos CSV da Receita Federal copiados para os Volumes `workspace.default.*`
# MAGIC **Destino:** `workspace.bronze.<tabela>`, uma tabela Delta por arquivo, particionada por `dt_referencia`
# MAGIC
# MAGIC **Princípio da bronze:** guardar o dado **como veio**. Todas as colunas de origem entram como `string`; nenhuma regra de negócio é aplicada aqui. Acrescentamos só três colunas de controle:
# MAGIC
# MAGIC | Coluna | Para quê |
# MAGIC |---|---|
# MAGIC | `dt_referencia` | Mês da publicação da Receita. Permite guardar vários meses e reprocessar um só |
# MAGIC | `_arquivo_origem` | Caminho do arquivo que gerou a linha (lineage) |
# MAGIC | `_dt_carga` | Quando a linha foi carregada |
# MAGIC
# MAGIC **Formato dos arquivos:** CSV sem cabeçalho, separador `;`, aspas `"`, encoding ISO-8859-1.
# MAGIC
# MAGIC **Quem roda:** o dono do catálogo (único com escrita na bronze). Primeira vez: rode antes o `00_setup` com a migração.

# COMMAND ----------

# MAGIC %run ./utils_pipeline

# COMMAND ----------

DT_REFERENCIA = obter_dt_referencia()
print(f"dt_referencia: {DT_REFERENCIA}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1. Catálogo de fontes
# MAGIC
# MAGIC Uma entrada por tabela: Volume de origem, padrão do nome do arquivo e colunas na ordem do layout da Receita.
# MAGIC
# MAGIC ⚠️ **Confirme os Volumes de `motivos` e `municipios`** com `SHOW VOLUMES` (célula seguinte). Os demais caminhos são os que já estavam em uso.

# COMMAND ----------

# MAGIC %sql
# MAGIC SHOW VOLUMES IN workspace.default;

# COMMAND ----------

VOLUME_BASE = "/Volumes/workspace/default"

fontes = {
    "empresas": {
        "volume": f"{VOLUME_BASE}/cad_receita_cnpj",
        "padrao": "*.EMPRECSV",
        "colunas": ["cnpj_basico", "razao_social", "cod_natureza_juridica", "cod_qualificacao_responsavel",
                    "capital_social", "porte_empresa", "ente_federativo_responsavel"],
    },
    "estabelecimentos": {
        "volume": f"{VOLUME_BASE}/cad_receita_estabelecimentos",
        "padrao": "*.ESTABELE*",
        "colunas": ["cnpj_basico", "cnpj_ordem", "cnpj_dv", "identificador_matriz_filial", "nome_fantasia",
                    "situacao_cadastral", "data_situacao_cadastral", "cod_motivo_situacao_cadastral",
                    "nome_cidade_exterior", "cod_pais", "data_inicio_atividade", "cnae_fiscal_principal",
                    "cnae_fiscal_secundaria", "tipo_logradouro", "logradouro", "numero", "complemento",
                    "bairro", "cep", "uf", "cod_municipio", "ddd_1", "telefone_1", "ddd_2", "telefone_2",
                    "ddd_fax", "fax", "correio_eletronico", "situacao_especial", "data_situacao_especial"],
        "multiline": True,  # há campos com quebra de linha dentro de aspas
    },
    "socios": {
        "volume": f"{VOLUME_BASE}/cad_receita_socios",
        "padrao": "*.SOCIOCSV",
        "colunas": ["cnpj_basico", "identificador_socio", "nome_socio", "cnpj_cpf_socio", "cod_qualificacao_socio",
                    "data_entrada_sociedade", "cod_pais", "representante_legal", "nome_representante",
                    "cod_qualificacao_representante", "faixa_etaria"],
    },
    "simples": {
        "volume": f"{VOLUME_BASE}/cad_receita_simples",
        "padrao": "*.SIMPLES.CSV.*",
        "colunas": ["cnpj_basico", "opcao_simples", "data_opcao_simples", "data_exclusao_simples",
                    "opcao_mei", "data_opcao_mei", "data_exclusao_mei"],
    },
    "cnaes": {
        "volume": f"{VOLUME_BASE}/cad_receita_cnaes",
        "padrao": "*.CNAECSV",
        "colunas": ["cod_cnae", "descricao_cnae"],
    },
    "naturezas_juridicas": {
        "volume": f"{VOLUME_BASE}/cad_receita_nat_juridica",
        "padrao": "*.NATJUCSV",
        "colunas": ["cod_natureza_juridica", "descricao_natureza_juridica"],
    },
    "qualificacoes_socios": {
        "volume": f"{VOLUME_BASE}/cad_receita_quali_socios",
        "padrao": "*.QUALSCSV",
        "colunas": ["cod_qualificacao", "descricao_qualificacao"],
    },
    "paises": {
        "volume": f"{VOLUME_BASE}/paises",
        "padrao": "*.PAISCSV",
        "colunas": ["cod_pais", "descricao_pais"],
    },
    "motivos": {
        "volume": f"{VOLUME_BASE}/cad_receita_motivos",        # CONFIRMAR
        "padrao": "*.MOTICSV",
        "colunas": ["cod_motivo", "descricao_motivo"],
    },
    "municipios": {
        "volume": f"{VOLUME_BASE}/cad_receita_municipios",     # CONFIRMAR
        "padrao": "*.MUNICCSV",
        "colunas": ["cod_municipio", "descricao_municipio"],
    },
}

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2. Inventário dos arquivos
# MAGIC
# MAGIC Lista o que existe em cada Volume antes de ler. Se um Volume não existir ou não tiver arquivos com o padrão esperado, o notebook para aqui, antes de gravar qualquer coisa.

# COMMAND ----------

from fnmatch import fnmatch

inventario = []
problemas = []

for tabela, cfg in fontes.items():
    try:
        arquivos = [a for a in dbutils.fs.ls(cfg["volume"]) if fnmatch(a.name, cfg["padrao"])]
    except Exception as erro:
        problemas.append(f"{tabela}: Volume inacessível ({cfg['volume']}) — {type(erro).__name__}")
        continue
    if not arquivos:
        problemas.append(f"{tabela}: nenhum arquivo '{cfg['padrao']}' em {cfg['volume']}")
    for a in arquivos:
        inventario.append((tabela, a.name, a.size))

display(spark.createDataFrame(inventario, "tabela STRING, arquivo STRING, tamanho_bytes LONG").orderBy("tabela", "arquivo"))

if problemas:
    raise FileNotFoundError("Fontes com problema:\n" + "\n".join(problemas))

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3. Leitura e gravação
# MAGIC
# MAGIC `FAILFAST`: se alguma linha tiver número de colunas diferente do layout, a leitura falha em vez de descartar a linha em silêncio.

# COMMAND ----------

from pyspark.sql.types import StringType, StructField, StructType


def ler_fonte(cfg):
    schema = StructType([StructField(c, StringType(), True) for c in cfg["colunas"]])
    return (
        spark.read.format("csv")
        .schema(schema)
        .option("header", "false")
        .option("sep", ";")
        .option("encoding", "ISO-8859-1")
        .option("quote", '"')
        .option("escape", '"')
        .option("mode", "FAILFAST")
        .option("multiLine", cfg.get("multiline", False))
        .option("pathGlobFilter", cfg["padrao"])
        .load(cfg["volume"])
        .withColumn("_arquivo_origem", F.col("_metadata.file_path"))
        .withColumn("_dt_carga", F.current_timestamp())
        .withColumn("dt_referencia", F.lit(DT_REFERENCIA).cast("date"))
    )


for tabela, cfg in fontes.items():
    destino = f"workspace.bronze.{tabela}"
    with Cronometro("load_bronze_cnpj", tabela, DT_REFERENCIA):
        gravar_particao(ler_fonte(cfg), destino, DT_REFERENCIA)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 4. Quality checks da bronze
# MAGIC
# MAGIC Na bronze o check roda **depois** da gravação, lendo a tabela Delta (bem mais rápido que reler os CSVs). A bronze aceita o que a Receita publicou; se um check bloqueante falhar, o notebook para e **o Job não segue para a silver**, então o dado ruim não se propaga.

# COMMAND ----------

for tabela, cfg in fontes.items():
    df = ler_bronze(tabela, DT_REFERENCIA)
    chave = cfg["colunas"][0]
    with Cronometro("load_bronze_cnpj", f"checks_{tabela}", DT_REFERENCIA):
        executar_checks(
            df, f"workspace.bronze.{tabela}", DT_REFERENCIA,
            regras=[
                regra(f"{chave} preenchido", "bloqueia", F.coalesce(F.trim(F.col(chave)), F.lit("")) == ""),
            ],
            resultados_extras=[
                resultado("tabela não vazia", "bloqueia", 0 if df.limit(1).count() > 0 else 1, 1),
            ],
        )

# COMMAND ----------

# MAGIC %md
# MAGIC ## 5. Volumetria (item 2 do trabalho)
# MAGIC
# MAGIC Gera a tabela de volume da base para o README: registros, arquivos e tamanho em disco de origem.

# COMMAND ----------

import pandas as pd

tamanhos = pd.DataFrame(inventario, columns=["tabela", "arquivo", "tamanho_bytes"])
resumo_arquivos = tamanhos.groupby("tabela").agg(arquivos=("arquivo", "count"), bytes=("tamanho_bytes", "sum"))

linhas = []
for tabela in fontes:
    registros = ler_bronze(tabela, DT_REFERENCIA).count()
    linhas.append((tabela, registros, int(resumo_arquivos.loc[tabela, "arquivos"]),
                   round(resumo_arquivos.loc[tabela, "bytes"] / 1024 ** 3, 3)))

volumetria = pd.DataFrame(linhas, columns=["tabela", "registros", "arquivos", "tamanho_gb_csv"])
display(spark.createDataFrame(volumetria))

print(f"| Tabela | Registros | Arquivos | Tamanho CSV (GB) |  ← dt_referencia {DT_REFERENCIA}")
print("|---|---:|---:|---:|")
for _, l in volumetria.iterrows():
    print(f"| {l.tabela} | {l.registros:,} | {l.arquivos} | {l.tamanho_gb_csv:.3f} |".replace(",", "."))
print(f"| **Total** | **{volumetria.registros.sum():,}** | **{volumetria.arquivos.sum()}** | "
      f"**{volumetria.tamanho_gb_csv.sum():.2f}** |".replace(",", "."))
