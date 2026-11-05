# Databricks notebook source
# /// script
# [tool.databricks.environment]
# environment_version = "6"
# ///
# MAGIC %md
# MAGIC ## Problemática

# COMMAND ----------

# MAGIC %md
# MAGIC Através da base aberta da Receita Federal, iremos mapear oportunidades e riscos de saturação do mercado de TI nas diversas regiões brasileiras. Para isso, trabalharemos com 4 frentes:
# MAGIC
# MAGIC 1. Mapeamento Geográfico e Distribuição Regional;
# MAGIC 2. Demografia de Empresas e Tempo de Vida do Setor;
# MAGIC 3. Risco de Descontinuidade Operacional (Mortalidade e Saúde Cadastral);
# MAGIC 4. Risco Societário e de Governança.
# MAGIC
# MAGIC O dados obtidos foram extraídos da base da Receita no mês de Setembro/2026 (https://dados.gov.br/dados/conjuntos-dados/cadastro-nacional-da-pessoa-juridica---cnpj)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Imports

# COMMAND ----------

from pyspark.sql.functions import (
    col, lit, when, count, sum as spark_sum, avg,
    max as spark_max, min as spark_min, round as spark_round,
    current_timestamp, percentile_approx, stddev, countDistinct, regexp_replace
)
from datetime import datetime
from pyspark.sql.types import StructType, StructField, StringType

# COMMAND ----------

# MAGIC %md
# MAGIC ## Raw

# COMMAND ----------

# MAGIC %md
# MAGIC ### Caminhos

# COMMAND ----------


VOLUME_PATH_CNPJ = "/Volumes/workspace/default/cad_receita_cnpj/*.EMPRECSV"
VOLUME_PATH_SOCIOS = "/Volumes/workspace/default/cad_receita_socios/*.SOCIOCSV"
VOLUME_PATH_ESTABELECIMENTOS = "/Volumes/workspace/default/cad_receita_estabelecimentos/*.ESTABELE"
VOLUME_PATH_QUALIFICACAO_SOCIOS = "/Volumes/workspace/default/cad_receita_quali_socios/*.QUALSCSV"
VOLUME_PATH_CNAES = "/Volumes/workspace/default/cad_receita_cnaes/*.CNAECSV"
VOLUME_PATH_NAT_JURIDICA = "/Volumes/workspace/default/cad_receita_nat_juridica/*.NATJUCSV"
VOLUME_PATH_SIMPLES = "/Volumes/workspace/default/cad_receita_simples/*.SIMPLES.CSV.D60912"


# COMMAND ----------

# MAGIC %md
# MAGIC ### Schemas

# COMMAND ----------

## Base de empresas
schema_empresas = StructType([
    StructField("cnpj_basico", StringType(), True),
    StructField("razao_social", StringType(), True),
    StructField("cod_natureza_juridica", StringType(), True),
    StructField("cod_qualificacao_responsavel", StringType(), True),
    StructField("capital_social_str", StringType(), True),
    StructField("porte_empresa", StringType(), True),
    StructField("ente_federativo_responsavel", StringType(), True),
])


## Base de sócios
schema_socios = StructType([
    StructField("cnpj_basico", StringType(), True),
    StructField("identificador_socio", StringType(), True),
    StructField("nome_socio", StringType(), True),
    StructField("cnpj_cpf_socio", StringType(), True),
    StructField("qualificaca_socio", StringType(), True),
    StructField("data_entrada_sociedade", StringType(), True),
    StructField("nacionalidade_socio", StringType(), True),
    StructField("representante_legal", StringType(), True),
    StructField("nome_representante", StringType(), True),
    StructField("qualificacao_representante", StringType(), True),
    StructField("faixa_etaria", StringType(), True),
])


## Base de estabelecimentos
schema_estabelecimentos = StructType([
    StructField("cnpj_basico", StringType(), True),
    StructField("cnpj_ordem", StringType(), True),
    StructField("cnpj_dv", StringType(), True),
    StructField("iden_matriz_filial", StringType(), True),
    StructField("nome_fantasia", StringType(), True),
    StructField("situacao_cadastral", StringType(), True),
    StructField("motivo_situacao_cadastral", StringType(), True),
    StructField("nome_cidade_exterior", StringType(), True),
    StructField("pais", StringType(), True),
    StructField("data_inicio", StringType(), True),
    StructField("cnae_primario", StringType(), True),
    StructField("cnae_secundario", StringType(), True),
    StructField("tipo_logradouro", StringType(), True),
    StructField("logradouro", StringType(), True),
    StructField("numero", StringType(), True),
    StructField("complemento", StringType(), True),
    StructField("bairro", StringType(), True),
    StructField("cep", StringType(), True),
    StructField("uf", StringType(), True),
    StructField("municipio", StringType(), True),
    StructField("ddd1", StringType(), True),
    StructField("telefone1", StringType(), True),
    StructField("ddd2", StringType(), True),
    StructField("telefone2", StringType(), True),
    StructField("ddd_fax", StringType(), True),
    StructField("fax", StringType(), True),
    StructField("correio_eletronico", StringType(), True),
    StructField("situacao_especial", StringType(), True),
    StructField("data_situacao_especial", StringType(), True),
])

## Qualificação dos sócios
schema_quali_socios = StructType([
    StructField("cod_qualificacao_responsavel", StringType(), True),
    StructField("desc_qualificacao_responsavel", StringType(), True),
])

## Natureza Jurídica
schema_natureza_juridica = StructType([
    StructField("cod_natureza_juridica", StringType(), True),
    StructField("desc_natureza_juridica", StringType(), True),
])

## CNAEs
schema_cnae = StructType([
    StructField("cod_cnae", StringType(), True),
    StructField("desc_cnae", StringType(), True),
])

## Simples
schema_simples = StructType([
    StructField("cnpj_basico", StringType(), True),
    StructField("opcao_simples", StringType(), True),
    StructField("data_opcao_simples", StringType(), True),
    StructField("data_exclusao_simples", StringType(), True),
    StructField("opcao_mei", StringType(), True),
    StructField("data_opcao_mei", StringType(), True),
    StructField("data_exclusao_mei", StringType(), True),
])


# COMMAND ----------

# MAGIC %md
# MAGIC ### Carregamento

# COMMAND ----------

## Empresas
df_raw_cnpj = spark.read.csv(
    VOLUME_PATH_CNPJ,
    header=False, # arquivos da Receita não tem o cabeçalho
    sep=";",
    encoding="ISO-8859-1",
    quote='"',
    schema=schema_empresas
)

## Sócios
df_raw_socios = spark.read.csv(
    VOLUME_PATH_SOCIOS,
    header=False,
    sep=";",
    encoding="ISO-8859-1",
    quote='"',
    schema=schema_empresas
)

## Estabelecimentos
df_raw_estabelecimentos = spark.read.csv(
    VOLUME_PATH_ESTABELECIMENTOS,
    header=False,
    sep=";",
    encoding="ISO-8859-1",
    quote='"',
    schema=schema_estabelecimentos
)

## Qualificação sócios
df_raw_qual_socios = spark.read.csv(
    VOLUME_PATH_QUALIFICACAO_SOCIOS,
    header=False,
    sep=";",
    encoding="ISO-8859-1",
    quote='"',
    schema=schema_quali_socios
)

## Simples
df_raw_simples = spark.read.csv(
    VOLUME_PATH_SIMPLES,
    header=False,
    sep=";",
    encoding="ISO-8859-1",
    quote='"',
    schema=schema_simples
)

## CNAEs
df_raw_cnaes = spark.read.csv(
    VOLUME_PATH_CNAES,
    header=False,
    sep=";",
    encoding="ISO-8859-1",
    quote='"',
    schema=schema_cnae
)

df_raw_nat_juridica = spark.read.csv(
    VOLUME_PATH_NAT_JURIDICA,
    header=False,
    sep=";",
    encoding="ISO-8859-1",
    quote='"',
    schema=schema_natureza_juridica
)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Validação

# COMMAND ----------

total_raw_cnpj = df_raw_cnpj.count()
print(f"{'-'*50}")
print(f"Dataset EMPRESAS carregado com sucesso!")
print(f"{'-'*50}")
print(f"Registros: {total_raw_cnpj:,}")
print(f"Colunas:   {df_raw_cnpj.columns}")
print(f"Origem:    {VOLUME_PATH_CNPJ}")

total_raw_socios = df_raw_socios.count()
print(f"{'-'*50}")
print(f"Dataset SOCIOS carregado com sucesso!")
print(f"{'-'*50}")
print(f"Registros: {total_raw_socios:,}")
print(f"Colunas:   {df_raw_socios.columns}")
print(f"Origem:    {VOLUME_PATH_SOCIOS}")

total_raw_estabelecimentos = df_raw_estabelecimentos.count()
print(f"{'-'*50}")
print(f"Dataset ESTABELECIMENTOS carregado com sucesso!")
print(f"{'-'*50}")
print(f"Registros: {total_raw_estabelecimentos:,}")
print(f"Colunas:   {df_raw_estabelecimentos.columns}")
print(f"Origem:    {VOLUME_PATH_ESTABELECIMENTOS}")

# COMMAND ----------

display(df_raw_cnpj)

# COMMAND ----------

display(df_raw_qual_socios)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Silver