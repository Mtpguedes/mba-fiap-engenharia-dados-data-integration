# Databricks notebook source
# /// script
# [tool.databricks.environment]
# environment_version = "6"
# ///
# MAGIC %md
# MAGIC # Carga Bronze — Dados Abertos do CNPJ
# MAGIC
# MAGIC Leitura dos arquivos CSV dos Volumes e gravação de tabelas Delta
# MAGIC no catálogo cad_receita, schema bronze.
# MAGIC
# MAGIC Os campos de origem serão preservados como string.

# COMMAND ----------

# MAGIC %sql
# MAGIC SHOW VOLUMES IN workspace.default;

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE SCHEMA IF NOT EXISTS workspace.bronze;
# MAGIC CREATE SCHEMA IF NOT EXISTS workspace.silver;
# MAGIC CREATE SCHEMA IF NOT EXISTS workspace.gold;

# COMMAND ----------

usuarios = [
    "helentalarico@icloud.com",
    "mtpguedes@gmail.com",
]

for usuario in usuarios:
    principal = f"`{usuario.replace('`', '``')}`"

    spark.sql(
        f"GRANT USE CATALOG ON CATALOG workspace TO {principal}"
    )

    for camada in ["bronze", "silver", "gold"]:
        spark.sql(f"""
            GRANT USE SCHEMA
            ON SCHEMA workspace.{camada}
            TO {principal}
        """)

        spark.sql(f"""
            GRANT SELECT
            ON SCHEMA workspace.{camada}
            TO {principal}
        """)

    for camada in ["silver", "gold"]:
        spark.sql(f"""
            GRANT CREATE TABLE, MODIFY
            ON SCHEMA workspace.{camada}
            TO {principal}
        """)

    print(f"Permissões concedidas: {usuario}")

# COMMAND ----------

# MAGIC %sql
# MAGIC SHOW GROUPS;

# COMMAND ----------

volumes_origem = {
    "empresas": "/Volumes/workspace/default/cad_receita_cnpj",
    "socios": "/Volumes/workspace/default/cad_receita_socios",
    "estabelecimentos": "/Volumes/workspace/default/cad_receita_estabelecimentos",
    "qualificacoes_socios": "/Volumes/workspace/default/cad_receita_quali_socios",
    "naturezas_juridicas": "/Volumes/workspace/default/cad_receita_nat_juridica",
    "cnaes": "/Volumes/workspace/default/cad_receita_cnaes",
    "simples": "/Volumes/workspace/default/cad_receita_simples",
    "paises": "/Volumes/workspace/default/paises",
}

# COMMAND ----------

arquivos_encontrados = []

for tabela, caminho in volumes_origem.items():
    for arquivo in dbutils.fs.ls(caminho):
        arquivos_encontrados.append((
            tabela,
            arquivo.name,
            arquivo.path,
            arquivo.size,
            arquivo.isDir(),
        ))

display(spark.createDataFrame(
    arquivos_encontrados,
    schema="""
        tabela STRING,
        nome_arquivo STRING,
        caminho STRING,
        tamanho_bytes LONG,
        eh_diretorio BOOLEAN
    """
))

# COMMAND ----------

padroes_arquivos = {
    "empresas": "*.EMPRECSV",
    "socios": "*.SOCIOCSV",
    "estabelecimentos": "*.ESTABELE*",
    "qualificacoes_socios": "*.QUALSCSV",
    "naturezas_juridicas": "*.NATJUCSV",
    "cnaes": "*.CNAECSV",
    "simples": "*.SIMPLES.CSV.*",
    "paises": "*.PAISCSV"
}

# COMMAND ----------

import csv

volume = volumes_origem["estabelecimentos"]

for arquivo in dbutils.fs.ls(volume):
    if arquivo.name.startswith("parte_"):
        primeira_linha = (
            spark.read
            .option("encoding", "ISO-8859-1")
            .text(arquivo.path)
            .first()["value"]
        )

        campos = next(csv.reader([primeira_linha], delimiter=";"))

        print(f"Arquivo: {arquivo.name}")
        print(f"Quantidade de campos: {len(campos)}")
        print(f"Primeiros 6 campos: {campos[:6]}")
        print()

# COMMAND ----------

from pyspark.sql.types import StructType, StructField, StringType

colunas = {
    "empresas": [
        "cnpj_basico",
        "razao_social",
        "cod_natureza_juridica",
        "cod_qualificacao_responsavel",
        "capital_social",
        "porte_empresa",
        "ente_federativo_responsavel",
    ],

    "socios": [
        "cnpj_basico",
        "identificador_socio",
        "nome_socio",
        "cnpj_cpf_socio",
        "cod_qualificacao_socio",
        "data_entrada_sociedade",
        "cod_pais",
        "representante_legal",
        "nome_representante",
        "cod_qualificacao_representante",
        "faixa_etaria",
    ],

    "estabelecimentos": [
        "cnpj_basico",
        "cnpj_ordem",
        "cnpj_dv",
        "identificador_matriz_filial",
        "nome_fantasia",
        "situacao_cadastral",
        "data_situacao_cadastral",
        "cod_motivo_situacao_cadastral",
        "nome_cidade_exterior",
        "cod_pais",
        "data_inicio_atividade",
        "cnae_fiscal_principal",
        "cnae_fiscal_secundaria",
        "tipo_logradouro",
        "logradouro",
        "numero",
        "complemento",
        "bairro",
        "cep",
        "uf",
        "cod_municipio",
        "ddd_1",
        "telefone_1",
        "ddd_2",
        "telefone_2",
        "ddd_fax",
        "fax",
        "correio_eletronico",
        "situacao_especial",
        "data_situacao_especial",
    ],

    "qualificacoes_socios": [
        "cod_qualificacao",
        "descricao_qualificacao",
    ],

    "naturezas_juridicas": [
        "cod_natureza_juridica",
        "descricao_natureza_juridica",
    ],

    "cnaes": [
        "cod_cnae",
        "descricao_cnae",
    ],

    "simples": [
        "cnpj_basico",
        "opcao_simples",
        "data_opcao_simples",
        "data_exclusao_simples",
        "opcao_mei",
        "data_opcao_mei",
        "data_exclusao_mei",
    ],
    "paises": [
        "cod_pais",
        "descricao_pais",
    ],
}

schemas = {
    tabela: StructType([
        StructField(nome, StringType(), True)
        for nome in nomes
    ])
    for tabela, nomes in colunas.items()
}

for tabela, schema in schemas.items():
    print(f"{tabela}: {len(schema.fields)} colunas")

# COMMAND ----------

dfs_bronze = {}

for tabela, schema in schemas.items():
    dfs_bronze[tabela] = (
        spark.read
        .format("csv")
        .schema(schema)
        .option("header", "false")
        .option("sep", ";")
        .option("encoding", "ISO-8859-1")
        .option("quote", '"')
        .option("escape", '"')
        .option("mode", "FAILFAST")
        .option("pathGlobFilter", padroes_arquivos[tabela])
        .option("multiLine", tabela == "estabelecimentos")
        .load(volumes_origem[tabela])
    )

    print(f"Leitura configurada: {tabela}")

# COMMAND ----------

tabelas_pendentes = [
    "paises",
]
for tabela in tabelas_pendentes:
    destino = f"workspace.bronze.{tabela}"

    print(f"Gravando: {destino}")

    (
        dfs_bronze[tabela].write
        .format("delta")
        .mode("error")
        .saveAsTable(destino)
    )

    print(f"Concluído: {destino}")

# COMMAND ----------

for tabela in schemas:
    destino = f"workspace.bronze.{tabela}"

    formato = (
        spark.sql(f"DESCRIBE DETAIL {destino}")
        .select("format")
        .first()["format"]
    )

    quantidade = spark.table(destino).count()

    print(f"{destino} | formato: {formato} | registros: {quantidade:,}")

# COMMAND ----------

