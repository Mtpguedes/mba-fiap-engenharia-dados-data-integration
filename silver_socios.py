# Databricks notebook source
# /// script
# [tool.databricks.environment]
# environment_version = "6"
# ///
from pyspark.sql import functions as F

df_socios = spark.table("workspace.bronze.socios")

df_qualificacoes = spark.table(
    "workspace.bronze.qualificacoes_socios"
)

# COMMAND ----------

display(
    df_qualificacoes.orderBy("cod_qualificacao")
)

display(
    df_qualificacoes
    .groupBy("cod_qualificacao")
    .count()
    .filter(F.col("count") > 1)
)

# COMMAND ----------

# Domínio para a qualificação do sócio
qualificacao_socio = df_qualificacoes.select(
    F.col("cod_qualificacao").alias("cod_qualificacao_socio"),
    F.col("descricao_qualificacao").alias("descricao_qualificacao_socio"),
)

# Mesmo domínio para a qualificação do representante
qualificacao_representante = df_qualificacoes.select(
    F.col("cod_qualificacao").alias("cod_qualificacao_representante"),
    F.col("descricao_qualificacao").alias(
        "descricao_qualificacao_representante"
    ),
)

df_socios_silver = (
    df_socios
    .join(
        qualificacao_socio,
        on="cod_qualificacao_socio",
        how="left",
    )
    .join(
        qualificacao_representante,
        on="cod_qualificacao_representante",
        how="left",
    )
)

display(
    df_socios_silver.select(
        "cnpj_basico",
        "nome_socio",
        "cod_qualificacao_socio",
        "descricao_qualificacao_socio",
        "nome_representante",
        "descricao_qualificacao_representante",
    ).limit(10)
)

# COMMAND ----------

faixas_etarias = {
    "0": "Não se aplica",
    "1": "0 a 12 anos",
    "2": "13 a 20 anos",
    "3": "21 a 30 anos",
    "4": "31 a 40 anos",
    "5": "41 a 50 anos",
    "6": "51 a 60 anos",
    "7": "61 a 70 anos",
    "8": "71 a 80 anos",
    "9": "Mais de 80 anos",
}

mapa_faixas = F.create_map(
    *[
        F.lit(item)
        for par in faixas_etarias.items()
        for item in par
    ]
)

df_socios_silver = (
    df_socios_silver
    .withColumn(
        "tipo_socio",
        F.when(F.col("identificador_socio") == "1", "Pessoa jurídica")
        .when(F.col("identificador_socio") == "2", "Pessoa física")
        .when(F.col("identificador_socio") == "3", "Estrangeiro")
        .otherwise("Não identificado")
    )
    .withColumn(
        "descricao_faixa_etaria",
        F.coalesce(
            mapa_faixas[F.col("faixa_etaria")],
            F.lit("Não informada ou inválida")
        )
    )
)

display(
    df_socios_silver.select(
        "cnpj_basico",
        "nome_socio",
        "tipo_socio",
        "descricao_qualificacao_socio",
        "faixa_etaria",
        "descricao_faixa_etaria",
    ).limit(10)
)

# COMMAND ----------

df_socios_silver = (
    df_socios_silver
    .withColumn(
        "data_referencia",
        F.lit("2026-09-30").cast("date")
    )
    .withColumn(
        "data_entrada_sociedade_dt",
        F.expr("""
            CAST(
                try_to_timestamp(trim(data_entrada_sociedade), 'yyyyMMdd')
                AS DATE
            )
        """)
    )
    .withColumn(
        "flag_data_entrada_invalida",
        F.col("data_entrada_sociedade_dt").isNull()
    )
    .withColumn(
        "flag_data_entrada_futura",
        F.coalesce(
            F.col("data_entrada_sociedade_dt") > F.col("data_referencia"),
            F.lit(False)
        )
    )
    .withColumn(
        "tempo_vinculo_meses",
        F.when(
            F.col("data_entrada_sociedade_dt").isNotNull()
            & ~F.col("flag_data_entrada_futura"),
            F.floor(
                F.months_between(
                    F.col("data_referencia"),
                    F.col("data_entrada_sociedade_dt")
                )
            ).cast("int")
        )
    )
)

display(
    df_socios_silver.select(
        "cnpj_basico",
        "data_entrada_sociedade",
        "data_entrada_sociedade_dt",
        "data_referencia",
        "tempo_vinculo_meses",
        "flag_data_entrada_invalida",
        "flag_data_entrada_futura",
    ).limit(10)
)

# COMMAND ----------

documento_representante = F.trim(
    F.coalesce(F.col("representante_legal"), F.lit(""))
)

nome_representante = F.trim(
    F.coalesce(F.col("nome_representante"), F.lit(""))
)

df_socios_silver = (
    df_socios_silver
    .withColumn(
        "flag_socio_pf",
        F.coalesce(F.col("identificador_socio") == "2", F.lit(False))
    )
    .withColumn(
        "flag_socio_pj",
        F.coalesce(F.col("identificador_socio") == "1", F.lit(False))
    )
    .withColumn(
        "flag_socio_estrangeiro",
        F.coalesce(F.col("identificador_socio") == "3", F.lit(False))
    )
    .withColumn(
        "flag_possui_representante",
        (nome_representante != "")
        | (
            (documento_representante != "")
            & ~documento_representante.rlike(r"^[0*.\-/\s]+$")
        )
    )
)

display(
    df_socios_silver.select(
        "cnpj_basico",
        "nome_socio",
        "tipo_socio",
        "flag_socio_pf",
        "flag_socio_pj",
        "flag_socio_estrangeiro",
        "nome_representante",
        "flag_possui_representante",
    ).limit(10)
)

# COMMAND ----------

total_bronze = df_socios.count()
total_silver = df_socios_silver.count()

print(f"Bronze: {total_bronze:,}")
print(f"Silver: {total_silver:,}")

assert total_silver == total_bronze, (
    "A quantidade mudou após os joins. Verifique códigos "
    "duplicados no domínio de qualificações."
)

# COMMAND ----------

