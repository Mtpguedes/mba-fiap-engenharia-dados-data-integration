# Databricks notebook source
# /// script
# [tool.databricks.environment]
# environment_version = "6"
# ///
from pyspark.sql import functions as F

df_cnaes = spark.table("workspace.bronze.cnaes")

display(
    df_cnaes
    .filter(
        F.col("cod_cnae").startswith("62")
        | F.col("cod_cnae").startswith("631")
    )
    .orderBy("cod_cnae")
)

# COMMAND ----------

cnaes_ti = [
    "6201500",
    "6201501",
    "6201502",
    "6202300",
    "6203100",
    "6204000",
    "6209100",
    "6311900",
    "6319400",
]

df_empresas_ti = (
    spark.table("workspace.bronze.estabelecimentos")
    .filter(F.col("cnae_fiscal_principal").isin(cnaes_ti))
    .select("cnpj_basico")
    .distinct()
)

# COMMAND ----------

df_socios_ti = (
    spark.table("workspace.silver.cnpj_socios_compliance")
    .join(
        df_empresas_ti,
        on="cnpj_basico",
        how="left_semi",
    )
)

print(f"Empresas no recorte de TI: {df_empresas_ti.count():,}")
print(
    "Empresas de TI com vínculos societários: "
    f"{df_socios_ti.select('cnpj_basico').distinct().count():,}"
)
print(f"Vínculos societários no recorte: {df_socios_ti.count():,}")

# COMMAND ----------

df_indicadores_societarios = (
    df_socios_ti
    .groupBy("cnpj_basico", "data_referencia")
    .agg(
        F.count("*").alias("qtd_vinculos"),

        F.sum(F.col("flag_socio_pf").cast("int"))
        .alias("qtd_vinculos_pf"),

        F.sum(F.col("flag_socio_pj").cast("int"))
        .alias("qtd_vinculos_pj"),

        F.sum(F.col("flag_socio_estrangeiro").cast("int"))
        .alias("qtd_vinculos_estrangeiros"),

        F.sum(F.col("flag_possui_representante").cast("int"))
        .alias("qtd_vinculos_com_representante"),

        F.min("tempo_vinculo_meses")
        .alias("menor_tempo_vinculo_meses"),

        F.max("tempo_vinculo_meses")
        .alias("maior_tempo_vinculo_meses"),

        F.round(F.avg("tempo_vinculo_meses"), 1)
        .alias("media_tempo_vinculo_meses"),

        F.sum(F.col("flag_data_entrada_invalida").cast("int"))
        .alias("qtd_datas_entrada_invalidas"),

        F.sum(F.col("flag_data_entrada_futura").cast("int"))
        .alias("qtd_datas_entrada_futuras"),
    )
)

# COMMAND ----------

df_gold_societario = (
    df_empresas_ti
    .withColumn(
        "data_referencia",
        F.lit("2026-09-30").cast("date")
    )
    .join(
        df_indicadores_societarios,
        on=["cnpj_basico", "data_referencia"],
        how="left",
    )
    .withColumn(
        "flag_dados_societarios_localizados",
        F.col("qtd_vinculos").isNotNull()
    )
)

display(df_gold_societario.limit(10))

# COMMAND ----------

resumo = df_gold_societario.agg(
    F.count("*").alias("total_empresas"),
    F.countDistinct("cnpj_basico").alias("cnpjs_distintos"),
    F.sum(
        F.col("flag_dados_societarios_localizados").cast("int")
    ).alias("empresas_com_vinculos"),
    F.sum("qtd_vinculos").alias("total_vinculos"),
).first()

print(resumo.asDict())

assert resumo["total_empresas"] == 625285
assert resumo["cnpjs_distintos"] == resumo["total_empresas"]
assert resumo["empresas_com_vinculos"] == 362523
assert resumo["total_vinculos"] == 812623

print("Validação concluída.")

# COMMAND ----------

(
    df_gold_societario.write
    .format("delta")
    .mode("error")
    .saveAsTable("workspace.gold.perfil_societario_ti")
)

print("Gold criada: workspace.gold.perfil_societario_ti")

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT
# MAGIC     COUNT(*) AS total_empresas_ti,
# MAGIC
# MAGIC     COUNT_IF(flag_dados_societarios_localizados)
# MAGIC         AS empresas_com_dados_societarios,
# MAGIC
# MAGIC     COUNT_IF(NOT flag_dados_societarios_localizados)
# MAGIC         AS empresas_sem_dados_societarios,
# MAGIC
# MAGIC     ROUND(
# MAGIC         100.0 * COUNT_IF(flag_dados_societarios_localizados) / COUNT(*),
# MAGIC         2
# MAGIC     ) AS percentual_cobertura,
# MAGIC
# MAGIC     COUNT_IF(qtd_vinculos_pj > 0)
# MAGIC         AS empresas_com_vinculos_pj,
# MAGIC
# MAGIC     COUNT_IF(qtd_vinculos_estrangeiros > 0)
# MAGIC         AS empresas_com_vinculos_estrangeiros,
# MAGIC
# MAGIC     COUNT_IF(qtd_vinculos_com_representante > 0)
# MAGIC         AS empresas_com_representacao_legal
# MAGIC
# MAGIC FROM workspace.gold.perfil_societario_ti;

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT
# MAGIC     CASE
# MAGIC         WHEN menor_tempo_vinculo_meses IS NULL
# MAGIC             THEN 'Sem data válida'
# MAGIC         WHEN menor_tempo_vinculo_meses < 12
# MAGIC             THEN 'Menos de 1 ano'
# MAGIC         WHEN menor_tempo_vinculo_meses < 36
# MAGIC             THEN '1 a menos de 3 anos'
# MAGIC         WHEN menor_tempo_vinculo_meses < 60
# MAGIC             THEN '3 a menos de 5 anos'
# MAGIC         ELSE '5 anos ou mais'
# MAGIC     END AS tempo_desde_entrada_mais_recente,
# MAGIC
# MAGIC     COUNT(*) AS quantidade_empresas,
# MAGIC
# MAGIC     ROUND(
# MAGIC         100.0 * COUNT(*) / SUM(COUNT(*)) OVER (),
# MAGIC         2
# MAGIC     ) AS percentual
# MAGIC
# MAGIC FROM workspace.gold.perfil_societario_ti
# MAGIC WHERE flag_dados_societarios_localizados
# MAGIC GROUP BY 1
# MAGIC ORDER BY quantidade_empresas DESC;

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT
# MAGIC     s.faixa_etaria,
# MAGIC     s.descricao_faixa_etaria,
# MAGIC     COUNT(*) AS quantidade_vinculos_pf,
# MAGIC     COUNT(DISTINCT s.cnpj_basico) AS quantidade_empresas,
# MAGIC     ROUND(
# MAGIC         100.0 * COUNT(*) / SUM(COUNT(*)) OVER (),
# MAGIC         2
# MAGIC     ) AS percentual_vinculos_pf
# MAGIC
# MAGIC FROM workspace.silver.cnpj_socios_compliance s
# MAGIC INNER JOIN workspace.gold.perfil_societario_ti g
# MAGIC     ON s.cnpj_basico = g.cnpj_basico
# MAGIC     AND s.data_referencia = g.data_referencia
# MAGIC
# MAGIC WHERE s.flag_socio_pf
# MAGIC
# MAGIC GROUP BY
# MAGIC     s.faixa_etaria,
# MAGIC     s.descricao_faixa_etaria
# MAGIC
# MAGIC ORDER BY s.faixa_etaria;

# COMMAND ----------

