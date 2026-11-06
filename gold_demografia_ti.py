# Databricks notebook source
# /// script
# [tool.databricks.environment]
# environment_version = "6"
# ///
# MAGIC %md
# MAGIC # Gold — Demografia de Empresas de TI e Tempo de Vida do Setor
# MAGIC
# MAGIC **Pergunta de negócio:** quantas empresas de TI nascem por ano, onde, com que perfil (porte, natureza jurídica, MEI), e quantas seguem ativas e com que idade.
# MAGIC
# MAGIC **Origem:** `workspace.silver.cnpj_estabelecimentos` + `workspace.silver.cnpj_empresas`
# MAGIC **Destino:** `workspace.gold.demografia_ti`
# MAGIC
# MAGIC **Universo:** só **matrizes** com CNAE principal de TI. Filiais ficam de fora para cada empresa ser contada uma vez.
# MAGIC
# MAGIC **Grão:** `dt_referencia × ano_abertura × uf × desc_porte × grupo_natureza_juridica × flag_mei`
# MAGIC
# MAGIC **Fronteira com a frente de Mortalidade (Helen):** esta gold trata de **estoque e entradas**. Tempo de vida das baixadas e motivo de baixa ficam com ela.

# COMMAND ----------

# MAGIC %run ./utils_pipeline

# COMMAND ----------

DT_REFERENCIA = obter_dt_referencia()
TABELA_DESTINO = "workspace.gold.demografia_ti"

print(f"dt_referencia: {DT_REFERENCIA}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1. Universo: matrizes de TI com o perfil da empresa

# COMMAND ----------

df_matrizes_ti = (
    spark.table("workspace.silver.cnpj_estabelecimentos")
    .filter((F.col("dt_referencia") == DT_REFERENCIA) & F.col("flag_matriz") & F.col("flag_ti"))
    .select("dt_referencia", "cnpj_basico", "uf", "ano_abertura", "cod_situacao", "flag_ativa", "idade_meses")
)

df_empresas = (
    spark.table("workspace.silver.cnpj_empresas")
    .filter(F.col("dt_referencia") == DT_REFERENCIA)
    .select("dt_referencia", "cnpj_basico", "desc_porte", "grupo_natureza_juridica", "flag_mei", "capital_social")
)

df_base = (
    df_matrizes_ti
    .join(df_empresas, on=["dt_referencia", "cnpj_basico"], how="left")
    .withColumn("desc_porte", F.coalesce("desc_porte", F.lit("Sem cadastro de empresa")))
    .withColumn("grupo_natureza_juridica", F.coalesce("grupo_natureza_juridica", F.lit("Sem cadastro de empresa")))
    .withColumn("flag_mei", F.coalesce("flag_mei", F.lit(False)))
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2. Agregação

# COMMAND ----------

dimensoes = ["dt_referencia", "ano_abertura", "uf", "desc_porte", "grupo_natureza_juridica", "flag_mei"]

idade_ativa = F.when(F.col("flag_ativa"), F.col("idade_meses"))

df_gold = (
    df_base
    .groupBy(*dimensoes)
    .agg(
        F.count(F.lit(1)).alias("qtd_empresas_abertas"),
        F.count_if(F.col("flag_ativa")).alias("qtd_ativas"),
        F.count_if(F.col("cod_situacao").isin("03", "04")).alias("qtd_suspensas_inaptas"),
        F.count_if(F.col("cod_situacao") == "08").alias("qtd_baixadas"),
        F.round(F.avg(idade_ativa), 1).alias("idade_media_meses_ativas"),
        F.percentile_approx(idade_ativa, 0.5).alias("idade_mediana_meses_ativas"),
        F.percentile_approx(F.col("capital_social").cast("double"), 0.5).alias("capital_social_mediano"),
    )
    .withColumn("pct_ativas", F.round(100.0 * F.col("qtd_ativas") / F.col("qtd_empresas_abertas"), 2))
)

display(df_gold.orderBy(F.desc("qtd_empresas_abertas")).limit(20))

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3. Quality checks
# MAGIC
# MAGIC O check de reconciliação garante que a soma da gold bate com o universo da silver. Se um join duplicar ou perder empresas, a gold não é gravada.

# COMMAND ----------

with Cronometro("gold_demografia_ti", "checks", DT_REFERENCIA):
    total_universo = df_matrizes_ti.count()
    soma_gold = df_gold.agg(F.sum("qtd_empresas_abertas")).first()[0] or 0

    executar_checks(
        df_gold, TABELA_DESTINO, DT_REFERENCIA,
        regras=[
            regra("ativas ≤ abertas", "bloqueia", F.col("qtd_ativas") > F.col("qtd_empresas_abertas")),
            regra("ativas + suspensas/inaptas + baixadas ≤ abertas", "bloqueia",
                  F.col("qtd_ativas") + F.col("qtd_suspensas_inaptas") + F.col("qtd_baixadas") > F.col("qtd_empresas_abertas")),
            regra("idade mediana negativa", "bloqueia", F.col("idade_mediana_meses_ativas") < 0),
            regra("grupo sem ano de abertura válido", "alerta", F.col("ano_abertura").isNull(), limite_pct=1.0),
            regra("grupo sem cadastro de empresa", "alerta",
                  F.col("desc_porte") == "Sem cadastro de empresa", limite_pct=1.0),
        ],
        resultados_extras=[
            resultado("universo de TI não vazio", "bloqueia", 0 if total_universo > 0 else 1, 1),
            check_contagem(total_universo, soma_gold, "reconciliação: soma da gold = matrizes de TI na silver"),
            check_unicidade(df_matrizes_ti, ["cnpj_basico"]),
            check_unicidade(df_gold, dimensoes),
        ],
    )

# COMMAND ----------

# MAGIC %md
# MAGIC ## 4. Gravação

# COMMAND ----------

with Cronometro("gold_demografia_ti", "gravacao", DT_REFERENCIA):
    gravar_particao(df_gold, TABELA_DESTINO, DT_REFERENCIA)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 5. Consultas de negócio
# MAGIC
# MAGIC Atenção: 2026 é um ano parcial (carga de setembro). Comparações entre anos devem usar só anos completos.

# COMMAND ----------

# MAGIC %md
# MAGIC ### 5.1 Quantas empresas de TI nascem por ano, e quantas sobrevivem

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT
# MAGIC     ano_abertura,
# MAGIC     SUM(qtd_empresas_abertas)                                      AS empresas_abertas,
# MAGIC     SUM(qtd_ativas)                                                AS ativas_hoje,
# MAGIC     ROUND(100.0 * SUM(qtd_ativas) / SUM(qtd_empresas_abertas), 1)  AS pct_ativas_hoje,
# MAGIC     ROUND(100.0 * SUM(CASE WHEN flag_mei THEN qtd_empresas_abertas ELSE 0 END)
# MAGIC           / SUM(qtd_empresas_abertas), 1)                          AS pct_mei
# MAGIC FROM workspace.gold.demografia_ti
# MAGIC WHERE dt_referencia = (SELECT MAX(dt_referencia) FROM workspace.gold.demografia_ti)
# MAGIC   AND ano_abertura BETWEEN 2010 AND 2026
# MAGIC GROUP BY ano_abertura
# MAGIC ORDER BY ano_abertura;

# COMMAND ----------

# MAGIC %md
# MAGIC ### 5.2 Onde o setor está crescendo: aberturas 2021–2025 vs. 2016–2020 por UF

# COMMAND ----------

# MAGIC %sql
# MAGIC WITH por_periodo AS (
# MAGIC     SELECT
# MAGIC         uf,
# MAGIC         SUM(CASE WHEN ano_abertura BETWEEN 2016 AND 2020 THEN qtd_empresas_abertas ELSE 0 END) AS abertas_2016_2020,
# MAGIC         SUM(CASE WHEN ano_abertura BETWEEN 2021 AND 2025 THEN qtd_empresas_abertas ELSE 0 END) AS abertas_2021_2025,
# MAGIC         SUM(qtd_ativas)                                                                         AS estoque_ativo
# MAGIC     FROM workspace.gold.demografia_ti
# MAGIC     WHERE dt_referencia = (SELECT MAX(dt_referencia) FROM workspace.gold.demografia_ti)
# MAGIC     GROUP BY uf
# MAGIC )
# MAGIC SELECT
# MAGIC     uf,
# MAGIC     estoque_ativo,
# MAGIC     abertas_2016_2020,
# MAGIC     abertas_2021_2025,
# MAGIC     ROUND(100.0 * (abertas_2021_2025 - abertas_2016_2020) / NULLIF(abertas_2016_2020, 0), 1) AS crescimento_pct
# MAGIC FROM por_periodo
# MAGIC WHERE uf IS NOT NULL AND uf <> 'EX'
# MAGIC ORDER BY crescimento_pct DESC;

# COMMAND ----------

# MAGIC %md
# MAGIC ### 5.3 Perfil do estoque ativo: porte, natureza jurídica e idade

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT
# MAGIC     grupo_natureza_juridica,
# MAGIC     desc_porte,
# MAGIC     CASE WHEN flag_mei THEN 'MEI' ELSE 'Não MEI' END              AS mei,
# MAGIC     SUM(qtd_ativas)                                                AS ativas,
# MAGIC     ROUND(100.0 * SUM(qtd_ativas) / SUM(SUM(qtd_ativas)) OVER (), 2) AS pct_do_estoque,
# MAGIC     ROUND(SUM(idade_media_meses_ativas * qtd_ativas) / NULLIF(SUM(qtd_ativas), 0) / 12, 1) AS idade_media_anos
# MAGIC FROM workspace.gold.demografia_ti
# MAGIC WHERE dt_referencia = (SELECT MAX(dt_referencia) FROM workspace.gold.demografia_ti)
# MAGIC GROUP BY 1, 2, 3
# MAGIC HAVING SUM(qtd_ativas) > 0
# MAGIC ORDER BY ativas DESC;