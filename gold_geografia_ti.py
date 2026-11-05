# Databricks notebook source
# MAGIC %md
# MAGIC # Gold — Mapeamento Geográfico e Distribuição Regional do Setor de TI
# MAGIC
# MAGIC **Pergunta de negócio:** onde estão as empresas de TI, onde o setor já está concentrado (risco de saturação) e onde está crescendo (oportunidade)?
# MAGIC
# MAGIC **Origem:** `silver.cnpj_estabelecimentos`
# MAGIC **Destino:** `workspace.gold.geografia_ti`
# MAGIC **Universo:** matrizes (uma por empresa) localizadas no Brasil. Todas as atividades entram, porque a penetração de TI é medida contra o total de empresas do município.
# MAGIC **Grão:** `dt_referencia × regiao × uf × cod_municipio`
# MAGIC
# MAGIC | Métrica | Definição |
# MAGIC |---|---|
# MAGIC | `qtd_ti_ativas` | Matrizes de TI com situação Ativa |
# MAGIC | `qtd_total_ativas` | Matrizes ativas de qualquer atividade |
# MAGIC | `pct_ti_no_municipio` | Penetração: `qtd_ti_ativas / qtd_total_ativas`. Alta = setor denso, possível saturação |
# MAGIC | `pct_ti_do_brasil` | Participação do município no estoque nacional de TI ativa |
# MAGIC | `abertas_ti_12m` / `abertas_ti_12m_anteriores` | Aberturas de TI nos 12 meses até `dt_referencia` e nos 12 meses antes disso |
# MAGIC | `crescimento_aberturas_pct` | Variação entre as duas janelas |
# MAGIC | `baixadas_ti_12m` | Empresas de TI baixadas nos últimos 12 meses (pela data da situação) |
# MAGIC | `saldo_liquido_ti_12m` | Aberturas − baixas nos últimos 12 meses |
# MAGIC
# MAGIC **Limitação:** a base da Receita não traz população nem PIB. Penetração é medida contra o total de empresas do município, não contra habitantes. Para densidade por habitante, o próximo passo seria cruzar com a estimativa de população do IBGE (que exige o de-para do código de município da Receita para o código IBGE).

# COMMAND ----------

# MAGIC %run ./utils_pipeline

# COMMAND ----------

DT_REFERENCIA = obter_dt_referencia()
TABELA_DESTINO = "workspace.gold.geografia_ti"

print(f"dt_referencia: {DT_REFERENCIA}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1. Base e janelas de tempo

# COMMAND ----------

dt_ref = F.lit(DT_REFERENCIA).cast("date")
inicio_12m = F.add_months(dt_ref, -12)
inicio_24m = F.add_months(dt_ref, -24)

df_base = (
    spark.table("workspace.silver.cnpj_estabelecimentos")
    .filter(
        (F.col("dt_referencia") == DT_REFERENCIA)
        & F.col("flag_matriz")
        & (F.coalesce(F.col("uf"), F.lit("EX")) != "EX")
    )
    .select("dt_referencia", "regiao", "uf", "cod_municipio", "desc_municipio",
            "flag_ti", "flag_ativa", "cod_situacao", "dt_inicio_atividade", "dt_situacao_cadastral")
    .withColumn("_ti_ativa", F.col("flag_ti") & F.col("flag_ativa"))
    .withColumn("_aberta_12m", F.col("flag_ti")
                & (F.col("dt_inicio_atividade") > inicio_12m) & (F.col("dt_inicio_atividade") <= dt_ref))
    .withColumn("_aberta_12m_ant", F.col("flag_ti")
                & (F.col("dt_inicio_atividade") > inicio_24m) & (F.col("dt_inicio_atividade") <= inicio_12m))
    .withColumn("_baixada_12m", F.col("flag_ti") & (F.col("cod_situacao") == "08")
                & (F.col("dt_situacao_cadastral") > inicio_12m) & (F.col("dt_situacao_cadastral") <= dt_ref))
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2. Agregação por município

# COMMAND ----------

from pyspark.sql.window import Window

dimensoes = ["dt_referencia", "regiao", "uf", "cod_municipio"]

df_gold = (
    df_base
    .groupBy(*dimensoes)
    .agg(
        F.max("desc_municipio").alias("desc_municipio"),
        F.count_if("flag_ativa").alias("qtd_total_ativas"),
        F.count_if("_ti_ativa").alias("qtd_ti_ativas"),
        F.count_if("_aberta_12m").alias("abertas_ti_12m"),
        F.count_if("_aberta_12m_ant").alias("abertas_ti_12m_anteriores"),
        F.count_if("_baixada_12m").alias("baixadas_ti_12m"),
    )
    .withColumn("pct_ti_no_municipio",
                F.round(100.0 * F.col("qtd_ti_ativas") / F.nullif(F.col("qtd_total_ativas"), F.lit(0)), 2))
    .withColumn("pct_ti_do_brasil",
                F.round(100.0 * F.col("qtd_ti_ativas")
                        / F.nullif(F.sum("qtd_ti_ativas").over(Window.partitionBy("dt_referencia")), F.lit(0)), 4))
    .withColumn("crescimento_aberturas_pct",
                F.round(100.0 * (F.col("abertas_ti_12m") - F.col("abertas_ti_12m_anteriores"))
                        / F.nullif(F.col("abertas_ti_12m_anteriores"), F.lit(0)), 1))
    .withColumn("saldo_liquido_ti_12m", F.col("abertas_ti_12m") - F.col("baixadas_ti_12m"))
)

display(df_gold.orderBy(F.desc("qtd_ti_ativas")).limit(20))

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3. Quality checks

# COMMAND ----------

with Cronometro("gold_geografia_ti", "checks", DT_REFERENCIA):
    ti_ativas_silver = df_base.filter("_ti_ativa").count()
    ti_ativas_gold = df_gold.agg(F.sum("qtd_ti_ativas")).first()[0] or 0

    executar_checks(
        df_gold, TABELA_DESTINO, DT_REFERENCIA,
        regras=[
            regra("TI ativas ≤ total de ativas", "bloqueia", F.col("qtd_ti_ativas") > F.col("qtd_total_ativas")),
            regra("penetração entre 0 e 100%", "bloqueia",
                  (F.col("pct_ti_no_municipio") < 0) | (F.col("pct_ti_no_municipio") > 100)),
            regra("município sem descrição", "alerta", F.col("desc_municipio").isNull(), limite_pct=0.5),
            regra("região não identificada", "alerta",
                  F.col("regiao") == "Exterior/Não informada", limite_pct=0.1),
        ],
        resultados_extras=[
            resultado("gold não vazia", "bloqueia", 0 if ti_ativas_gold > 0 else 1, 1),
            check_unicidade(df_gold, dimensoes),
            check_contagem(ti_ativas_silver, ti_ativas_gold, "reconciliação: TI ativas gold = silver"),
        ],
    )

# COMMAND ----------

with Cronometro("gold_geografia_ti", "gravacao", DT_REFERENCIA):
    gravar_particao(df_gold, TABELA_DESTINO, DT_REFERENCIA)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 4. Consultas de negócio

# COMMAND ----------

# MAGIC %md
# MAGIC ### 4.1 Distribuição por região e UF

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT
# MAGIC     regiao,
# MAGIC     uf,
# MAGIC     SUM(qtd_ti_ativas)                                                   AS ti_ativas,
# MAGIC     ROUND(SUM(pct_ti_do_brasil), 2)                                      AS pct_ti_do_brasil,
# MAGIC     ROUND(100.0 * SUM(qtd_ti_ativas) / SUM(qtd_total_ativas), 2)         AS penetracao_ti_pct,
# MAGIC     SUM(saldo_liquido_ti_12m)                                            AS saldo_liquido_12m
# MAGIC FROM workspace.gold.geografia_ti
# MAGIC WHERE dt_referencia = (SELECT MAX(dt_referencia) FROM workspace.gold.geografia_ti)
# MAGIC GROUP BY regiao, uf
# MAGIC ORDER BY ti_ativas DESC;

# COMMAND ----------

# MAGIC %md
# MAGIC ### 4.2 Saturação × oportunidade: municípios com pelo menos 500 empresas de TI
# MAGIC
# MAGIC - **Saturado:** penetração acima da mediana e saldo líquido baixo.
# MAGIC - **Oportunidade:** penetração abaixo da mediana e aberturas crescendo.

# COMMAND ----------

# MAGIC %sql
# MAGIC WITH base AS (
# MAGIC     SELECT *
# MAGIC     FROM workspace.gold.geografia_ti
# MAGIC     WHERE dt_referencia = (SELECT MAX(dt_referencia) FROM workspace.gold.geografia_ti)
# MAGIC       AND qtd_ti_ativas >= 500
# MAGIC ),
# MAGIC mediana AS (
# MAGIC     SELECT PERCENTILE_APPROX(pct_ti_no_municipio, 0.5) AS penetracao_mediana FROM base
# MAGIC )
# MAGIC SELECT
# MAGIC     b.uf,
# MAGIC     b.desc_municipio,
# MAGIC     b.qtd_ti_ativas,
# MAGIC     b.pct_ti_no_municipio,
# MAGIC     b.abertas_ti_12m,
# MAGIC     b.crescimento_aberturas_pct,
# MAGIC     b.saldo_liquido_ti_12m,
# MAGIC     CASE
# MAGIC         WHEN b.pct_ti_no_municipio >= m.penetracao_mediana AND b.crescimento_aberturas_pct <= 0 THEN 'Saturado'
# MAGIC         WHEN b.pct_ti_no_municipio <  m.penetracao_mediana AND b.crescimento_aberturas_pct >  0 THEN 'Oportunidade'
# MAGIC         ELSE 'Neutro'
# MAGIC     END AS classificacao
# MAGIC FROM base b CROSS JOIN mediana m
# MAGIC ORDER BY b.qtd_ti_ativas DESC;
