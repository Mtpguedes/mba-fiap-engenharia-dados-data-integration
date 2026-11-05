# Databricks notebook source
# MAGIC %md
# MAGIC # Gold — Risco Societário e de Governança (TI)
# MAGIC
# MAGIC **Pergunta de negócio:** como é a estrutura societária das empresas de TI? Quantas têm sócio pessoa jurídica, sócio estrangeiro ou representante legal, e há quanto tempo o quadro de sócios mudou pela última vez?
# MAGIC
# MAGIC **Origem:** `silver.cnpj_estabelecimentos` (universo) + `silver.cnpj_socios_compliance`
# MAGIC **Destino:** `workspace.gold.perfil_societario_ti`
# MAGIC **Universo:** matrizes com CNAE principal de TI (mesmo universo da gold de demografia, para os números das frentes baterem).
# MAGIC **Grão:** uma empresa (`cnpj_basico`) por linha, por `dt_referencia`.

# COMMAND ----------

# MAGIC %run ./utils_pipeline

# COMMAND ----------

DT_REFERENCIA = obter_dt_referencia()
TABELA_DESTINO = "workspace.gold.perfil_societario_ti"

print(f"dt_referencia: {DT_REFERENCIA}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1. Universo e indicadores por empresa

# COMMAND ----------

df_empresas_ti = (
    spark.table("workspace.silver.cnpj_estabelecimentos")
    .filter((F.col("dt_referencia") == DT_REFERENCIA) & F.col("flag_matriz") & F.col("flag_ti"))
    .select("dt_referencia", "cnpj_basico", "uf", "flag_ativa")
)

df_socios = (
    spark.table("workspace.silver.cnpj_socios_compliance")
    .filter(F.col("dt_referencia") == DT_REFERENCIA)
    .join(df_empresas_ti.select("dt_referencia", "cnpj_basico"), on=["dt_referencia", "cnpj_basico"], how="left_semi")
)

df_indicadores = (
    df_socios
    .groupBy("dt_referencia", "cnpj_basico")
    .agg(
        F.count(F.lit(1)).alias("qtd_vinculos"),
        F.count_distinct("id_socio_hash").alias("qtd_socios_distintos"),
        F.count_if("flag_socio_pf").alias("qtd_vinculos_pf"),
        F.count_if("flag_socio_pj").alias("qtd_vinculos_pj"),
        F.count_if("flag_socio_estrangeiro").alias("qtd_vinculos_estrangeiros"),
        F.count_if("flag_possui_representante").alias("qtd_vinculos_com_representante"),
        F.min("tempo_vinculo_meses").alias("menor_tempo_vinculo_meses"),
        F.max("tempo_vinculo_meses").alias("maior_tempo_vinculo_meses"),
        F.round(F.avg("tempo_vinculo_meses"), 1).alias("media_tempo_vinculo_meses"),
        F.count_if("flag_data_entrada_invalida").alias("qtd_datas_entrada_invalidas"),
    )
)

df_gold = (
    df_empresas_ti
    .join(df_indicadores, on=["dt_referencia", "cnpj_basico"], how="left")
    .withColumn("flag_dados_societarios_localizados", F.col("qtd_vinculos").isNotNull())
    .withColumn("flag_socio_pj", F.coalesce(F.col("qtd_vinculos_pj") > 0, F.lit(False)))
    .withColumn("flag_socio_estrangeiro", F.coalesce(F.col("qtd_vinculos_estrangeiros") > 0, F.lit(False)))
    .withColumn(
        "faixa_ultima_mudanca_societaria",
        F.when(F.col("menor_tempo_vinculo_meses").isNull(), "Sem data válida")
        .when(F.col("menor_tempo_vinculo_meses") < 12, "Menos de 1 ano")
        .when(F.col("menor_tempo_vinculo_meses") < 36, "1 a menos de 3 anos")
        .when(F.col("menor_tempo_vinculo_meses") < 60, "3 a menos de 5 anos")
        .otherwise("5 anos ou mais"),
    )
)

display(df_gold.limit(10))

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2. Quality checks
# MAGIC
# MAGIC Substituem os `assert` com números fixos da versão anterior: aqueles valores mudam todo mês e quebrariam o pipeline na próxima carga mesmo com os dados corretos. Aqui a validação é **relativa** (a gold tem de bater com a silver do mesmo mês).

# COMMAND ----------

with Cronometro("gold_risco_societario", "checks", DT_REFERENCIA):
    total_universo = df_empresas_ti.count()
    total_vinculos_silver = df_socios.count()
    total_vinculos_gold = df_gold.agg(F.sum("qtd_vinculos")).first()[0] or 0

    executar_checks(
        df_gold, TABELA_DESTINO, DT_REFERENCIA,
        regras=[
            regra("PF + PJ + estrangeiros = total de vínculos", "bloqueia",
                  F.col("qtd_vinculos_pf") + F.col("qtd_vinculos_pj") + F.col("qtd_vinculos_estrangeiros")
                  != F.col("qtd_vinculos")),
            regra("sócios distintos ≤ vínculos", "bloqueia", F.col("qtd_socios_distintos") > F.col("qtd_vinculos")),
            regra("empresa sem dados societários", "alerta",
                  ~F.col("flag_dados_societarios_localizados"), limite_pct=50.0),
        ],
        resultados_extras=[
            resultado("universo de TI não vazio", "bloqueia", 0 if total_universo > 0 else 1, 1),
            check_unicidade(df_gold, ["cnpj_basico"]),
            check_contagem(total_universo, df_gold.count(), "gold tem uma linha por empresa do universo"),
            check_contagem(total_vinculos_silver, total_vinculos_gold, "reconciliação: vínculos gold = silver"),
        ],
    )

# COMMAND ----------

with Cronometro("gold_risco_societario", "gravacao", DT_REFERENCIA):
    gravar_particao(df_gold, TABELA_DESTINO, DT_REFERENCIA)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3. Consultas de negócio

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT
# MAGIC     COUNT(*)                                                       AS total_empresas_ti,
# MAGIC     COUNT_IF(flag_dados_societarios_localizados)                   AS com_dados_societarios,
# MAGIC     ROUND(100.0 * COUNT_IF(flag_dados_societarios_localizados) / COUNT(*), 2) AS pct_cobertura,
# MAGIC     COUNT_IF(flag_socio_pj)                                        AS com_socio_pj,
# MAGIC     COUNT_IF(flag_socio_estrangeiro)                               AS com_socio_estrangeiro,
# MAGIC     COUNT_IF(qtd_vinculos_com_representante > 0)                   AS com_representante_legal
# MAGIC FROM workspace.gold.perfil_societario_ti
# MAGIC WHERE dt_referencia = (SELECT MAX(dt_referencia) FROM workspace.gold.perfil_societario_ti);

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT
# MAGIC     faixa_ultima_mudanca_societaria,
# MAGIC     COUNT(*)                                          AS empresas,
# MAGIC     ROUND(100.0 * COUNT(*) / SUM(COUNT(*)) OVER (), 2) AS pct
# MAGIC FROM workspace.gold.perfil_societario_ti
# MAGIC WHERE dt_referencia = (SELECT MAX(dt_referencia) FROM workspace.gold.perfil_societario_ti)
# MAGIC   AND flag_dados_societarios_localizados
# MAGIC GROUP BY 1
# MAGIC ORDER BY empresas DESC;

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT
# MAGIC     s.faixa_etaria,
# MAGIC     s.descricao_faixa_etaria,
# MAGIC     COUNT(*)                                          AS vinculos_pf,
# MAGIC     COUNT(DISTINCT s.cnpj_basico)                     AS empresas,
# MAGIC     ROUND(100.0 * COUNT(*) / SUM(COUNT(*)) OVER (), 2) AS pct_vinculos_pf
# MAGIC FROM workspace.silver.cnpj_socios_compliance s
# MAGIC INNER JOIN workspace.gold.perfil_societario_ti g
# MAGIC     ON s.cnpj_basico = g.cnpj_basico
# MAGIC    AND s.dt_referencia = g.dt_referencia
# MAGIC WHERE g.dt_referencia = (SELECT MAX(dt_referencia) FROM workspace.gold.perfil_societario_ti)
# MAGIC   AND s.flag_socio_pf
# MAGIC GROUP BY s.faixa_etaria, s.descricao_faixa_etaria
# MAGIC ORDER BY s.faixa_etaria;
