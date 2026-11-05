# Databricks notebook source
# MAGIC %md
# MAGIC # Gold — Risco de Descontinuidade Operacional do Setor de TI (v2, proposta)
# MAGIC
# MAGIC > **Proposta para a frente de Mortalidade (Helen)**, substituindo a `gold_situ_cadastral_TI`. Mudanças:
# MAGIC > - Lê da `silver.cnpj_status_cadastral` (v2), que tem uma empresa por linha e a idade padronizada.
# MAGIC > - Faixas de idade mais finas nos primeiros anos, que é onde a mortalidade se concentra (antes: "3 a 20 anos" numa faixa só). O rótulo "até 50" aplicado a empresas com 50 anos ou mais foi corrigido.
# MAGIC > - A regra de TI vem da `dim_cnae` (via silver), não de uma lista escrita no código.
# MAGIC
# MAGIC **Pergunta de negócio:** quantas empresas de TI fecham ou entram em situação irregular, com que idade e por quê?
# MAGIC
# MAGIC **Fronteira com a Demografia:** esta gold trata de **saídas** (baixa, suspensão, inaptidão). Entradas e estoque ficam na `gold.demografia_ti`.
# MAGIC
# MAGIC **Origem:** `silver.cnpj_status_cadastral`
# MAGIC **Destino:** `workspace.gold.situacao_cadastral_ti`
# MAGIC **Grão:** `dt_referencia × ano_situacao × desc_situacao × desc_motivo × faixa_idade × regiao`, só empresas de TI **não ativas**.

# COMMAND ----------

# MAGIC %run ./utils_pipeline

# COMMAND ----------

DT_REFERENCIA = obter_dt_referencia()
TABELA_DESTINO = "workspace.gold.situacao_cadastral_ti"

print(f"dt_referencia: {DT_REFERENCIA}")

# COMMAND ----------

df_saidas = (
    spark.table("workspace.silver.cnpj_status_cadastral")
    .filter((F.col("dt_referencia") == DT_REFERENCIA) & F.col("flag_ti") & ~F.col("flag_ativa"))
    .withColumn("desc_motivo", F.coalesce("desc_motivo", F.lit("Não informado")))
)

dimensoes = ["dt_referencia", "ano_situacao", "desc_situacao", "desc_motivo", "faixa_idade", "regiao"]

df_gold = (
    df_saidas
    .groupBy(*dimensoes)
    .agg(
        F.count(F.lit(1)).alias("qtd_empresas"),
        F.round(F.avg("idade_meses"), 1).alias("idade_media_meses"),
        F.percentile_approx("idade_meses", 0.5).alias("idade_mediana_meses"),
    )
)

display(df_gold.orderBy(F.desc("qtd_empresas")).limit(20))

# COMMAND ----------

with Cronometro("gold_situacao_cadastral_ti_v2", "checks", DT_REFERENCIA):
    total_saidas = df_saidas.count()
    soma_gold = df_gold.agg(F.sum("qtd_empresas")).first()[0] or 0

    executar_checks(
        df_gold, TABELA_DESTINO, DT_REFERENCIA,
        regras=[
            regra("idade mediana negativa", "bloqueia", F.col("idade_mediana_meses") < 0),
            regra("grupo sem ano da situação", "alerta", F.col("ano_situacao").isNull(), limite_pct=1.0),
        ],
        resultados_extras=[
            resultado("gold não vazia", "bloqueia", 0 if total_saidas > 0 else 1, 1),
            check_unicidade(df_gold, dimensoes),
            check_contagem(total_saidas, soma_gold, "reconciliação: soma da gold = saídas de TI na silver"),
        ],
    )

with Cronometro("gold_situacao_cadastral_ti_v2", "gravacao", DT_REFERENCIA):
    gravar_particao(df_gold, TABELA_DESTINO, DT_REFERENCIA)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Consultas de negócio

# COMMAND ----------

# MAGIC %md
# MAGIC ### 1. Empresas de TI encerradas por ano e idade mediana no fechamento

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT
# MAGIC     ano_situacao                                                       AS ano_baixa,
# MAGIC     SUM(qtd_empresas)                                                  AS empresas_baixadas,
# MAGIC     ROUND(SUM(idade_media_meses * qtd_empresas) / SUM(qtd_empresas) / 12, 1) AS idade_media_anos_na_baixa
# MAGIC FROM workspace.gold.situacao_cadastral_ti
# MAGIC WHERE dt_referencia = (SELECT MAX(dt_referencia) FROM workspace.gold.situacao_cadastral_ti)
# MAGIC   AND desc_situacao = 'Baixada'
# MAGIC   AND ano_situacao BETWEEN 2015 AND 2026
# MAGIC GROUP BY ano_situacao
# MAGIC ORDER BY ano_situacao;

# COMMAND ----------

# MAGIC %md
# MAGIC ### 2. Com que idade as empresas de TI fecham

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT
# MAGIC     faixa_idade,
# MAGIC     SUM(qtd_empresas)                                           AS empresas_baixadas,
# MAGIC     ROUND(100.0 * SUM(qtd_empresas) / SUM(SUM(qtd_empresas)) OVER (), 2) AS pct
# MAGIC FROM workspace.gold.situacao_cadastral_ti
# MAGIC WHERE dt_referencia = (SELECT MAX(dt_referencia) FROM workspace.gold.situacao_cadastral_ti)
# MAGIC   AND desc_situacao = 'Baixada'
# MAGIC GROUP BY faixa_idade
# MAGIC ORDER BY faixa_idade;

# COMMAND ----------

# MAGIC %md
# MAGIC ### 3. Principais motivos de baixa, suspensão e inaptidão

# COMMAND ----------

# MAGIC %sql
# MAGIC WITH por_motivo AS (
# MAGIC     SELECT
# MAGIC         desc_situacao,
# MAGIC         desc_motivo,
# MAGIC         SUM(qtd_empresas) AS empresas
# MAGIC     FROM workspace.gold.situacao_cadastral_ti
# MAGIC     WHERE dt_referencia = (SELECT MAX(dt_referencia) FROM workspace.gold.situacao_cadastral_ti)
# MAGIC     GROUP BY desc_situacao, desc_motivo
# MAGIC ),
# MAGIC ranqueado AS (
# MAGIC     SELECT *, ROW_NUMBER() OVER (PARTITION BY desc_situacao ORDER BY empresas DESC) AS posicao
# MAGIC     FROM por_motivo
# MAGIC )
# MAGIC SELECT desc_situacao, desc_motivo, empresas
# MAGIC FROM ranqueado
# MAGIC WHERE posicao <= 5
# MAGIC ORDER BY desc_situacao, empresas DESC;

# COMMAND ----------

# MAGIC %md
# MAGIC ### 4. Sobrevivência por coorte: % das empresas de TI abertas em cada ano que fecharam em até 3 anos
# MAGIC
# MAGIC Lê da silver porque precisa do ano de abertura de cada empresa. Só coortes com 3 anos completos até a data de referência.

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT
# MAGIC     ano_abertura                                                        AS coorte,
# MAGIC     COUNT(*)                                                            AS empresas_abertas,
# MAGIC     COUNT_IF(flag_encerrada AND idade_meses < 36)                       AS fechadas_em_ate_3_anos,
# MAGIC     ROUND(100.0 * COUNT_IF(flag_encerrada AND idade_meses < 36) / COUNT(*), 2) AS mortalidade_3_anos_pct
# MAGIC FROM workspace.silver.cnpj_status_cadastral
# MAGIC WHERE dt_referencia = (SELECT MAX(dt_referencia) FROM workspace.silver.cnpj_status_cadastral)
# MAGIC   AND flag_ti
# MAGIC   AND ano_abertura BETWEEN 2010 AND YEAR(dt_referencia) - 4
# MAGIC GROUP BY ano_abertura
# MAGIC ORDER BY ano_abertura;
