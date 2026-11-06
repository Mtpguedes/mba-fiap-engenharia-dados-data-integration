# Databricks notebook source
# /// script
# [tool.databricks.environment]
# environment_version = "6"
# ///
# MAGIC %md
# MAGIC # Silver — Sócios (Quadro Societário)
# MAGIC
# MAGIC **Origem:** `workspace.bronze.socios` (só o mês de `dt_referencia`) + `silver.dim_qualificacao` + `silver.dim_pais`
# MAGIC **Destino:** `workspace.silver.cnpj_socios_compliance`
# MAGIC **Depende de:** `silver_dominios`
# MAGIC **Grão:** um vínculo sócio × empresa por linha. Não existe chave natural única na base da Receita (o mesmo sócio pode aparecer com qualificações diferentes), por isso não há check de unicidade aqui.
# MAGIC
# MAGIC | Regra | Detalhe |
# MAGIC |---|---|
# MAGIC | Tipo de sócio | 1 Pessoa jurídica, 2 Pessoa física, 3 Estrangeiro |
# MAGIC | Faixa etária | Código 0–9 → rótulo (ex.: 3 → "21 a 30 anos") |
# MAGIC | `tempo_vinculo_meses` | `dt_referencia` − data de entrada na sociedade (nulo se a data for inválida ou futura) |
# MAGIC | Representante legal | Flag. O "CPF" vazio da Receita vem como `***000000**`, então só zeros e asteriscos contam como ausente |
# MAGIC | **LGPD** | Nome e CPF do sócio pessoa física **não passam para a silver**. Viram `id_socio_hash` (SHA-256), que permite contar o mesmo sócio em várias empresas sem expor quem é. Nome e documento do representante também saem. O CNPJ de sócio pessoa jurídica fica, porque é dado público de empresa |
# MAGIC
# MAGIC *O hash é pseudonimização (LGPD art. 13), não anonimização: com o nome em mãos, dá para recalcular. Em produção, o hash usaria um "sal" guardado em secret scope.*

# COMMAND ----------

# MAGIC %run ./utils_pipeline

# COMMAND ----------

DT_REFERENCIA = obter_dt_referencia()
TABELA_DESTINO = "workspace.silver.cnpj_socios_compliance"

print(f"dt_referencia: {DT_REFERENCIA}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1. Leitura e padronização

# COMMAND ----------

df_bronze = ler_bronze("socios", DT_REFERENCIA)

df_limpo = df_bronze.select(
    F.upper(F.trim("cnpj_basico")).alias("cnpj_basico"),
    F.trim("identificador_socio").alias("cod_tipo_socio"),
    F.upper(F.trim("nome_socio")).alias("_nome"),
    F.trim("cnpj_cpf_socio").alias("_documento"),
    F.lpad(F.trim("cod_qualificacao_socio"), 2, "0").alias("cod_qualificacao_socio"),
    F.expr("CAST(try_to_timestamp(nullif(trim(data_entrada_sociedade), ''), 'yyyyMMdd') AS DATE)")
     .alias("dt_entrada_sociedade"),
    F.when(F.trim("cod_pais") != "", F.lpad(F.trim("cod_pais"), 3, "0")).alias("cod_pais"),
    F.trim("representante_legal").alias("_doc_representante"),
    F.trim("nome_representante").alias("_nome_representante"),
    F.lpad(F.trim("cod_qualificacao_representante"), 2, "0").alias("cod_qualificacao_representante"),
    F.trim("faixa_etaria").alias("faixa_etaria"),
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2. Regras de negócio, LGPD e enriquecimento

# COMMAND ----------

tipos_socio = {"1": "Pessoa jurídica", "2": "Pessoa física", "3": "Estrangeiro"}
faixas_etarias = {
    "0": "Não se aplica", "1": "0 a 12 anos", "2": "13 a 20 anos", "3": "21 a 30 anos",
    "4": "31 a 40 anos", "5": "41 a 50 anos", "6": "51 a 60 anos", "7": "61 a 70 anos",
    "8": "71 a 80 anos", "9": "Mais de 80 anos",
}
mapa_tipo = F.create_map(*[F.lit(x) for par in tipos_socio.items() for x in par])
mapa_faixa = F.create_map(*[F.lit(x) for par in faixas_etarias.items() for x in par])

dt_ref = F.lit(DT_REFERENCIA).cast("date")
eh_pj = F.col("cod_tipo_socio") == "1"

doc_rep = F.coalesce(F.col("_doc_representante"), F.lit(""))
nome_rep = F.coalesce(F.col("_nome_representante"), F.lit(""))

qualif_socio = spark.table("workspace.silver.dim_qualificacao").select(
    F.col("cod_qualificacao").alias("cod_qualificacao_socio"),
    F.col("desc_qualificacao").alias("desc_qualificacao_socio"),
)
qualif_rep = spark.table("workspace.silver.dim_qualificacao").select(
    F.col("cod_qualificacao").alias("cod_qualificacao_representante"),
    F.col("desc_qualificacao").alias("desc_qualificacao_representante"),
)
pais = spark.table("workspace.silver.dim_pais").select("cod_pais", "desc_pais")

df_silver = (
    df_limpo
    .withColumn("dt_referencia", dt_ref)
    .withColumn("tipo_socio", F.coalesce(mapa_tipo[F.col("cod_tipo_socio")], F.lit("Não identificado")))
    .withColumn("flag_socio_pj", eh_pj)
    .withColumn("flag_socio_pf", F.col("cod_tipo_socio") == "2")
    .withColumn("flag_socio_estrangeiro", F.col("cod_tipo_socio") == "3")
    # LGPD: pessoa física/estrangeiro → só o hash; pessoa jurídica → CNPJ aberto
    .withColumn("id_socio_hash",
                F.sha2(F.concat_ws("|", F.coalesce("_nome", F.lit("")), F.coalesce("_documento", F.lit(""))), 256))
    .withColumn("cnpj_socio_pj", F.when(eh_pj, F.col("_documento")))
    .withColumn("flag_possui_representante",
                (nome_rep != "") | ((doc_rep != "") & ~doc_rep.rlike(r"^[0*.\-/\s]+$")))
    .withColumn("descricao_faixa_etaria",
                F.coalesce(mapa_faixa[F.col("faixa_etaria")], F.lit("Não informada ou inválida")))
    .withColumn("flag_data_entrada_invalida", F.col("dt_entrada_sociedade").isNull())
    .withColumn("flag_data_entrada_futura", F.coalesce(F.col("dt_entrada_sociedade") > dt_ref, F.lit(False)))
    .withColumn("tempo_vinculo_meses",
                F.when(F.col("dt_entrada_sociedade") <= dt_ref,
                       F.floor(F.months_between(dt_ref, F.col("dt_entrada_sociedade"))).cast("int")))
    .join(F.broadcast(qualif_socio), on="cod_qualificacao_socio", how="left")
    .join(F.broadcast(qualif_rep), on="cod_qualificacao_representante", how="left")
    .join(F.broadcast(pais), on="cod_pais", how="left")
)

colunas_finais = [
    "dt_referencia", "cnpj_basico", "cod_tipo_socio", "tipo_socio", "id_socio_hash", "cnpj_socio_pj",
    "flag_socio_pf", "flag_socio_pj", "flag_socio_estrangeiro",
    "cod_qualificacao_socio", "desc_qualificacao_socio",
    "dt_entrada_sociedade", "tempo_vinculo_meses", "flag_data_entrada_invalida", "flag_data_entrada_futura",
    "cod_pais", "desc_pais",
    "flag_possui_representante", "cod_qualificacao_representante", "desc_qualificacao_representante",
    "faixa_etaria", "descricao_faixa_etaria",
]

display(df_silver.select(*colunas_finais).limit(10))

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3. Quality checks
# MAGIC
# MAGIC O check de contagem bronze x silver protege contra código duplicado nos domínios: se a tabela de qualificações tivesse o mesmo código duas vezes, o join duplicaria vínculos.

# COMMAND ----------

with Cronometro("silver_socios", "checks", DT_REFERENCIA):
    total_bronze = df_bronze.count()
    total_silver = df_silver.count()

    executar_checks(
        df_silver, TABELA_DESTINO, DT_REFERENCIA,
        regras=[
            regra("cnpj_basico com 8 caracteres alfanuméricos (CNPJ alfanumérico desde 07/2026)", "bloqueia",
                  ~F.coalesce(F.col("cnpj_basico"), F.lit("")).rlike(r"^[A-Z0-9]{8}$")),
            regra("tipo de sócio no domínio (1,2,3)", "bloqueia", ~F.col("tipo_socio").isin(*tipos_socio.values())),
            regra("faixa etária no domínio (0–9)", "alerta",
                  F.col("descricao_faixa_etaria") == "Não informada ou inválida", limite_pct=0.1),
            regra("data de entrada inválida", "alerta", F.col("flag_data_entrada_invalida"), limite_pct=1.0),
            regra("data de entrada futura", "alerta", F.col("flag_data_entrada_futura"), limite_pct=0.1),
            regra("qualificação fora do domínio", "alerta",
                  F.col("desc_qualificacao_socio").isNull(), limite_pct=0.1),
            regra("sócio PJ sem CNPJ válido (12 alfanuméricos + 2 dígitos)", "alerta",
                  F.col("flag_socio_pj")
                  & ~F.coalesce(F.col("cnpj_socio_pj"), F.lit("")).rlike(r"^[A-Z0-9]{12}[0-9]{2}$"),
                  limite_pct=0.1),
        ],
        resultados_extras=[
            resultado("tabela não vazia", "bloqueia", 0 if total_silver > 0 else 1, 1),
            check_contagem(total_bronze, total_silver, "mesma contagem bronze x silver"),
            check_integridade(
                df_silver,
                ler_bronze("empresas", DT_REFERENCIA).select(F.upper(F.trim("cnpj_basico")).alias("cnpj_basico")),
                ["cnpj_basico"], "vínculo sem empresa correspondente", total=total_silver, limite_pct=0.1,
            ),
        ],
    )

# COMMAND ----------

# MAGIC %md
# MAGIC ## 4. Gravação

# COMMAND ----------

with Cronometro("silver_socios", "gravacao", DT_REFERENCIA):
    gravar_particao(df_silver.select(*colunas_finais), TABELA_DESTINO, DT_REFERENCIA)

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT
# MAGIC     tipo_socio,
# MAGIC     COUNT(*)                              AS vinculos,
# MAGIC     COUNT(DISTINCT id_socio_hash)         AS socios_distintos,
# MAGIC     COUNT(DISTINCT cnpj_basico)           AS empresas,
# MAGIC     COUNT_IF(flag_possui_representante)   AS com_representante
# MAGIC FROM workspace.silver.cnpj_socios_compliance
# MAGIC WHERE dt_referencia = (SELECT MAX(dt_referencia) FROM workspace.silver.cnpj_socios_compliance)
# MAGIC GROUP BY tipo_socio
# MAGIC ORDER BY vinculos DESC;