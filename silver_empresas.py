# Databricks notebook source
# MAGIC %md
# MAGIC # Silver — Empresas (com Simples/MEI)
# MAGIC
# MAGIC **Origem:** `workspace.bronze.empresas` + `workspace.bronze.simples` (só o mês de `dt_referencia`) + `silver.dim_natureza_juridica`
# MAGIC **Depende de:** `silver_dominios`
# MAGIC **Destino:** `workspace.silver.cnpj_empresas`
# MAGIC **Grão:** uma empresa por linha (`cnpj_basico`).
# MAGIC
# MAGIC | Regra | Detalhe |
# MAGIC |---|---|
# MAGIC | `capital_social` | Vem como texto com vírgula decimal (`"1000,00"`) → `decimal(20,2)` |
# MAGIC | Porte | 00 Não informado, 01 Micro, 03 EPP, 05 Demais |
# MAGIC | Grupo da natureza jurídica | 1º dígito do código: 1 Adm. Pública, 2 Entidade Empresarial, 3 Sem Fins Lucrativos, 4 Pessoa Física, 5 Org. Internacional |
# MAGIC | Simples / MEI | `left join` por `cnpj_basico`; sem registro no arquivo = não optante |
# MAGIC | LGPD | **`razao_social` sai.** Em MEI, a razão social é o nome da pessoa seguido do CPF |

# COMMAND ----------

# MAGIC %run ./utils_pipeline

# COMMAND ----------

DT_REFERENCIA = obter_dt_referencia()
TABELA_DESTINO = "workspace.silver.cnpj_empresas"

print(f"dt_referencia: {DT_REFERENCIA}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1. Leitura e limpeza

# COMMAND ----------

df_bronze = ler_bronze("empresas", DT_REFERENCIA)


def para_data(coluna):
    data = F.expr(f"CAST(try_to_timestamp(nullif(trim({coluna}), ''), 'yyyyMMdd') AS DATE)")
    return F.when(data >= F.lit("1900-01-01").cast("date"), data)


# capital_social: remove ponto de milhar (se houver), troca vírgula decimal por ponto, try_cast evita quebra
df_empresas = df_bronze.select(
    F.upper(F.trim("cnpj_basico")).alias("cnpj_basico"),
    F.lpad(F.trim("cod_natureza_juridica"), 4, "0").alias("cod_natureza_juridica"),
    F.lpad(F.trim("cod_qualificacao_responsavel"), 2, "0").alias("cod_qualificacao_responsavel"),
    F.expr("try_cast(nullif(regexp_replace(regexp_replace(trim(capital_social), '\\\\.', ''), ',', '.'), '') AS DECIMAL(20,2))")
     .alias("capital_social"),
    F.lpad(F.coalesce(F.nullif(F.trim("porte_empresa"), F.lit("")), F.lit("00")), 2, "0").alias("cod_porte"),
    F.nullif(F.trim("ente_federativo_responsavel"), F.lit("")).alias("ente_federativo_responsavel"),
    F.trim("capital_social").alias("_capital_original"),
)

df_natureza = spark.table("workspace.silver.dim_natureza_juridica").select(
    "cod_natureza_juridica", "desc_natureza_juridica", "grupo_natureza_juridica"
)

df_simples = (
    ler_bronze("simples", DT_REFERENCIA)
    .select(
        F.upper(F.trim("cnpj_basico")).alias("cnpj_basico"),
        (F.upper(F.trim("opcao_simples")) == "S").alias("flag_simples"),
        para_data("data_opcao_simples").alias("dt_opcao_simples"),
        para_data("data_exclusao_simples").alias("dt_exclusao_simples"),
        (F.upper(F.trim("opcao_mei")) == "S").alias("flag_mei"),
        para_data("data_opcao_mei").alias("dt_opcao_mei"),
        para_data("data_exclusao_mei").alias("dt_exclusao_mei"),
    )
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2. Regras de negócio e enriquecimento

# COMMAND ----------

portes = {"00": "Não informado", "01": "Micro empresa", "03": "Empresa de pequeno porte", "05": "Demais"}
mapa_porte = F.create_map(*[F.lit(x) for par in portes.items() for x in par])

df_silver = (
    df_empresas
    .join(F.broadcast(df_natureza), on="cod_natureza_juridica", how="left")
    .join(df_simples, on="cnpj_basico", how="left")
    .withColumn("dt_referencia", F.lit(DT_REFERENCIA).cast("date"))
    .withColumn("desc_porte", F.coalesce(mapa_porte[F.col("cod_porte")], F.lit("Não mapeado")))
    .withColumn("grupo_natureza_juridica", F.coalesce("grupo_natureza_juridica", F.lit("Não mapeado")))
    .withColumn("flag_simples", F.coalesce("flag_simples", F.lit(False)))
    .withColumn("flag_mei", F.coalesce("flag_mei", F.lit(False)))
)

colunas_finais = [
    "dt_referencia", "cnpj_basico",
    "cod_natureza_juridica", "desc_natureza_juridica", "grupo_natureza_juridica",
    "cod_qualificacao_responsavel", "capital_social", "cod_porte", "desc_porte",
    "ente_federativo_responsavel",
    "flag_simples", "dt_opcao_simples", "dt_exclusao_simples",
    "flag_mei", "dt_opcao_mei", "dt_exclusao_mei",
]

display(df_silver.select(*colunas_finais).limit(10))

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3. Quality checks
# MAGIC
# MAGIC O check de contagem bronze x silver também protege contra um erro silencioso: se o arquivo do Simples tiver o mesmo `cnpj_basico` duas vezes, o `left join` duplica a empresa.

# COMMAND ----------

with Cronometro("silver_empresas", "checks", DT_REFERENCIA):
    total_bronze = df_bronze.count()
    total_silver = df_silver.count()

    executar_checks(
        df_silver, TABELA_DESTINO, DT_REFERENCIA,
        regras=[
            regra("cnpj_basico com 8 caracteres alfanuméricos (CNPJ alfanumérico desde 07/2026)", "bloqueia",
                  ~F.coalesce(F.col("cnpj_basico"), F.lit("")).rlike(r"^[A-Z0-9]{8}$")),
            regra("porte no domínio (00,01,03,05)", "alerta",
                  ~F.col("cod_porte").isin(*portes.keys()), limite_pct=0.1),
            regra("capital social preenchido mas não convertido", "alerta",
                  (F.coalesce(F.col("_capital_original"), F.lit("")) != "") & F.col("capital_social").isNull(),
                  limite_pct=0.1),
            regra("capital social negativo", "alerta", F.col("capital_social") < 0),
            regra("natureza jurídica fora do domínio", "alerta",
                  F.col("desc_natureza_juridica").isNull(), limite_pct=0.1),
            regra("MEI que não é optante do Simples", "alerta",
                  F.col("flag_mei") & ~F.col("flag_simples"), limite_pct=1.0),
        ],
        resultados_extras=[
            resultado("tabela não vazia", "bloqueia", 0 if total_silver > 0 else 1, 1),
            check_contagem(total_bronze, total_silver, "mesma contagem bronze x silver"),
            check_unicidade(df_silver, ["cnpj_basico"]),
        ],
    )

# COMMAND ----------

# MAGIC %md
# MAGIC ## 4. Gravação

# COMMAND ----------

with Cronometro("silver_empresas", "gravacao", DT_REFERENCIA):
    gravar_particao(df_silver.select(*colunas_finais), TABELA_DESTINO, DT_REFERENCIA)

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT
# MAGIC     desc_porte,
# MAGIC     COUNT(*)                                        AS empresas,
# MAGIC     COUNT_IF(flag_mei)                              AS mei,
# MAGIC     COUNT_IF(flag_simples)                          AS simples,
# MAGIC     PERCENTILE_APPROX(CAST(capital_social AS DOUBLE), 0.5) AS capital_social_mediano
# MAGIC FROM workspace.silver.cnpj_empresas
# MAGIC WHERE dt_referencia = (SELECT MAX(dt_referencia) FROM workspace.silver.cnpj_empresas)
# MAGIC GROUP BY desc_porte
# MAGIC ORDER BY empresas DESC;
