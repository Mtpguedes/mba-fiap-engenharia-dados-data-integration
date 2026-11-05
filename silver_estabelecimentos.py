# Databricks notebook source
# /// script
# [tool.databricks.environment]
# environment_version = "6"
# ///
# MAGIC %md
# MAGIC # Silver — Estabelecimentos
# MAGIC
# MAGIC **Origem:** `workspace.bronze.estabelecimentos` (só o mês de `dt_referencia`) + `silver.dim_cnae` + `silver.dim_municipio`
# MAGIC **Destino:** `workspace.silver.cnpj_estabelecimentos`
# MAGIC **Depende de:** `silver_dominios`
# MAGIC **Grão:** um estabelecimento por linha (`cnpj_basico + cnpj_ordem + cnpj_dv`), Brasil inteiro.
# MAGIC **Consumidores:** Demografia (Matheus), Geografia (Rafael), Mortalidade (Helen).
# MAGIC
# MAGIC | Regra | Detalhe |
# MAGIC |---|---|
# MAGIC | Códigos padronizados | `situacao_cadastral` e motivo com 2 dígitos, CNAE com 7 dígitos (`lpad`) |
# MAGIC | Datas | `yyyyMMdd` → `date` com `try_to_timestamp + cast`; `0`, vazio, inválido ou anterior a 1900 viram nulo e ganham flag |
# MAGIC | `idade_meses` | Baixada (08): data da baixa − início (nula se a data da baixa faltar ou vier antes do início). Demais: `dt_referencia` − início |
# MAGIC | `flag_ti` | Vem da `silver.dim_cnae` (CNAE principal começando com `62` ou `631`) |
# MAGIC | Localização | `cod_municipio` com 4 dígitos + `desc_municipio` (da `dim_municipio`) + `regiao` derivada da UF |
# MAGIC | LGPD | Saem logradouro, número, complemento, CEP completo, telefones, fax, e-mail e nome fantasia. Ficam `cep_5` e `bairro` |

# COMMAND ----------

# MAGIC %run ./utils_pipeline

# COMMAND ----------

DT_REFERENCIA = obter_dt_referencia()
TABELA_DESTINO = "workspace.silver.cnpj_estabelecimentos"
TABELA_DIM_CNAE = "workspace.silver.dim_cnae"
TABELA_DIM_MUNICIPIO = "workspace.silver.dim_municipio"

print(f"dt_referencia: {DT_REFERENCIA}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2. Limpeza e padronização

# COMMAND ----------

df_bronze = ler_bronze("estabelecimentos", DT_REFERENCIA)


def para_data(coluna):
    """yyyyMMdd → date. Valores inválidos ou anteriores a 1900 viram nulo."""
    data = F.expr(f"CAST(try_to_timestamp(nullif(trim({coluna}), ''), 'yyyyMMdd') AS DATE)")
    return F.when(data >= F.lit("1900-01-01").cast("date"), data)


df_limpo = (
    df_bronze
    .select(
        F.upper(F.trim("cnpj_basico")).alias("cnpj_basico"),
        F.upper(F.trim("cnpj_ordem")).alias("cnpj_ordem"),
        F.trim("cnpj_dv").alias("cnpj_dv"),
        F.trim("identificador_matriz_filial").alias("identificador_matriz_filial"),
        F.lpad(F.trim("situacao_cadastral"), 2, "0").alias("cod_situacao"),
        para_data("data_situacao_cadastral").alias("dt_situacao_cadastral"),
        F.lpad(F.trim("cod_motivo_situacao_cadastral"), 2, "0").alias("cod_motivo_situacao"),
        F.trim("situacao_especial").alias("situacao_especial"),
        para_data("data_situacao_especial").alias("dt_situacao_especial"),
        para_data("data_inicio_atividade").alias("dt_inicio_atividade"),
        F.lpad(F.trim("cnae_fiscal_principal"), 7, "0").alias("cnae_principal"),
        F.nullif(F.trim("cnae_fiscal_secundaria"), F.lit("")).alias("cnae_secundaria"),
        F.upper(F.trim("uf")).alias("uf"),
        F.lpad(F.trim("cod_municipio"), 4, "0").alias("cod_municipio"),
        F.trim("bairro").alias("bairro"),
        F.substring(F.regexp_replace("cep", r"\D", ""), 1, 5).alias("cep_5"),
        F.when(F.trim("cod_pais") != "", F.lpad(F.trim("cod_pais"), 3, "0")).alias("cod_pais"),
        F.trim("nome_cidade_exterior").alias("nome_cidade_exterior"),
        # Guardamos o texto original só para os checks de conversão de data
        F.trim("data_inicio_atividade").alias("_dt_inicio_original"),
    )
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3. Regras de negócio

# COMMAND ----------

situacoes = {"01": "Nula", "02": "Ativa", "03": "Suspensa", "04": "Inapta", "08": "Baixada"}
mapa_situacao = F.create_map(*[F.lit(x) for par in situacoes.items() for x in par])

regioes = {
    "Norte": ["AC", "AM", "AP", "PA", "RO", "RR", "TO"],
    "Nordeste": ["AL", "BA", "CE", "MA", "PB", "PE", "PI", "RN", "SE"],
    "Centro-Oeste": ["DF", "GO", "MS", "MT"],
    "Sudeste": ["ES", "MG", "RJ", "SP"],
    "Sul": ["PR", "RS", "SC"],
}
mapa_regiao = F.create_map(*[F.lit(x) for regiao, ufs in regioes.items() for uf in ufs for x in (uf, regiao)])

dt_ref = F.lit(DT_REFERENCIA).cast("date")
eh_baixada = F.col("cod_situacao") == "08"

# Fim da "vida" contada:
#   - baixada: data da baixa (se for nula ou anterior ao início, não sabemos quando fechou → idade nula)
#   - demais situações: data de referência
dt_fim_idade = F.when(eh_baixada, F.col("dt_situacao_cadastral")).otherwise(dt_ref)

df_silver = (
    df_limpo
    .withColumn("dt_referencia", dt_ref)
    .withColumn("cnpj_completo", F.concat("cnpj_basico", "cnpj_ordem", "cnpj_dv"))
    .withColumn("flag_matriz", F.col("identificador_matriz_filial") == "1")
    .withColumn("desc_situacao", F.coalesce(mapa_situacao[F.col("cod_situacao")], F.lit("Não mapeada")))
    .withColumn("flag_ativa", F.col("cod_situacao") == "02")
    .withColumn("ano_abertura", F.year("dt_inicio_atividade"))
    .withColumn(
        "flag_dt_inicio_invalida",
        F.col("dt_inicio_atividade").isNull(),
    )
    .withColumn(
        "flag_dt_inicio_futura",
        F.coalesce(F.col("dt_inicio_atividade") > dt_ref, F.lit(False)),
    )
    .withColumn(
        "idade_meses",
        F.when(
            F.col("dt_inicio_atividade").isNotNull() & (F.col("dt_inicio_atividade") <= dt_fim_idade),
            F.floor(F.months_between(dt_fim_idade, F.col("dt_inicio_atividade"))).cast("int"),
        ),
    )
    .withColumn(
        "qtd_cnaes_secundarios",
        F.when(F.col("cnae_secundaria").isNull(), F.lit(0))
        .otherwise(F.size(F.split("cnae_secundaria", ","))),
    )
    .join(
        F.broadcast(spark.table(TABELA_DIM_CNAE).select(
            F.col("cod_cnae").alias("cnae_principal"),
            F.col("desc_cnae").alias("desc_cnae_principal"),
            F.col("flag_ti"),
        )),
        on="cnae_principal",
        how="left",
    )
    .withColumn("flag_ti", F.coalesce("flag_ti", F.lit(False)))
    .join(
        F.broadcast(spark.table(TABELA_DIM_MUNICIPIO).select("cod_municipio", "desc_municipio")),
        on="cod_municipio",
        how="left",
    )
    .withColumn("regiao", F.coalesce(mapa_regiao[F.col("uf")], F.lit("Exterior/Não informada")))
)

colunas_finais = [
    "dt_referencia", "cnpj_basico", "cnpj_ordem", "cnpj_dv", "cnpj_completo",
    "flag_matriz", "cod_situacao", "desc_situacao", "flag_ativa", "dt_situacao_cadastral",
    "cod_motivo_situacao", "situacao_especial", "dt_situacao_especial",
    "dt_inicio_atividade", "ano_abertura", "idade_meses",
    "flag_dt_inicio_invalida", "flag_dt_inicio_futura",
    "cnae_principal", "desc_cnae_principal", "flag_ti", "cnae_secundaria", "qtd_cnaes_secundarios",
    "regiao", "uf", "cod_municipio", "desc_municipio", "bairro", "cep_5", "cod_pais", "nome_cidade_exterior",
]

display(df_silver.select(*colunas_finais).limit(10))

# COMMAND ----------

# MAGIC %md
# MAGIC ## 4. Quality checks
# MAGIC
# MAGIC Rodam **antes** da gravação. Se um check bloqueante falhar, o notebook para e a silver do mês não é escrita.

# COMMAND ----------

def regras_estabelecimentos(dt_referencia):
    dt = F.lit(dt_referencia).cast("date")
    texto_data_preenchido = ~F.coalesce(F.col("_dt_inicio_original"), F.lit("")).isin("", "0", "00000000")
    return [
        regra("chave cnpj não nula", "bloqueia",
              F.col("cnpj_basico").isNull() | F.col("cnpj_ordem").isNull() | F.col("cnpj_dv").isNull()),
        # Desde 31/07/2026 a Receita emite CNPJ alfanumérico: 12 posições A-Z/0-9 + 2 dígitos verificadores
        regra("cnpj_completo no formato (12 alfanuméricos + 2 dígitos)", "bloqueia",
              ~F.coalesce(F.col("cnpj_completo"), F.lit("")).rlike(r"^[A-Z0-9]{12}[0-9]{2}$")),
        regra("situação cadastral no domínio (01,02,03,04,08)", "bloqueia",
              ~F.col("cod_situacao").isin(*situacoes.keys()) | F.col("cod_situacao").isNull()),
        regra("matriz/filial no domínio (1,2)", "bloqueia",
              ~F.coalesce(F.col("identificador_matriz_filial"), F.lit("")).isin("1", "2")),
        regra("data de início preenchida mas não convertida", "alerta",
              texto_data_preenchido & F.col("dt_inicio_atividade").isNull(), limite_pct=1.0),
        regra("data de início posterior à data de referência", "alerta",
              F.col("dt_inicio_atividade") > dt, limite_pct=0.1),
        regra("data de situação anterior à data de início", "alerta",
              F.col("dt_situacao_cadastral") < F.col("dt_inicio_atividade"), limite_pct=0.1),
        regra("CNAE principal fora do domínio", "alerta",
              F.col("desc_cnae_principal").isNull(), limite_pct=0.1),
        regra("UF vazia", "alerta",
              F.coalesce(F.col("uf"), F.lit("")) == "", limite_pct=0.1),
        regra("município fora do domínio", "alerta",
              F.col("desc_municipio").isNull() & (F.coalesce(F.col("uf"), F.lit("EX")) != "EX"), limite_pct=0.1),
    ]

# COMMAND ----------

# MAGIC %md
# MAGIC ### 4.1 Demonstração: o que acontece quando um check falha
# MAGIC
# MAGIC Montamos uma amostra de 5 linhas reais e injetamos dois erros: **uma linha duplicada** e **uma situação cadastral `99`**. O resultado esperado é o log com `FALHA` e o erro que impede a gravação. O log da demonstração não é salvo.

# COMMAND ----------

amostra = df_silver.limit(5).collect()
colunas = df_silver.columns

if len(amostra) < 2:
    print("Demonstração pulada: não há linhas suficientes neste mês.")
else:
    linha_invalida = amostra[1].asDict()
    linha_invalida["cod_situacao"] = "99"

    df_teste = spark.createDataFrame(
        [tuple(r) for r in amostra]
        + [tuple(amostra[0])]                               # duplicada
        + [tuple(linha_invalida[c] for c in colunas)],      # situação inválida
        schema=df_silver.schema,
    )

    try:
        executar_checks(
            df_teste, "DEMONSTRACAO_FALHA", DT_REFERENCIA,
            regras=regras_estabelecimentos(DT_REFERENCIA),
            resultados_extras=[check_unicidade(df_teste, ["cnpj_basico", "cnpj_ordem", "cnpj_dv"])],
            gravar_log=False,
        )
    except ValueError as erro:
        print(f"Comportamento esperado — a gravação foi bloqueada:\n{erro}")

# COMMAND ----------

# MAGIC %md
# MAGIC ### 4.2 Checks na carga real

# COMMAND ----------

with Cronometro("silver_estabelecimentos", "checks", DT_REFERENCIA):
    total_bronze = df_bronze.count()
    total_silver = df_silver.count()

    executar_checks(
        df_silver, TABELA_DESTINO, DT_REFERENCIA,
        regras=regras_estabelecimentos(DT_REFERENCIA),
        resultados_extras=[
            resultado("tabela não vazia", "bloqueia", 0 if total_silver > 0 else 1, 1),
            check_contagem(total_bronze, total_silver, "mesma contagem bronze x silver"),
            check_unicidade(df_silver, ["cnpj_basico", "cnpj_ordem", "cnpj_dv"]),
            check_integridade(
                df_silver,
                ler_bronze("empresas", DT_REFERENCIA).select(F.upper(F.trim("cnpj_basico")).alias("cnpj_basico")),
                ["cnpj_basico"], "estabelecimento sem empresa correspondente",
                total=total_silver, limite_pct=0.1,
            ),
        ],
    )

# COMMAND ----------

# MAGIC %md
# MAGIC ## 5. Gravação

# COMMAND ----------

with Cronometro("silver_estabelecimentos", "gravacao", DT_REFERENCIA):
    gravar_particao(df_silver.select(*colunas_finais), TABELA_DESTINO, DT_REFERENCIA)

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT
# MAGIC     desc_situacao,
# MAGIC     COUNT(*)                                   AS estabelecimentos,
# MAGIC     COUNT_IF(flag_matriz)                      AS matrizes,
# MAGIC     COUNT_IF(flag_ti)                          AS estabelecimentos_ti,
# MAGIC     ROUND(AVG(idade_meses) / 12, 1)            AS idade_media_anos
# MAGIC FROM workspace.silver.cnpj_estabelecimentos
# MAGIC WHERE dt_referencia = (SELECT MAX(dt_referencia) FROM workspace.silver.cnpj_estabelecimentos)
# MAGIC GROUP BY desc_situacao
# MAGIC ORDER BY estabelecimentos DESC;