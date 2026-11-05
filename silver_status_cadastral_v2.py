# Databricks notebook source
# MAGIC %md
# MAGIC # Silver — Status Cadastral (v2, proposta)
# MAGIC
# MAGIC > **Proposta para a frente de Mortalidade (Helen).** Mantém o objetivo da `silver_cnpj_status_cadastral` original e corrige quatro pontos:
# MAGIC > 1. **Filtro `cod_pais is not null`:** removido. `cod_pais` só vem preenchido para estabelecimentos no exterior, então o filtro descartava quase todas as empresas brasileiras.
# MAGIC > 2. **`ROW_NUMBER` por situação mais recente:** trocado pela **matriz**. O `ROW_NUMBER` podia escolher uma filial baixada para representar uma empresa ativa.
# MAGIC > 3. **Idade:** passa a vir da `silver.cnpj_estabelecimentos`, para existir **uma só definição de idade** no projeto (data de referência, não `CURRENT_DATE`; suspensas e inaptas não são tratadas como encerradas).
# MAGIC > 4. **Faixas de idade:** `FLOOR` em vez de `ROUND`. Com `ROUND`, 2,6 anos virava 3 e caía na faixa errada.
# MAGIC >
# MAGIC > Também passa a ter `dt_referencia`, quality checks e um nome que reflete o conteúdo (a tabela original se chamava `cnpj_socios_status_cadastral`, mas não tem dados de sócios).
# MAGIC
# MAGIC **Origem:** `silver.cnpj_estabelecimentos` + `silver.dim_motivo`
# MAGIC **Destino:** `workspace.silver.cnpj_status_cadastral`
# MAGIC **Grão:** uma empresa (matriz) por linha, Brasil inteiro, com `flag_ti`.

# COMMAND ----------

# MAGIC %run ./utils_pipeline

# COMMAND ----------

DT_REFERENCIA = obter_dt_referencia()
TABELA_DESTINO = "workspace.silver.cnpj_status_cadastral"

print(f"dt_referencia: {DT_REFERENCIA}")

# COMMAND ----------

df_matrizes = (
    spark.table("workspace.silver.cnpj_estabelecimentos")
    .filter((F.col("dt_referencia") == DT_REFERENCIA) & F.col("flag_matriz"))
)

df_motivo = spark.table("workspace.silver.dim_motivo").select(
    F.col("cod_motivo").alias("cod_motivo_situacao"), "desc_motivo"
)

idade_anos = F.col("idade_meses") / 12

df_silver = (
    df_matrizes
    .join(F.broadcast(df_motivo), on="cod_motivo_situacao", how="left")
    .withColumn("flag_encerrada", F.col("cod_situacao") == "08")
    .withColumn("flag_risco_cadastral", F.col("cod_situacao").isin("03", "04"))  # suspensa ou inapta
    .withColumn("ano_situacao", F.year("dt_situacao_cadastral"))
    .withColumn(
        "faixa_idade",
        F.when(F.col("idade_meses").isNull(), "Sem data válida")
        .when(idade_anos < 1, "01. Menos de 1 ano")
        .when(idade_anos < 2, "02. 1 a 2 anos")
        .when(idade_anos < 3, "03. 2 a 3 anos")
        .when(idade_anos < 5, "04. 3 a 5 anos")
        .when(idade_anos < 10, "05. 5 a 10 anos")
        .when(idade_anos < 20, "06. 10 a 20 anos")
        .otherwise("07. 20 anos ou mais"),
    )
    .select(
        "dt_referencia", "cnpj_basico", "flag_ti", "cnae_principal", "desc_cnae_principal",
        "cod_situacao", "desc_situacao", "flag_ativa", "flag_risco_cadastral", "flag_encerrada",
        "cod_motivo_situacao", "desc_motivo", "dt_situacao_cadastral", "ano_situacao",
        "dt_inicio_atividade", "ano_abertura", "idade_meses", "faixa_idade",
        "regiao", "uf", "cod_municipio", "desc_municipio",
    )
)

display(df_silver.filter("flag_ti AND NOT flag_ativa").limit(10))

# COMMAND ----------

with Cronometro("silver_status_cadastral_v2", "checks", DT_REFERENCIA):
    total_matrizes = df_matrizes.count()
    total_silver = df_silver.count()

    executar_checks(
        df_silver, TABELA_DESTINO, DT_REFERENCIA,
        regras=[
            regra("motivo fora do domínio", "alerta", F.col("desc_motivo").isNull(), limite_pct=0.1),
            regra("encerrada sem data de baixa válida", "alerta",
                  F.col("flag_encerrada") & F.col("idade_meses").isNull(), limite_pct=1.0),
        ],
        resultados_extras=[
            resultado("tabela não vazia", "bloqueia", 0 if total_silver > 0 else 1, 1),
            check_unicidade(df_silver, ["cnpj_basico"]),
            check_contagem(total_matrizes, total_silver, "uma linha por matriz da silver de estabelecimentos"),
        ],
    )

with Cronometro("silver_status_cadastral_v2", "gravacao", DT_REFERENCIA):
    gravar_particao(df_silver, TABELA_DESTINO, DT_REFERENCIA)
