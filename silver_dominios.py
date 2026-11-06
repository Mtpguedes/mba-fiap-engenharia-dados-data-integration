# Databricks notebook source
# /// script
# [tool.databricks.environment]
# environment_version = "6"
# ///
# MAGIC %md
# MAGIC # Silver — Tabelas de domínio
# MAGIC
# MAGIC As tabelas pequenas de código → descrição que a Receita publica junto com a base. Cada uma vira uma `dim_*` limpa e **única**, para que todas as frentes usem a mesma tradução e a mesma regra de TI.
# MAGIC
# MAGIC | Destino | Origem (bronze) | Padronização da chave |
# MAGIC |---|---|---|
# MAGIC | `silver.dim_cnae` (+ `flag_ti`) | `cnaes` | 7 dígitos (`lpad`) |
# MAGIC | `silver.dim_motivo` | `motivos` | 2 dígitos |
# MAGIC | `silver.dim_municipio` | `municipios` | 4 dígitos (código interno da Receita, **não é o código IBGE**) |
# MAGIC | `silver.dim_pais` | `paises` | 3 dígitos |
# MAGIC | `silver.dim_qualificacao` | `qualificacoes_socios` | 2 dígitos |
# MAGIC | `silver.dim_natureza_juridica` (+ grupo) | `naturezas_juridicas` | 4 dígitos |
# MAGIC
# MAGIC **Regra de TI (`flag_ti`):** CNAE começando com `62` (desenvolvimento, consultoria e suporte em TI) ou `631` (tratamento de dados, hospedagem e portais). É a mesma lista que a gold de risco societário usava escrita no código.
# MAGIC
# MAGIC Domínios mudam raramente e não precisam de histórico: cada execução **sobrescreve** a dim com a versão do mês.

# COMMAND ----------

# MAGIC %run ./utils_pipeline

# COMMAND ----------

DT_REFERENCIA = obter_dt_referencia()
print(f"dt_referencia: {DT_REFERENCIA}")

# COMMAND ----------

def montar_dim(tabela_bronze, col_cod, col_desc, nome_cod, nome_desc, digitos):
    return (
        ler_bronze(tabela_bronze, DT_REFERENCIA)
        .select(
            F.lpad(F.trim(col_cod), digitos, "0").alias(nome_cod),
            F.trim(col_desc).alias(nome_desc),
        )
        .dropDuplicates([nome_cod])
    )


grupos_natureza = {
    "1": "Administração Pública",
    "2": "Entidade Empresarial",
    "3": "Entidade sem Fins Lucrativos",
    "4": "Pessoa Física",
    "5": "Organização Internacional",
}
mapa_grupo = F.create_map(*[F.lit(x) for par in grupos_natureza.items() for x in par])

dims = {
    "dim_cnae": (
        montar_dim("cnaes", "cod_cnae", "descricao_cnae", "cod_cnae", "desc_cnae", 7)
        .withColumn("cod_divisao", F.substring("cod_cnae", 1, 2))
        .withColumn("flag_ti", F.col("cod_cnae").startswith("62") | F.col("cod_cnae").startswith("631"))
    ),
    "dim_motivo": montar_dim("motivos", "cod_motivo", "descricao_motivo", "cod_motivo", "desc_motivo", 2),
    "dim_municipio": montar_dim("municipios", "cod_municipio", "descricao_municipio",
                                "cod_municipio", "desc_municipio", 4),
    "dim_pais": montar_dim("paises", "cod_pais", "descricao_pais", "cod_pais", "desc_pais", 3),
    "dim_qualificacao": montar_dim("qualificacoes_socios", "cod_qualificacao", "descricao_qualificacao",
                                   "cod_qualificacao", "desc_qualificacao", 2),
    "dim_natureza_juridica": (
        montar_dim("naturezas_juridicas", "cod_natureza_juridica", "descricao_natureza_juridica",
                   "cod_natureza_juridica", "desc_natureza_juridica", 4)
        .withColumn("grupo_natureza_juridica",
                    F.coalesce(mapa_grupo[F.substring("cod_natureza_juridica", 1, 1)], F.lit("Não mapeado")))
    ),
}

display(dims["dim_cnae"].filter("flag_ti").orderBy("cod_cnae"))

# COMMAND ----------

# MAGIC %md
# MAGIC ## Checks e gravação

# COMMAND ----------

for nome, df in dims.items():
    cod = df.columns[0]
    desc = df.columns[1]
    destino = f"workspace.silver.{nome}"
    total = df.count()

    extras = [resultado("tabela não vazia", "bloqueia", 0 if total > 0 else 1, 1)]
    if nome == "dim_cnae":
        extras.append(resultado("existe ao menos um CNAE de TI", "bloqueia",
                                0 if df.filter("flag_ti").limit(1).count() > 0 else 1, 1))

    with Cronometro("silver_dominios", nome, DT_REFERENCIA):
        executar_checks(
            df, destino, DT_REFERENCIA,
            regras=[
                regra(f"{cod} só com dígitos", "bloqueia", ~F.coalesce(F.col(cod), F.lit("")).rlike(r"^\d+$")),
                regra("descrição preenchida", "alerta", F.coalesce(F.col(desc), F.lit("")) == ""),
            ],
            resultados_extras=extras,
        )
        (
            df.withColumn("dt_referencia", F.lit(DT_REFERENCIA).cast("date"))
            .write.format("delta").mode("overwrite").option("overwriteSchema", "true")
            .saveAsTable(destino)
        )
    print(f"Gravado: {destino} ({total} linhas)")