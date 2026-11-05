# Databricks notebook source
# MAGIC %md
# MAGIC # Utilitários do pipeline — quality checks e gravação por partição
# MAGIC
# MAGIC Funções compartilhadas pelos notebooks de silver_estabelecimentos, silver_empresas e gold_demografia_ti. P

# COMMAND ----------

from datetime import datetime

from pyspark.sql import functions as F

TABELA_DQ = "workspace.silver.dq_resultados"


def obter_dt_referencia(padrao="2026-09-30"):
    """Lê o mês de referência do widget e valida o formato yyyy-MM-dd."""
    dbutils.widgets.text("dt_referencia", padrao)
    valor = dbutils.widgets.get("dt_referencia").strip()
    datetime.strptime(valor, "%Y-%m-%d")
    return valor

# COMMAND ----------

def regra(nome, severidade, condicao_falha, limite_pct=0.0):
    assert severidade in ("bloqueia", "alerta")
    return {"nome": nome, "severidade": severidade, "condicao": condicao_falha, "limite_pct": limite_pct}


def resultado(nome, severidade, qtd_falhas, total, limite_pct=0.0):
    assert severidade in ("bloqueia", "alerta")
    return {"nome": nome, "severidade": severidade, "qtd_falhas": int(qtd_falhas),
            "total": int(total), "limite_pct": limite_pct}


def check_unicidade(df, chaves, severidade="bloqueia"):
    linha = df.agg(
        F.count(F.lit(1)).alias("total"),
        F.count_distinct(F.struct(*chaves)).alias("distintos"),
    ).first()
    return resultado(f"chave única ({', '.join(chaves)})", severidade,
                     linha["total"] - linha["distintos"], linha["total"])


def check_contagem(qtd_esperada, qtd_obtida, descricao, severidade="bloqueia"):
    return resultado(descricao, severidade, abs(qtd_esperada - qtd_obtida), qtd_esperada)


def check_integridade(df_filho, df_pai, chaves, descricao, total, severidade="alerta", limite_pct=0.0):
    orfaos = (
        df_filho.select(*chaves)
        .join(df_pai.select(*chaves).distinct(), on=chaves, how="left_anti")
        .count()
    )
    return resultado(descricao, severidade, orfaos, total, limite_pct)

# COMMAND ----------

def executar_checks(df, tabela, dt_referencia, regras, resultados_extras=None, gravar_log=True):
    """
    Avalia as regras de linha numa única passada sobre o DataFrame, junta os
    resultados calculados à parte, grava o log e levanta erro se algum check
    bloqueante falhar.
    """
    aggs = [F.count(F.lit(1)).alias("_total")] + [
        F.sum(F.when(r["condicao"], 1).otherwise(0)).alias(f"_r{i}")
        for i, r in enumerate(regras)
    ]
    linha = df.agg(*aggs).first()
    total = linha["_total"]

    resultados = [
        resultado(r["nome"], r["severidade"], linha[f"_r{i}"] or 0, total, r["limite_pct"])
        for i, r in enumerate(regras)
    ] + list(resultados_extras or [])

    linhas_log = []
    for r in resultados:
        pct = round(100.0 * r["qtd_falhas"] / r["total"], 4) if r["total"] else 0.0
        falhou = r["qtd_falhas"] > r["total"] * r["limite_pct"] / 100.0
        if not falhou:
            status = "OK"
        elif r["severidade"] == "bloqueia":
            status = "FALHA"
        else:
            status = "ALERTA"
        linhas_log.append((tabela, dt_referencia, r["nome"], r["severidade"],
                           r["qtd_falhas"], r["total"], pct, float(r["limite_pct"]), status))

    df_log = (
        spark.createDataFrame(
            linhas_log,
            "tabela STRING, dt_referencia STRING, check_nome STRING, severidade STRING, "
            "qtd_falhas LONG, total_registros LONG, pct_falhas DOUBLE, limite_pct DOUBLE, status STRING",
        )
        .withColumn("dt_referencia", F.col("dt_referencia").cast("date"))
        .withColumn("dt_execucao", F.current_timestamp())
    )
    display(df_log)

    if gravar_log:
        df_log.write.format("delta").mode("append").saveAsTable(TABELA_DQ)

    falhas = [l[2] for l in linhas_log if l[8] == "FALHA"]
    if falhas:
        raise ValueError(f"[{tabela}] Quality check: {falhas}")

    print(f"[{tabela}] {len(linhas_log)} checks executados. "
          f"Alertas: {sum(l[8] == 'ALERTA' for l in linhas_log)}. ")

# COMMAND ----------

def gravar_particao(df, tabela, dt_referencia):
    """Cria a tabela particionada na primeira carga; nas seguintes, substitui só o mês informado."""
    if spark.catalog.tableExists(tabela):
        (
            df.write.format("delta")
            .mode("overwrite")
            .option("replaceWhere", f"dt_referencia = '{dt_referencia}'")
            .saveAsTable(tabela)
        )
    else:
        df.write.format("delta").partitionBy("dt_referencia").saveAsTable(tabela)

    print(f"Gravado: {tabela} | dt_referencia = {dt_referencia}")