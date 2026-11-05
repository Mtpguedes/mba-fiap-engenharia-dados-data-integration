# Databricks notebook source
# MAGIC %md
# MAGIC # 00 — Setup do ambiente (rodar uma vez, pelo dono do catálogo)
# MAGIC
# MAGIC **Não faz parte do Job mensal.** Cria os schemas, dá permissão ao grupo e, se pedido, faz a migração única para o formato particionado por `dt_referencia`.
# MAGIC
# MAGIC Quem roda: o dono do catálogo `workspace` (quem criou os schemas). Os outros integrantes não têm permissão de `GRANT` nem de escrita na bronze.

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE SCHEMA IF NOT EXISTS workspace.bronze;
# MAGIC CREATE SCHEMA IF NOT EXISTS workspace.silver;
# MAGIC CREATE SCHEMA IF NOT EXISTS workspace.gold;

# COMMAND ----------

# MAGIC %md
# MAGIC ## Permissões
# MAGIC
# MAGIC | Camada | Leitura | Escrita |
# MAGIC |---|---|---|
# MAGIC | bronze | todos | só o dono (carga) |
# MAGIC | silver | todos | todos |
# MAGIC | gold | todos | todos |
# MAGIC
# MAGIC Em produção, a escrita seria de uma *service principal* do Job, e as pessoas teriam só leitura. Aqui todos escrevem em silver/gold porque cada um desenvolve uma frente.

# COMMAND ----------

usuarios = [
    "helentalarico@icloud.com",
    "mtpguedes@gmail.com",
    "rafaciarelli@gmail.com",
]

for usuario in usuarios:
    principal = f"`{usuario.replace('`', '``')}`"
    spark.sql(f"GRANT USE CATALOG ON CATALOG workspace TO {principal}")

    for camada in ["bronze", "silver", "gold"]:
        spark.sql(f"GRANT USE SCHEMA, SELECT ON SCHEMA workspace.{camada} TO {principal}")

    for camada in ["silver", "gold"]:
        spark.sql(f"GRANT CREATE TABLE, MODIFY ON SCHEMA workspace.{camada} TO {principal}")

    print(f"Permissões concedidas: {usuario}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Migração única (só na primeira vez)
# MAGIC
# MAGIC As tabelas abaixo foram criadas antes do padrão atual (sem `dt_referencia`, sem particionamento ou com colunas diferentes). A gravação por partição não consegue substituir uma tabela com outro formato, então elas precisam ser apagadas **uma vez** antes da recarga completa.
# MAGIC
# MAGIC Todas são reconstruídas pelo pipeline a partir dos arquivos nos Volumes. Nenhum dado se perde.
# MAGIC
# MAGIC Para executar, mude o widget `executar_migracao` para `sim`.

# COMMAND ----------

dbutils.widgets.dropdown("executar_migracao", "nao", ["nao", "sim"])

tabelas_legadas = [
    # bronze sem dt_referencia / lineage
    "workspace.bronze.empresas",
    "workspace.bronze.socios",
    "workspace.bronze.estabelecimentos",
    "workspace.bronze.qualificacoes_socios",
    "workspace.bronze.naturezas_juridicas",
    "workspace.bronze.cnaes",
    "workspace.bronze.simples",
    "workspace.bronze.paises",
    "workspace.bronze.motivos",
    "workspace.bronze.municipios",
    # silver/gold com schema antigo
    "workspace.silver.cnpj_socios_compliance",
    "workspace.silver.dim_cnae",
    "workspace.gold.perfil_societario_ti",
]

if dbutils.widgets.get("executar_migracao") == "sim":
    for tabela in tabelas_legadas:
        spark.sql(f"DROP TABLE IF EXISTS {tabela}")
        print(f"Removida: {tabela}")
else:
    print("Migração não executada. Tabelas que seriam removidas:")
    for tabela in tabelas_legadas:
        existe = spark.catalog.tableExists(tabela)
        print(f"  {tabela} — {'existe' if existe else 'não existe'}")
