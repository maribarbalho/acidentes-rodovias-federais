# Databricks notebook source
# MAGIC %md
# MAGIC # Acidentes nas rodovias federais · pipeline medalhão em PySpark
# MAGIC
# MAGIC Mesmo pipeline da versão local em Polars (`src/rodovias/pipeline.py`), escrito em PySpark e gravado em tabelas Delta no Unity Catalog:
# MAGIC
# MAGIC | Camada | Tabelas | Conteúdo |
# MAGIC |---|---|---|
# MAGIC | bronze | `bronze_acidentes`, `bronze_pessoas` | CSVs da PRF como publicados (texto), com arquivo de origem e data de carga |
# MAGIC | silver | `silver_acidentes`, `silver_vitimas` | tipos, padronização, flags de qualidade; uma linha por acidente e uma por pessoa |
# MAGIC | gold | `gold_anual`, `gold_causas`, `gold_trechos`, `gold_condicoes`, `gold_uf` | agregados para análise |
# MAGIC
# MAGIC Fonte: [PRF, dados abertos](https://www.gov.br/prf/pt-br/acesso-a-informacao/dados-abertos/dados-abertos-da-prf), 2017 a 2025.
# MAGIC A última célula reconcilia os totais com a versão em Polars.

# COMMAND ----------

CATALOG, SCHEMA = "workspace", "rodovias"
VOLUME = f"/Volumes/{CATALOG}/{SCHEMA}/raw"
ANOS_CAUSA = (2021, 2025)

spark.sql(f"CREATE SCHEMA IF NOT EXISTS {CATALOG}.{SCHEMA}")
spark.sql(f"CREATE VOLUME IF NOT EXISTS {CATALOG}.{SCHEMA}.raw")
spark.sql(f"USE {CATALOG}.{SCHEMA}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Ingestão
# MAGIC Os ZIPs oficiais da PRF ficam no Volume `raw`. Se ainda não estiverem lá, a célula tenta baixá-los do Google Drive da PRF
# MAGIC (os mesmos links da página de dados abertos) e extrai os CSVs.

# COMMAND ----------

import os, urllib.request, zipfile, hashlib

DRIVE = "https://drive.usercontent.google.com/download?id={id}&export=download&confirm=t"
ARQUIVOS = {
    "datatran": {
        2017: "1HPLWt5f_l4RIX3tKjI4tUXyZOev52W0N", 2018: "1cM4IgGMIiR-u4gBIH5IEe3DcvBvUzedi",
        2019: "1pN3fn2wY34GH6cY-gKfbxRJJBFE0lb_l", 2020: "1esu6IiH5TVTxFoedv6DBGDd01Gvi8785",
        2021: "12xH8LX9aN2gObR766YN3cMcuycwyCJDz", 2022: "1PRQjuV5gOn_nn6UNvaJyVURDIfbSAK4-",
        2023: "1-WO3SfNrwwZ5_l7fRTiwBKRw7mi1-HUq", 2024: "14lB0vqMFkaZj8HZ44b0njYgxs9nAN8KO",
        2025: "1-G3MdmHBt6CprDwcW99xxC4BZ2DU5ryR",
    },
    "pessoas": {
        2017: "1Kv5mNgZvxtl0xwqsmDrxcaLY2KELxR-3", 2018: "1J-012nSnIafOASNFvIYY_vDKKpM51w5_",
        2019: "1DAJYKVfkTcPhQodSmHp9rsG1Q8XJW-m3", 2020: "1yQtVOsAlupPHQVVTmbJo0NR3XMzgHANO",
        2021: "1Gk3U6cMOZIevsDZHLi6J503xoCRS_lnI", 2022: "1wskEgRC3ame7rncSDQ7qWhKsoKw1lohY",
        2023: "1-caam_dahYOf2eorq4mez04Om6DD5d_3", 2024: "14qBOhrE1gioVtuXgxkCJ9kCA8YtUGXKA",
        2025: "1-PJGRbfSe7PVjU37A3wTCls_NRXyVGRD",
    },
}
NOME = {"datatran": "datatran{y}.zip", "pessoas": "acidentes{y}_todas_causas_tipos.zip"}

for kind, ids in ARQUIVOS.items():
    os.makedirs(f"{VOLUME}/{kind}", exist_ok=True)
    for y, fid in ids.items():
        z = f"{VOLUME}/zips/{NOME[kind].format(y=y)}"
        os.makedirs(f"{VOLUME}/zips", exist_ok=True)
        if not os.path.exists(z):
            req = urllib.request.Request(DRIVE.format(id=fid), headers={"User-Agent": "acidentes-rodovias-federais"})
            with urllib.request.urlopen(req, timeout=300) as r, open(z, "wb") as f:
                f.write(r.read())
        with zipfile.ZipFile(z) as zf:
            for info in zf.infolist():
                name = os.path.basename(info.filename)
                if name.lower().endswith(".csv") and not os.path.exists(f"{VOLUME}/{kind}/{name}"):
                    with zf.open(info) as src, open(f"{VOLUME}/{kind}/{name}", "wb") as dst:
                        dst.write(src.read())

display(dbutils.fs.ls(f"{VOLUME}/datatran"))

# COMMAND ----------

# MAGIC %md
# MAGIC ## Bronze

# COMMAND ----------

from pyspark.sql import functions as F, Window

def bronze(kind):
    return (spark.read.option("header", True).option("sep", ";").option("encoding", "ISO-8859-1")
            .option("inferSchema", False).option("nullValue", "NA")
            .csv(f"{VOLUME}/{kind}/*.csv")
            .withColumn("arquivo", F.col("_metadata.file_name"))
            .withColumn("carregado_em", F.current_timestamp()))

bronze("datatran").write.mode("overwrite").option("overwriteSchema", True).saveAsTable("bronze_acidentes")
bronze("pessoas").write.mode("overwrite").option("overwriteSchema", True).saveAsTable("bronze_pessoas")
display(spark.sql("SELECT 'acidentes' AS tabela, COUNT(*) AS linhas FROM bronze_acidentes UNION ALL SELECT 'pessoas', COUNT(*) FROM bronze_pessoas"))

# COMMAND ----------

# MAGIC %md
# MAGIC ## Silver
# MAGIC - Números com vírgula decimal, ids em notação científica ("1e+05") e "NA" tratados.
# MAGIC - Coordenadas fora do retângulo do Brasil ficam marcadas (`coord_valida = false`).
# MAGIC - O arquivo de pessoas repete cada pessoa para cada causa e tipo do acidente: fica uma linha por pessoa (a da causa principal).

# COMMAND ----------

num = lambda c: F.regexp_replace(F.col(c), ",", ".").cast("double")
txt = lambda c: F.trim(F.regexp_replace(F.col(c), r"\s+", " "))
idn = lambda c: F.col(c).cast("double").cast("bigint")

silver_ac = (
    spark.table("bronze_acidentes")
    .select(
        idn("id").alias("id"),
        F.to_date("data_inversa").alias("data"),
        F.lower(txt("dia_semana")).alias("dia_semana"),
        F.substring("horario", 1, 2).cast("int").alias("hora"),
        txt("uf").alias("uf"), F.col("br").cast("int").alias("br"), num("km").alias("km"),
        txt("municipio").alias("municipio"), txt("causa_acidente").alias("causa"), txt("tipo_acidente").alias("tipo"),
        txt("classificacao_acidente").alias("classificacao"), txt("fase_dia").alias("fase_dia"),
        txt("condicao_metereologica").alias("condicao_meteo"), txt("tipo_pista").alias("tipo_pista"),
        F.split(txt("tracado_via"), ";").getItem(0).alias("tracado"),
        (F.col("uso_solo") == "Sim").alias("urbano"),
        *[F.col(c).cast("int").alias(c) for c in ["pessoas", "mortos", "feridos_leves", "feridos_graves", "ilesos", "veiculos"]],
        num("latitude").alias("lat"), num("longitude").alias("lon"),
    )
    .withColumn("ano", F.year("data")).withColumn("mes", F.month("data"))
    .withColumn("coord_valida", F.coalesce(F.col("lat").between(-34.0, 5.5) & F.col("lon").between(-74.5, -32.0), F.lit(False)))
    .dropDuplicates(["id"])
)
silver_ac.write.mode("overwrite").option("overwriteSchema", True).saveAsTable("silver_acidentes")

w = Window.partitionBy("pesid").orderBy(F.col("causa_principal").desc(), F.col("ordem_tipo").asc())
silver_vi = (
    spark.table("bronze_pessoas")
    .select(
        idn("id").alias("id_acidente"), idn("pesid").alias("pesid"),
        F.year(F.to_date("data_inversa")).alias("ano"), txt("uf").alias("uf"),
        (F.col("causa_principal") == "Sim").alias("causa_principal"),
        F.col("ordem_tipo_acidente").cast("int").alias("ordem_tipo"),
        txt("tipo_veiculo").alias("tipo_veiculo"), txt("tipo_envolvido").alias("tipo_envolvido"),
        txt("estado_fisico").alias("estado_fisico"),
        F.when(F.col("idade").cast("int").between(0, 110), F.col("idade").cast("int")).alias("idade"),
        txt("sexo").alias("sexo"),
    )
    .where(F.col("pesid").isNotNull())
    .withColumn("rn", F.row_number().over(w)).where("rn = 1")
    .drop("rn", "causa_principal", "ordem_tipo")
)
silver_vi.write.mode("overwrite").option("overwriteSchema", True).saveAsTable("silver_vitimas")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Gold
# MAGIC A PRF ampliou a lista de causas ao longo de 2020. As análises por causa, tipo, condição e trecho usam 2021 a 2025, período com taxonomia estável.

# COMMAND ----------

agg = [F.count("*").alias("acidentes"), F.sum("mortos").alias("mortos"),
       F.sum("feridos_graves").alias("feridos_graves"), F.sum((F.col("mortos") > 0).cast("int")).alias("acidentes_fatais")]
let = lambda df: df.withColumn("mortos_por_100", F.round(F.col("mortos") / F.col("acidentes") * 100, 2))

ac = spark.table("silver_acidentes")
recorte = ac.where(F.col("ano").between(*ANOS_CAUSA))

let(ac.groupBy("ano").agg(*agg)).orderBy("ano").write.mode("overwrite").saveAsTable("gold_anual")
let(recorte.groupBy("causa").agg(*agg)).where("acidentes >= 300").write.mode("overwrite").saveAsTable("gold_causas")
let(recorte.groupBy("uf").agg(*agg)).write.mode("overwrite").saveAsTable("gold_uf")
(let(recorte.withColumn("km_ini", (F.floor(F.col("km") / 10) * 10).cast("int"))
     .where("br IS NOT NULL AND km IS NOT NULL")
     .groupBy("uf", "br", "km_ini").agg(*agg, F.mode("municipio").alias("municipio")))
 .write.mode("overwrite").saveAsTable("gold_trechos"))

from functools import reduce
cond = reduce(lambda a, b: a.unionByName(b), [
    let(recorte.groupBy(F.col(d).cast("string").alias("categoria")).agg(*agg)).withColumn("dimensao", F.lit(d))
    for d in ["fase_dia", "condicao_meteo", "tipo_pista", "tracado", "urbano"]
]).where("acidentes >= 300")
cond.write.mode("overwrite").saveAsTable("gold_condicoes")

display(spark.table("gold_anual"))

# COMMAND ----------

# MAGIC %md
# MAGIC ### Trechos de 10 km com mais mortes (2021–2025)

# COMMAND ----------

display(spark.sql("""
  SELECT uf, br, km_ini, municipio, acidentes, mortos, mortos_por_100
  FROM gold_trechos ORDER BY mortos DESC, acidentes DESC LIMIT 15
"""))

# COMMAND ----------

# MAGIC %md
# MAGIC ### Causas mais letais (2021–2025)

# COMMAND ----------

display(spark.sql("SELECT causa, acidentes, mortos, mortos_por_100 FROM gold_causas ORDER BY mortos DESC LIMIT 15"))

# COMMAND ----------

# MAGIC %md
# MAGIC ## Reconciliação com a versão em Polars
# MAGIC Valores esperados, gerados localmente por `python -m rodovias.pipeline`:
# MAGIC acidentes 632.713 · mortos 50.825 · pessoas 1.509.580 · óbitos 2021–2025 28.671 · mortos 2021–2025 28.671.

# COMMAND ----------

esperado = {"acidentes": 632713, "mortos": 50825, "pessoas": 1509580, "obitos_2021_2025": 28671, "mortos_2021_2025": 28671}
obtido = {
    "acidentes": ac.count(),
    "mortos": ac.agg(F.sum("mortos")).first()[0],
    "pessoas": spark.table("silver_vitimas").count(),
    "obitos_2021_2025": spark.table("silver_vitimas").where(F.col("ano").between(*ANOS_CAUSA) & (F.col("estado_fisico") == "Óbito")).count(),
    "mortos_2021_2025": recorte.agg(F.sum("mortos")).first()[0],
}
rows = [(k, esperado[k], obtido[k], esperado[k] == obtido[k]) for k in esperado]
display(spark.createDataFrame(rows, "metrica string, polars long, pyspark long, confere boolean"))
assert all(r[3] for r in rows), "Divergência entre Polars e PySpark"
