from pyspark.sql import functions as F
from pyspark.sql.window import Window

# Caminho do arquivo no volume do Databricks (ex: Unity Catalog Volumes)
file_path = "/Volumes/workspace/dados/raw/incidentes_locaweb.xlsx"

# Leitura do Excel distribuído (Exige a biblioteca com.crealytics:spark-excel no cluster)
df_raw = spark.read.format("com.crealytics.spark.excel") \
    .option("header", "true") \
    .option("inferSchema", "true") \
    .option("dataAddress", "'Planilha1'!A1") \
    .load(file_path)

# Salvando o dado cru na camada Bronze
df_raw.write.format("delta").mode("overwrite").saveAsTable("bronze.incidentes_locaweb")
