from pyspark.sql import functions as F

# Caminho do arquivo no volume do Databricks (ex: Unity Catalog Volumes)
file_path = "/Volumes/workspace/dados/raw/incidentes_locaweb.parquet"

# Leitura do Excel distribuído (Exige a biblioteca com.crealytics:spark-excel no cluster)
df_raw = spark.read.parquet(file_path)

# Salvando o dado cru na camada Bronze
df_raw.write.format("delta").mode("overwrite").saveAsTable("bronze.incidentes_locaweb")



