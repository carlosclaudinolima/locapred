import pyspark.sql.functions as F

# Caminho do arquivo no volume do Databricks (ex: Unity Catalog Volumes)
file_path = "/Volumes/workspace/dados/raw/incidentes_locaweb.xlsx"

# 1. Lê o Excel ignorando o cabeçalho oficial da biblioteca
df_raw = spark.read.format("excel") \
    .option("header", "false") \
    .option("inferSchema", "false") \
    .option("dataAddress", "Dataset Geral") \
    .load(file_path)

# 2. Pega a primeira linha
headers = df_raw.first()

# 3. Renomeia as colunas usando os nomes EXATOS da planilha original
for i, col_name in enumerate(headers):
    if col_name:
        df_raw = df_raw.withColumnRenamed(f"_c{i}", str(col_name))

# 4. Remove a linha de cabeçalho dos dados
coluna_id = df_raw.columns[0]
df_raw = df_raw.filter(F.col(coluna_id) != headers[0])

# 5. Salva na Bronze ativando o Column Mapping diretamente na tabela
df_raw.writeTo("bronze.incidentes_locaweb") \
    .tableProperty("delta.columnMapping.mode", "name") \
    .createOrReplace()



