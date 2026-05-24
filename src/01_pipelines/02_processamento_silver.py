
# Colunas que vem do bronze
# 'numero', 'prioridade', 'produto', 'categoria', 'subcategoria',
#        'grupo_designado', 'item_de_configuracao', 'aberto', 'resolvido',
#        'encerrado', 'duracao', 'codigo_de_fechamento', 'descricao_resumida',
#        'solucao', 'aberto_por', 'incidente_pai', 'status', 'entrou_para_kpi',
#        'kpi_violado'


from pyspark.sql import functions as F

# Lendo da Bronze
df_bronze = spark.table("bronze.incidentes_locaweb")

# 1. Limpeza, Tipagem Forte e Normalização
df_silver = df_bronze \
    .withColumn("aberto_ts", F.to_timestamp(F.col("aberto"), "dd/MM/yyyy HH:mm:ss")) \
    .withColumn("resolvido_ts", F.to_timestamp(F.col("resolvido"), "dd/MM/yyyy HH:mm:ss")) \
    .withColumn("encerrado_ts", F.to_timestamp(F.col("encerrado"), "dd/MM/yyyy HH:mm:ss")) \
    .withColumn("duracao_horas", F.col("duracao") / 3600.0)

# 2. Features Lógicas Baseadas nas Regras de Negócio Locaweb
df_silver = df_silver \
    .withColumn("valido_kpi", 
                F.when(
                    (F.col("status") != "Sem Intervenção") &  # Status Sem Intervenção não entra no KPI [cite: 474]
                    (F.col("incidente_pai").isNull()) &       # Valor preenchido não entra no KPI [cite: 473]
                    (F.col("prioridade").isin(["1 - Crítica", "2 - Alta", "3 - Média"])), # Apenas P1, P2 e P3 [cite: 466]
                    True
                ).otherwise(False)) \
    .withColumn("data_abertura", F.to_date(F.col("Aberto_ts")))

# Salvando na Silver
df_silver.write.format("delta") \
    .mode("overwrite") \
    .saveAsTable("silver.incidentes_locaweb_tratados")