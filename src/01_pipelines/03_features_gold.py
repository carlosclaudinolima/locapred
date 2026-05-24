# Visão Gold A: Modelagem Preditiva (XGBoost / MLlib)

# O objetivo é antecipar o volume de incidentes em D+1 e D+7. 
# Precisamos de um dataset de séries temporais agregado por dia, 
# usando a função lead para criar nossas variáveis alvo (o que 
#                                              queremos prever).


df_silver = spark.table("silver.incidentes_tratados")

# Agregando dados diários de incidentes válidos para o negócio
gold_ts = df_silver.filter(F.col("valido_kpi") == True) \
    .groupBy("data_abertura", "dia_semana", "mes", "is_fim_semana") \
    .agg(
        F.count("Número").alias("volume_diario"),
        F.sum(F.when(F.col("ola_violado") == True, 1).otherwise(0)).alias("total_ola_violado")
    )

# Engenharia do Target (Variável Resposta) para D+1 e D+7 usando Window Functions
# Ordenamos o tempo para olhar para o "futuro" daquela linha
window_spec = Window.orderBy("data_abertura")

gold_predictive = gold_ts \
    .withColumn("target_volume_D_mais_1", F.lead("volume_diario", 1).over(window_spec)) \
    .withColumn("target_volume_D_mais_7", F.lead("volume_diario", 7).over(window_spec)) \
    .dropna(subset=["target_volume_D_mais_7"]) # Removemos as últimas 7 linhas que não têm futuro para prever

gold_predictive.write.format("delta").mode("overwrite").saveAsTable("gold.ml_timeseries_features")
# RESULTADO: Este DataFrame está perfeitamente formatado para o XGBoost. 
# As features são dia_semana, mes, is_fim_semana, volume_diario. O Label é o target_volume.

#-------

# Visão Gold B: Clusterização (K-Means / MLlib)

# O desafio pede para aplicar agrupamento para identificar padrões de incidentes
# críticos e segmentar comportamentos semelhantes. Vamos gerar o dataset numérico
# para o K-Means agrupar os ofensores crônicos


# Agrupando por Produto e Categoria para encontrar Clusters de Risco
gold_cluster = df_silver.filter(F.col("valido_kpi") == True) \
    .groupBy("Produto", "Categoria") \
    .agg(
        F.count("Número").alias("volume_total"),
        F.avg("Duração").alias("mttr_segundos"),
        F.sum(F.when(F.col("ola_violado") == True, 1).otherwise(0)).alias("violacoes_ola"),
        F.countDistinct("item_configuracao").alias("qtd_ativos_impactados")
    ) \
    .withColumn("taxa_violacao_ola", F.col("violacoes_ola") / F.col("volume_total"))

gold_cluster.write.format("delta").mode("overwrite").saveAsTable("gold.ml_clustering_features")
# RESULTADO: Para rodar o K-Means no Spark MLlib, você passará essas colunas numéricas 
# por um VectorAssembler e um StandardScaler. O modelo encontrará clusters (ex: "Cluster 1: 
# Alto volume, Baixo MTTR" vs "Cluster 2: Baixo Volume, Altíssima taxa de violação").
