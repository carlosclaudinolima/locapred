
# Colunas que vem do bronze
# 'numero', 'prioridade', 'produto', 'categoria', 'subcategoria',
#        'grupo_designado', 'item_de_configuracao', 'aberto', 'resolvido',
#        'encerrado', 'duracao', 'codigo_de_fechamento', 'descricao_resumida',
#        'solucao', 'aberto_por', 'incidente_pai', 'status', 'entrou_para_kpi',
#        'kpi_violado'

from pyspark.sql import functions as F

# 1. Leitura do Bronze
df_silver = spark.table("bronze.incidentes_locaweb")

# 2. Higienização de Datas e Engenharia de Sazonalidade (Para o XGBoost capturar padrões temporais)
df_silver = df_silver \
    .withColumn("aberto_ts", F.to_timestamp(F.col("aberto"), "dd/MM/yyyy HH:mm:ss")) \
    .withColumn("data_abertura", F.to_date(F.col("aberto_ts"))) \
    .withColumn("mes", F.month(F.col("aberto_ts"))) \
    .withColumn("dia_semana", F.dayofweek(F.col("aberto_ts"))) \
    .withColumn("hora_dia", F.hour(F.col("aberto_ts"))) \
    .withColumn("is_fim_semana", F.when(F.dayofweek(F.col("aberto_ts")).isin([1, 7]), 1).otherwise(0))

# 3. Aplicação rigorosa das regras de Negócio Locaweb
df_silver = df_silver \
    .withColumn("valido_kpi", 
        F.when(
            (F.col("status") != "Sem Intervenção") & 
            (F.col("incidente_pai").isNull()) &      
            (F.col("prioridade").isin(["1 - Crítica", "2 - Alta", "3 - Média"])), 
            True
        ).otherwise(False)
    ) \
    .withColumn("ola_violado",
        F.when(F.col("valido_kpi") == False, False)
         # Duração está em segundos. P1 e P2 = até 4h (14400s) 
         .when(F.col("prioridade").isin(["1 - Crítica", "2 - Alta"]) & (F.col("duracao") > 14400), True)
         # P3 = até 12h (43200s)
         .when((F.col("prioridade") == "3 - Média") & (F.col("duracao") > 43200), True)
         .otherwise(False)
    )

# Persistindo a Silver
df_silver.write.format("delta").mode("overwrite").saveAsTable("silver.incidentes_locaweb_tratados")