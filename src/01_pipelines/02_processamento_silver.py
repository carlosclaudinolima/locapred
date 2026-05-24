# 1. Renomeando colunas para o padrão Snake Case (Parquet odeia espaços e acentos)
cols_rename = {
    "Incidente Pai": "incidente_pai",
    "Item de configuração": "item_configuracao",
    "Grupo designado": "grupo_designado",
    "Aberto por": "aberto_por",
    "Descrição resumida": "descricao_resumida"
}
df_silver = df_bronze
for old_name, new_name in cols_rename.items():
    df_silver = df_silver.withColumnRenamed(old_name, new_name)

# 2. Higienização de Datas e Engenharia de Sazonalidade (Para o XGBoost capturar padrões temporais)
df_silver = df_silver \
    .withColumn("aberto_ts", F.to_timestamp(F.col("Aberto"), "dd/MM/yyyy HH:mm:ss")) \
    .withColumn("data_abertura", F.to_date(F.col("aberto_ts"))) \
    .withColumn("mes", F.month(F.col("aberto_ts"))) \
    .withColumn("dia_semana", F.dayofweek(F.col("aberto_ts"))) \
    .withColumn("hora_dia", F.hour(F.col("aberto_ts"))) \
    .withColumn("is_fim_semana", F.when(F.dayofweek(F.col("aberto_ts")).isin([1, 7]), 1).otherwise(0))

# 3. Aplicação rigorosa das regras de Negócio Locaweb
df_silver = df_silver \
    .withColumn("valido_kpi", 
        F.when(
            (F.col("Status") != "Sem Intervenção") & # [cite: 312]
            (F.col("incidente_pai").isNull()) &      # [cite: 311]
            (F.col("Prioridade").isin(["1 - Crítica", "2 - Alta", "3 - Média"])), # [cite: 304]
            True
        ).otherwise(False)
    ) \
    .withColumn("ola_violado",
        F.when(F.col("valido_kpi") == False, False)
         # Duração está em segundos[cite: 299]. P1 e P2 = até 4h (14400s) [cite: 306, 307]
         .when(F.col("Prioridade").isin(["1 - Crítica", "2 - Alta"]) & (F.col("Duração") > 14400), True)
         # P3 = até 12h (43200s) [cite: 308]
         .when((F.col("Prioridade") == "3 - Média") & (F.col("Duração") > 43200), True)
         .otherwise(False)
    )

# Persistindo a Silver
df_silver.write.format("delta").mode("overwrite").saveAsTable("silver.incidentes_tratados")
