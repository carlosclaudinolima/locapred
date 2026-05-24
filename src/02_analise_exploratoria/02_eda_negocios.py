# ==============================================================================
# SCRIPT: 02_eda_negocios.py
# OBJETIVO: Análise Exploratória focada em extração de insights para a Locaweb
# CAMADA: Consome da Silver (dados já limpos, tipados e com regras de KPI)
# ==============================================================================

from pyspark.sql import functions as F

# Lendo os dados já tratados da camada Silver
df_silver = spark.table("silver.incidentes_tratados")

print("=========================================================")
print(" INSIGHT 1: SAZONALIDADE (Onde o caos se concentra?)")
print("=========================================================")
# Desafio: Identificar sazonalidade (dia da semana, horário, mês)[cite: 436, 450, 451].

eda_sazonalidade = df_silver \
    .filter(F.col("valido_kpi") == True) \
    .groupBy("dia_semana", "hora_dia") \
    .agg(
        F.count("Número").alias("volume_incidentes"),
        F.round(F.avg("Duração") / 3600, 2).alias("tempo_medio_resolucao_horas")
    ) \
    .orderBy("volume_incidentes", ascending=False)

# Dica de Visualização no Databricks: Usar gráfico de Heatmap (Eixo X: Hora, Eixo Y: Dia)
display(eda_sazonalidade)


print("=========================================================")
print(" INSIGHT 2: AGRUPAMENTOS CRÍTICOS (Foco em P2 e P3)")
print("=========================================================")
# Desafio: Criar agrupamentos críticos (produto + categoria + prioridade)[cite: 453, 454, 455].
# Obrigatório: Foco em Prioridade 2 - Alta e Prioridade 3 - Média[cite: 81, 395].

eda_criticos = df_silver \
    .filter((F.col("Prioridade").isin(["2 - Alta", "3 - Média"])) & (F.col("valido_kpi") == True)) \
    .groupBy("Produto", "Categoria", "Prioridade") \
    .agg(
        F.count("Número").alias("total_incidentes"),
        F.sum(F.when(F.col("ola_violado") == True, 1).otherwise(0)).alias("violacoes_ola")
    ) \
    .withColumn("taxa_falha_ola_perc", F.round((F.col("violacoes_ola") / F.col("total_incidentes")) * 100, 2)) \
    .filter(F.col("total_incidentes") > 30) \
    .orderBy("taxa_falha_ola_perc", ascending=False)

# Dica de Visualização no Databricks: Usar gráfico de Barras Horizontais Empilhadas
display(eda_criticos)


print("=========================================================")
print(" INSIGHT 3: INCIDENTES RECORRENTES (Ofensores Crônicos)")
print("=========================================================")
# Desafio: Detectar incidentes recorrentes[cite: 98, 456].

eda_recorrencia = df_silver \
    .filter(F.col("item_configuracao").isNotNull()) \
    .groupBy("item_configuracao", "Categoria", "Prioridade") \
    .agg(
        F.count("Número").alias("qtd_falhas_no_ativo"),
        F.countDistinct("data_abertura").alias("dias_distintos_com_falha"),
        F.countDistinct("grupo_designado").alias("qtd_equipes_acionadas") # Mostra se o problema fica "pingando" entre equipes
    ) \
    .orderBy("qtd_falhas_no_ativo", ascending=False) \
    .limit(20)

# Dica de Visualização no Databricks: Tabela simples ordenada pelos maiores ofensores
display(eda_recorrencia)
