# Lendo da Bronze para a investigação
df_bronze = spark.table("bronze.incidentes_locaweb")

# A. Verificação de Nulos nas colunas críticas para o negócio
print("--- Taxa de Nulos por Coluna Crítica ---")
df_bronze.select([
    F.round((F.sum(F.when(F.col(c).isNull(), 1).otherwise(0)) / F.count("*")) * 100, 2).alias(c)
    for c in ["Produto", "Categoria", "Item de configuração", "Incidente Pai"]
]).show()
# Insight esperado: Se "Item de configuração" tiver 80% de nulos, o agrupamento crônico por ativo de TI perde força.

# B. Investigando a regra do KPI e o viés de monitoramento
# O dicionário diz que "Sem Intervenção" geralmente vem de "Monitoramento"[cite: 301, 302]. 
# Vamos provar isso estatisticamente antes de dropar ou usar na Silver.
print("--- Cruzamento: Status x Origem de Abertura ---")
display(
    df_bronze.groupBy("Status", "Aberto por")
    .count()
    .orderBy(F.desc("count"))
)
# Insight esperado: Validar se realmente existe uma correlação forte que justifique a regra de negócio.
