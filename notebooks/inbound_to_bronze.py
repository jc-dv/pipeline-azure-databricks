# Databricks notebook source
# MAGIC %md
# MAGIC ##### Conferindo se os dados foram montados e se temos acesso a pasta inbound

# COMMAND ----------

display(dbutils.fs.ls('/mnt/dados/inbound'))

# COMMAND ----------

# MAGIC %md
# MAGIC ### Lendo os dados na camada de inbound

# COMMAND ----------

path = 'dbfs:/mnt/dados/inbound/dados_brutos_imoveis.json'
dados = spark.read.json(path)

# COMMAND ----------

display(dados)

# COMMAND ----------

dados.show(n=5, truncate=True)

# COMMAND ----------

# MAGIC %md
# MAGIC #### Removendo colunas

# COMMAND ----------

df_bronze = dados.drop('imagens', 'usuario')
display(df_bronze)
# df_bronze.limit(10).show(truncate=False)

# COMMAND ----------

# MAGIC %md
# MAGIC #### Criando uma coluna de identificação
# MAGIC

# COMMAND ----------

from pyspark.sql.functions import col

# COMMAND ----------

df_bronze = dados.withColumn('id', col('anuncio.id'))
display(df_bronze)
# df_bronze.limit(10).toPandas()

# COMMAND ----------

# MAGIC %md
# MAGIC #### Salvando na camada bronze

# COMMAND ----------

display(dbutils.fs.ls('/mnt/dados'))

# COMMAND ----------

path_to_bronze = 'dbfs:/mnt/dados/bronze/dataset_imoveis'
df_bronze.write.format('delta').mode('overwrite').save(path_to_bronze)

# COMMAND ----------

display(dbutils.fs.ls('/mnt/dados/bronze/dataset_imoveis'))

# COMMAND ----------


