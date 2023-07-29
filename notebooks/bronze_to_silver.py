# Databricks notebook source
# MAGIC %md
# MAGIC #### Conferindo se os dados foram montados e se temos acesso a pasta bronze

# COMMAND ----------

display(dbutils.fs.ls('/mnt/dados/bronze'))

# COMMAND ----------

# MAGIC %md
# MAGIC #### Lendo os dados na camada bronze

# COMMAND ----------

path = 'dbfs:/mnt/dados/bronze/dataset_imoveis/'
df = spark.read.format('delta').load(path)

# COMMAND ----------

df.limit(5).show(truncate=False)

# COMMAND ----------

# MAGIC %md
# MAGIC #### Transformando os campos do json em colunas

# COMMAND ----------

df\
    .select('anuncio.*')\
    .toPandas()

# COMMAND ----------

df\
    .select('anuncio.*',
            'anuncio.endereco.*')\
    .toPandas()

# COMMAND ----------

dados_detalhados = df.select('anuncio.*',
                             'anuncio.endereco.*')

# COMMAND ----------

df_silver = dados_detalhados.drop('caracteristicas', 'endereco')
df_silver.limit(10).toPandas()

# COMMAND ----------

# MAGIC %md
# MAGIC #### Salvando na camada silver

# COMMAND ----------

display(dbutils.fs.ls('dbfs:/mnt/dados'))

# COMMAND ----------

path_to_silver = 'dbfs:/mnt/dados/silver/dataset_imoveis'
df_silver.write.mode('overwrite').format('delta').save(path_to_silver)

# COMMAND ----------


