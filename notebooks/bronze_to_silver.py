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
df_silver = spark.read.format('delta').load(path)
display(df_silver)

# COMMAND ----------

# df_silver.limit(5).show(truncate=False)

# COMMAND ----------

# MAGIC %md
# MAGIC #### Transformando os campos do json em colunas

# COMMAND ----------

display(df_silver.select('anuncio.*'))

# COMMAND ----------

# df_silver\
#     .select('anuncio.*')\
#     .toPandas()

# COMMAND ----------

display(df_silver.select('anuncio.*', 'anuncio.endereco.*'))

# COMMAND ----------

# df_silver\
#     .select('anuncio.*',
#             'anuncio.endereco.*')\
#     .toPandas()

# COMMAND ----------

df_silver = df_silver.select('anuncio.*',
                            'anuncio.endereco.*')
display(df_silver)

# COMMAND ----------

# MAGIC %md
# MAGIC #### Removendo colunas

# COMMAND ----------

df_silver = df_silver.drop('caracteristicas', 'endereco')
display(df_silver)
# df_silver.limit(10).toPandas()

# COMMAND ----------

# MAGIC %md
# MAGIC #### Salvando na camada silver

# COMMAND ----------

display(dbutils.fs.ls('dbfs:/mnt/dados'))

# COMMAND ----------

path_to_silver = 'dbfs:/mnt/dados/silver/dataset_imoveis'
df_silver.write.format('delta').mode('overwrite').save(path_to_silver)
