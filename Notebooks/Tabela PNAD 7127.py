# Databricks notebook source
# MAGIC %md
# MAGIC # Média de anos de estudo

# COMMAND ----------

# MAGIC %md
# MAGIC ## Configuração do ambiente

# COMMAND ----------

#conexão ao armazenamento Blob
spark.conf.set('fs.azure.account.key.m03storage.blob.core.windows.net','3Ts0JpdSfDvYx8JOerU9CrExx1Ankj3W1q5J3Cscu2hXlQR90cpMlVy6p2mBB0NpKndKWixIcp1poqcFrAJ+oQ==')

# COMMAND ----------

#caminho do Blob
Blob_Path = 'wasbs://m03container@m03storage.blob.core.windows.net'

# COMMAND ----------

# MAGIC %md
# MAGIC ###Importar os pacotes

# COMMAND ----------

#importar pacotes
import requests
import json

# COMMAND ----------

# MAGIC %md
# MAGIC ###Fazer request para a url e transformar o arquivo json em DataFrame do Spark

# COMMAND ----------

#fazer request e verificar conexão
api = 'http://api.sidra.ibge.gov.br/values/t/7127/n1/1/v/3593/p/2017-2019/c86/all/c58/2792,100052,108866/f/n?formato=json'
r = requests.get(api)
print (r)

# COMMAND ----------

#mostrar dados em json
r.json()

# COMMAND ----------

#transformar em texto - se tentar transformar em dataframe direto ele dá erro 'classe = 'requests.models.Response'
json_data = json.loads(r.text)
#transformar em DataFrame
df = spark.createDataFrame(json_data)
#mostrar Dataframe
df.show()

# COMMAND ----------

# MAGIC %md
# MAGIC ###Salvar na camada bronze e fazer as transformações

# COMMAND ----------

#salvar no delta lake (camada bronze)
df.write.format('delta').mode('overwrite').save(Blob_Path + '/mnt/bronze/pnad7127')

# COMMAND ----------

#ler como dataframe novo
df_prata = spark.read.format('delta').load(Blob_Path + '/mnt/bronze/pnad7127')  
#mostrar novo dataframe
df_prata.show()

# COMMAND ----------

#mudar tipo das colunas
df_prata = df_prata.withColumn('Valor',df_prata['V'].cast('float')).withColumn('Ano',df_prata['D3N'].cast('date')).withColumn('Cor_raca',df_prata['D4N']).withColumn('Grupo_de_Idade',df_prata['D5N'])
#verificar o esquema
df_prata.printSchema()

# COMMAND ----------

#mostrar apenas as colunas que vamos usar
df_prata = df_prata.select('Ano','Cor_raca','Grupo_de_Idade','Valor')
#mostrar dataframe
display(df_prata)

# COMMAND ----------

#apenas valores não nulos
df_prata = df_prata.filter('Valor IS NOT NULL')

# COMMAND ----------

#ordenar valores das colunas
df_prata = df_prata.sort('Ano','Grupo_de_Idade','Cor_raca')
#mostrar dataframe
display(df_prata)

# COMMAND ----------

# MAGIC %md
# MAGIC ###Salvar na camada prata e verificar o arquivo

# COMMAND ----------

#Salvar na camada prata
df_prata.write.format('delta').mode('overwrite').save(Blob_Path + '/mnt/prata/pnad7127')

# COMMAND ----------

#verificar o esquema e os dados
df_ouro = spark.read.format('delta').load(Blob_Path + '/mnt/prata/pnad7127') 
df_ouro.printSchema()
display(df_ouro)

# COMMAND ----------

# MAGIC %md
# MAGIC ###Salvar na camada ouro para consumo pelo PowerBI

# COMMAND ----------

#salvar na camada ouro para alimentar o PBI 
df_ouro.write.format('delta').mode('overwrite').save(Blob_Path + '/mnt/ouro/pnad7127')
df_ouro.write.format('delta').mode('overwrite').saveAsTable('Anos_Estudo')