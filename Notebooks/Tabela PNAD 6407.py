# Databricks notebook source
# MAGIC %md
# MAGIC # População residente no Brasil

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
# MAGIC ###Importar pacotes

# COMMAND ----------

#importar pacotes
import requests
import json

# COMMAND ----------

# MAGIC %md
# MAGIC ###Fazer request para a url e transformar o arquivo json em DataFrame do Spark

# COMMAND ----------

#fazer request e verificar conexão
api = 'http://api.sidra.ibge.gov.br/values/t/6407/n1/1/v/606/p/2017-2019/c2/all/c58/1140,104868,114535,2793,1144,1145,3299,3300,3301,3302/f/n?formato=json'
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
df.write.format('delta').mode('overwrite').save(Blob_Path + '/mnt/bronze/pnad6407')

# COMMAND ----------

#ler como dataframe novo para fazer as transformações
df_prata = spark.read.format('delta').load(Blob_Path + '/mnt/bronze/pnad6407')  
#mostrar novo dataframe
df_prata.show()

# COMMAND ----------

#mudar tipo das colunas e multiplicar por mil para obter o número absoluto de pessoas
df_prata = df_prata.withColumn('Valor',df_prata['V'].cast('int')*1000).withColumn('Ano',df_prata['D3N'].cast('date')).withColumn('Grupo_de_Idade',df_prata['D5N']).withColumn('Sexo',df_prata['D4N'])
#verificar o esquema
df_prata.printSchema()

# COMMAND ----------

#mostrar apenas as colunas que vamos usar
df_prata = df_prata.select('Ano','Grupo_de_Idade','Sexo','Valor')
#mostrar dataframe
display(df_prata)

# COMMAND ----------

#apenas valores não nulos
df_prata = df_prata.filter('Valor IS NOT NULL')

# COMMAND ----------

#ordenar dados
df_prata = df_prata.sort('Ano','Grupo_de_Idade','Sexo')
#mostrar dataframe
display(df_prata)

# COMMAND ----------

# MAGIC %md
# MAGIC ###Salvar na camada prata e verificar o arquivo

# COMMAND ----------

#Salvar na camada prata
df_prata.write.format('delta').mode('overwrite').save(Blob_Path + '/mnt/prata/pnad6407')

# COMMAND ----------

#verificar o esquema e os dados
df_ouro = spark.read.format('delta').load(Blob_Path + '/mnt/prata/pnad6407')  
df_ouro.printSchema()
display(df_ouro)

# COMMAND ----------

# MAGIC %md
# MAGIC ###Salvar na camada ouro para consumo pelo PowerBI

# COMMAND ----------

#salvar na camada ouro para alimentar o PBI
df_ouro.write.format('delta').mode('overwrite').save(Blob_Path + '/mnt/ouro/pnad6407')
df_ouro.write.format('delta').mode('overwrite').saveAsTable('Populacao')