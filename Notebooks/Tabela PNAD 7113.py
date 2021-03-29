# Databricks notebook source
# MAGIC %md
# MAGIC # Analfabetismo

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
# MAGIC ###Importando os pacotes

# COMMAND ----------

#importar pacotes
import requests
import json

# COMMAND ----------

# MAGIC %md
# MAGIC ###Fazendo request (url) e transformando o Json em DataFrame do Spark

# COMMAND ----------

#fazer request e verificar conexão
api = 'http://api.sidra.ibge.gov.br/values/t/7113/n1/1/v/10267/p/2017-2019/c2/all/c58/2795/f/n?formato=json'
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
# MAGIC ###Salvando na camada bronze e fazendo as transformações

# COMMAND ----------

#salvar no delta lake (tabela bronze)
df.write.format('delta').mode('overwrite').save(Blob_Path + '/mnt/bronze/pnad7113')

# COMMAND ----------

#ler como dataframe novo
df_prata = spark.read.format('delta').load(Blob_Path + '/mnt/bronze/pnad7113')  
#mostrar novo dataframe e verificar esquema
df_prata.show()
df_prata.printSchema()

# COMMAND ----------

#mudar tipo das colunas
df_prata = df_prata.withColumn('Valor',df_prata['V'].cast('float')).withColumn('Ano',df_prata['D3N'].cast('date')).withColumn('Grupo_de_Idade',df_prata['D5N']).withColumn('Sexo',df_prata['D4N'])
#verificar se as mudanças deram certo
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
df_prata = df_prata.sort('Ano','Grupo_de_idade','Sexo')
#mostrar dataframe
display(df_prata)

# COMMAND ----------

# MAGIC %md
# MAGIC ###Salvando na camada prata e verificando os dados

# COMMAND ----------

#Salvar na camada prata
df_prata.write.format('delta').mode('overwrite').save(Blob_Path + '/mnt/prata/pnad7113')

# COMMAND ----------

#verificar os dados
df_ouro = spark.read.format('delta').load(Blob_Path + '/mnt/prata/pnad7113') 
df_ouro.printSchema()
display(df_ouro)

# COMMAND ----------

# MAGIC %md
# MAGIC ###Salvando na camada ouro para alimentar o PowerBI

# COMMAND ----------

#salvar na camada ouro para alimentar o PBI 
df_ouro.write.format('delta').mode('overwrite').save(Blob_Path + '/mnt/ouro/pnad7113')
df_ouro.write.format('delta').mode('overwrite').saveAsTable('Analfabetismo')