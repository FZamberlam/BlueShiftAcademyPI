# Databricks notebook source
# MAGIC %md
# MAGIC # Crianças em creche/escola (0 a 5 anos)

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
import pandas as pd
import json

# COMMAND ----------

# MAGIC %md
# MAGIC ###Fazendo request para a url e transformando o arquivo json em DataFrame do Spark

# COMMAND ----------

#fazer request e verificar conexão
api = 'http://api.sidra.ibge.gov.br/values/t/7140/n1/1/v/10278/p/2017-2019/c58/all/c12081/all/f/n?formato=json'
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
df.write.format('delta').mode('overwrite').save(Blob_Path + '/mnt/bronze/pnad7140')

# COMMAND ----------

#ler como dataframe novo
df_prata = spark.read.format('delta').load(Blob_Path + '/mnt/bronze/pnad7140')  
#mostrar novo dataframe
df_prata.show()

# COMMAND ----------

#mudar tipo das colunas e multiplicar por mil para obter o número absoluto de pessoas
df_prata = df_prata.withColumn('Valor',df_prata['V'].cast('int')*1000).withColumn('Ano',df_prata['D3N'].cast('date')).withColumn('Grupo_de_Idade',df_prata['D4N']).withColumn('Frequenta_Escola_Creche',df_prata['D5N'])
#verificar o esquema
df_prata.printSchema()

# COMMAND ----------

#mostrar apenas as colunas que vamos usar
df_prata = df_prata.select('Ano','Grupo_de_Idade','Frequenta_Escola_Creche', 'Valor')
#mostrar dataframe
display(df_prata)

# COMMAND ----------

#apenas valores não nulos
df_prata = df_prata.filter('Valor IS NOT NULL')

# COMMAND ----------

#ordenar dados
df_prata = df_prata.sort('Ano','Grupo_de_Idade','Frequenta_Escola_Creche')
#mostrar dataframe
display(df_prata)

# COMMAND ----------

#transfrormar em dataframe do pandas
df_prata = df_prata.toPandas()
#mudar valores nas linhas da coluna 'Frequenta_Escola_Creche' 
dict_dados = {'Frequenta escola ou creche':'Sim',
                  'Não frequenta escola ou creche':'Não',
                  'Total':'Total'}
df_prata['Frequenta_Escola_Creche'] = df_prata['Frequenta_Escola_Creche'].map(dict_dados)
display(df_prata)

# COMMAND ----------

#habilitar arrow para otimização e criar spark dataframe a partir de pandas dataframe
spark.conf.set('spark.sql.execution.arrow.enabled', True)
df_prata = spark.createDataFrame(df_prata)

# COMMAND ----------

# MAGIC %md
# MAGIC ###Salvando na camada prata e verificando o arquivo

# COMMAND ----------

#Salvar na camada prata
df_prata.write.format('delta').mode('overwrite').save(Blob_Path + '/mnt/prata/pnad7140')

# COMMAND ----------

#verificando o esquema e os dados
df_ouro = spark.read.format('delta').load(Blob_Path + '/mnt/prata/pnad7140')  
df_ouro.printSchema()
display(df_ouro)

# COMMAND ----------

# MAGIC %md
# MAGIC ###Salvanda na camada ouro para consumo pelo PowerBI

# COMMAND ----------

#salvar na camada ouro para alimentar o PBI
df_ouro.write.format('delta').mode('overwrite').save(Blob_Path + '/mnt/ouro/pnad7140')
df_ouro.write.format('delta').mode('overwrite').saveAsTable('Criancas_0_a_5')