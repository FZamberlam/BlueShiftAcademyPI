# Databricks notebook source
# MAGIC %md
# MAGIC # Tabela notas IDEB

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
# MAGIC instalar o interpretador necessário para a leitura do arquivo .xlsx

# COMMAND ----------

pip install openpyxl

# COMMAND ----------

# MAGIC %md
# MAGIC ###Importar os pacotes

# COMMAND ----------

#importar pacotes
from pyspark.sql.functions import *
from pyspark.sql.types import *
import pandas as pd
from pyspark.sql.functions import lit

# COMMAND ----------

# MAGIC %md
# MAGIC ###Extração dos arquivos da url

# COMMAND ----------

# MAGIC %sh
# MAGIC wget http://download.inep.gov.br/educacao_basica/portal_ideb/planilhas_para_download/2019/divulgacao_brasil_ideb_2019.zip  -O /tmp/IDEB.zip

# COMMAND ----------

# MAGIC %md
# MAGIC ###Fazer backup do arquivo .zip para o DBFS

# COMMAND ----------

#salvar do diretório temporário no dbfs para não perder o arquivo
dbutils.fs.mv('file:/tmp/IDEB.zip', 'dbfs:/tmp/IDEB.zip')

# COMMAND ----------

#salvar na máquina para extração dos dados
dbutils.fs.cp('dbfs:/tmp/IDEB.zip', 'file:/tmp/nota_IDEB.zip')

# COMMAND ----------

#extração dos dados
%sh
unzip -j /tmp/nota_IDEB.zip -d /tmp/resultado_IDEB/

# COMMAND ----------

# MAGIC %md
# MAGIC ###Ler os arquivos xlsx, criar o dataframes verificando o esquema e salvar na camada bronze

# COMMAND ----------

#abrir o arquivo de excel, apenas com as informações necessárias e transformar em um dataframe pandas
#primeira aba
excel_file = '/tmp/resultado_IDEB/divulgacao_brasil_ideb_2019.xlsx'
cols = ['Rede','IDEB_2013','IDEB_2015','IDEB_2017','IDEB_2019','Projecao_2007','Projecao_2009','Projecao_2011','Projecao_2013','Projecao_2015','Projecao_2017','Projecao_2019','Projecao_2021']
df_iniciais = pd.read_excel(excel_file,sheet_name=0,names=cols,engine='openpyxl',usecols=[1,86,87,88,89,90,91,92,93,94,95,96,97],skiprows=10,skipfooter=3,header=None)
#verificar o dataframe
df_iniciais

# COMMAND ----------

#verificar o esquema
print(df_iniciais.info())

# COMMAND ----------

#abrir o arquivo de excel, apenas com as informações necessárias e transformar em um dataframe pandas
#segunda aba
excel_file = '/tmp/resultado_IDEB/divulgacao_brasil_ideb_2019.xlsx'
cols = ['Rede','IDEB_2013','IDEB_2015','IDEB_2017','IDEB_2019','Projecao_2007','Projecao_2009','Projecao_2011','Projecao_2013','Projecao_2015','Projecao_2017','Projecao_2019','Projecao_2021']
df_finais = pd.read_excel(excel_file,sheet_name=1,names=cols,engine='openpyxl',usecols=[1,78,79,80,81,82,83,84,85,86,87,88,89],skiprows=10,skipfooter=3,header=None)
#verificar o dataframe
df_finais

# COMMAND ----------

#verificar o esquema
print(df_finais.info())

# COMMAND ----------

#abrir o arquivo de excel, apenas com as informações necessárias e transformar em um dataframe pandas
#terceira aba
excel_file = '/tmp/resultado_IDEB/divulgacao_brasil_ideb_2019.xlsx'
cols = ['Rede','IDEB_2013','IDEB_2015','IDEB_2017','IDEB_2019','Projecao_2007','Projecao_2009','Projecao_2011','Projecao_2013','Projecao_2015','Projecao_2017','Projecao_2019','Projecao_2021']
df_medio = pd.read_excel(excel_file,sheet_name=2,names=cols,engine='openpyxl',usecols=[1,78,79,80,81,82,83,84,85,86,87,88,89],skiprows=10,skipfooter=3,header=None)
#verificar o dataframe
df_medio

# COMMAND ----------

#verificar o esquema
print(df_medio.info())

# COMMAND ----------

#habilitar arrow para otimização e criar spark dataframe a partir de pandas dataframe
spark.conf.set('spark.sql.execution.arrow.enabled', True)
df_iniciais = spark.createDataFrame(df_iniciais)

# COMMAND ----------

#habilitar arrow para otimização e criar spark dataframe a partir de pandas dataframe
spark.conf.set('spark.sql.execution.arrow.enabled', True)
df_finais = spark.createDataFrame(df_finais)

# COMMAND ----------

#habilitar arrow para otimização e criar spark dataframe a partir de pandas dataframe
spark.conf.set('spark.sql.execution.arrow.enabled', True)
df_medio = spark.createDataFrame(df_medio)

# COMMAND ----------

#salvar no delta lake (camada bronze) dados sem modificação
df_iniciais.write.format('delta').mode('overwrite').save(Blob_Path  + '/mnt/bronze/IDEB_iniciais')

# COMMAND ----------

#salvar no delta lake (camada bronze) dados sem modificação
df_finais.write.format('delta').mode('overwrite').save(Blob_Path  + '/mnt/bronze/IDEB_finais')

# COMMAND ----------

#salvar no delta lake (camada bronze) dados sem modificação
df_medio.write.format('delta').mode('overwrite').save(Blob_Path  + '/mnt/bronze/IDEB_medio')

# COMMAND ----------

# MAGIC %md
# MAGIC ###Ler o dataframe e fazer as transformações - salvando na camada prata (todos como um dataframe único)

# COMMAND ----------

#ler o dataframe de anos iniciais para fazer as transformações
df_iniciais = spark.read.format('delta').load(Blob_Path  + '/mnt/bronze/IDEB_iniciais')
display(df_iniciais)

# COMMAND ----------

#adicionando coluna de valor 'anos iniciais' ao dataframe 
df_iniciais = df_iniciais.withColumn('Nivel_Escolaridade', lit('Fundamental_Anos_Iniciais'))
display(df_iniciais)

# COMMAND ----------

# selecionar as colunas que vamos usar
df_iniciais = df_iniciais.select('Nivel_Escolaridade','Rede','IDEB_2017','IDEB_2019','Projecao_2021')
display(df_iniciais)

# COMMAND ----------

#ler o dataframe de anos finais para fazer as transformações
df_finais = spark.read.format('delta').load(Blob_Path  + '/mnt/bronze/IDEB_finais')
display(df_finais)

# COMMAND ----------

#adicionar coluna de valor 'anos finais' ao dataframe 
df_finais = df_finais.withColumn('Nivel_Escolaridade', lit('Fundamental_Anos_Finais'))
display(df_finais)

# COMMAND ----------

# selecionar as colunas que vamos usar
df_finais = df_finais.select('Nivel_Escolaridade','Rede','IDEB_2017','IDEB_2019','Projecao_2021')
display(df_finais)

# COMMAND ----------

#ler o dataframe de ensino médio para fazer as transformações
df_medio = spark.read.format('delta').load(Blob_Path  + '/mnt/bronze/IDEB_medio')
display(df_medio)

# COMMAND ----------

#adicionar coluna de valor 'ensino médio' ao dataframe 
df_medio = df_medio.withColumn('Nivel_Escolaridade', lit('Ensino_Médio'))
display(df_medio)

# COMMAND ----------

# selecionar as colunas que vamos usar
df_medio = df_medio.select('Nivel_Escolaridade','Rede','IDEB_2017','IDEB_2019','Projecao_2021')
display(df_medio)

# COMMAND ----------

#salvar no delta lake (camada prata)
df_iniciais.write.format('delta').mode('append').save(Blob_Path  + '/mnt/prata/IDEB')

# COMMAND ----------

#salvar no delta lake (camada prata) 
df_finais.write.format('delta').mode('append').save(Blob_Path  + '/mnt/prata/IDEB')

# COMMAND ----------

#salvar no delta lake (camada prata) 
df_medio.write.format('delta').mode('append').save(Blob_Path  + '/mnt/prata/IDEB')

# COMMAND ----------

# MAGIC %md
# MAGIC ###Ler e verificar o arquivo (camada prata)

# COMMAND ----------

#ler a junção de todos os arquivos como um DataFrame único
df_IDEB = spark.read.format('delta').load(Blob_Path  + '/mnt/prata/IDEB')  
df_IDEB.printSchema()
display(df_IDEB)

# COMMAND ----------

#transformar em pandas para fazer as últimas transformações
df_IDEB = df_IDEB.toPandas()

# COMMAND ----------

#usar nomes das colunas originais
colunas_originais = df_IDEB.columns
# Obter novos nomes de colunas para renomear e poder separar o que são numéricos
colunas_rename = [x if not x.split('_')[-1].isdigit() else x.split('_')[-1] for x in  colunas_originais]
print(f'Novos nomes de colunas: {colunas_rename}')
# separar colunas que são digitos
colunas_melt = [x for x in colunas_rename if x.isdigit()]
print(f'Colunas que vão virar linhas: {colunas_melt}')
# separar as demais colunas
colunas_index = [x for x in colunas_rename if x not in colunas_melt]
print(f'Colunas para manter como index: {colunas_index}')

# obter um dicionario DE>PARA com nomes originais > novos nomes para garantir que não vai perder
## ordem das colunas com df_IDEB.columns = 
dict_colunas_rename= {k:v for k,v in zip(colunas_originais, colunas_rename)}
print(dict_colunas_rename)

# renomear:
df_IDEB.rename(columns=dict_colunas_rename, inplace=True)
display(df_IDEB)

# COMMAND ----------

#função melt para transformar as colunas de 2017/2019/2021 em valores de uma coluna 'ano'
df_IDEB = df_IDEB.melt(id_vars=colunas_index, value_vars=colunas_melt, var_name='Ano', value_name='Valor')
display(df_IDEB)

# COMMAND ----------

#habilitar arrow para otimização e criar spark dataframe a partir de pandas dataframe
spark.conf.set('spark.sql.execution.arrow.enabled', True)
df_IDEB = spark.createDataFrame(df_IDEB)
display(df_IDEB)

# COMMAND ----------

# MAGIC %md
# MAGIC ###Salvar na camada ouro para consumo pelo PowerBI

# COMMAND ----------

#ler como dataframe novo(ouro) e persistir na camada ouro para alimentar o PBI
df_IDEB.write.format('delta').mode('overwrite').save(Blob_Path  + '/mnt/ouro/IDEB')
df_IDEB.write.format('delta').mode('overwrite').saveAsTable('IDEB')