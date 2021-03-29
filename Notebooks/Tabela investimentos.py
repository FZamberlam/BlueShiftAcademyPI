# Databricks notebook source
# MAGIC %md
# MAGIC # Investimentos em educação

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
# MAGIC ###Instalando e importando pacotes

# COMMAND ----------

#instalar o pacote para abrir arquivos .xlsx
pip install openpyxl

# COMMAND ----------

#importar pacotes
import pandas as pd

# COMMAND ----------

# MAGIC %md
# MAGIC ###Buscando e extraindo o arquivo na url

# COMMAND ----------

# MAGIC   %sh
# MAGIC wget  https://download.inep.gov.br/informacoes_estatisticas/investimentos_publicos_em_educacao/indicadores_financeiros_educacionais/investimento_pib_total.zip -O /tmp/investimentos.zip

# COMMAND ----------

dbutils.fs.mv('file:/tmp/investimentos.zip', 'dbfs:/tmp/investimentos.zip')

# COMMAND ----------

dbutils.fs.cp('dbfs:/tmp/investimentos.zip', 'file:/tmp/investimentosPIB.zip')

# COMMAND ----------

#extrair os arquivos
%sh
unzip /tmp/investimentosPIB.zip -d /tmp/investimentos

# COMMAND ----------

# MAGIC %md
# MAGIC ###Lendo o arquivo necessário e salvando na camada bronze

# COMMAND ----------

#abrir o arquivo de excel, apenas com as informações necessárias e transformar em um dataframe pandas
excel_file = '/tmp/investimentos/Investimento_pib_total/Investimento_pib_total.xlsx'
cols = ['Ano','Todos_niveis','Ed.Basica','Ed.Infantil','E.F.Anos_Iniciais','E.F.Anos_Finais','Ensino_Medio','Ed.Superior']
df = pd.read_excel(excel_file,names=cols,engine='openpyxl',usecols='A:H',skiprows=7,skipfooter=58, header=None)
#verificar o dataframe
df

# COMMAND ----------

#habilitar arrow para otimização e criar spark dataframe a partir de pandas dataframe
spark.conf.set('spark.sql.execution.arrow.enabled', True)
df = spark.createDataFrame(df)

# COMMAND ----------

#salvar no delta lake (tabela bronze) dados sem modificação
df.write.format('delta').mode('overwrite').save(Blob_Path + '/mnt/bronze/investimentos') 

# COMMAND ----------

# MAGIC %md
# MAGIC ###Lendo o arquivo e fazendo as transformações

# COMMAND ----------

#ler como dataframe novo para fazer transformações
df_prata = spark.read.format('delta').load(Blob_Path + '/mnt/bronze/investimentos') 
display(df_prata)

# COMMAND ----------

#transformar em datafrane do Pandas para fazer as primeiras transformações
df_prata = df_prata.toPandas()

# COMMAND ----------

#mudar a tabela ano para data e verificar as informações
df_prata['Ano'] = pd.to_datetime(df_prata['Ano'],format='%Y')
print(df_prata.info())

# COMMAND ----------

#mudar precisão do float e verificar o dataframe
df_prata['Todos_niveis'] = [round(float(x),1) for x in df_prata['Todos_niveis'].values]
df_prata['Ed.Basica'] = [round(float(x),1) for x in df_prata['Ed.Basica'].values]
df_prata['Ed.Infantil'] = [round(float(x),1) for x in df_prata['Ed.Infantil'].values]
df_prata['E.F.Anos_Iniciais'] = [round(float(x),1) for x in df_prata['E.F.Anos_Iniciais'].values]
df_prata['E.F.Anos_Finais'] = [round(float(x),1) for x in df_prata['E.F.Anos_Finais'].values]
df_prata['Ensino_Medio'] = [round(float(x),1) for x in df_prata['Ensino_Medio'].values]
df_prata['Ed.Superior'] = [round(float(x),1) for x in df_prata['Ed.Superior'].values]
display(df_prata)

# COMMAND ----------

#habilitar arrow para otimização e criar spark dataframe a partir de pandas dataframe
spark.conf.set('spark.sql.execution.arrow.enabled', True)
df_prata = spark.createDataFrame(df_prata)

# COMMAND ----------

# MAGIC %md
# MAGIC ###Salvando na camada Prata com as transformações

# COMMAND ----------

#salvar no delta lake (tabela prata)
df_prata.write.format('delta').mode('overwrite').save(Blob_Path + '/mnt/prata/investimentos')

# COMMAND ----------

# MAGIC %md
# MAGIC ###Verificando o DataFrame e salvando na camada ouro para alimentar o Power BI

# COMMAND ----------

#ler como dataframe novo(ouro) para verificar o esquema
df_ouro = spark.read.format('delta').load(Blob_Path + '/mnt/prata/investimentos')  
df_ouro.printSchema()
display(df_ouro)

# COMMAND ----------

#Salvar na camada ouro para alimentar o PBI
df_ouro.write.format('delta').mode('overwrite').save(Blob_Path + '/mnt/ouro/investimentos')
df_ouro.write.format('delta').mode('overwrite').saveAsTable('Investimentos') 