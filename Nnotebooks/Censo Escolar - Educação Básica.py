# Databricks notebook source
# MAGIC %md
# MAGIC # Censo Escolar - Matrículas

# COMMAND ----------

# MAGIC %md
# MAGIC ## Configuração do ambiente

# COMMAND ----------

spark.conf.set(
  "fs.azure.account.key.m03storage.blob.core.windows.net",
  "3Ts0JpdSfDvYx8JOerU9CrExx1Ankj3W1q5J3Cscu2hXlQR90cpMlVy6p2mBB0NpKndKWixIcp1poqcFrAJ+oQ==")

# COMMAND ----------

#caminho do Blob
blobPath = 'wasbs://csvmatricula@m03storage.blob.core.windows.net/'
Caminho = 'wasbs://m03container@m03storage.blob.core.windows.net/'

# COMMAND ----------

#importar os pacotes
from pyspark.sql.types import *
from pyspark.sql.functions import *

# COMMAND ----------

# MAGIC %md
# MAGIC ## Download das Bases de Dados

# COMMAND ----------

# MAGIC %sh
# MAGIC echo 'Iniciando download do Censo Escolar 2017...'
# MAGIC wget https://download.inep.gov.br/microdados/micro_censo_escolar_2017.zip -O /tmp/censo_escolar_2017.zip

# COMMAND ----------

# MAGIC %sh
# MAGIC echo 'Iniciando download do Censo Escolar 2018...'
# MAGIC wget https://download.inep.gov.br/microdados/microdados_educacao_basica_2018.zip -O /tmp/censo_escolar_2018.zip

# COMMAND ----------

# MAGIC %sh 
# MAGIC echo 'Iniciando download do Censo Escolar 2019...'
# MAGIC wget https://download.inep.gov.br/microdados/microdados_educacao_basica_2019.zip -O /tmp/censo_escolar_2019.zip

# COMMAND ----------

dbutils.fs.cp('file:/tmp/censo_escolar_2017.zip', Caminho + 'censo_escolar_2017.zip')
dbutils.fs.cp('file:/tmp/censo_escolar_2018.zip', Caminho + 'censo_escolar_2018.zip')
dbutils.fs.cp('file:/tmp/censo_escolar_2019.zip', Caminho + 'censo_escolar_2019.zip')

# COMMAND ----------

# MAGIC %md
# MAGIC Descompactação dos arquivos **.zip** sem a sua estrutura de diretórios em pastas para cada ano do censo

# COMMAND ----------

# MAGIC %sh
# MAGIC unzip -j /tmp/censo_escolar_2019.zip -d /tmp/matricula_2019/

# COMMAND ----------

# MAGIC %sh
# MAGIC unzip -j /tmp/censo_escolar_2018.zip -d /tmp/matricula_2018/

# COMMAND ----------

# MAGIC %sh
# MAGIC unzip -j /tmp/censo_escolar_2017.zip -d /tmp/matricula_2017/

# COMMAND ----------

# MAGIC %sh
# MAGIC unzip -j /tmp/matricula_2018/'*.zip' -d /tmp/matricula_2018/

# COMMAND ----------

# MAGIC %sh
# MAGIC unzip -j /tmp/matricula_2017/'*.zip' -d /tmp/matricula_2017/

# COMMAND ----------

# MAGIC %md
# MAGIC ## Censo Escolar

# COMMAND ----------

# MAGIC %md
# MAGIC ### 1. Matrículas

# COMMAND ----------

# MAGIC %md
# MAGIC #### 1.1. Ingestão dos dados no Delta Lake

# COMMAND ----------

# Leitura dos arquivos .CSV de cada ano e região, armazenando-os em dataframes separados.

matricula_co_2017 = (spark.read.option("sep", "|").option("header", "true").csv('file:/tmp/matricula_2017/MATRICULA_CO.CSV'))
matricula_norte_2017 = (spark.read.option("sep", "|").option("header", "true").csv('file:/tmp/matricula_2017/MATRICULA_NORTE.CSV'))
matricula_nordeste_2017 = (spark.read.option("sep", "|").option("header", "true").csv('file:/tmp/matricula_2017/MATRICULA_NORDESTE.CSV'))
matricula_sul_2017 = (spark.read.option("sep", "|").option("header", "true").csv('file:/tmp/matricula_2017/MATRICULA_SUL.CSV'))
matricula_sudeste_2017 = (spark.read.option("sep", "|").option("header", "true").csv('file:/tmp/matricula_2017/MATRICULA_SUDESTE.CSV'))

matricula_co_2018 = (spark.read.option("sep", "|").option("header", "true").csv('file:/tmp/matricula_2018/MATRICULA_CO.CSV'))
matricula_norte_2018 = (spark.read.option("sep", "|").option("header", "true").csv('file:/tmp/matricula_2018/MATRICULA_NORTE.CSV'))
matricula_nordeste_2018 = (spark.read.option("sep", "|").option("header", "true").csv('file:/tmp/matricula_2018/MATRICULA_NORDESTE.CSV'))
matricula_sul_2018 = (spark.read.option("sep", "|").option("header", "true").csv('file:/tmp/matricula_2018/MATRICULA_SUL.CSV'))
matricula_sudeste_2018 = (spark.read.option("sep", "|").option("header", "true").csv('file:/tmp/matricula_2018/MATRICULA_SUDESTE.CSV'))

matricula_co_2019 = (spark.read.option("sep", "|").option("header", "true").csv('file:/tmp/matricula_2019/MATRICULA_CO.CSV'))
matricula_norte_2019 = (spark.read.option("sep", "|").option("header", "true").csv('file:/tmp/matricula_2019/MATRICULA_NORTE.CSV'))
matricula_nordeste_2019 = (spark.read.option("sep", "|").option("header", "true").csv('file:/tmp/matricula_2019/MATRICULA_NORDESTE.CSV'))
matricula_sul_2019 = (spark.read.option("sep", "|").option("header", "true").csv('file:/tmp/matricula_2019/MATRICULA_SUL.CSV'))
matricula_sudeste_2019 = (spark.read.option("sep", "|").option("header", "true").csv('file:/tmp/matricula_2019/MATRICULA_SUDESTE.CSV'))

# COMMAND ----------

# MAGIC %md
# MAGIC #### 1.2. Transformação dos dados

# COMMAND ----------

# Lista de colunas a serem selecionadas do dataframe original porém com novo título

renamedFields = ['NU_ANO_CENSO as Ano', 
                 'NU_IDADE_REFERENCIA as Idade_Referencia', 
                 'NU_IDADE as Idade',
                 'TP_SEXO as Sexo',
                 'TP_COR_RACA as Cor_Raca', 
                 'TP_NACIONALIDADE as Nacionalidade', 
                 'TP_ZONA_RESIDENCIAL as Zona_Residencial', 
                 'IN_NECESSIDADE_ESPECIAL as Necessidade_Especial', 
                 'TP_ETAPA_ENSINO as Etapa_Ensino', 
                 'IN_ESPECIAL_EXCLUSIVA as Especial_Exclusiva', 
                 'IN_REGULAR as Ensino_Regular', 
                 'IN_EJA as Ensino_EJA', 
                 'IN_PROFISSIONALIZANTE as Ensino_Profissionalizante', 
                 'TP_MEDIACAO_DIDATICO_PEDAGO as Mediacao_Didatico_Pedago', 
                 'TP_DEPENDENCIA as Dependencia_Escola', 
                 'TP_LOCALIZACAO as Localizacao_Escola']

# COMMAND ----------

# Alteração dos dataframes carregados com base na lista definida anteriormente

matricula_co_2017 = matricula_co_2017.selectExpr(renamedFields)
matricula_norte_2017 = matricula_norte_2017.selectExpr(renamedFields)
matricula_nordeste_2017 = matricula_nordeste_2017.selectExpr(renamedFields)
matricula_sul_2017 = matricula_sul_2017.selectExpr(renamedFields)
matricula_sudeste_2017 = matricula_sudeste_2017.selectExpr(renamedFields)

matricula_co_2018 = matricula_co_2018.selectExpr(renamedFields)
matricula_norte_2018 = matricula_norte_2018.selectExpr(renamedFields)
matricula_nordeste_2018 = matricula_nordeste_2018.selectExpr(renamedFields)
matricula_sul_2018 = matricula_sul_2018.selectExpr(renamedFields)
matricula_sudeste_2018 = matricula_sudeste_2018.selectExpr(renamedFields)

matricula_co_2019 = matricula_co_2019.selectExpr(renamedFields)
matricula_norte_2019 = matricula_norte_2019.selectExpr(renamedFields)
matricula_nordeste_2019 = matricula_nordeste_2019.selectExpr(renamedFields)
matricula_sul_2019 = matricula_sul_2019.selectExpr(renamedFields)
matricula_sudeste_2019 = matricula_sudeste_2019.selectExpr(renamedFields)

# COMMAND ----------

# Unificação dos dataframes das regiões em um único dataframe para cada ano e armazenando na camada Prata
#2017

matricula_co_2017_silver.write.format('delta').mode('overwrite').save(blobPath + silverPath + 'matricula_2017_')
matricula_norte_2017_silver.write.format('delta').mode('append').save(blobPath + silverPath + 'matricula_2017_')
matricula_nordeste_2017_silver.write.format('delta').mode('append').save(blobPath + silverPath + 'matricula_2017_')
matricula_sul_2017_silver.write.format('delta').mode('append').save(blobPath + silverPath + 'matricula_2017_')
matricula_sudeste_2017_silver.write.format('delta').mode('append').save(blobPath + silverPath + 'matricula_2017_')

# COMMAND ----------

#2018
matricula_co_2018_silver.write.format('delta').mode('overwrite').save(blobPath + silverPath + 'matricula_2018_')
matricula_norte_2018_silver.write.format('delta').mode('append').save(blobPath + silverPath + 'matricula_2018_')
matricula_nordeste_2018_silver.write.format('delta').mode('append').save(blobPath + silverPath + 'matricula_2018_')
matricula_sul_2018_silver.write.format('delta').mode('append').save(blobPath + silverPath + 'matricula_2018_')
matricula_sudeste_2018_silver.write.format('delta').mode('append').save(blobPath + silverPath + 'matricula_2018_')

# COMMAND ----------

#2019
matricula_co_2019_silver.write.format('delta').mode('overwrite').save(blobPath + silverPath + 'matricula_2019_')
matricula_norte_2019_silver.write.format('delta').mode('append').save(blobPath + silverPath + 'matricula_2019_')
matricula_nordeste_2019_silver.write.format('delta').mode('append').save(blobPath + silverPath + 'matricula_2019_')
matricula_sul_2019_silver.write.format('delta').mode('append').save(blobPath + silverPath + 'matricula_2019_')
matricula_sudeste_2019_silver.write.format('delta').mode('append').save(blobPath + silverPath + 'matricula_2019_')

# COMMAND ----------

# Leitura dos dataframes unificados

matricula_2017_silver = spark.read.format("delta").load(blobPath + silverPath + 'matricula_2017_')
matricula_2018_silver = spark.read.format("delta").load(blobPath + silverPath + 'matricula_2018_')
matricula_2019_silver = spark.read.format("delta").load(blobPath + silverPath + 'matricula_2019_')

# COMMAND ----------

# Validação da unificação dos dataframes, comparando o total de registros

totalBronzeRows = matricula_co_2017_silver.count() + matricula_norte_2017_silver.count() + matricula_nordeste_2017_silver.count() + matricula_sul_2017_silver.count() + matricula_sudeste_2017_silver.count()

print("2017:", matricula_2017_silver.count() == totalBronzeRows)

totalBronzeRows = matricula_co_2018_silver.count() + matricula_norte_2018_silver.count() + matricula_nordeste_2018_silver.count() + matricula_sul_2018_silver.count() + matricula_sudeste_2018_silver.count()

print("2018:", matricula_2018_silver.count() == totalBronzeRows)

totalBronzeRows = matricula_co_2019_silver.count() + matricula_norte_2019_silver.count() + matricula_nordeste_2019_silver.count() + matricula_sul_2019_silver.count() + matricula_sudeste_2019_silver.count()

print("2019:", matricula_2019_silver.count() == totalBronzeRows)

# COMMAND ----------

# Verificando a quantidade de registros null por coluna de cada dataframe

display(matricula_2017_silver.select([count(when(col(c).isNull(), c)).alias(c) for c in matricula_2017_silver.columns]))
display(matricula_2018_silver.select([count(when(col(c).isNull(), c)).alias(c) for c in matricula_2018_silver.columns]))
display(matricula_2019_silver.select([count(when(col(c).isNull(), c)).alias(c) for c in matricula_2019_silver.columns]))

# COMMAND ----------

#Definição de dicionário para substituição dos valores nulos no formato -> 'Coluna':'Novo Valor'

nullSubstDict = {'Etapa_Ensino': '0', 'Especial_Exclusiva': '2', 'Ensino_Regular': '2', 'Ensino_EJA': '2', 'Ensino_Profissionalizante': '2'}

# COMMAND ----------

# Substituição dos valores nulos de cada dataframe com base no dicionário definido

matricula_2017_silver = matricula_2017_silver.fillna(nullSubstDict)
matricula_2018_silver = matricula_2018_silver.fillna(nullSubstDict)
matricula_2019_silver = matricula_2019_silver.fillna(nullSubstDict)

# COMMAND ----------

# Verificando a quantidade de registros null por coluna de cada dataframe

display(matricula_2017_silver.select([count(when(col(c).isNull(), c)).alias(c) for c in matricula_2017_silver.columns]))
display(matricula_2018_silver.select([count(when(col(c).isNull(), c)).alias(c) for c in matricula_2018_silver.columns]))
display(matricula_2019_silver.select([count(when(col(c).isNull(), c)).alias(c) for c in matricula_2019_silver.columns]))

# COMMAND ----------

# Unificando os 3 dataframes em um único dataframe
#2017
matricula_2017_silver.write.format('delta').mode('overwrite').save(blobPath + silverPath + 'matriculas_')


# COMMAND ----------

#2018
matricula_2018_silver.write.format('delta').mode('append').save(blobPath + silverPath + 'matriculas_')


# COMMAND ----------

#2019
matricula_2019_silver.write.format('delta').mode('append').save(blobPath + silverPath + 'matriculas_')

# COMMAND ----------

# Leitura do dataframe final para execução das últimas transformações

matricula_silver = spark.read.format("delta").load(blobPath + silverPath + 'matriculas_')

# COMMAND ----------

#verificar o dataframe
display(matricula_silver)

# COMMAND ----------

# Alteração dos valores de cada coluna de acordo com as informações correspondentes

matricula_silver = matricula_silver.replace(['1', '2'], 
                                            ['Masculino', 'Feminino'], 'Sexo')

matricula_silver = matricula_silver.replace(['0', '1', '2', '3', '4', '5'], 
                                            ['Não Declarada', 'Branca', 'Preta', 'Parda', 'Amarela', 'Indígena'], 'Cor_Raca')

matricula_silver = matricula_silver.replace(['1', '2', '3'], 
                                            ['Brasileira', 'Brasileira - nascido no exterior ou naturalizado', 'Estrangeira'], 'Nacionalidade')

matricula_silver = matricula_silver.replace(['0', '1'], 
                                            ['Não', 'Sim'], 'Necessidade_Especial')

matricula_silver = matricula_silver.replace(['0', '1', '2'], 
                                            ['Não', 'Sim', 'Não aplicável'], 'Especial_Exclusiva')

matricula_silver = matricula_silver.replace(['0', '1', '2'], 
                                            ['Não', 'Sim', 'Não aplicável'], 'Ensino_Regular')

matricula_silver = matricula_silver.replace(['0', '1', '2'], 
                                            ['Não', 'Sim', 'Não aplicável'], 'Ensino_EJA')

matricula_silver = matricula_silver.replace(['0', '1', '2'], 
                                            ['Não', 'Sim', 'Não aplicável'], 'Ensino_Profissionalizante')


# COMMAND ----------

# Devido ao tamanho dos dados da coluna 'Etapa_Ensino' é definido duas variáveis para os valores para melhor visualização

etapaOriginais = ['0', '1', '2', '4', '5', '6', '7', '8', '9', '10', '11', '14', '15', '16', '17', '18', '19', '20', '21', '41', '25', '26', '27', '28', '29', '30', '31', '32', '33', '34', '35', '36', '37', '38', '39', '40', '68', '65', '67', '69', '70', '71', '72', '73', '74']

etapaNovos = [
  'Não aplicável',
  'Educação Infantil - Creche',
  'Educação Infantil - Pré-escola',
  'Ensino Fundamental de 8 anos - 1ª Série',
  'Ensino Fundamental de 8 anos - 2ª Série',
  'Ensino Fundamental de 8 anos - 3ª Série',
  'Ensino Fundamental de 8 anos - 4ª Série',
  'Ensino Fundamental de 8 anos - 5ª Série',
  'Ensino Fundamental de 8 anos - 6ª Série',
  'Ensino Fundamental de 8 anos - 7ª Série',
  'Ensino Fundamental de 8 anos - 8ª Série',
  'Ensino Fundamental de 9 anos - 1º Ano',
  'Ensino Fundamental de 9 anos - 2º Ano',
  'Ensino Fundamental de 9 anos - 3º Ano',
  'Ensino Fundamental de 9 anos - 4º Ano',
  'Ensino Fundamental de 9 anos - 5º Ano',
  'Ensino Fundamental de 9 anos - 6º Ano',
  'Ensino Fundamental de 9 anos - 7º Ano',
  'Ensino Fundamental de 9 anos - 8º Ano',
  'Ensino Fundamental de 9 anos - 9º Ano',
  'Ensino Médio - 1ª Série',
  'Ensino Médio - 2ª Série',
  'Ensino Médio - 3ª Série',
  'Ensino Médio - 4ª Série',
  'Ensino Médio - Não Seriada',
  'Curso Técnico Integrado (Ensino Médio Integrado) 1ª Série',
  'Curso Técnico Integrado (Ensino Médio Integrado) 2ª Série',
  'Curso Técnico Integrado (Ensino Médio Integrado) 3ª Série',
  'Curso Técnico Integrado (Ensino Médio Integrado) 4ª Série',
  'Curso Técnico Integrado (Ensino Médio Integrado) Não Seriada',
  'Ensino Médio - Normal/Magistério 1ª Série',
  'Ensino Médio - Normal/Magistério 2ª Série',
  'Ensino Médio - Normal/Magistério 3ª Série',
  'Ensino Médio - Normal/Magistério 4ª Série',
  'Curso Técnico - Concomitante',
  'Curso Técnico - Subsequente',
  'Curso FIC Concomitante',
  'EJA - Ensino Fundamental - Projovem Urbano',
  'Curso FIC integrado na modalidade EJA - Nivel Médio',
  'EJA - Ensino Fundamental - Anos Iniciais',
  'EJA - Ensino Fundamental - Anos Finais',
  'EJA - Ensino Médio',
  'EJA - Ensino Fundamental - Anos Iniciais e Anos Finais',
  'Curso FIC Integrado EJA - Nivel Fundamental',
  'Curso Técnico Integrado EJA - Nivel Médio'
]

# COMMAND ----------

# Alteração dos valores da coluna 'Etapa_Ensino'

matricula_silver = matricula_silver.replace(etapaOriginais, etapaNovos, 'Etapa_Ensino')

# COMMAND ----------

# Visualização do dataframe após alteração

display(matricula_silver)

# COMMAND ----------

#Transformar os tipos de dados
matricula_silver = matricula_silver.withColumn('Idade', matricula_silver['Idade'].cast(IntegerType())).withColumn('Ano', matricula_silver['Ano'].cast(DateType()))
matricula_silver.printSchema()

# COMMAND ----------

#Salvar o dataframe na camada ouro para consumo pelo Power BI
matricula_silver.write.format('delta').mode('overwrite').save(blobPath + 'teste/matriculas')
matricula_silver.write.format('delta').mode('overwrite').saveAsTable('teste_Matriculas')