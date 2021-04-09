# Databricks notebook source
# MAGIC %md
# MAGIC # Censo Escolar - Matrículas

# COMMAND ----------

# MAGIC %md
# MAGIC ## Configuração do ambiente

# COMMAND ----------

#integrar o notebook ao blob
spark.conf.set(
  "fs.azure.account.key.m03storage.blob.core.windows.net",
  "3Ts0JpdSfDvYx8JOerU9CrExx1Ankj3W1q5J3Cscu2hXlQR90cpMlVy6p2mBB0NpKndKWixIcp1poqcFrAJ+oQ==")

# COMMAND ----------

#salvarc= caminhos do blob
blobPath = 'wasbs://csvmatricula@m03storage.blob.core.windows.net/'
Caminho = 'wasbs://m03container@m03storage.blob.core.windows.net/'


# COMMAND ----------

#importar as bibliotecas
from pyspark.sql.types import *
from pyspark.sql.functions import *

# COMMAND ----------

# MAGIC %md
# MAGIC ## Download das Bases de Dados do Blob

# COMMAND ----------

dbutils.fs.cp(Caminho + 'censo_escolar_2017.zip', "file:/tmp/censo_escolar_2017.zip")

# COMMAND ----------

dbutils.fs.cp(Caminho + 'censo_escolar_2018.zip', "file:/tmp/censo_escolar_2018.zip")

# COMMAND ----------

dbutils.fs.cp(Caminho + 'censo_escolar_2019.zip', "file:/tmp/censo_escolar_2019.zip")

# COMMAND ----------

# MAGIC %md
# MAGIC Descompactação dos arquivos **.zip**

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

# MAGIC 
# MAGIC %sh
# MAGIC unzip -j /tmp/matricula_2018/'*.zip' -d /tmp/matricula_2018/

# COMMAND ----------

# MAGIC %sh
# MAGIC unzip -j /tmp/matricula_2017/'*.zip' -d /tmp/matricula_2017/

# COMMAND ----------

# MAGIC %md
# MAGIC #### Ingestão dos dados

# COMMAND ----------

# Leitura dos arquivos .CSV de cada ano e região, armazenando-os em dataframes separados.

matricula_co_2017 = (spark.read.option("sep", "|").option("header", "true").csv('file:/tmp/matricula_2017/MATRICULA_CO.CSV'))
matricula_norte_2017 = (spark.read.option("sep", "|").option("header", "true").csv('file:/tmp/matricula_2017/MATRICULA_NORTE.CSV'))
matricula_nordeste_2017 = (spark.read.option("sep", "|").option("header", "true").csv('file:/tmp/matricula_2017/MATRICULA_NORDESTE.CSV'))
matricula_sul_2017 = (spark.read.option("sep", "|").option("header", "true").csv('file:/tmp/matricula_2017/MATRICULA_SUL.CSV'))
matricula_sudeste_2017 = (spark.read.option("sep", "|").option("header", "true").csv('file:/tmp/matricula_2017/MATRICULA_SUDESTE.CSV'))



# COMMAND ----------

matricula_co_2018 = (spark.read.option("sep", "|").option("header", "true").csv('file:/tmp/matricula_2018/MATRICULA_CO.CSV'))
matricula_norte_2018 = (spark.read.option("sep", "|").option("header", "true").csv('file:/tmp/matricula_2018/MATRICULA_NORTE.CSV'))
matricula_nordeste_2018 = (spark.read.option("sep", "|").option("header", "true").csv('file:/tmp/matricula_2018/MATRICULA_NORDESTE.CSV'))
matricula_sul_2018 = (spark.read.option("sep", "|").option("header", "true").csv('file:/tmp/matricula_2018/MATRICULA_SUL.CSV'))
matricula_sudeste_2018 = (spark.read.option("sep", "|").option("header", "true").csv('file:/tmp/matricula_2018/MATRICULA_SUDESTE.CSV'))



# COMMAND ----------

matricula_co_2019 = (spark.read.option("sep", "|").option("header", "true").csv('file:/tmp/matricula_2019/MATRICULA_CO.CSV'))
matricula_norte_2019 = (spark.read.option("sep", "|").option("header", "true").csv('file:/tmp/matricula_2019/MATRICULA_NORTE.CSV'))
matricula_nordeste_2019 = (spark.read.option("sep", "|").option("header", "true").csv('file:/tmp/matricula_2019/MATRICULA_NORDESTE.CSV'))
matricula_sul_2019 = (spark.read.option("sep", "|").option("header", "true").csv('file:/tmp/matricula_2019/MATRICULA_SUL.CSV'))
matricula_sudeste_2019 = (spark.read.option("sep", "|").option("header", "true").csv('file:/tmp/matricula_2019/MATRICULA_SUDESTE.CSV'))

# COMMAND ----------

# MAGIC %md
# MAGIC #### Transformação dos dados

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

#verificar o dataframe
display(matricula_co_2017)

# COMMAND ----------

# Unificação dos dataframes das regiões em um único dataframe para cada ano e armazenando no Blob

matricula_co_2017.coalesce(1).write.option("header","true").option("inferSchema", "true").mode('overwrite').csv(blobPath + 'tmp/matricula2017.csv')
matricula_norte_2017.coalesce(1).write.option("header","true").option("inferSchema", "true").mode('append').csv(blobPath + 'tmp/matricula2017.csv')
matricula_nordeste_2017.coalesce(1).write.option("header","true").option("inferSchema", "true").mode('append').csv(blobPath + 'tmp/matricula2017.csv')
matricula_sul_2017.coalesce(1).write.option("header","true").option("inferSchema", "true").mode('append').csv(blobPath + 'tmp/matricula2017.csv')
matricula_sudeste_2017.coalesce(1).write.option("header","true").option("inferSchema", "true").mode('append').csv(blobPath + 'tmp/matricula2017.csv')



# COMMAND ----------


matricula_co_2018.coalesce(1).write.option("header","true").option("inferSchema", "true").mode('overwrite').csv(blobPath + 'tmp/matricula2018.csv')
matricula_norte_2018.coalesce(1).write.option("header","true").option("inferSchema", "true").mode('append').csv(blobPath + 'tmp/matricula2018.csv')
matricula_nordeste_2018.coalesce(1).write.option("header","true").option("inferSchema", "true").mode('append').csv(blobPath + 'tmp/matricula2018.csv')
matricula_sul_2018.coalesce(1).write.option("header","true").option("inferSchema", "true").mode('append').csv(blobPath + 'tmp/matricula2018.csv')
matricula_sudeste_2018.coalesce(1).write.option("header","true").option("inferSchema", "true").mode('append').csv(blobPath + 'tmp/matricula2018.csv')


# COMMAND ----------


matricula_co_2019.coalesce(1).write.option("header","true").option("inferSchema", "true").mode('overwrite').csv(blobPath + 'tmp/matricula2019.csv')
matricula_norte_2019.coalesce(1).write.option("header","true").option("inferSchema", "true").mode('append').csv(blobPath + 'tmp/matricula2019.csv')
matricula_nordeste_2019.coalesce(1).write.option("header","true").option("inferSchema", "true").mode('append').csv(blobPath + 'tmp/matricula2019.csv')
matricula_sul_2019.coalesce(1).write.option("header","true").option("inferSchema", "true").mode('append').csv(blobPath + 'tmp/matricula2019.csv')
matricula_sudeste_2019.coalesce(1).write.option("header","true").option("inferSchema", "true").mode('append').csv(blobPath + 'tmp/matricula2019.csv')

# COMMAND ----------

# Leitura dos dataframes unificados

matricula_2017 = spark.read.format("csv").option("header","true").option("inferSchema", "true").load(blobPath + 'tmp/matricula2017.csv')
matricula_2018 = spark.read.format("csv").option("header","true").option("inferSchema", "true").load(blobPath + 'tmp/matricula2018.csv')
matricula_2019 = spark.read.format("csv").option("header","true").option("inferSchema", "true").load(blobPath + 'tmp/matricula2019.csv')

# COMMAND ----------

#verificar o dataframe
display(matricula_2017)

# COMMAND ----------

# Validação da unificação dos dataframes, comparando o total de registros

totalBronzeRows = matricula_co_2017.count() + matricula_norte_2017.count() + matricula_nordeste_2017.count() + matricula_sul_2017.count() + matricula_sudeste_2017.count()

print("2017:", matricula_2017.count() == totalBronzeRows)

totalBronzeRows = matricula_co_2018.count() + matricula_norte_2018.count() + matricula_nordeste_2018.count() + matricula_sul_2018.count() + matricula_sudeste_2018.count()

print("2018:", matricula_2018.count() == totalBronzeRows)

totalBronzeRows = matricula_co_2019.count() + matricula_norte_2019.count() + matricula_nordeste_2019.count() + matricula_sul_2019.count() + matricula_sudeste_2019.count()

print("2019:", matricula_2019.count() == totalBronzeRows)

# COMMAND ----------

# Verificando a quantidade de registros null por coluna de cada dataframe

display(matricula_2017.select([count(when(col(c).isNull(), c)).alias(c) for c in matricula_2017.columns]))
display(matricula_2018.select([count(when(col(c).isNull(), c)).alias(c) for c in matricula_2018.columns]))
display(matricula_2019.select([count(when(col(c).isNull(), c)).alias(c) for c in matricula_2019.columns]))

# COMMAND ----------

#Definição de dicionário para substituição dos valores nulos no formato -> 'Coluna':'Novo Valor'

nullSubstDict = {'Zona_Residencial': '0', 'Etapa_Ensino': '0', 'Especial_Exclusiva': '2', 'Ensino_Regular': '2', 'Ensino_EJA': '2', 'Ensino_Profissionalizante': '2'}

# COMMAND ----------

# Substituição dos valores nulos de cada dataframe com base no dicionário definido

matricula_2017 = matricula_2017.fillna(nullSubstDict)
matricula_2018 = matricula_2018.fillna(nullSubstDict)
matricula_2019 = matricula_2019.fillna(nullSubstDict)

# COMMAND ----------

# Verificando a quantidade de registros null por coluna de cada dataframe

display(matricula_2017.select([count(when(col(c).isNull(), c)).alias(c) for c in matricula_2017.columns]))
display(matricula_2018.select([count(when(col(c).isNull(), c)).alias(c) for c in matricula_2018.columns]))
display(matricula_2019.select([count(when(col(c).isNull(), c)).alias(c) for c in matricula_2019.columns]))

# COMMAND ----------

# Unificando os 3 dataframes em um único dataframe

matricula_2017.coalesce(1).write.option("header","true").option("inferSchema", "true").mode('overwrite').csv(blobPath + 'tmp/matriculas.csv')
matricula_2018.coalesce(1).write.option("header","true").option("inferSchema", "true").mode('append').csv(blobPath + 'tmp/matriculas.csv')
matricula_2019.coalesce(1).write.option("header","true").option("inferSchema", "true").mode('append').csv(blobPath + 'tmp/matriculas.csv')


# COMMAND ----------

# Leitura do dataframe final para execução das últimas transformações

matricula = spark.read.format("csv").option("header","true").load(blobPath + 'tmp/matriculas.csv')

# COMMAND ----------

# Alteração dos valores de cada coluna de acordo com as informações correspondentes

matricula = matricula.replace(['1', '2'], 
                                            ['Masculino', 'Feminino'], 'Sexo')

matricula = matricula.replace(['0', '1', '2', '3', '4', '5'], 
                                            ['Não Declarada', 'Branca', 'Preta', 'Parda', 'Amarela', 'Indígena'], 'Cor_Raca')

matricula = matricula.replace(['1', '2', '3'], 
                                            ['Brasileira', 'Brasileira - nascido no exterior ou naturalizado', 'Estrangeira'], 'Nacionalidade')

matricula = matricula.replace(['0', '1', '2'], 
                                            ['Não aplicável', 'Urbana', 'Rural'], 'Zona_Residencial')

matricula = matricula.replace(['0', '1'], 
                                            ['Não', 'Sim'], 'Necessidade_Especial')

matricula = matricula.replace(['1', '2', '3'], 
                                            ['Presencial', 'Semipresencial', 'Educação a Distância - EAD'], 'Mediacao_Didatico_Pedago')

matricula = matricula.replace(['0', '1', '2'], 
                                            ['Não', 'Sim', 'Não aplicável'], 'Especial_Exclusiva')

matricula = matricula.replace(['0', '1', '2'], 
                                            ['Não', 'Sim', 'Não aplicável'], 'Ensino_Regular')

matricula = matricula.replace(['0', '1', '2'], 
                                            ['Não', 'Sim', 'Não aplicável'], 'Ensino_EJA')

matricula = matricula.replace(['0', '1', '2'], 
                                            ['Não', 'Sim', 'Não aplicável'], 'Ensino_Profissionalizante')

matricula = matricula.replace(['1', '2', '3', '4'], 
                                            ['Federal', 'Estadual', 'Municipal', 'Privada'], 'Dependencia_Escola')

matricula = matricula.replace(['1', '2'], 
                                            ['Urbana', 'Rural'], 'Localizacao_Escola')

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

matricula = matricula.replace(etapaOriginais, etapaNovos, 'Etapa_Ensino')

# COMMAND ----------

# Visualização do dataframe após alteração

display(matricula)

# COMMAND ----------

#Transformar os tipos de dados
matricula = matricula.withColumn('Idade_Referencia', matricula['Idade_Referencia'].cast(IntegerType())).withColumn('Idade', matricula['Idade'].cast(IntegerType())).withColumn('Ano', matricula['Ano'].cast(DateType()))
matricula.printSchema()

# COMMAND ----------

#salvar como arquivo único no Blob
matricula.coalesce(1).write.option("header","true").option("inferSchema", "true").mode('overwrite').csv(blobPath + '/tmp/matriculas_final.csv')
