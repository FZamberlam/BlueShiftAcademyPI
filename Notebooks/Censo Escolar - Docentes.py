# Databricks notebook source
# MAGIC %md
# MAGIC # Censo Escolar - Docentes

# COMMAND ----------

# MAGIC %md
# MAGIC ## Configuração do ambiente

# COMMAND ----------

spark.conf.set(
  "fs.azure.account.key.m03storage.blob.core.windows.net",
  "3Ts0JpdSfDvYx8JOerU9CrExx1Ankj3W1q5J3Cscu2hXlQR90cpMlVy6p2mBB0NpKndKWixIcp1poqcFrAJ+oQ==")

# COMMAND ----------

#Caminhos do Blob
blobPath = 'wasbs://m03container@m03storage.blob.core.windows.net/'
bronzePath = 'mnt/bronze/censo_escolar/'
silverPath = 'mnt/prata/censo_escolar/'
goldPath = 'mnt/ouro/censo_escolar/'

# COMMAND ----------

#importar pacotes
from pyspark.sql.types import *
from pyspark.sql.functions import *

# COMMAND ----------

# MAGIC %md
# MAGIC #Download das bases de dados

# COMMAND ----------

# MAGIC %sh
# MAGIC echo 'Iniciando download do Censo Escolar 2017...'
# MAGIC wget https://download.inep.gov.br/microdados/micro_censo_escolar_2017.zip -O /tmp/censo_escolar_2017.zip

# COMMAND ----------

# MAGIC %sh
# MAGIC echo 'Iniciando download do Censo Escolar 2018...'
# MAGIC wget https://download.inep.gov.br/microdados/micro_censo_escolar_2018.zip -O /tmp/censo_escolar_2018.zip

# COMMAND ----------

# MAGIC %sh
# MAGIC echo 'Iniciando download do Censo Escolar 2019...'
# MAGIC wget https://download.inep.gov.br/microdados/micro_censo_escolar_2019.zip -O /tmp/censo_escolar_2019.zip

# COMMAND ----------

dbutils.fs.cp(blobPath + "censo_escolar_2017.zip", "file:/tmp/censo_escolar_2017.zip")
dbutils.fs.cp(blobPath + "censo_escolar_2018.zip", "file:/tmp/censo_escolar_2018.zip")
dbutils.fs.cp(blobPath + "censo_escolar_2019.zip", "file:/tmp/censo_escolar_2019.zip")

# COMMAND ----------

# MAGIC %md
# MAGIC Descompactação dos arquivos **.zip** sem a sua estrutura de diretórios em pastas para cada ano do censo

# COMMAND ----------

# MAGIC %sh
# MAGIC unzip -j /tmp/censo_escolar_2019.zip -d /tmp/censo2019/
# MAGIC unzip -j /tmp/censo_escolar_2018.zip -d /tmp/censo2018/
# MAGIC unzip -j /tmp/censo_escolar_2017.zip -d /tmp/censo2017/
# MAGIC unzip -j /tmp/censo2018/'*.zip' -d /tmp/censo2018/
# MAGIC unzip -j /tmp/censo2017/'*.zip' -d /tmp/censo2017/

# COMMAND ----------

# MAGIC %md
# MAGIC Remoção de todos os arquivos que não possuem a extensão **.CSV**

# COMMAND ----------

# MAGIC %sh
# MAGIC shopt -s extglob
# MAGIC rm -v /tmp/censo2019/!(*.CSV)
# MAGIC rm -v /tmp/censo2018/!(*.CSV)
# MAGIC rm -v /tmp/censo2017/!(*.CSV)
# MAGIC shopt -u extglob

# COMMAND ----------

# MAGIC %md
# MAGIC ## Censo Escolar

# COMMAND ----------

# MAGIC %md
# MAGIC ### 1. Docentes

# COMMAND ----------

# MAGIC %md
# MAGIC #### 1.1. Ingestão dos dados no Delta Lake

# COMMAND ----------

# Leitura dos arquivos .CSV de cada ano e região, armazenando-os em dataframes separados.

docentes_co_2017_bronze = (spark.read.option("sep", "|").option("header", "true").csv('file:/tmp/censo2017/DOCENTES_CO.CSV'))
docentes_norte_2017_bronze = (spark.read.option("sep", "|").option("header", "true").csv('file:/tmp/censo2017/DOCENTES_NORTE.CSV'))
docentes_nordeste_2017_bronze = (spark.read.option("sep", "|").option("header", "true").csv('file:/tmp/censo2017/DOCENTES_NORDESTE.CSV'))
docentes_sul_2017_bronze = (spark.read.option("sep", "|").option("header", "true").csv('file:/tmp/censo2017/DOCENTES_SUL.CSV'))
docentes_sudeste_2017_bronze = (spark.read.option("sep", "|").option("header", "true").csv('file:/tmp/censo2017/DOCENTES_SUDESTE.CSV'))

docentes_co_2018_bronze = (spark.read.option("sep", "|").option("header", "true").csv('file:/tmp/censo2018/DOCENTES_CO.CSV'))
docentes_norte_2018_bronze = (spark.read.option("sep", "|").option("header", "true").csv('file:/tmp/censo2018/DOCENTES_NORTE.CSV'))
docentes_nordeste_2018_bronze = (spark.read.option("sep", "|").option("header", "true").csv('file:/tmp/censo2018/DOCENTES_NORDESTE.CSV'))
docentes_sul_2018_bronze = (spark.read.option("sep", "|").option("header", "true").csv('file:/tmp/censo2018/DOCENTES_SUL.CSV'))
docentes_sudeste_2018_bronze = (spark.read.option("sep", "|").option("header", "true").csv('file:/tmp/censo2018/DOCENTES_SUDESTE.CSV'))

docentes_co_2019_bronze = (spark.read.option("sep", "|").option("header", "true").csv('file:/tmp/censo2019/DOCENTES_CO.CSV'))
docentes_norte_2019_bronze = (spark.read.option("sep", "|").option("header", "true").csv('file:/tmp/censo2019/DOCENTES_NORTE.CSV'))
docentes_nordeste_2019_bronze = (spark.read.option("sep", "|").option("header", "true").csv('file:/tmp/censo2019/DOCENTES_NORDESTE.CSV'))
docentes_sul_2019_bronze = (spark.read.option("sep", "|").option("header", "true").csv('file:/tmp/censo2019/DOCENTES_SUL.CSV'))
docentes_sudeste_2019_bronze = (spark.read.option("sep", "|").option("header", "true").csv('file:/tmp/censo2019/DOCENTES_SUDESTE.CSV'))

# COMMAND ----------

# Armazenamento dos dataframes na camada Bronze do Delta Lake

docentes_co_2017_bronze.write.format('delta').mode('overwrite').save(blobPath + bronzePath + 'docentes_co_2017')
docentes_norte_2017_bronze.write.format('delta').mode('overwrite').save(blobPath + bronzePath + 'docentes_norte_2017')
docentes_nordeste_2017_bronze.write.format('delta').mode('overwrite').save(blobPath + bronzePath + 'docentes_nordeste_2017')
docentes_sul_2017_bronze.write.format('delta').mode('overwrite').save(blobPath + bronzePath + 'docentes_sul_2017')
docentes_sudeste_2017_bronze.write.format('delta').mode('overwrite').save(blobPath + bronzePath + 'docentes_sudeste_2017')

docentes_co_2018_bronze.write.format('delta').mode('overwrite').save(blobPath + bronzePath + 'docentes_co_2018')
docentes_norte_2018_bronze.write.format('delta').mode('overwrite').save(blobPath + bronzePath + 'docentes_norte_2018')
docentes_nordeste_2018_bronze.write.format('delta').mode('overwrite').save(blobPath + bronzePath + 'docentes_nordeste_2018')
docentes_sul_2018_bronze.write.format('delta').mode('overwrite').save(blobPath + bronzePath + 'docentes_sul_2018')
docentes_sudeste_2018_bronze.write.format('delta').mode('overwrite').save(blobPath + bronzePath + 'docentes_sudeste_2018')

docentes_co_2019_bronze.write.format('delta').mode('overwrite').save(blobPath + bronzePath + 'docentes_co_2019')
docentes_norte_2019_bronze.write.format('delta').mode('overwrite').save(blobPath + bronzePath + 'docentes_norte_2019')
docentes_nordeste_2019_bronze.write.format('delta').mode('overwrite').save(blobPath + bronzePath + 'docentes_nordeste_2019')
docentes_sul_2019_bronze.write.format('delta').mode('overwrite').save(blobPath + bronzePath + 'docentes_sul_2019')
docentes_sudeste_2019_bronze.write.format('delta').mode('overwrite').save(blobPath + bronzePath + 'docentes_sudeste_2019')

# COMMAND ----------

# MAGIC %md
# MAGIC #### 1.2. Transformação dos dados

# COMMAND ----------

# Leitura dos dataframes salvos na camada Bronze

docentes_co_2017_silver = spark.read.format("delta").load(blobPath + bronzePath + 'docentes_co_2017')
docentes_norte_2017_silver = spark.read.format("delta").load(blobPath + bronzePath + 'docentes_norte_2017') 
docentes_nordeste_2017_silver = spark.read.format("delta").load(blobPath + bronzePath + 'docentes_nordeste_2017') 
docentes_sul_2017_silver = spark.read.format("delta").load(blobPath + bronzePath + 'docentes_sul_2017') 
docentes_sudeste_2017_silver = spark.read.format("delta").load(blobPath + bronzePath + 'docentes_sudeste_2017')

docentes_co_2018_silver = spark.read.format("delta").load(blobPath + bronzePath + 'docentes_co_2018')
docentes_norte_2018_silver = spark.read.format("delta").load(blobPath + bronzePath + 'docentes_norte_2018') 
docentes_nordeste_2018_silver = spark.read.format("delta").load(blobPath + bronzePath + 'docentes_nordeste_2018') 
docentes_sul_2018_silver = spark.read.format("delta").load(blobPath + bronzePath + 'docentes_sul_2018') 
docentes_sudeste_2018_silver = spark.read.format("delta").load(blobPath + bronzePath + 'docentes_sudeste_2018')

docentes_co_2019_silver = spark.read.format("delta").load(blobPath + bronzePath + 'docentes_co_2019')
docentes_norte_2019_silver = spark.read.format("delta").load(blobPath + bronzePath + 'docentes_norte_2019') 
docentes_nordeste_2019_silver = spark.read.format("delta").load(blobPath + bronzePath + 'docentes_nordeste_2019') 
docentes_sul_2019_silver = spark.read.format("delta").load(blobPath + bronzePath + 'docentes_sul_2019') 
docentes_sudeste_2019_silver = spark.read.format("delta").load(blobPath + bronzePath + 'docentes_sudeste_2019')

# COMMAND ----------

# Lista de colunas a serem selecionadas do dataframe original porém com novo título

renamedFields = ['NU_ANO_CENSO as Ano', 
                 'NU_IDADE_REFERENCIA as Idade_Referencia', 
                 'NU_IDADE as Idade',
                 'TP_SEXO as Sexo',
                 'TP_COR_RACA as Cor_Raca', 
                 'TP_NACIONALIDADE as Nacionalidade', 
                 'TP_ZONA_RESIDENCIAL as Zona_Residencial',
                 'TP_ESCOLARIDADE as Escolaridade',
                 'TP_SITUACAO_CURSO_1 as Situacao_Curso_1',
                 'CO_AREA_CURSO_1 as Area_Curso_1',
                 'CO_CURSO_1 as Curso_1',
                 'IN_LICENCIATURA_1 as Licenciatura_Curso_1',
                 'TP_SITUACAO_CURSO_2 as Situacao_Curso_2',
                 'CO_AREA_CURSO_2 as Area_Curso_2',
                 'CO_CURSO_2 as Curso_2',
                 'IN_LICENCIATURA_2 as Licenciatura_Curso_2',
                 'TP_SITUACAO_CURSO_3 as Situacao_Curso_3',
                 'CO_AREA_CURSO_3 as Area_Curso_3',
                 'CO_CURSO_3 as Curso_3',
                 'IN_LICENCIATURA_3 as Licenciatura_Curso_3',
                 'IN_ESPECIALIZACAO as Especializacao',
                 'IN_MESTRADO as Mestrado',
                 'IN_DOUTORADO as Doutorado',
                 'IN_POS_NENHUM as Pos_Graduacao',
                 'TP_TIPO_DOCENTE as Funcao']

# COMMAND ----------

# Alteração dos dataframes carregados com base na lista definida anteriormente

docentes_co_2017_silver = docentes_co_2017_silver.selectExpr(renamedFields)
docentes_norte_2017_silver = docentes_norte_2017_silver.selectExpr(renamedFields)
docentes_nordeste_2017_silver = docentes_nordeste_2017_silver.selectExpr(renamedFields)
docentes_sul_2017_silver = docentes_sul_2017_silver.selectExpr(renamedFields)
docentes_sudeste_2017_silver = docentes_sudeste_2017_silver.selectExpr(renamedFields)

docentes_co_2018_silver = docentes_co_2018_silver.selectExpr(renamedFields)
docentes_norte_2018_silver = docentes_norte_2018_silver.selectExpr(renamedFields)
docentes_nordeste_2018_silver = docentes_nordeste_2018_silver.selectExpr(renamedFields)
docentes_sul_2018_silver = docentes_sul_2018_silver.selectExpr(renamedFields)
docentes_sudeste_2018_silver = docentes_sudeste_2018_silver.selectExpr(renamedFields)

docentes_co_2019_silver = docentes_co_2019_silver.selectExpr(renamedFields)
docentes_norte_2019_silver = docentes_norte_2019_silver.selectExpr(renamedFields)
docentes_nordeste_2019_silver = docentes_nordeste_2019_silver.selectExpr(renamedFields)
docentes_sul_2019_silver = docentes_sul_2019_silver.selectExpr(renamedFields)
docentes_sudeste_2019_silver = docentes_sudeste_2019_silver.selectExpr(renamedFields)

# COMMAND ----------

# Unificação dos dataframes das regiões em um único dataframe para cada ano e armazenando na camada Prata

docentes_co_2017_silver.write.format('delta').mode('overwrite').save(blobPath + silverPath + 'docentes_2017')
docentes_norte_2017_silver.write.format('delta').mode('append').save(blobPath + silverPath + 'docentes_2017')
docentes_nordeste_2017_silver.write.format('delta').mode('append').save(blobPath + silverPath + 'docentes_2017')
docentes_sul_2017_silver.write.format('delta').mode('append').save(blobPath + silverPath + 'docentes_2017')
docentes_sudeste_2017_silver.write.format('delta').mode('append').save(blobPath + silverPath + 'docentes_2017')

docentes_co_2018_silver.write.format('delta').mode('overwrite').save(blobPath + silverPath + 'docentes_2018')
docentes_norte_2018_silver.write.format('delta').mode('append').save(blobPath + silverPath + 'docentes_2018')
docentes_nordeste_2018_silver.write.format('delta').mode('append').save(blobPath + silverPath + 'docentes_2018')
docentes_sul_2018_silver.write.format('delta').mode('append').save(blobPath + silverPath + 'docentes_2018')
docentes_sudeste_2018_silver.write.format('delta').mode('append').save(blobPath + silverPath + 'docentes_2018')

docentes_co_2019_silver.write.format('delta').mode('overwrite').save(blobPath + silverPath + 'docentes_2019')
docentes_norte_2019_silver.write.format('delta').mode('append').save(blobPath + silverPath + 'docentes_2019')
docentes_nordeste_2019_silver.write.format('delta').mode('append').save(blobPath + silverPath + 'docentes_2019')
docentes_sul_2019_silver.write.format('delta').mode('append').save(blobPath + silverPath + 'docentes_2019')
docentes_sudeste_2019_silver.write.format('delta').mode('append').save(blobPath + silverPath + 'docentes_2019')

# COMMAND ----------

# Leitura dos dataframes unificados

docentes_2017_silver = spark.read.format("delta").load(blobPath + silverPath + 'docentes_2017')
docentes_2018_silver = spark.read.format("delta").load(blobPath + silverPath + 'docentes_2018')
docentes_2019_silver = spark.read.format("delta").load(blobPath + silverPath + 'docentes_2019')

# COMMAND ----------

# Validação da unificação dos dataframes, comparando o total de registros

totalBronzeRows = docentes_co_2017_silver.count() + docentes_norte_2017_silver.count() + docentes_nordeste_2017_silver.count() + docentes_sul_2017_silver.count() + docentes_sudeste_2017_silver.count()

print("2017:", docentes_2017_silver.count() == totalBronzeRows)

totalBronzeRows = docentes_co_2018_silver.count() + docentes_norte_2018_silver.count() + docentes_nordeste_2018_silver.count() + docentes_sul_2018_silver.count() + docentes_sudeste_2018_silver.count()

print("2018:", docentes_2018_silver.count() == totalBronzeRows)

totalBronzeRows = docentes_co_2019_silver.count() + docentes_norte_2019_silver.count() + docentes_nordeste_2019_silver.count() + docentes_sul_2019_silver.count() + docentes_sudeste_2019_silver.count()

print("2019:", docentes_2019_silver.count() == totalBronzeRows)

# COMMAND ----------

# Verificando a quantidade de registros null por coluna de cada dataframe

display(docentes_2017_silver.select([count(when(col(c).isNull(), c)).alias(c) for c in docentes_2017_silver.columns]))
display(docentes_2018_silver.select([count(when(col(c).isNull(), c)).alias(c) for c in docentes_2018_silver.columns]))
display(docentes_2019_silver.select([count(when(col(c).isNull(), c)).alias(c) for c in docentes_2019_silver.columns]))

# COMMAND ----------

#Definição de dicionário para substituição dos valores nulos no formato -> 'Coluna':'Novo Valor'

nullSubstDict = {'Zona_Residencial': '3', 
                 'Situacao_Curso_1': '0', 'Area_Curso_1': '0', 'Curso_1': '000000', 'Licenciatura_Curso_1': '2', 
                 'Situacao_Curso_2': '0', 'Area_Curso_2': '0', 'Curso_2': '000000', 'Licenciatura_Curso_2': '2', 
                 'Situacao_Curso_3': '0', 'Area_Curso_3': '0', 'Curso_3': '000000', 'Licenciatura_Curso_3': '2', 
                 'Especializacao': '3', 'Mestrado': '3', 'Doutorado': '3', 'Pos_Graduacao': '3'}

# COMMAND ----------

# Substituição dos valores nulos de cada dataframe com base no dicionário definido

docentes_2017_silver = docentes_2017_silver.fillna(nullSubstDict)
docentes_2018_silver = docentes_2018_silver.fillna(nullSubstDict)
docentes_2019_silver = docentes_2019_silver.fillna(nullSubstDict)

# COMMAND ----------

# Verificando a quantidade de registros null por coluna de cada dataframe

display(docentes_2017_silver.select([count(when(col(c).isNull(), c)).alias(c) for c in docentes_2017_silver.columns]))
display(docentes_2018_silver.select([count(when(col(c).isNull(), c)).alias(c) for c in docentes_2018_silver.columns]))
display(docentes_2019_silver.select([count(when(col(c).isNull(), c)).alias(c) for c in docentes_2019_silver.columns]))

# COMMAND ----------

# Unificando os 3 dataframes em um único dataframe

docentes_2017_silver.write.format('delta').mode('overwrite').save(blobPath + silverPath + 'docentes')
docentes_2018_silver.write.format('delta').mode('append').save(blobPath + silverPath + 'docentes')
docentes_2019_silver.write.format('delta').mode('append').save(blobPath + silverPath + 'docentes')

# COMMAND ----------

# Leitura do dataframe final para execução das últimas transformações

docentes_silver = spark.read.format("delta").load(blobPath + silverPath + 'docentes')

# COMMAND ----------

#verificar o dataframe
display(docentes_silver)

# COMMAND ----------

# Devido ao tamanho dos dados da coluna 'Curso_*' é definido duas variáveis para os valores para melhor visualização

codCursos = ['142A01', '142C01', '142P01', '144F12', '144F13', '145F01', '145F02', '145F05', '145F08', '145F09', '145F10', '145F11', '145F14', '145F15', '145F17', '145F18', '145F21', '145F24', '145F28', '146F02', '146F04', '146F05', '146F07', '146F09', '146F15', '146F20', '146F22', '146P01', '210A01', '211A02', '212C02', '212D01', '212M02', '212T01', '213A05', '213C06', '213C07', '213F01', '213P02', '213P03', '213P05', '213P07', '214D02', '214D05', '214D06', '214M01', '214P01', '215C02', '215F01', '220H01', '220L03', '221T01', '222L01', '223C01', '223L01', '223L02', '225A01', '225H01', '225M01', '225M02', '226F01', '310C02', '311P02', '312A01', '313C01', '313R01', '314E02', '321C01', '321C02', '321J01', '321R01', '322A01', '322B01', '340N02', '341N01', '342C01', '342M02', '342P02', '342R01', '343S01', '344C02', '345A01', '345A02', '345A07', '345A10', '345C01', '345G09', '345G10', '345G13', '345G16', '345G17', '345G26', '346S01', '346S03', '380D01', '421B07', '421B12', '421C01', '422S01', '440C01', '441F01', '441R01', '442Q01', '443C01', '443G03', '443G05', '443G06', '443M01', '443O01', '461M01', '462C01', '462E01', '481A01', '481B01', '481C01', '481T01', '481T02', '482U01', '483S01', '483S02', '520A01', '520E01', '520E04', '520E05', '520E09', '520G01', '520M01', '520P02', '520T01', '521E05', '521E06', '521M03', '521T02', '521T03', '522D02', '522E06', '522E08', '522R01', '522T02', '523B01', '523E04', '523E09', '523E10', '523E11', '523E12', '523M01', '523S03', '523T01', '523T04', '523T05', '523T06', '524E01', '524E06', '524E07', '524T03', '524T04', '525A01', '525C04', '525E04', '525E05', '525E08', '525M01', '525S01', '540F02', '540F03', '541E01', '541I02', '541P05', '541P09', '541T01', '541T02', '541T03', '542B01', '542E03', '542I01', '542I02', '543C01', '543F03', '543F05', '543P06', '544E01', '544E05', '544E07', '544M02', '544R01', '544T01', '581A05', '582A01', '582A02', '582C05', '582E02', '582E03', '582M02', '582O01', '582T04', '621A03', '621A04', '621A06', '621E03', '621M02', '621T01', '621T03', '621T04', '621T05', '621Z01', '622H01', '623E01', '623S01', '624A01', '624E01', '624T01', '641M01', '720E01', '720N01', '720S01', '721M01', '721O02', '723E01', '724O01', '725T06', '726F01', '726F03', '726N02', '726O01', '726Q01', '726T01', '727F01', '762S01', '811G01', '811H02', '811H03', '812E01', '812P01', '812T01', '813F02', '813G02', '814E02', '815E01', '840A01', '840C04', '840C05', '840N02', '840S01', '840S02', '840T02', '850G01', '861S02', '861S03', '862S01', '863C01', '863C02', '863F01', '999990', '999991', '999992', '000000']

nomeCursos = ['Processos Escolares - Tecnológico', 'Pedagogia (Ciências da Educação) - Bacharelado', 'Pedagogia - Licenciatura', 'Licenciatura Interdisciplinar em Ciências Humanas - Licenciatura', 'Licenciatura Intercultural Indígena - Licenciatura', 'Ciências Biológicas - Licenciatura', 'Ciências Naturais - Licenciatura', 'Educação Religiosa - Licenciatura', 'Filosofia - Licenciatura', 'Física - Licenciatura', 'Geografia - Licenciatura', 'História - Licenciatura', 'Letras - Língua Estrangeira - Licenciatura', 'Letras - Língua Portuguesa - Licenciatura', 'Letras - Língua Portuguesa e Estrangeira - Licenciatura', 'Matemática - Licenciatura', 'Química - Licenciatura', 'Ciências Sociais - Licenciatura', 'Libras - Licenciatura', 'Licenciatura Interdisciplinar em Artes (Educação Artística) - Licenciatura', 'Artes Visuais - Licenciatura', 'Informática - Licenciatura', 'Dança - Licenciatura', 'Licenciatura Interdisciplinar em Educação no Campo - Licenciatura', 'Educação Física - Licenciatura', 'Música - Licenciatura', 'Teatro - Licenciatura', 'Licenciatura para a Educação Profissional e Tecnológica - Licenciatura', 'Bacharelado Interdisciplinar em Artes - Bacharelado', 'Artes Visuais - Bacharelado', 'Produção cênica - Tecnológico', 'Dança - Bacharelado', 'Música - Bacharelado', 'Teatro - Bacharelado', 'Produção audiovisual - Tecnológico', 'Design gráfico - Tecnológico', 'Carnaval - Tecnológico', 'Fotografia - Tecnológico', 'Produção multimídia - Tecnológico', 'Produção fonográfica - Tecnológico', 'Produção publicitária - Tecnológico', 'Produção Cultural - Tecnológico', 'Design de moda - Tecnológico', 'Design - Bacharelado', 'Design de Interiores  - Tecnológico', 'Moda - Bacharelado', 'Design de produto - Tecnológico', 'Conservação e restauro - Tecnológico', 'Fabricação de Instrumentos Musicais - Tecnológico', 'Bacharelado Interdisciplinar Ciências Humanas - Bacharelado', 'Letras - Língua Portuguesa e Estrangeira - Bacharelado', 'Teologia - Bacharelado', 'Letras - Língua Estrangeira - Bacharelado', 'Comunicação assistiva - Tecnológico', 'Letras - Língua Portuguesa - Bacharelado', 'Libras - Bacharelado', 'Arqueologia - Bacharelado', 'História - Bacharelado', 'Museologia - Bacharelado', 'Museografia - Tecnológico', 'Filosofia - Bacharelado', 'Ciências Sociais - Bacharelado', 'Psicologia - Bacharelado', 'Antropologia - Bacharelado', 'Ciência política - Bacharelado', 'Relações Internacionais - Bacharelado', 'Ciências Econômicas - Bacharelado', 'Cinema e Audiovisual - Bacharelado', 'Comunicação Social (Área Geral) - Bacharelado', 'Jornalismo - Bacharelado', 'Radio, TV, Internet - Bacharelado', 'Arquivologia - Bacharelado', 'Biblioteconomia - Bacharelado', 'Comércio exterior - Tecnológico', 'Negócios imobiliários - Tecnológico', 'Comunicação institucional - Tecnológico', 'Marketing - Tecnológico', 'Publicidade e Propaganda - Bacharelado', 'Relações Públicas - Bacharelado', 'Gestão de Seguros - Teconológico', 'Ciências Contábeis - Bacharelado', 'Administração - Bacharelado', 'Gestão de cooperativas  - Tecnológico', 'Gestão hospitalar  - Tecnológico', 'Gestão pública  - Tecnológico', 'Processos gerenciais  - Tecnológico', 'Gestão de recursos humanos  - Tecnológico', 'Gestão da qualidade  - Tecnológico', 'Logística  - Tecnológico', 'Gestão comercial  - Tecnológico', 'Gestão financeira  - Tecnológico', 'Gestão de segurança privada - Tecnológico', 'Secretariado  - Tecnológico', 'Secretariado Executivo - Bacharelado', 'Direito - Bacharelado', 'Biomedicina - Bacharelado', 'Biotecnologia - Tecnológico', 'Ciências Biológicas - Bacharelado', 'Saneamento ambiental - Tecnológico', 'Bacharelado Interdisciplinar em Ciência e Tecnologia  - Bacharelado', 'Física - Bacharelado', 'Física Medica e Radioterapia - Bacharelado', 'Química - Bacharelado', 'Ciência da Terra - Licenciatura', 'Geofísica - Bacharelado', 'Geografia - Bacharelado', 'Geologia - Bacharelado', 'Meteorologia - Bacharelado', 'Oceanografia - Bacharelado', 'Matemática - Bacharelado', 'Ciências Atuariais - Bacharelado', 'Estatística - Bacharelado', 'Redes de computadores - Tecnológico', 'Banco de dados - Tecnológico', 'Ciência da Computação - Bacharelado', 'Gestão da tecnologia da informação - Tecnológico', 'Jogos Digitais - Tecnológico', 'Sistemas para internet - Tecnológico', 'Análise e desenvolvimento de sistemas / Segurança da informação - Tecnológico', 'Sistemas de Informação - Bacharelado', 'Automação industrial - Tecnológico', 'Engenharia - Bacharelado', 'Engenharia de Materiais - Bacharelado', 'Engenharia de Produção - Bacharelado', 'Engenharia Ambiental e Sanitária - Bacharelado', 'Geoprocessamento - Tecnológico', 'Manutenção industrial - Tecnológico', 'Gestão da produção industrial - Tecnológico', 'Gestão de telecomunicações - Tecnológico', 'Engenharia Mecânica - Bacharelado', 'Engenharia Metalúrgica - Bacharelado', 'Mecânica de precisão - Tecnológico', 'Processos metalúrgicos - Tecnológico', 'Fabricação mecânica - Tecnológico', 'Sistemas elétricos - Tecnológico', 'Engenharia Elétrica - Bacharelado', 'Sistemas de energia - Tecnológico', 'Refrigeração/Aquecimento - Tecnológico', 'Eletrotécnica industrial - Tecnológico', 'Engenharia Biomédica - Bacharelado', 'Engenharia de Computação - Bacharelado', 'Engenharia Eletrônica - Bacharelado', 'Engenharia mecatrônica - Bacharelado', 'Engenharia de Controle e Automação - Bacharelado', 'Engenharia de Telecomunicações - Bacharelado', 'Sistemas biomédicos - Tecnológico', 'Sistemas eletrônicos - Tecnológico', 'Redes de telecomunicações / Sistemas de telecomunicações - Tecnológico', 'Mecatrônica industrial - Tecnológico', 'Telemática - Tecnológico', 'Eletrônica industrial - Tecnológico', 'Engenharia de Bioprocessos - Bacharelado', 'Engenharia nuclear - Bacharelado', 'Engenharia Química - Bacharelado', 'Processos químicos - Tecnológico', 'Biocombustíveis - Tecnológico', 'Mecanização Agrícola - Tecnológico', 'Construção naval - Tecnológico', 'Engenharia Aeronáutica - Bacharelado', 'Engenharia automotiva - Bacharelado', 'Engenharia Naval - Bacharelado', 'Manutenção de aeronaves - Tecnológico', 'Sistemas Automotivos - Tecnológico', 'Produção Joalheira/Design de jóias e gemas - Tecnológico', 'Produção Gráfica  - Tecnológico', 'Engenharia de Alimentos - Bacharelado', 'Laticínios  - Tecnológico', 'Processamento de Carnes  - Tecnológico', 'Viticultura e Enologia  - Tecnológico', 'Alimentos  - Tecnológico', 'Produção Sucroalcooleira  - Tecnológico', 'Produção de Cachaça  - Tecnológico', 'Bioenergia - Tecnológico', 'Engenharia Têxtil - Bacharelado', 'Produção de vestuário  - Tecnológico', 'Produção têxtil  - Tecnológico', 'Cerâmica - Tecnológico', 'Produção moveleira  - Tecnológico', 'Papel e celulose  - Tecnológico', 'Polímeros - Tecnológico', 'Engenharia de Minas - Bacharelado', 'Petróleo e gás  - Tecnológico', 'Engenharia de Petróleo - Bacharelado', 'Mineração e extração - Tecnológico', 'Rochas ornamentais  - Tecnológico', 'Tecnologia de Mineração - Tecnológico', 'Arquitetura e Urbanismo - Bacharelado', 'Obras hidráulicas - Tecnológico', 'Agrimensura - Tecnológico', 'Construção de Edifícios - Tecnológico', 'Engenharia Cartográfica e de Agrimensura - Bacharelado', 'Engenharia Civil - Bacharelado', 'Material de construção - Tecnológico', 'Controle de obras - Tecnológico', 'Estradas - Tecnológico', 'Agroindústria - Tecnológico', 'Agronomia - Bacharelado', 'Agroecologia - Tecnológico', 'Engenharia Agrícola - Bacharelado', 'Produção Agrícola - Tecnológico', 'Irrigação e Drenagem - Tecnológico', 'Agronegócio - Tecnológico', 'Cafeicultura - Tecnológico', 'Produção de Grãos - Tecnológico', 'Zootecnia - Bacharelado', 'Horticultura - Tecnológico', 'Engenharia Florestal - Bacharelado', 'Silvicultura - Tecnológico', 'Aqüicultura - Tecnológico', 'Engenharia de Pesca - Bacharelado', 'Produção Pesqueira - Tecnológico', 'Medicina Veterinária - Bacharelado', 'Educação Física - Bacharelado', 'Naturologia - Bacharelado', 'Bacharelado Interdisciplinar Ciências da Saúde - Bacharelado', 'Medicina - Bacharelado', 'Oftálmica - Tecnológico', 'Enfermagem - Bacharelado', 'Odontologia - Bacharelado', 'Radiologia - Tecnológico', 'Fisioterapia - Bacharelado', 'Fonoaudiologia - Bacharelado', 'Nutrição - Bacharelado', 'Óptica e Optometria - Tecnológico', 'Quiropraxia - Bacharelado', 'Terapia Ocupacional - Bacharelado', 'Farmácia - Bacharelado', 'Serviço Social - Bacharelado', 'Gastronomia - Tecnológico', 'Hotelaria - Tecnológico', 'Hotelaria Hospitalar - Tecnológico', 'Eventos - Tecnológico', 'Gestão de Turismo - Tecnológico', 'Turismo - Bacharelado', 'Futebol - Tecnológico', 'Gestão desportiva e de lazer - Tecnológico', 'Economia doméstica - Bacharelado', 'Estética e Cosmética - Tecnológico', 'Pilotagem profissional de aeronaves - Tecnológico', 'Ciências Aeronáuticas - Bacharelado', 'Ciências Navais - Bacharelado', 'Sistemas de navegação fluvial - Tecnológico', 'Gestão portuária - Tecnológico', 'Transporte aéreo - Tecnológico', 'Transporte terrestre - Tecnológico', 'Processos ambientais / Gestão ambiental - Tecnológico', 'Segurança no trânsito / Segurança pública - Tecnológico', 'Serviços penais - Tecnológico', 'Segurança no trabalho - Tecnológico', 'Ciências Militares - Bacharelado', 'Ciências da Logística - Bacharelado', 'Formação Militar - Bacharelado', 'Outro curso de formação superior - Licenciatura', 'Outro curso de formação superior - Bacharelado', 'Outro curso de formação superior - Tecnológico', 'Não Informado']

# COMMAND ----------

# Alteração dos valores de cada coluna de acordo com as informações correspondentes

docentes_silver = docentes_silver.replace(['1', '2'], 
                                            ['Masculino', 'Feminino'], 'Sexo')

docentes_silver = docentes_silver.replace(['0', '1', '2', '3', '4', '5'], 
                                            ['Não Declarada', 'Branca', 'Preta', 'Parda', 'Amarela', 'Indígena'], 'Cor_Raca')

docentes_silver = docentes_silver.replace(['1', '2', '3'], 
                                            ['Brasileira', 'Brasileira - nascido no exterior ou naturalizado', 'Estrangeira'], 'Nacionalidade')

docentes_silver = docentes_silver.replace(['0', '1', '2', '3'], 
                                            ['Não Aplicável', 'Urbana', 'Rural', 'Não consta'], 'Zona_Residencial')

docentes_silver = docentes_silver.replace(['1', '2', '3', '4'], 
                                          ['Ensino Fundamental Incompleto', 'Ensino Fundamental Completo', 'Ensino Médio Completo', 'Ensino Superior Completo'],
                                          'Escolaridade')

docentes_silver = docentes_silver.replace(['0', '1', '2'], 
                                          ['Não consta', 'Concluído', 'Em andamento'], 'Situacao_Curso_1')

docentes_silver = docentes_silver.replace(['0', '1', '2', '3', '4', '5', '6', '7', '8', '9'], 
                                          ['Não consta','Educação', 'Humanidades e Artes', 'Ciências sociais, negócios e direitos', 'Ciências, Matemática e Computação', 
                                           'Engenharia, Produção e Construção', 'Agricultura e Veterinária', 'Saúde e Bem-estar Social', 'Serviços', 'Outros'], 
                                          'Area_Curso_1')

docentes_silver = docentes_silver.replace(codCursos, nomeCursos, 'Curso_1')

docentes_silver = docentes_silver.replace(['0', '1', '2'], ['Não', 'Sim', 'Não consta'], 'Licenciatura_Curso_1')

docentes_silver = docentes_silver.replace(['0', '1', '2'], 
                                          ['Não consta', 'Concluído', 'Em andamento'], 'Situacao_Curso_2')

docentes_silver = docentes_silver.replace(['0', '1', '2', '3', '4', '5', '6', '7', '8', '9'], 
                                          ['Não consta','Educação', 'Humanidades e Artes', 'Ciências sociais, negócios e direitos', 'Ciências, Matemática e Computação', 
                                           'Engenharia, Produção e Construção', 'Agricultura e Veterinária', 'Saúde e Bem-estar Social', 'Serviços', 'Outros'], 
                                          'Area_Curso_2')

docentes_silver = docentes_silver.replace(codCursos, nomeCursos, 'Curso_2')

docentes_silver = docentes_silver.replace(['0', '1', '2'], ['Não', 'Sim', 'Não consta'], 'Licenciatura_Curso_2')

docentes_silver = docentes_silver.replace(['0', '1', '2'], 
                                          ['Não consta', 'Concluído', 'Em andamento'], 'Situacao_Curso_3')

docentes_silver = docentes_silver.replace(['0','1', '2', '3', '4', '5', '6', '7', '8', '9'], 
                                          ['Não consta','Educação', 'Humanidades e Artes', 'Ciências sociais, negócios e direitos', 'Ciências, Matemática e Computação', 
                                           'Engenharia, Produção e Construção', 'Agricultura e Veterinária', 'Saúde e Bem-estar Social', 'Serviços', 'Outros'], 
                                          'Area_Curso_3')

docentes_silver = docentes_silver.replace(codCursos, nomeCursos, 'Curso_3')

docentes_silver = docentes_silver.replace(['0', '1', '2'], ['Não', 'Sim', 'Não consta'], 'Licenciatura_Curso_3')

docentes_silver = docentes_silver.replace(['0', '1', '2', '3'], 
                                          ['Não', 'Sim', 'Não Aplicável (Curso Superior Não Concluído)', 'Não consta'], 'Especializacao')

docentes_silver = docentes_silver.replace(['0', '1', '2', '3'], 
                                          ['Não', 'Sim', 'Não Aplicável (Curso Superior Não Concluído)', 'Não consta'], 'Mestrado')

docentes_silver = docentes_silver.replace(['0', '1', '2', '3'], 
                                          ['Não', 'Sim', 'Não Aplicável (Curso Superior Não Concluído)', 'Não consta'], 'Doutorado')

docentes_silver = docentes_silver.replace(['0', '1', '2', '3'], 
                                          ['Não', 'Sim', 'Não Aplicável (Curso Superior Não Concluído)', 'Não consta'], 'Pos_Graduacao')

docentes_silver = docentes_silver.replace(['1', '2', '3', '4', '5', '6', '7', '8'], 
                                          ['Docente', 'Auxiliar/Assistente Educacional', 'Profissional/Monitor de atividade complementar', 
                                           'Tradutor Intérprete de Libras', 'Docente Titular - Coordenador de Tutoria EAD', 'Docente Tutor - Auxiliar EAD', 
                                           'Guia intérprete', 'Profissional de Apoio Escolar para Alunos com Deficiência'], 'Funcao')

# COMMAND ----------

#Transformar os tipos de dados
docentes_silver = docentes_silver.withColumn('Idade_Referencia', docentes_silver['Idade_Referencia'].cast(IntegerType())).withColumn('Idade', docentes_silver['Idade'].cast(IntegerType())).withColumn('Ano', docentes_silver['Ano'].cast(DateType()))
docentes_silver.printSchema()

# COMMAND ----------

# Visualização do dataframe após alteração

display(docentes_silver)

# COMMAND ----------

#salvar o dataframe na camada ouro e em formato de tabela para consumo do PowerBI
docentes_silver.write.format('delta').mode('overwrite').save(blobPath + goldPath + 'docentes_')
docentes_silver.write.format('delta').mode('overwrite').saveAsTable('Censo_Escolar_Docentes_')

# COMMAND ----------

