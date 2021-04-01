# Databricks notebook source
# MAGIC %md
# MAGIC # Censo Escolar da Educação Superior - Docentes

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
from pyspark.sql.functions import *
from pyspark.sql.types import *
from pyspark.sql.functions import udf
from pyspark.sql.types import IntegerType

# COMMAND ----------

# MAGIC %md
# MAGIC ###Extração dos arquivos da url

# COMMAND ----------

# MAGIC %sh
# MAGIC wget https://download.inep.gov.br/microdados/microdados_educacao_superior_2017.zip -O /tmp/Superior_2017.zip

# COMMAND ----------

# MAGIC %sh
# MAGIC wget https://download.inep.gov.br/microdados/microdados_educacao_superior_2018.zip -O /tmp/Superior_2018.zip

# COMMAND ----------

# MAGIC %sh
# MAGIC wget https://download.inep.gov.br/microdados/microdados_educacao_superior_2019.zip -O /tmp/Superior_2019.zip

# COMMAND ----------

# MAGIC %md
# MAGIC ###Fazer backup dos arquivos .zip para o DBFS

# COMMAND ----------

#salvar do diretório temporário no dbfs para não perder o arquivo
dbutils.fs.mv('file:/tmp/Superior_2017.zip', 'dbfs:/tmp/Superior_2017.zip')
dbutils.fs.mv('file:/tmp/Superior_2018.zip', 'dbfs:/tmp/Superior_2018.zip')
dbutils.fs.mv('file:/tmp/Superior_2019.zip', 'dbfs:/tmp/Superior_2019.zip')

# COMMAND ----------

#salvando na máquina para extração dos dados
dbutils.fs.cp('dbfs:/tmp/Superior_2017.zip', 'file:/tmp/Censo_Superior_2017.zip')
dbutils.fs.cp('dbfs:/tmp/Superior_2018.zip', 'file:/tmp/Censo_Superior_2018.zip')
dbutils.fs.cp('dbfs:/tmp/Superior_2019.zip', 'file:/tmp/Censo_Superior_2019.zip')

# COMMAND ----------

# MAGIC %sh
# MAGIC unzip -j /tmp/Censo_Superior_2017.zip -d /tmp/Superior2017

# COMMAND ----------

# MAGIC %sh
# MAGIC unzip -j /tmp/Censo_Superior_2018.zip -d /tmp/Superior2018

# COMMAND ----------

# MAGIC %sh
# MAGIC unzip -j /tmp/Censo_Superior_2019.zip -d /tmp/Superior2019

# COMMAND ----------

# MAGIC %md
# MAGIC As pastas dentro do .zip inicial no arquivo 2017 também estão zipadas, extrair novamente

# COMMAND ----------

# MAGIC %sh
# MAGIC unzip -j /tmp/Superior2017/DM_DOCENTE.zip -d /tmp/SuperiorDocentes_2017

# COMMAND ----------

# MAGIC %md
# MAGIC Remove todos os arquivos do diretório que não possuem a extensão **.zip**
# MAGIC 
# MAGIC - **rm**: remove arquivos
# MAGIC - **-v**: visualiza cada arquivo que está sendo deletado
# MAGIC - **/tmp/censoSuperior_2019/**: diretório
# MAGIC - **!(*.CSV)**: arquivos que não possuem a extensão .CSV
# MAGIC - o último arquivo de 2017 já está apenas o CSV, não precisa fazer essa etapa

# COMMAND ----------

# MAGIC %sh
# MAGIC shopt -s extglob
# MAGIC rm -v /tmp/Superior2018/!(*.CSV) 
# MAGIC shopt -u extglob
# MAGIC 
# MAGIC echo -ne '\n'
# MAGIC echo -ne '\n'
# MAGIC ls /tmp/Superior2018

# COMMAND ----------

# MAGIC %sh
# MAGIC shopt -s extglob
# MAGIC rm -v /tmp/Superior2019/!(*.CSV) 
# MAGIC shopt -u extglob
# MAGIC 
# MAGIC echo -ne '\n'
# MAGIC echo -ne '\n'
# MAGIC ls /tmp/Superior2019

# COMMAND ----------

# MAGIC %md
# MAGIC ###Ler os arquivos csv

# COMMAND ----------

#abrir o arquivo csv e salvar em um DataFrame
#2017
csv = 'file:/tmp/SuperiorDocentes_2017/DM_DOCENTE.CSV'
df_2017 = (spark.read
      .option('sep', '|')
      .option('header', 'true')
      .csv(csv)
)
df_2017.printSchema()

# COMMAND ----------

#2018
csv = 'file:/tmp/Superior2018/DM_DOCENTE.CSV'
df_2018 = (spark.read
      .option('sep', '|')
      .option('header', 'true')
      .csv(csv)
)
df_2018.printSchema()

# COMMAND ----------

#2019
csv = 'file:/tmp/Superior2019/SUP_DOCENTE_2019.CSV'
df_2019 = (spark.read
      .option('sep', '|')
      .option('header', 'true')
      .csv(csv)
)
df_2019.printSchema()

# COMMAND ----------

#salvar no delta lake (camada bronze) dados sem modificação
df_2017.write.format('delta').mode('overwrite').save(Blob_Path + '/mnt/bronze/censoSuperiorDocentes_2017')

# COMMAND ----------

#salvar no delta lake (camada bronze) dados sem modificação
#2018
df_2018.write.format('delta').mode('append').save(Blob_Path + '/mnt/bronze/censoSuperiorDocentes_2018_19')

# COMMAND ----------

#salvar no delta lake (camada bronze) dados sem modificação
#2019
df_2019.write.format('delta').mode('append').save(Blob_Path + '/mnt/bronze/censoSuperiorDocentes_2018_19')

# COMMAND ----------

# MAGIC %md
# MAGIC ###Ler o dataframe de 2017 e fazer as transformações para unificação

# COMMAND ----------

#ler o dataframe de 2017 para fazer as transformações
df_2017 = spark.read.format('delta').load(Blob_Path + '/mnt/bronze/censoSuperiorDocentes_2017')
display(df_2017)

# COMMAND ----------

#verificar se existem valores nulos nas colunas
display(df_2017.select([count(when(col(c).isNull(), c)).alias(c) for c in df_2017.columns]))

# COMMAND ----------

#Transformar valores nulos em 'x'
df_2017 = df_2017.na.fill('x')
display(df_2017)

# COMMAND ----------

#mudar as colunas para bater com o esquema das outras tabelas (duas colunas tem esquema diferente)
df_2017 = df_2017.withColumn('ID_DOCENTE',df_2017['CO_DOCENTE']).withColumn('IN_DEFICIENCIA',df_2017['TP_DEFICIENCIA'])
df_2017.printSchema()
display(df_2017)

# COMMAND ----------

#retirar as colunas com esquema diferente
df_2017 = df_2017.select('NU_ANO_CENSO','CO_IES','TP_CATEGORIA_ADMINISTRATIVA','TP_ORGANIZACAO_ACADEMICA','CO_DOCENTE_IES','ID_DOCENTE','TP_SITUACAO','TP_ESCOLARIDADE','TP_REGIME_TRABALHO','TP_SEXO','NU_ANO_NASCIMENTO','NU_MES_NASCIMENTO','NU_DIA_NASCIMENTO','NU_IDADE','TP_COR_RACA','CO_PAIS_ORIGEM','TP_NACIONALIDADE','CO_UF_NASCIMENTO','CO_MUNICIPIO_NASCIMENTO','IN_DEFICIENCIA','IN_DEFICIENCIA_CEGUEIRA','IN_DEFICIENCIA_BAIXA_VISAO','IN_DEFICIENCIA_SURDEZ','IN_DEFICIENCIA_AUDITIVA','IN_DEFICIENCIA_FISICA','IN_DEFICIENCIA_SURDOCEGUEIRA','IN_DEFICIENCIA_MULTIPLA','IN_DEFICIENCIA_INTELECTUAL','IN_ATUACAO_EAD','IN_ATUACAO_EXTENSAO','IN_ATUACAO_GESTAO','IN_ATUACAO_GRAD_PRESENCIAL','IN_ATUACAO_POS_EAD','IN_ATUACAO_POS_PRESENCIAL','IN_ATUACAO_SEQUENCIAL','IN_ATUACAO_PESQUISA','IN_BOLSA_PESQUISA','IN_SUBSTITUTO','IN_EXERCICIO_DATA_REFERENCIA','IN_VISITANTE','TP_VISITANTE_IFES_VINCULO')

# COMMAND ----------

#trocar os números pelas informações referentes na coluna 'TP_CATEGORIA_ADMINISTRATIVA'
df_2017 = df_2017.replace(['x','1','2','3','4','5','6','7'],['Não consta','Pública Federal','Pública Estadual','Pública Municipal','Privada com fins lucrativos','Privada sem fins lucrativos','Privada confessional','Especial'], 'TP_CATEGORIA_ADMINISTRATIVA')
display(df_2017)

# COMMAND ----------

#salvar no delta lake (camada bronze) 
df_2017.write.format('delta').mode('append').save(Blob_Path + '/mnt/bronze/censoSuperiorDocentes')

# COMMAND ----------

# MAGIC %md
# MAGIC ###Ler o dataframe de 2018 e 2019 e fazer as transformações para unificação

# COMMAND ----------

#ler os dataframes de 2018 e 2019 para fazer as transformações
df_2018_19 = spark.read.format('delta').load(Blob_Path + '/mnt/bronze/censoSuperiorDocentes_2018_19')
display(df_2018_19)
df_2018_19.printSchema()

# COMMAND ----------

#verificar se existem valores nulos nas colunas
display(df_2018_19.select([count(when(col(c).isNull(), c)).alias(c) for c in df_2018_19.columns]))

# COMMAND ----------

#Transformar valores nulos em 'x'
df_2018_19 = df_2018_19.na.fill('x')
display(df_2018_19)

# COMMAND ----------

#trocar os números pelas informações referentes
df_2018_19 = df_2018_19.replace(['x','1','2','3','4','5','6','7','8','9'],['Não consta','Pública Federal','Pública Estadual','Pública Municipal','Privada com fins lucrativos','Privada sem fins lucrativos','Privada - Particular em sentido estrito','Especial','Privada comunitária','Privada confessional'], 'TP_CATEGORIA_ADMINISTRATIVA')
display(df_2018_19)

# COMMAND ----------

#salvar no delta lake (camada bronze) 
df_2018_19.write.format('delta').mode('append').save(Blob_Path + '/mnt/bronze/censoSuperiorDocentes') 

# COMMAND ----------

# MAGIC %md
# MAGIC ###Ler as informações dos DataFrames (2017/2018_19) como Dataframe único de Educação Superior

# COMMAND ----------

#ler a junção de todos os arquivos como um DataFrame único
df_superior = spark.read.format('delta').load(Blob_Path + '/mnt/bronze/censoSuperiorDocentes')  
display(df_superior)

# COMMAND ----------

#verificar se deu certo o append entre todas as tabelas verificando a quantidade de linhas
df_superior.count()

# COMMAND ----------

# MAGIC %md
# MAGIC ###Fazer as transformações no dataframe único

# COMMAND ----------

# MAGIC %md
# MAGIC Alterar as informações de cada coluna - trocando os números por sua informações equivalentes

# COMMAND ----------

#Transformar a coluna de idade em número para construir nova coluna com grupo de idade
df_superior = df_superior.withColumn('NU_IDADE', df_superior['NU_IDADE'].cast(IntegerType()))

# COMMAND ----------

Grupo_de_Idade = udf(lambda NU_IDADE: 'menos de 15' if NU_IDADE < 15 else  
                       '15 a 19' if (NU_IDADE  >= 15 and NU_IDADE  < 20) else
                       '20 a 29' if (NU_IDADE  >= 20 and NU_IDADE  < 30) else
                       '30 a 49' if (NU_IDADE  >= 30 and NU_IDADE  < 40) else
                       '40 a 59' if (NU_IDADE  >= 40 and NU_IDADE  < 50) else
                       '50 a 69' if (NU_IDADE  >= 50 and NU_IDADE  < 60) else
                       '60+'  if (NU_IDADE  >= 60) else '')
df_superior = df_superior.withColumn('Grupo_de_Idade', Grupo_de_Idade(df_superior.NU_IDADE))
display(df_superior)
df_superior.printSchema()

# COMMAND ----------

#mudar valores nas linhas da coluna 'TP_ORGANIZACAO_ACADEMICA'
df_superior = df_superior.replace(['x','1','2','3','4','5'],['Não consta','Universidade','Centro Universitário','Faculdade','Instituto Federal de Educação, Ciência e Tecnologia','Centro Federal de Educação Tecnológica'], 'TP_ORGANIZACAO_ACADEMICA')
display(df_superior)

# COMMAND ----------

#mudar valores nas linhas da coluna 'TP_SITUACAO' de acordo com as informações correspondentes
df_superior = df_superior.replace(['x','1','2','3','4','5','6'],['Não consta','Em exercício','Afastado para qualificação', 'Afastado para exercício em outros órgãos/entidades', 'Afastado por outros motivos', 'Afastado para tratamento de saúde', 'Falecido'], 'TP_SITUACAO')
display(df_superior)

# COMMAND ----------

#mudar valores nas linhas da coluna 'TP_ESCOLARIDADE' de acordo com as informações correspondentes
df_superior = df_superior.replace(['x','1','2','3','4','5'],['Não consta','Sem Graduação','Graduação','Especialização','Mestrado','Doutorado'], 'TP_ESCOLARIDADE')
display(df_superior)

# COMMAND ----------

#mudar valores nas linhas da coluna 'TP_REGIME_TRABALHO' de acordo com as informações correspondentes
df_superior = df_superior.replace(['x','0','1','2','3','4'],['Não consta','Não informado','Tempo Integral com dedicação exclusiva','Tempo Integral sem dedicação exclusiva','Tempo Parcial', 'Horista'], 'TP_REGIME_TRABALHO')
display(df_superior)

# COMMAND ----------

#mudar valores nas linhas da coluna 'TP_SEXO' de acordo com as informações correspondentes
df_superior = df_superior.replace(['x','1','2'],['Não consta','Feminino','Masculino'], 'TP_SEXO')
display(df_superior)

# COMMAND ----------

#mudar valores nas linhas da coluna 'TP_COR_RACA' de acordo com as informações correspondentes
df_superior = df_superior.replace(['x','0','1','2','3','4','5','9'],['Não consta','Não quis declarar','Branca', 'Preta', 'Parda', 'Amarela', 'Indígena', 'Não resposta'], 'TP_COR_RACA')
display(df_superior)

# COMMAND ----------

#mudar valores nas linhas da coluna 'TP_NACIONALIDADE' de acordo com as informações correspondentes
df_superior = df_superior.replace(['x','1','2','3'],['Não consta','Brasileira','Brasileira - nascido no exterior ou naturalizado', 'Estrangeira'], 'TP_NACIONALIDADE')
display(df_superior)

# COMMAND ----------

#mudar valores nas linhas da coluna 'IN_DEFICIENCIA' de acordo com as informações correspondentes
df_superior = df_superior.replace(['x','0','1','9'],['Não consta','Não','Sim', 'Não resposta'], 'IN_DEFICIENCIA')
display(df_superior)

# COMMAND ----------

# MAGIC %md
# MAGIC Mudar os tipos dos dados e os nomes das colunas

# COMMAND ----------

#mudar o tipo da coluna ano para data com uma coluna nova
df_superior = df_superior.withColumn('Ano', df_superior['NU_ANO_CENSO'].cast(DateType()))
display(df_superior)
df_superior.printSchema()

# COMMAND ----------

#mudar nomes das colunas
df_superior = df_superior.withColumn('Categoria_Administrativa',df_superior['TP_CATEGORIA_ADMINISTRATIVA']).withColumn('Organizacao_Academica',df_superior['TP_ORGANIZACAO_ACADEMICA']).withColumn('Situacao',df_superior['TP_SITUACAO']).withColumn('Escolaridade',df_superior['TP_ESCOLARIDADE']).withColumn('Regime_Trabalho',df_superior['TP_REGIME_TRABALHO']).withColumn('Cor_Raca',df_superior['TP_COR_RACA']).withColumn('Sexo',df_superior['TP_SEXO']).withColumn('Idade',df_superior['NU_IDADE']).withColumn('Nacionalidade',df_superior['TP_NACIONALIDADE']).withColumn('Tem_Deficiencia',df_superior['IN_DEFICIENCIA'])
df_superior.printSchema()
display(df_superior)

# COMMAND ----------

#Manter as colunas que iremos usar
df_superior = df_superior.select('Ano','Categoria_Administrativa','Organizacao_Academica','Situacao','Escolaridade','Regime_Trabalho','Cor_Raca','Sexo','Idade','Grupo_de_Idade','Nacionalidade','Tem_Deficiencia')
df_superior.printSchema()
display(df_superior)

# COMMAND ----------

#ordenar dados e verificar
df_superior = df_superior.sort('Ano')
display(df_superior)

# COMMAND ----------

# MAGIC %md
# MAGIC ###Salvar na camada prata e verificar o arquivo

# COMMAND ----------

#Salvar na camada prata
df_superior.write.format('delta').mode('overwrite').save(Blob_Path + '/mnt/prata/censoSuperiorDocente')

# COMMAND ----------

#verificar o esquema e os dados
df_superior_prata = spark.read.format('delta').load(Blob_Path + '/mnt/prata/censoSuperiorDocente')  
df_superior_prata.printSchema()
display(df_superior_prata)

# COMMAND ----------

#verificar a quantidade de linhas
df_superior_prata.count()

# COMMAND ----------

# MAGIC %md
# MAGIC ###Salvar na camada ouro para consumo pelo PowerBI

# COMMAND ----------

#ler como dataframe novo(ouro) e persistir na camada ouro para alimentar o PBI
df_superior_prata.write.format('delta').mode('overwrite').save(Blob_Path + '/mnt/ouro/censoSuperiorDocente')
df_superior_prata.write.format('delta').mode('overwrite').saveAsTable('CensoSuperiorDocentes')