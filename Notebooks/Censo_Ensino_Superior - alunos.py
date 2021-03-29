# Databricks notebook source
# MAGIC %md
# MAGIC # CENSO EDUCAÇÃO SUPERIOR - ALUNOS

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
from pyspark.sql.functions import *
from pyspark.sql.types import *
from pyspark.sql.functions import udf
from pyspark.sql.types import IntegerType

# COMMAND ----------

#salvando na máquina para extração dos dados
dbutils.fs.cp('dbfs:/tmp/Superior_2017.zip', 'file:/tmp/Censo_Superior_2017.zip')
dbutils.fs.cp('dbfs:/tmp/Superior_2018.zip', 'file:/tmp/Censo_Superior_2018.zip')
dbutils.fs.cp('dbfs:/tmp/Superior_2019.zip', 'file:/tmp/Censo_Superior_2019.zip')

# COMMAND ----------

# MAGIC %md
# MAGIC ###Extraindo os arquivos

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
# MAGIC unzip -j /tmp/Superior2017/DM_ALUNO.zip -d /tmp/SuperiorAlunos_2017

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
# MAGIC ###Lendo os arquivos csv, criando os dataframes, verificando os esquemas e salvando na camada bronze

# COMMAND ----------

#abrir o arquivo csv e salvar em um DataFrame
#2017
csv = 'file:/tmp/SuperiorAlunos_2017/DM_ALUNO.CSV'
df_2017 = (spark.read
      .option('sep', '|')
      .option('header', 'true')
      .csv(csv)
)
df_2017.printSchema()

# COMMAND ----------

#2018
csv = 'file:/tmp/Superior2018/DM_ALUNO.CSV'
df_2018 = (spark.read
      .option('sep', '|')
      .option('header', 'true')
      .csv(csv)
)
df_2018.printSchema()

# COMMAND ----------

#2019
csv = 'file:/tmp/Superior2019/SUP_ALUNO_2019.CSV'
df_2019 = (spark.read
      .option('sep', '|')
      .option('header', 'true')
      .csv(csv)
)
df_2019.printSchema()

# COMMAND ----------

#salvar no delta lake (tabela bronze) dados sem modificação
df_2017.write.format('delta').mode('overwrite').save(Blob_Path + '/mnt/bronze/SuperiorAlunos_2017')

# COMMAND ----------

#salvar no delta lake (tabela bronze) dados sem modificação
#2018
df_2018.write.format('delta').mode('append').save(Blob_Path + '/mnt/bronze/SuperiorAlunos_2018_19')

# COMMAND ----------

#salvar no delta lake (tabela bronze) dados sem modificação
#2019
df_2019.write.format('delta').mode('append').save(Blob_Path + '/mnt/bronze/SuperiorAlunos_2018_19')

# COMMAND ----------

# MAGIC %md
# MAGIC ###Lendo o dataframe de 2017 e fazendo as transformações para ler como único

# COMMAND ----------

#lendo o dataframe de 2017 para fazer as transformações
df_2017 = spark.read.format('delta').load(Blob_Path + '/mnt/bronze/SuperiorAlunos_2017')
display(df_2017)

# COMMAND ----------

#verificando se existem valores nulos na tabela
display(df_2017.select([count(when(col(c).isNull(), c)).alias(c) for c in df_2017.columns]))

# COMMAND ----------

#Transformar valores nulos em 'x'
df_2017 = df_2017.na.fill('x')
display(df_2017)

# COMMAND ----------

#mudar as colunas do Dataframe de 2017 para bater com o esquema das outras tabelas (quatro colunas tem nome diferente)
df_2017 = df_2017.withColumn('ID_ALUNO',df_2017['CO_ALUNO']).withColumn('IN_DEFICIENCIA',df_2017['TP_DEFICIENCIA']).withColumn('IN_TGD_AUTISMO',df_2017['IN_TGD_AUTISMO_INFANTIL']).withColumn('CO_CINE_ROTULO',df_2017['CO_OCDE'])
df_2017.printSchema()

# COMMAND ----------

#retirar as colunas com esquema diferente - dataframe 2017
df_2017 = df_2017.select('NU_ANO_CENSO','CO_IES','TP_CATEGORIA_ADMINISTRATIVA','TP_ORGANIZACAO_ACADEMICA','CO_CURSO','CO_CURSO_POLO','TP_TURNO','TP_GRAU_ACADEMICO','TP_MODALIDADE_ENSINO','TP_NIVEL_ACADEMICO','CO_CINE_ROTULO','ID_ALUNO','CO_ALUNO_CURSO','CO_ALUNO_CURSO_ORIGEM','TP_COR_RACA','TP_SEXO','NU_ANO_NASCIMENTO','NU_MES_NASCIMENTO','NU_DIA_NASCIMENTO','NU_IDADE','TP_NACIONALIDADE','CO_PAIS_ORIGEM','CO_UF_NASCIMENTO','CO_MUNICIPIO_NASCIMENTO','IN_DEFICIENCIA','IN_DEFICIENCIA_AUDITIVA','IN_DEFICIENCIA_FISICA','IN_DEFICIENCIA_INTELECTUAL','IN_DEFICIENCIA_MULTIPLA','IN_DEFICIENCIA_SURDEZ','IN_DEFICIENCIA_SURDOCEGUEIRA','IN_DEFICIENCIA_BAIXA_VISAO','IN_DEFICIENCIA_CEGUEIRA','IN_DEFICIENCIA_SUPERDOTACAO','IN_TGD_AUTISMO','IN_TGD_SINDROME_ASPERGER','IN_TGD_SINDROME_RETT','IN_TGD_TRANSTOR_DESINTEGRATIVO','TP_SITUACAO','QT_CARGA_HORARIA_TOTAL','QT_CARGA_HORARIA_INTEG','DT_INGRESSO_CURSO','IN_INGRESSO_VESTIBULAR','IN_INGRESSO_ENEM','IN_INGRESSO_AVALIACAO_SERIADA','IN_INGRESSO_SELECAO_SIMPLIFICA','IN_INGRESSO_OUTRO_TIPO_SELECAO','IN_INGRESSO_VAGA_REMANESC','IN_INGRESSO_VAGA_PROG_ESPECIAL','IN_INGRESSO_TRANSF_EXOFFICIO','IN_INGRESSO_DECISAO_JUDICIAL','IN_INGRESSO_CONVENIO_PECG','IN_INGRESSO_EGRESSO','IN_INGRESSO_OUTRA_FORMA','IN_RESERVA_VAGAS','IN_RESERVA_ETNICO','IN_RESERVA_DEFICIENCIA','IN_RESERVA_ENSINO_PUBLICO','IN_RESERVA_RENDA_FAMILIAR','IN_RESERVA_OUTRA','IN_FINANCIAMENTO_ESTUDANTIL','IN_FIN_REEMB_FIES','IN_FIN_REEMB_ESTADUAL','IN_FIN_REEMB_MUNICIPAL','IN_FIN_REEMB_PROG_IES','IN_FIN_REEMB_ENT_EXTERNA','IN_FIN_REEMB_OUTRA','IN_FIN_NAOREEMB_PROUNI_INTEGR','IN_FIN_NAOREEMB_PROUNI_PARCIAL','IN_FIN_NAOREEMB_ESTADUAL','IN_FIN_NAOREEMB_MUNICIPAL','IN_FIN_NAOREEMB_PROG_IES','IN_FIN_NAOREEMB_ENT_EXTERNA','IN_FIN_NAOREEMB_OUTRA','IN_APOIO_SOCIAL','IN_APOIO_ALIMENTACAO','IN_APOIO_BOLSA_PERMANENCIA','IN_APOIO_BOLSA_TRABALHO','IN_APOIO_MATERIAL_DIDATICO','IN_APOIO_MORADIA','IN_APOIO_TRANSPORTE','IN_ATIVIDADE_EXTRACURRICULAR','IN_COMPLEMENTAR_ESTAGIO','IN_COMPLEMENTAR_EXTENSAO','IN_COMPLEMENTAR_MONITORIA','IN_COMPLEMENTAR_PESQUISA','IN_BOLSA_ESTAGIO','IN_BOLSA_EXTENSAO','IN_BOLSA_MONITORIA','IN_BOLSA_PESQUISA','TP_ESCOLA_CONCLUSAO_ENS_MEDIO','IN_ALUNO_PARFOR','TP_SEMESTRE_CONCLUSAO','TP_SEMESTRE_REFERENCIA','IN_MOBILIDADE_ACADEMICA','TP_MOBILIDADE_ACADEMICA','TP_MOBILIDADE_ACADEMICA_INTERN','CO_IES_DESTINO','CO_PAIS_DESTINO','IN_MATRICULA','IN_CONCLUINTE','IN_INGRESSO_TOTAL','IN_INGRESSO_VAGA_NOVA','IN_INGRESSO_PROCESSO_SELETIVO','NU_ANO_INGRESSO')

# COMMAND ----------

#trocar os números pelas informações referentes na coluna 'TP_CATEGORIA_ADMINISTRATIVA'
df_2017 = df_2017.replace(['x','1','2','3','4','5','6','7'],['Não consta','Pública Federal','Pública Estadual','Pública Municipal','Privada com fins lucrativos','Privada sem fins lucrativos','Privada confessional','Especial'], 'TP_CATEGORIA_ADMINISTRATIVA')
display(df_2017)

# COMMAND ----------

#salvar no delta lake (tabela bronze) dados sem modificação
df_2017.write.format('delta').mode('append').save(Blob_Path + '/mnt/bronze/SuperiorAlunos')

# COMMAND ----------

# MAGIC %md
# MAGIC ###Lendo o dataframe de 2018 e 2019 e fazendo as transformações e salvando como um único

# COMMAND ----------

#lendo os dataframes de 2018 e 2019 para fazer as transformações
df_2018_19 = spark.read.format('delta').load(Blob_Path + '/mnt/bronze/SuperiorAlunos_2018_19')
display(df_2018_19)
df_2018_19.printSchema()

# COMMAND ----------

#verificando se existem valores nulos na tabela
display(df_2018_19.select([count(when(col(c).isNull(), c)).alias(c) for c in df_2018_19.columns]))

# COMMAND ----------

#Transformar valores nulos em 'x'
df_2018_19 = df_2018_19.na.fill('x')
display(df_2018_19)

# COMMAND ----------

#trocar os números pelas informações referentes na coluna 'TP_CATEGORIA_ADMINISTRATIVA'
df_2018_19 = df_2018_19.replace(['x','1','2','3','4','5','6','7','8','9'],['Não consta','Pública Federal','Pública Estadual','Pública Municipal','Privada com fins lucrativos','Privada sem fins lucrativos','Privada - Particular em sentido estrito','Especial','Privada comunitária','Privada confessional'], 'TP_CATEGORIA_ADMINISTRATIVA')
display(df_2018_19)

# COMMAND ----------

#salvar no delta lake (tabela bronze) dados sem modificação
df_2018_19.write.format('delta').mode('append').save(Blob_Path + '/mnt/bronze/SuperiorAlunos')

# COMMAND ----------

# MAGIC %md
# MAGIC ###Lendo os dataframes como um dataframe único

# COMMAND ----------

#ler a junção de todos os arquivos como um DataFrame único
df_superior = spark.read.format('delta').load(Blob_Path + '/mnt/bronze/SuperiorAlunos')  
display(df_superior)

# COMMAND ----------

#verificar se deu certo o append entre todas as tabelas verificando a quantidade de linhas
df_superior.count()

# COMMAND ----------

# MAGIC %md
# MAGIC ###Fazendo as transformações no dataframe único

# COMMAND ----------

# MAGIC %md
# MAGIC Alterando as informações de cada coluna - trocando os números por sua informações equivalentes

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

#mudar valores nas linhas da coluna 'TP_TURNO' de acordo com as informações correspondentes
df_superior = df_superior.replace(['x','1','2','3','4'],['Não Aplicável - Ead','Matutino','Vespertino','Noturno','Integral'], 'TP_TURNO')
display(df_superior)

# COMMAND ----------

#mudar valores nas linhas da coluna 'TP_GRAU_ACADEMICO' de acordo com as informações correspondentes
df_superior = df_superior.replace(['x','1','2','3','4','5'],['Não consta','Não aplicável','Bacharelado','Licenciatura','Tecnológico','Bacharelado e licenciatura'], 'TP_GRAU_ACADEMICO')
display(df_superior)

# COMMAND ----------

#mudar valores nas linhas da coluna 'TP_MODALIDADE_ENSINO' de acordo com as informações correspondentes
df_superior = df_superior.replace(['x','1','2'],['Não consta','Presencial','Curso a distância'], 'TP_MODALIDADE_ENSINO')
display(df_superior)

# COMMAND ----------

#mudar valores nas linhas da coluna 'TP_NIVEL_ACADEMICO' de acordo com as informações correspondentes
df_superior = df_superior.replace(['x','1','2'],['Não consta','Graduação','Sequencial de formação específica'], 'TP_NIVEL_ACADEMICO')
display(df_superior)

# COMMAND ----------

#mudar valores nas linhas da coluna 'TP_COR_RACA' de acordo com as informações correspondentes
df_superior = df_superior.replace(['x','0','1','2','3','4','5','9'],['Não consta','Não quis declarar','Branca', 'Preta', 'Parda', 'Amarela', 'Indígena', 'Não resposta'], 'TP_COR_RACA')
display(df_superior)

# COMMAND ----------

#mudar valores nas linhas da coluna 'TP_SEXO' de acordo com as informações correspondentes
df_superior = df_superior.replace(['x','1','2'],['Não consta','Feminino','Masculino'], 'TP_SEXO')
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

#mudar valores nas linhas da coluna 'TP_SITUACAO' de acordo com as informações correspondentes
df_superior = df_superior.replace(['x','2','3','4','5','6','7'],['Não consta','Cursando','Matrícula trancada', 'Desvinculado do curso', 'Transferido', 'Formado', 'Falecido'], 'TP_SITUACAO')
display(df_superior)

# COMMAND ----------

#mudar valores nas linhas da coluna 'IN_INGRESSO_VESTIBULAR' de acordo com as informações correspondentes
df_superior = df_superior.replace(['x','0','1'],['Não consta','Não','Sim'], 'IN_INGRESSO_VESTIBULAR')
display(df_superior)

# COMMAND ----------

#mudar valores nas linhas da coluna 'IN_INGRESSO_ENEM' de acordo com as informações correspondentes
df_superior = df_superior.replace(['x','0','1'],['Não consta','Não','Sim'], 'IN_INGRESSO_ENEM')
display(df_superior)

# COMMAND ----------

#mudar valores nas linhas da coluna 'IN_APOIO_SOCIAL' de acordo com as informações correspondentes
df_superior = df_superior.replace(['x','0','1'],['Não consta','Não','Sim'], 'IN_APOIO_SOCIAL')
display(df_superior)

# COMMAND ----------

# MAGIC %md
# MAGIC Mudando os tipos dos dados e os nomes das colunas

# COMMAND ----------

#mudar o tipo da coluna ano para data com uma coluna nova
df_superior = df_superior.withColumn('Ano', df_superior['NU_ANO_CENSO'].cast(DateType()))
display(df_superior)
df_superior.printSchema()

# COMMAND ----------

#mudar nomes das colunas
df_superior = df_superior.withColumn('Categoria_Administrativa',df_superior['TP_CATEGORIA_ADMINISTRATIVA']).withColumn('Organizacao_Academica',df_superior['TP_ORGANIZACAO_ACADEMICA']).withColumn('Turno',df_superior['TP_TURNO']).withColumn('Grau_Academico',df_superior['TP_GRAU_ACADEMICO']).withColumn('Modalidade_Ensino',df_superior['TP_MODALIDADE_ENSINO']).withColumn('Nivel_Academico',df_superior['TP_NIVEL_ACADEMICO']).withColumn('Cor_Raca',df_superior['TP_COR_RACA']).withColumn('Sexo',df_superior['TP_SEXO']).withColumn('Idade',df_superior['NU_IDADE']).withColumn('Nacionalidade',df_superior['TP_NACIONALIDADE']).withColumn('Tem_Deficiencia',df_superior['IN_DEFICIENCIA']).withColumn('Situacao',df_superior['TP_SITUACAO']).withColumn('Ingressou_por_Vestibular',df_superior['IN_INGRESSO_VESTIBULAR']).withColumn('Ingressou_por_Enem',df_superior['IN_INGRESSO_ENEM']).withColumn('Apoio_Social',df_superior['IN_APOIO_SOCIAL'])
df_superior.printSchema()
display(df_superior)

# COMMAND ----------

#Manter as colunas que iremos usar e verificar
df_superior = df_superior.select('Ano','Categoria_Administrativa','Organizacao_Academica','Turno','Grau_Academico','Modalidade_Ensino','Nivel_Academico','Cor_Raca','Sexo','Idade','Grupo_de_Idade','Nacionalidade','Tem_Deficiencia','Situacao','Ingressou_por_Vestibular','Ingressou_por_Enem','Apoio_Social')
df_superior.printSchema()
display(df_superior)

# COMMAND ----------

#ordenar dados e verificar
df_superior = df_superior.sort('Ano')
display(df_superior)

# COMMAND ----------

# MAGIC %md
# MAGIC ###Salvando na camada prata e verificando o arquivo

# COMMAND ----------

#Salvar na camada prata
df_superior.write.format('delta').mode('overwrite').save(Blob_Path + '/mnt/prata/SuperiorAlunos')

# COMMAND ----------

#verificando o esquema e os dados
df_superior_prata = spark.read.format('delta').load(Blob_Path + '/mnt/prata/SuperiorAlunos')  
df_superior_prata.printSchema()
display(df_superior_prata)

# COMMAND ----------

df_superior_prata.count()

# COMMAND ----------

# MAGIC %md
# MAGIC ###Salvanda na camada ouro para consumo pelo PowerBI

# COMMAND ----------

#ler como dataframe novo(ouro) e persistir na camada ouro para alimentar o PBI
df_superior_prata.write.format('delta').mode('overwrite').save(Blob_Path + '/mnt/ouro/censoSuperiorAlunos')
df_superior_prata.write.format('delta').mode('overwrite').saveAsTable('CensoSuperiorAlunos')