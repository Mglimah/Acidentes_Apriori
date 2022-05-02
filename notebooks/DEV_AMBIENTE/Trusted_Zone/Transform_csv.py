# Databricks notebook source
# MAGIC %md
# MAGIC IMPORTANDO BIBLIOTECAS

# COMMAND ----------

#Biblioteca de funções pyspark sql para tratamento de dados
import pyspark.sql.functions as f
import pyspark.sql.types as t

# COMMAND ----------

#Função para verificar pastas e arquivos
dbutils.fs.ls('mnt/brazil-highway/raw')

# COMMAND ----------

#Transformação dos dados csv armazenados na pasta de montagem com o blob para DataFrame Spark
df2020 = spark.read.option('delimiter', ';').option("encoding", "utf-8").csv('dbfs:/mnt/brazil-highway/raw/acidentes2020_todas_causas_tipos.csv', header=True)
df2021 = spark.read.option('delimiter', ';').option("encoding", "ISO-8859-1").csv('dbfs:/mnt/brazil-highway/raw/acidentes2021_todas_causas_tipos.csv', header=True)

dataDF2020 = spark.read.option('delimiter', ';').option("encoding", "ISO-8859-1").csv('dbfs:/mnt/brazil-highway/raw/datatran2020.csv', header=True)
dataDF2021 = spark.read.option('delimiter', ';').option("encoding", "ISO-8859-1").csv('dbfs:/mnt/brazil-highway/raw/datatran2021.csv', header=True)

# COMMAND ----------

#Visualização dos dados
display(dataDF2021)
display(df2021)

# COMMAND ----------

#Visualização dos dados
display(dataDF2020)
display(df2020)

# COMMAND ----------

# MAGIC %md %md
# MAGIC Tratamento de tipos e seleção das colunas que serão utilizadas

# COMMAND ----------

def trataDataFrameCT(DataFrame):
    DataFrame = DataFrame.select(f.col('id').cast(t.IntegerType()), 
                                    f.to_timestamp(f.concat(f.col('data_inversa'), f.lit(' '), f.col('horario')), 'yyyy-MM-dd HH:mm:ss').alias('data'),
                                    f.col('dia_semana'),
                                    f.col('uf'),
                                    f.col('br').cast(t.IntegerType()),
                                    f.regexp_replace(f.col('km'), ',', '.').alias('km').cast(t.FloatType()),
                                    f.col('municipio'),
                                    f.col('causa_acidente'),
                                    f.col('tipo_acidente'),
                                    f.col('classificacao_acidente'),
                                    f.col('fase_dia'),
                                    f.col('sentido_via'),
                                    f.col('condicao_metereologica'),
                                    f.col('tipo_pista'),
                                    f.col('uso_solo'),
                                    f.col('tracado_via'),
                                    f.col('mortos').cast(t.IntegerType()),
                                    f.col('ilesos').cast(t.IntegerType()),
                                    f.col('marca'),
                                    f.col('tipo_veiculo'),
                                    f.col('ano_fabricacao_veiculo').cast(t.IntegerType()), 
                                    f.col('tipo_envolvido'),
                                    f.col('idade').cast(t.IntegerType()), 
                                    f.col('sexo')
                                   )
    return DataFrame

# COMMAND ----------

def trataDataFrameData(DataFrame):
    DataFrame = DataFrame.select(f.col('id').cast(t.IntegerType()), 
                                    f.to_timestamp(f.concat(f.col('data_inversa'), f.lit(' '), f.col('horario')), 'yyyy-MM-dd HH:mm:ss').alias('data'),
                                    f.col('dia_semana'),
                                    f.col('uf'),
                                    f.col('br').cast(t.IntegerType()),
                                    f.regexp_replace(f.col('km'), ',', '.').alias('km').cast(t.FloatType()),
                                    f.col('municipio'),
                                    f.col('causa_acidente'),
                                    f.col('tipo_acidente'),
                                    f.col('classificacao_acidente'),
                                    f.col('fase_dia'),
                                    f.col('sentido_via'),
                                    f.col('condicao_metereologica'),
                                    f.col('tipo_pista'),
                                    f.col('uso_solo'),
                                    f.col('tracado_via'),
                                    f.col('pessoas').cast(t.IntegerType()),
                                    f.col('mortos').cast(t.IntegerType()),
                                    f.col('feridos_leves').cast(t.IntegerType()),
                                    f.col('feridos_graves').cast(t.IntegerType()),
                                    f.col('ignorados').cast(t.IntegerType()),
                                    f.col('feridos').cast(t.IntegerType()),
                                    f.col('veiculos').cast(t.IntegerType()),
                                    f.col('ilesos').cast(t.IntegerType()),
                                   )
    return DataFrame

# COMMAND ----------

new_df2020 = trataDataFrameCT(df2020)
new_df2021 = trataDataFrameCT(df2021)

new_dataDF2021 = trataDataFrameData(dataDF2021)
new_dataDF2020 = trataDataFrameData(dataDF2020)

# COMMAND ----------

display(new_dataDF2021.limit(10))
new_dataDF2021.dtypes

display(new_df2021.limit(10))
new_df2021.dtypes

# COMMAND ----------

# MAGIC %md Verificando se possui valores em branco e nulos na base de dados

# COMMAND ----------

#Contagem de linhas no dataset
display(new_df2021.select(f.count('id')))
display(new_df2020.select(f.count('id')))

display(new_dataDF2021.select(f.count('id')))
display(new_dataDF2020.select(f.count('id')))

#Contagem de valores nulos no dataset
display(new_df2021.select([f.count(f.when(f.col(c).isNull(), c)).alias(c) for c in new_df2021.columns]))
display(new_df2020.select([f.count(f.when(f.col(c).isNull(), c)).alias(c) for c in new_df2020.columns]))

display(new_dataDF2021.select([f.count(f.when(f.col(c).isNull(), c)).alias(c) for c in new_dataDF2021.columns]))
display(new_dataDF2020.select([f.count(f.when(f.col(c).isNull(), c)).alias(c) for c in new_dataDF2020.columns]))

#Contagem de valores em branco no dataset
#display(new_df.select([f.count(f.when(f.col(c) == ' ', c)).alias(c) for c in new_df.columns]))
#display(new_df1.select([f.count(f.when(f.col(c) == ' ', c)).alias(c) for c in new_df1.columns]))

# COMMAND ----------

# DBTITLE 1,Inserindo valor "0" para colunas do tipo int nulas
def trataNuloCT(Dataframe):
    Dataframe = Dataframe.withColumn('br', f.when(f.col('br').isNull(), 0).otherwise(Dataframe.br))\
                         .withColumn('km', f.when(f.col('km').isNull(), 0).otherwise(Dataframe.km))\
                         .withColumn('ano_fabricacao_veiculo', f.when(f.col('ano_fabricacao_veiculo').isNull(), 0).otherwise(Dataframe.ano_fabricacao_veiculo))\
                         .withColumn('idade', f.when(f.col('idade').isNull(), 0).otherwise(Dataframe.idade))\
                         .withColumn('marca', f.when(f.col('marca').isNull(), "Não Informado").otherwise(Dataframe.marca))\
                         .withColumn('tipo_acidente', f.when(f.col('tipo_acidente').isNull(), "Não Informado").otherwise(Dataframe.tipo_acidente))
    return Dataframe

# COMMAND ----------

def trataNuloData(Dataframe):
    Dataframe = Dataframe.withColumn('br', f.when(f.col('br').isNull(), 0).otherwise(Dataframe.br))\
                         .withColumn('km', f.when(f.col('km').isNull(), 0).otherwise(Dataframe.km))\
                         .withColumn('tipo_acidente', f.when(f.col('tipo_acidente').isNull(), "Não Informado").otherwise(Dataframe.tipo_acidente))
    return Dataframe

# COMMAND ----------

finalDF2021 = trataNuloCT(new_df2021)
finalDF2020 = trataNuloCT(new_df2020)

finalDataDF2021 = trataNuloData(new_dataDF2021)
finalDataDF2020 = trataNuloData(new_dataDF2020)

# COMMAND ----------

#Contagem de linhas no dataset
display(finalDF2021.select(f.count('id')))
display(finalDataDF2021.select(f.count('id')))

display(finalDF2020.select([f.count(f.when(f.col(c).isNull(), c)).alias(c) for c in finalDF2020.columns]))
display(finalDataDF2020.select([f.count(f.when(f.col(c).isNull(), c)).alias(c) for c in finalDataDF2021.columns]))

# COMMAND ----------

# DBTITLE 1,Excluindo linhas com ids nulas
finalDF2021 = finalDF2021.na.drop(subset=['id'])
finalDF2020 = finalDF2020.na.drop(subset=['id'])

finalDataDF2021 = finalDataDF2021.na.drop(subset=['id'])
finalDataDF2020 = finalDataDF2020.na.drop(subset=['id'])

# COMMAND ----------

# DBTITLE 1,Salvando dataframes como parquet
finalDF2021.write.parquet("dbfs:/mnt/brazil-highway/trusted/trusted_df_2021", mode='overwrite')
finalDF2020.write.parquet("dbfs:/mnt/brazil-highway/trusted/trusted_df_2020", mode='overwrite')

finalDataDF2021.write.parquet("dbfs:/mnt/brazil-highway/trusted/trusted_DataDf_2021", mode='overwrite')
finalDataDF2020.write.parquet("dbfs:/mnt/brazil-highway/trusted/trusted_DataDf_2020", mode='overwrite')

# COMMAND ----------

display(finalDF2021)

# COMMAND ----------

dbutils.fs.ls('dbfs:/mnt/brazil-highway/trusted/')