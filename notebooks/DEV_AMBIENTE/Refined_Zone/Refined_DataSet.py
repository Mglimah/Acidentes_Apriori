# Databricks notebook source
#Biblioteca de funções pyspark sql para tratamento de dados
import pyspark.sql.functions as f
import pyspark.sql.types as t

# COMMAND ----------

dbutils.fs.ls('dbfs:/mnt/brazil-highway/trusted/')

# COMMAND ----------

DataDF2021 = spark.read.parquet('dbfs:/mnt/brazil-highway/trusted/trusted_DataDf_2021')
PessoasDF2021 = spark.read.parquet('dbfs:/mnt/brazil-highway/trusted/trusted_df_2021')

DataDF2020 = spark.read.parquet('dbfs:/mnt/brazil-highway/trusted/trusted_DataDf_2020')
PessoasDF2020 = spark.read.parquet('dbfs:/mnt/brazil-highway/trusted/trusted_df_2020')

# COMMAND ----------

#DataDF2021 = DataDF2021.drop('id', 'data')
display(DataDF2021)
display(PessoasDF2021)

# COMMAND ----------

display(DataDF2020)
display(PessoasDF2020)

# COMMAND ----------

df_join2021 = PessoasDF2021.join(DataDF2021, DataDF2021.id == PessoasDF2021.id).where(PessoasDF2021.tipo_envolvido == 'Condutor')\
                       .select(PessoasDF2021.id,
                               DataDF2021.dia_semana,
                               DataDF2021.uf,
                               DataDF2021.br,
                               DataDF2021.km,
                               DataDF2021.municipio,
                               DataDF2021.causa_acidente,
                               DataDF2021.tipo_acidente,
                               DataDF2021.uso_solo,
                               DataDF2021.classificacao_acidente,
                               DataDF2021.fase_dia,
                               DataDF2021.sentido_via,
                               DataDF2021.condicao_metereologica,
                               DataDF2021.tipo_pista,
                               DataDF2021.tracado_via,
                               'pessoas', 
                               'veiculos',
                               DataDF2021.mortos, 
                               'feridos', 
                               'tipo_veiculo', 
                               'ano_fabricacao_veiculo', 
                               'idade', 
                               'sexo', 
                               'tipo_envolvido')

# COMMAND ----------

df_join2020 = PessoasDF2020.join(DataDF2020, DataDF2020.id == PessoasDF2020.id).where(PessoasDF2020.tipo_envolvido == 'Condutor')\
                       .select(PessoasDF2020.id, 
                               DataDF2020.dia_semana, 
                               DataDF2020.uf, 
                               DataDF2020.br, 
                               DataDF2020.km, 
                               DataDF2020.municipio, 
                               DataDF2020.causa_acidente, 
                               DataDF2020.tipo_acidente, 
                               DataDF2020.uso_solo,
                               DataDF2020.classificacao_acidente, 
                               DataDF2020.fase_dia, 
                               DataDF2020.sentido_via, 
                               DataDF2020.condicao_metereologica, 
                               DataDF2020.tipo_pista, 
                               DataDF2020.tracado_via, 
                               'pessoas', 
                               'veiculos',
                               DataDF2020.mortos, 
                               'feridos', 
                               'tipo_veiculo', 
                               'ano_fabricacao_veiculo', 
                               'idade', 
                               'sexo', 
                               'tipo_envolvido')

# COMMAND ----------

df_union = df_join2020.union(df_join2021)

# COMMAND ----------

display(df_union)

# COMMAND ----------

df_distinct2021 = df_join2021.distinct()
df_distinct = df_union.distinct()
#display(df_distinct)
print(df_distinct2021.count())

# COMMAND ----------

#
df_distinct1 = df_distinct2021.withColumn('br', f.col('br').cast('string'))\
                         .withColumn('km', f.col('km').cast('string'))\
                         .withColumn('pessoas', f.col('pessoas').cast('string'))\
                         .withColumn('veiculos', f.col('veiculos').cast('string'))\
                         .withColumn('mortos', f.col('mortos').cast('string'))\
                         .withColumn('feridos', f.col('feridos').cast('string'))\
                         .withColumn('ano_fabricacao_veiculo', f.col('ano_fabricacao_veiculo').cast('string'))\
                         .withColumn('idade', f.col('idade').cast('string'))

#df_distinct1 = df_distinct1.select(f.lit(df_distinct1.pessoas + ' Pessoa(s)'))

#display(df_distinct1)

# COMMAND ----------

#Inserindo nome das colunas nas rows para identificação no apriori
df_pessoas_causa = df_distinct1.withColumn('pessoas', f.regexp_replace(f.col('pessoas'), "(\d{1,2})", "$0 pessoas"))\
                         .withColumn('veiculos', f.regexp_replace(f.col('veiculos'), "(\d{1,2})", "$0 veiculos"))\
                         .withColumn('mortos', f.regexp_replace(f.col('mortos'), "(\d{1,2})", "$0 mortos"))\
                         .withColumn('feridos', f.regexp_replace(f.col('feridos'), "(\d{1,2})", "$0 feridos"))
display(df_pessoas_causa)

# COMMAND ----------

# MAGIC %md
# MAGIC #Jogando inteiros para string e dropando id

# COMMAND ----------

df_ = df_pessoas_causa.drop('id', 'dia_semana', 'br', 'km', 'municipio', 'classificacao_acidente', 'fase_dia', 'sentido_via', 'ano_fabricacao_veiculo', 'idade', 'idade', 'tipo_envolvido', 'sentido_via', 'tracado_via', 'sexo', 'pessoas', 'mortos')
display(df_)

# COMMAND ----------

# MAGIC %md
# MAGIC #Criando lista de listas

# COMMAND ----------

df = df_.collect()
new_list = []
for i in range(len(df)):
    new_list.append(list(df[i]))
    
print(len(new_list))

# COMMAND ----------

new_list

# COMMAND ----------

# MAGIC %sql
# MAGIC --create schema if not exists relacoes_acidentes

# COMMAND ----------

# MAGIC %sql
# MAGIC --create or replace table relacoes_acidentes.fato_acidentes(
# MAGIC --    dia_semana varchar(15)
# MAGIC --  , causa_acidente varchar(100)
# MAGIC --  , tipo_acidente varchar(100)
# MAGIC --  , classificacao_acidente varchar(50)
# MAGIC --  , fase_dia varchar(50)
# MAGIC --  , condicao_metereologica varchar(50)
# MAGIC --  , tipo_pista varchar(50)
# MAGIC --  , tipo_veiculo varchar(50)
# MAGIC --)

# COMMAND ----------

df_.createOrReplaceTempView('fato_acidentes')

# COMMAND ----------

# MAGIC %sql
# MAGIC --insert into relacoes_acidentes.fato_acidentes
# MAGIC --select * from fato_acidentes

# COMMAND ----------

from mlxtend.preprocessing import TransactionEncoder
import pandas as pd

# COMMAND ----------

te = TransactionEncoder()
te_ary = te.fit(new_list).transform(new_list)
pd_Pandas = pd.DataFrame(te_ary, columns = te.columns_)

# COMMAND ----------

pd_Pandas.head(10)

# COMMAND ----------

from mlxtend.frequent_patterns import apriori, association_rules

# COMMAND ----------

#suport ---- porcentagem de vezes que a ocorrencia aparece
#itemset ---- ocorrencia
frequent_itemsets = apriori(pd_Pandas, min_support = 0.05, use_colnames = True)
frequent_itemsets

# COMMAND ----------

    #sort_values ---- ordena valores
#ascending = False ---- Faz com que ordene do maior para o menor
frequent_itemsets.sort_values(by = ['support'], ascending = False)

# COMMAND ----------

from mlxtend.frequent_patterns import association_rules

# COMMAND ----------

rules = association_rules(frequent_itemsets, metric = 'confidence', min_threshold = 0.5)

# COMMAND ----------

rules.sort_values(by = ['lift'], ascending = False).drop(['antecedent support', 'consequent support', 'leverage'], axis = 1)

# COMMAND ----------

rules['id'] = [x for x in range(len(rules))]

# COMMAND ----------

#from pyspark.sql import SparkSession, Row
 
lst_antecedents = [list(x) for x in rules['antecedents']]
lst_consequents = [list(x) for x in rules['consequents']]

#R_antecedents = Row("antecedents")
#R_consequents = Row('consequents')


#lista_rules = [(rules['support'], rules['confidence'], rules['lift'], rules['conviction'])]

#df_antecedents = spark.sparkContext.parallelize([R_antecedents(r) for r in list(lst_antecedents)]).toDF()
#df_consequents = spark.sparkContext.parallelize([R_consequents(r) for r in list(lst_consequents)]).toDF()
df_sequencia = spark.createDataFrame(rules.drop(['antecedents', 'consequents', 'antecedent support', 'consequent support', 'leverage'], axis = 1))

# COMMAND ----------

#R_teste = Row("id")
#list_id = [x for x in rules['id']]
#list_df = [x for x in list(lst_antecedents)]

columns_antecedents = ['antecedents', 'id']
data_antecedents = [([x for x in list(lst_antecedents)], [x for x in rules['id']])]

columns_consequents = ['consequents', 'id']
data_consequents = [([x for x in list(lst_consequents)], [x for x in rules['id']])]

df_antecedents = spark.sparkContext.parallelize(data_antecedents).toDF(columns_antecedents)
df_consequents = spark.sparkContext.parallelize(data_consequents).toDF(columns_consequents)

df_antecedents = df_antecedents.withColumn('new', f.arrays_zip('antecedents', 'id'))\
                               .withColumn('new', f.explode('new'))\
                               .select(f.col("new.antecedents").alias("antecedents"), f.col("new.id").alias("id"))

df_consequents = df_consequents.withColumn('new', f.arrays_zip('consequents', 'id'))\
                               .withColumn('new', f.explode('new'))\
                               .select(f.col("new.consequents").alias("consequents"), f.col("new.id").alias("id"))

# COMMAND ----------

df_total = df_antecedents.join(df_consequents, df_consequents.id == df_antecedents.id)\
                         .join(df_sequencia, df_sequencia.id == df_antecedents.id)\
                         .select(df_antecedents.id, 'antecedents', 'consequents', 'support', 'confidence', 'lift', 'conviction')   

# COMMAND ----------

df_total.printSchema()

# COMMAND ----------

df_total.createOrReplaceTempView('df_total')

# COMMAND ----------

df_total.write.format('delta').mode('overwrite').saveAsTable("relacoes_acidentes.apriori_statistics")

# COMMAND ----------

# MAGIC %sql
# MAGIC DELETE FROM relacoes_acidentes.apriori_statistics
# MAGIC WHERE lift < 1;

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from relacoes_acidentes.apriori_statistics
# MAGIC --WHERE lift < 1;

# COMMAND ----------

# MAGIC %md
# MAGIC ##Testes

# COMMAND ----------

rules.describe()

# COMMAND ----------

import seaborn as sns

sns.pairplot(rules[['confidence', 'lift', 'conviction', 'support']])

# COMMAND ----------

