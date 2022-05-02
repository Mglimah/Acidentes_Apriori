# Databricks notebook source
import os

# COMMAND ----------

#Declarando variáveis do username e key do Kaggle, ambos valores foram gerados por token por meio de um arquivo .JSON. O username e key foram armazenados no Key Vault. Para lermos essas credenciais, utilizamos o metodo secrets.get do utilitário do databricks (dbutils).
kaggle_username = dbutils.secrets.get(scope='GustavoTcc', key='kaggle-username')
kaggle_key      = dbutils.secrets.get(scope='GustavoTcc', key='kaggle-key')
 
#Necessário gerar variavel de ambiente kaggle antes de importar a biblioteca para a importação da biblioteca do kaggle que precisa de autenticação 
#Armazenando as credenciais do kaggle em variávies de ambiente.
os.environ['KAGGLE_USERNAME'] = kaggle_username
os.environ['KAGGLE_KEY']      = kaggle_key

# COMMAND ----------

import kaggle

# COMMAND ----------

storage_account = 'stgtcc0122'
container = 'brazil-highway-traffic-accidents'
config = f"fs.azure.account.key.{storage_account}.blob.core.windows.net"
mount = 'brazil-highway'
str_path = f"/mnt/{mount}"
input_path = f'{str_path}/raw/'

#Exclui ponto de montagem caso exista
if any(mount.mountPoint == str_path for mount in dbutils.fs.mounts()):
    dbutils.fs.unmount(str_path)

#Cria novo ponto de montagem
dbutils.fs.mount(
    source = f"wasbs://{container}@{storage_account}.blob.core.windows.net",
    mount_point = str_path,
    extra_configs = {config:dbutils.secrets.get(scope = "GustavoTcc", key = "stg-key")})

# COMMAND ----------

dbutils.fs.ls(str_path)

# COMMAND ----------

# DBTITLE 1,Extração do dataset
#Confirmando credenciais e realizando autenticação.
kaggle.api.authenticate()
 
#Realizando a extração do dataset. O mesmo será armazenandos no diretório raíz do Databricks, file:/databricks/driver.
!kaggle datasets download -d mcamera/brazil-highway-traffic-accidents

# COMMAND ----------

dbutils.fs.ls(str_path)

# COMMAND ----------

# DBTITLE 1,Descompactando arquivo para pasta tmp
# MAGIC %sh
# MAGIC unzip /databricks/driver/brazil-highway-traffic-accidents.zip

# COMMAND ----------

dbutils.fs.mv("file:/databricks/driver/por_ocorrencia/datatran2018.csv", f"dbfs:/mnt/{mount}/raw/datatran2018.csv")
dbutils.fs.mv("file:/databricks/driver/por_ocorrencia/datatran2019.csv", f"dbfs:/mnt/{mount}/raw/datatran2019.csv")
dbutils.fs.mv("file:/databricks/driver/por_ocorrencia/datatran2020.csv", f"dbfs:/mnt/{mount}/raw/datatran2020.csv")
dbutils.fs.mv("file:/databricks/driver/por_ocorrencia/datatran2021.csv", f"dbfs:/mnt/{mount}/raw/datatran2021.csv")

dbutils.fs.mv("file:/databricks/driver/por_pessoa_todos tipos/acidentes2018_todas_causas_tipos.csv",             f"dbfs:/mnt/{mount}/raw/acidentes2018_todas_causas_tipos.csv")
dbutils.fs.mv("file:/databricks/driver/por_pessoa_todos tipos/acidentes2019_todas_causas_tipos.csv", f"dbfs:/mnt/{mount}/raw/acidentes2019_todas_causas_tipos.csv")
dbutils.fs.mv("file:/databricks/driver/por_pessoa_todos tipos/acidentes2020_todas_causas_tipos.csv", f"dbfs:/mnt/{mount}/raw/acidentes2020_todas_causas_tipos.csv")
dbutils.fs.mv("file:/databricks/driver/por_pessoa_todos tipos/acidentes2021_todas_causas_tipos.csv", f"dbfs:/mnt/{mount}/raw/acidentes2021_todas_causas_tipos.csv")