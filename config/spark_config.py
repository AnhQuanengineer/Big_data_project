import os
from typing import Optional,List,Dict
from pyspark.sql import SparkSession
from config.mysql_config import get_database_config
from pyspark.sql.types import *
from pyspark.sql.functions import *


def create_spark_session(
        app_name : str
        ,master_url : str = 'local[*]'
        ,executor_memory : Optional[str] = '4g'
        ,executor_cores : Optional[int] = 2
        ,driver_memory : Optional[str] = '2g'
        ,num_executors : Optional[int] = 3
        ,jars : Optional[List[str]] = None
        ,spark_conf : Optional[Dict[str,str]] = None
        ,log_level : str = 'INFO'
) -> SparkSession:
    builder = SparkSession.builder \
        .appName(app_name) \
        .master(master_url)

    if executor_memory:
        builder.config('spark.executor.memory',executor_memory)
    if executor_cores:
        builder.config('spark.executor.cores',executor_cores)
    if driver_memory:
        builder.config('spark.driver.memory',driver_memory)
    if num_executors:
        builder.config('spark.executor.instances',num_executors)

    if jars:
        jars_path = ",".join([os.path.abspath(jar) for jar in jars])
        builder.config('spark.jars',jars_path)

    if spark_conf:
        for key,value in spark_conf.items():
            builder.config(key, value)

    spark = builder.getOrCreate()
    spark.sparkContext.setLogLevel(log_level)

    return spark

# spark = create_spark_session(
#         app_name = 'quandz'
#         ,master_url = 'local[*]'
#         ,executor_memory = '4g'
#         ,executor_cores = 2
#         ,driver_memory = '2g'
#         ,num_executors = 3
#         ,jars = None
#         ,spark_conf = None
#         ,log_level = 'INFO'
# )
#
# data = [['quan',18],['minh anh',17],['dat',22]]
# df = spark.createDataFrame(data,['ten','tuoi'])
# df.show()

def connect_to_mysql(spark : SparkSession, config : Dict[str,str], table_name : str):
    mysql_url = config['mysql_url']
    properties = {
        "user": config['user'],
        "password": config['password'],
        "driver": config['mysql_driver']
    }

    df = spark.read \
        .jdbc(url = mysql_url,
              table = table_name,
              properties = properties)
    return df

    # df = spark.read \
    #         .format("jdbc") \
    #         .option('url',config['mysql_url']) \
    #         .option('dbtable',table_name) \
    #         .option('user',config['user']) \
    #         .option('password',config['password']) \
    #         .option('driver',config['mysql_driver']) \
    #         .load()
    # return df

def write_to_mysql(df : DataFrame , config : Dict[str,str], table_name : str, mode : str):
    mysql_url = config['mysql_url']
    properties = {
        "user": config['user'],
        "password": config['password'],
        "driver": config['mysql_driver']
    }

    return df.write \
        .jdbc(url = mysql_url,
              table = table_name,
              mode= mode,
              properties = properties)


mysql_jar_path = '../lib/mysql-connector-j-9.2.0.jar'
mongodb_jar_path = '../lib/mongodb-jdbc-2.2.2.jar'
redis_jar_path = '../lib/cdata.jdbc.redis.jar'

spark = create_spark_session(
    app_name = 'quandz'
    ,master_url = 'local[*]'
    ,executor_memory = '4g'
    ,jars = [mysql_jar_path,mongodb_jar_path,redis_jar_path]
    ,log_level = 'INFO'
)

# schema = StructType([
#     StructField('id',IntegerType(),False),
#     StructField('name',StringType(),True),
#     StructField('age',IntegerType(),True),
# ])

schema = StructType([
    StructField('actor', StructType([
        StructField('id', LongType(), False),
        StructField('login', StringType(), True),
        StructField('gravatar_id', StringType(), True),
        StructField('url', StringType(), True),
        StructField('avatar_url', StringType(), True),
    ]), True),
    StructField('repo', StructType([
        StructField('id', LongType(), False),
        StructField('name', StringType(), True),
        StructField('url', StringType(), True),
    ]), True)
    ])

df = spark.read.schema(schema).json("/home/victo/PycharmProjects/Big_data_project/data/2015-03-01-17.json")
df_Users = df.select(
    col('actor.id').alias('user_id')
    ,col('actor.login').alias('login')
    ,col('actor.gravatar_id').alias('gravatar_id')
    ,col('actor.avatar_url').alias('avatar_url')
    ,col('actor.url').alias('url')
)

df_Repositories = df.select(
    col('repo.id').alias('repo_id')
    ,col('repo.name').alias('name')
    ,col('repo.url').alias('url')
)

db_config = get_database_config()
mysql_table_users = 'Users'
mysql_table_repositories = 'Repositories'


write_to_mysql(df_Users,db_config,mysql_table_users,"overwrite")
write_to_mysql(df_Repositories,db_config,mysql_table_repositories, "overwrite")


df_mysql = connect_to_mysql(spark,db_config,mysql_table_repositories)
df_mysql.show()



