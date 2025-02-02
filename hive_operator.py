from pyspark.sql import SparkSession
from pyspark.sql.functions import col
from pyspark.sql.types import StringType
from hivemodule import HiveOperator

# Создаем SparkSession с доступом к Hive
spark = SparkSession \
    .builder \
    .appName("HiveOperatorExample") \
    .config("hive.metastore.uris", "thrift://your_hive_metastore_host:9083") \
    .enableHiveSupport() \
    .getOrCreate()

# Загружаем данные в DataFrame
df = spark.read.json("/path/to/data.json")

# Применяем преобразование к данным
transformed_df = df.withColumn("new_column", col("old_column").cast(StringType()))

# Используем HiveOperator для записи данных обратно в Hive
HiveOperator.writeDataFrame(transformed_df, table_name="your_table_name", mode="overwrite")