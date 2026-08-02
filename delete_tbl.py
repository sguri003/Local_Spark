from delta.tables import DeltaTable
from pyspark.sql import functions as F
from spark_session import get_spark
import pyspark
import shutil

spark = get_spark("del")
# Load an existing Delta table by path
shutil.rmtree('spark-warehouse/stocks_2', ignore_errors=False)
#shutil.rmtree('spark-warehouse/silver', ignore_errors=False)
spark.sql("show tables")
spark.stop()