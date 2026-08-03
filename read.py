from delta.tables import DeltaTable
from pyspark.sql import functions as F
from spark_session import get_spark


spark = get_spark("gold-test")
# Load an existing Delta table by path
delta_table = DeltaTable.forName(spark, "gld")
delta_table.history().show()
delta_table.toDF().show()
df = delta_table.toDF()
df.describe().show()
df = df.filter(F.year('Date_')==2025)
df.show(n=df.count())
#df = df.withColumn(F.col(F.year('Date_'))==2025)
input("Press Enter to stop Spark and exit...")

spark.stop()

# View version history — same time-travel data you'd see in Databricks
#delta_table.history().show()
#delta_table.toDF().show()

# Update rows 
# matching a condition
#delta_table.update(
#    condition="Gld_Close > 1150",
#    set={"Gld_Close": "Gld_Close * 1.0"}  # example — replace with real logic
#)

# Delete rows matching a condition
#delta_table.delete(condition="Gld_Close IS NULL")

# Merge / upsert — the classic Delta feature
#delta_table.alias("target").merge(
#    source=new_df.alias("source"),
#    condition="target.Date_ = source.Date_"
#).whenMatchedUpdateAll() \
# .whenNotMatchedInsertAll() \
# .execute()                                                                                                           