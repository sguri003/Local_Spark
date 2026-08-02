from spark_session import get_spark

spark = get_spark("my-job")
df = spark.read.csv("Silver_Price.csv", header=True, inferSchema=True)
df.show()
df.write.format("delta").mode("overwrite").saveAsTable("gld")
input("Press Enter to stop Spark and exit...")

spark.stop()
