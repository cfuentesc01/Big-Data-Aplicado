from pyspark.sql import SparkSession
from pyspark.sql.functions import to_date, col

spark = SparkSession.builder.appName("bronze").getOrCreate()

# Rutas de los archivos a leer
pedidos = spark.read.option("header", True).option("sep", ";").csv("data/pedidos.csv")
tracking = spark.read.option("multiline", "true").json("data/tracking.json")

# Escribir los resultados solo si el DataFrame no está vacío
pedidos.write.mode("overwrite").parquet("bronze/pedidos")
pedidos.show()
tracking.write.mode("overwrite").parquet("bronze/tracking")
tracking.show()

spark.stop()