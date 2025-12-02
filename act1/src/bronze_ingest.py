from pyspark.sql import SparkSession
from pyspark.sql.functions import to_date, col

spark = SparkSession.builder.appName("bronze").getOrCreate()

# Rutas de los archivos a leer
clientes = spark.read.option("header", True).csv("data/clientes.csv")
ventas = spark.read.option("header", True).csv("data/ventas_mes.csv")
fact = spark.read.option("header", True).csv("data/facturas_meta.csv")
web = spark.read.option("multiline", "true").json("data/logs_web.json")

# Escribir los resultados solo si el DataFrame no está vacío
ventas.write.mode("overwrite").parquet("bronze/ventas")
clientes.write.mode("overwrite").parquet("bronze/clientes")
fact.write.mode("overwrite").parquet("bronze/facturas_meta")
web.write.mode("overwrite").parquet("bronze/web")
web.show()

spark.stop()