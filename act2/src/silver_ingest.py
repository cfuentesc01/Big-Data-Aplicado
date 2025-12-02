from pyspark.sql import SparkSession
from pyspark.sql.functions import to_date, col, coalesce
from pyspark.sql.types import *

spark = SparkSession.builder.appName("silver").getOrCreate()

# Leer archivos
pedidos = spark.read.parquet("bronze/pedidos")
tracking = spark.read.parquet("bronze/tracking")

# Normalizar fechas usando coalesce para intentar ambos formatos
pedidos = pedidos.withColumn("id_pedido", col("id_pedido").cast(IntegerType()))
pedidos = pedidos.withColumn("id_cliente", col("id_cliente").cast(IntegerType()))
pedidos = pedidos.withColumn("fecha_pedido", coalesce(
    to_date(col("fecha_pedido"), "yyyy-MM-dd"),
    to_date(col("fecha_pedido"), "dd/MM/yyyy")
))

pedidos = pedidos.withColumn("fecha_prometida", coalesce(
    to_date(col("fecha_prometida"), "yyyy-MM-dd"),
    to_date(col("fecha_prometida"), "dd/MM/yyyy")
))

tracking = tracking.withColumn("id_pedido", col("id_pedido").cast(IntegerType()))
tracking = tracking.withColumn("ts", col("ts").cast(DateType()))

# Relacionar los campos de los distintos parquets
ids = tracking.select(col("id_pedido").cast(IntegerType()))
pedidos_ok = (pedidos.join(ids, "id_pedido", "inner"))

# Guardar los resultados
pedidos.write.mode("overwrite").parquet("silver/pedidos")
pedidos.show()
tracking.write.mode("overwrite").parquet("silver/tracking")
tracking.show()

spark.stop()
