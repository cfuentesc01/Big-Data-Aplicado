from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *

spark = SparkSession.builder.appName("gold").getOrCreate()

# Leer archivos
pedidos = spark.read.parquet("silver/pedidos")
tracking = spark.read.parquet("silver/tracking")

tracking_ordenada = tracking.withColumn(
    "fecha_entrega",when(col("evento") == "delivered", to_date(col("ts"))).otherwise(None)
)
tracking_ordenada.show()

tracking_sinnull = tracking_ordenada.filter(col("fecha_entrega").isNotNull())
tracking_sinnull.show()

# Relación
relacion = pedidos.join(tracking_sinnull, "id_pedido")
relacion = relacion.filter(col("fecha_pedido").isNotNull())
relacion.show()

# On-time rate
on_time = relacion.withColumn(
    "on_time",
    when(col("fecha_entrega") <= col("fecha_prometida"), 1).otherwise(0)
)

on_time_result = on_time.groupBy().agg(
    (sum("on_time") / count("*")).alias("on_time_rate")
)

on_time_result.write.mode("overwrite").parquet("gold/on_time")


# Retraso medio (en días)
retraso = relacion.withColumn(
    "dias_retraso",
    datediff(col("fecha_entrega"), col("fecha_prometida"))
)

retraso_result = retraso.groupBy().agg(
    avg("dias_retraso").alias("dias_retraso_avg")
)

retraso_result.write.mode("overwrite").parquet("gold/retraso")


# Tiempo puerta a puerta (order -> delivered)
tiempo = relacion.select("id_pedido", "fecha_pedido", "fecha_entrega").withColumn("dias",
    datediff(col("fecha_entrega"), col("fecha_pedido"))
)

tiempo.write.mode("overwrite").parquet("gold/tiempo_puerta")

# Comparativa transportista por provincia
transportista = relacion.groupBy("transportista", "provincia").agg(
    count("*").alias("pedidos_entregados"),
    avg(datediff(col("fecha_entrega"), col("fecha_pedido"))).alias("tiempo_medio_dias")
)

transportista.write.mode("overwrite").parquet("gold/transportista")

# Guardar los resultados
on_time.show()
retraso.show()
tiempo.show()
transportista.show()

spark.stop()
