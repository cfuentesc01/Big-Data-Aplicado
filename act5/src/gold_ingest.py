from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *

spark = SparkSession.builder.appName("gold").getOrCreate()

# Leer parquet
productos = spark.read.parquet("bronze/products")

# 5 marcas mas frecuentes
frecuentes = productos.groupBy("marca").count().orderBy(col("count").desc()).limit(5)
frecuentes.write.mode("overwrite").parquet("gold/frecuentes")
frecuentes.show()

# Productos mas caros
caros = productos.orderBy(desc("precio"))
caros.write.mode("overwrite").parquet("gold/caros")
caros.show(5)

# Relación entre rating y precio
relacion = productos.withColumn("mejores",
    when(col("valoracion") != 0, col("precio") / col("valoracion")).otherwise(None)
)
relacion.write.mode("overwrite").parquet("gold/relacion")
relacion.show()

# Categoria con productos mejor valorados
mejor_valorados = productos.groupBy("categoria").agg(avg("valoracion").alias("media_valoracion"))
mejor_valorados = mejor_valorados.orderBy(desc("media_valoracion"))
mejor_valorados.write.mode("overwrite").parquet("gold/mejor_valorados")
mejor_valorados.show()

# Productos sobrevalorados (precio alto / rating bajo)
sobrevalorados = productos.withColumn("indice_valor",
    when(col("valoracion") != 0, col("valoracion") / col("precio")).otherwise(None)).orderBy(asc("indice_valor"))

sobrevalorados.write.mode("overwrite").parquet("gold/sobrevalorados")
sobrevalorados.show(5)

# Final del codigo
spark.stop()