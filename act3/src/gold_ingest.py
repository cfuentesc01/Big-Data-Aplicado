from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *

spark = SparkSession.builder.appName("gold").getOrCreate()

# Leer parquet
libros = spark.read.parquet("bronze/books")

# Top 5
top5 = libros.orderBy(col("precio").desc()).limit(5)
top5.write.mode("overwrite").parquet("gold/top5_libros")

top5.show()

# Precio máximo
maximo = libros.agg(max("precio").alias("precio_maximo"))
maximo.write.mode("overwrite").parquet("gold/maximo_libros")

maximo.show()


# Precio minimo
minimo = libros.agg(min("precio").alias("precio_minimo"))
minimo.write.mode("overwrite").parquet("gold/minimo_libros")

minimo.show()

# Precio medio
media = libros.agg(avg("precio").alias("precio_medio"))
media.write.mode("overwrite").parquet("gold/precio_medio")

media.show()

# Final del codigo
spark.stop()