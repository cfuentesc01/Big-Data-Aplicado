from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *

spark = SparkSession.builder.appName("gold").getOrCreate()

# Leer parquet
tablets = spark.read.parquet("bronze/tablets")

# Precio medio de todas las tablets
precio_medio = tablets.agg(mean("precio").alias("Precio medio"))
precio_medio.write.mode("overwrite").parquet("gold/precio_medio")

precio_medio.show()

# Precio minimo
precio_minimo = tablets.agg(min("precio").alias("Precio mínimo"))
precio_minimo.write.mode("overwrite").parquet("gold/precio_minimo")

precio_minimo.show()

# Precio máximo
precio_maximo = tablets.agg(max("precio").alias("Precio máximo"))
precio_maximo.write.mode("overwrite").parquet("gold/precio_maximo")

precio_maximo.show()

# Clasificaciones de tablets rango por rango (barato, medio, caro)
clasificacion = tablets.withColumn("gama",
                                when(col('precio') <= 60, 'Gama Baja').
                                when((col('precio') > 60) & (col('precio') <= 120), 'Gama Media').otherwise('Gama Alta'))
clasificacion.show()

# Mejores tablets calidad precio, (precio / valoracion)
mejores = tablets.withColumn("mejores",
    when(col("valoración") != 0, col("precio") / col("valoración")).otherwise(None)
)
mejores.orderBy("mejores").show(3)

# Final del codigo
spark.stop()