from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *

spark = SparkSession.builder.appName("gold").getOrCreate()

# Leer parquet
libros = spark.read.parquet("silver/libros")
autores = spark.read.parquet("silver/autores")
idiomas = spark.read.parquet("silver/idiomas")

# ¿Cuántos libros se publican por año?
num_pub = libros.groupBy("año_publicacion").agg(count("titulo").alias("cantidad_libros"))
num_pub.write.mode("overwrite").parquet("gold/num_pub")
num_pub.show()

# ¿Qué autores son los más frecuentes?
autor_frecuente = autores.groupBy("autor").agg(count("clave").alias("libros por autor"))
autor_frecuente.write.mode("overwrite").parquet("gold/autor_frecuente")
autor_frecuente.show()

# ¿Cuáles son los libros más reeditados?
reeditados = libros.orderBy(col("numero_ediciones").desc()).limit(5)
reeditados.write.mode("overwrite").parquet("gold/reeditados")
reeditados.show()

# ¿Qué idiomas predominan?
predominan_idiomas = idiomas.groupBy("idioma").count().orderBy(col("count").desc()).limit(5)
predominan_idiomas.write.mode("overwrite").parquet("gold/predominan_idiomas")
predominan_idiomas.show()

# ¿Qué antigüedad tienen los libros publicados?
antiguedad = libros.withColumn("antiguedad",
    when(col("año_publicacion") != 0, year(current_date()) - col("año_publicacion")).otherwise(None))
antiguedad.write.mode("overwrite").parquet("gold/antiguedad")
antiguedad.show()

# ¿Qué idioma tiene más publicaciones?
idioma_publicaciones = idiomas.groupBy("idioma").count().orderBy(col("count").desc()).limit(1)
idioma_publicaciones.write.mode("overwrite").parquet("gold/idioma_publicaciones")
idioma_publicaciones.show()

# ¿Qué idioma tiene libros más antiguos por media?
relacion = libros.join(idiomas, libros["clave"] == idiomas["clave"], "inner")

media_idioma = (relacion.groupBy("idioma").agg(avg("año_publicacion").alias("media_año")).orderBy(asc("media_año")))
media_idioma.write.mode("overwrite").parquet("gold/media_idioma")
media_idioma.show()

# Final del codigo
spark.stop()