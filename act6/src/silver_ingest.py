from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *

spark = SparkSession.builder.appName("silver").getOrCreate()

# Leer archivos
libros = spark.read.parquet("bronze/docs")

# Normalizar fechas usando coalesce para intentar ambos formatos
df_autores = (
    libros.select(
        col("clave"),
        explode(col("autores")).alias("autor")
    )
    .withColumn("autor", upper(trim(col("autor"))))
)

df_idiomas = (
    libros.select(
        col("clave"),
        explode(col("idiomas")).alias("idioma")
    )
    .withColumn("idioma", upper(trim(col("idioma"))))
)

libros = libros.drop (col("idiomas"), col("autores"))

# Guardar los resultados
libros.write.mode("overwrite").parquet("silver/libros")
df_autores.write.mode("overwrite").parquet("silver/autores")
df_idiomas.write.mode("overwrite").parquet("silver/idiomas")

# Mostrar
df_autores.show()
df_idiomas.show()
libros.show()

spark.stop()