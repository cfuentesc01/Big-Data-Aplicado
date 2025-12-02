# Importar librerias
from pyspark.sql import SparkSession
from pyspark.sql.functions import sum, col, to_date, when
from pyspark.sql.types import IntegerType, DecimalType, DateType 

spark = SparkSession.builder.appName("gold").getOrCreate()

# Rutas de los archivos a leer
ventas = spark.read.csv("data/ventas_mes.csv", header=True, inferSchema=True)
facturas = spark.read.csv("data/facturas_meta.csv", header=True, inferSchema=True)

# Formatear fecha
#ventas = ventas.withColumn('fecha', 
#    when(col('fecha').rlike('-'), to_date('fecha', 'yyyy-MM-dd'))
#    .otherwise(to_date('fecha', 'dd/MM/yyyy'))
#)

# Ventas por día
ventas_por_dia = ventas.groupBy("fecha").agg(sum("importe").alias("ventas_totales"), sum("unidades").alias("unidades_totales")).orderBy("fecha")
ventas_por_dia.write.mode("overwrite").parquet("gold/ventas_por_dia")

# Top 5 productos
top5 = ventas.groupBy("id_producto").agg(sum("unidades").alias("unidades_totales"), sum("importe").alias("importe_total")).orderBy(col("unidades_totales").desc()).limit(5)
top5.write.mode("overwrite").parquet("gold/top5_productos")

# Relación ventas con facturas
ventas_agrupadas = ventas.groupBy('fecha', 'id_cliente').agg(sum('importe').alias('ventas_totales'),sum('unidades').alias('unidades_totales'))

relacion = ventas_agrupadas.join(facturas, on=['fecha', 'id_cliente'], how='inner')
relacion.write.mode("overwrite").parquet("gold/ventas_facturas")

# Final del código
print ("Gold Listo")

# Mostrar tablas en la terminal
df_ventas = spark.read.parquet ("gold/ventas_por_dia")
df_productos = spark.read.parquet ("gold/top5_productos")
df_relacion = spark.read.parquet ("gold/ventas_facturas")

df_ventas.show (10, truncate = False)
df_productos.show (10, truncate = False)
df_relacion.show (10, truncate = False)

print ("Filas ventas_por_dia:", df_ventas.count())
print ("Filas top5_productos:", df_productos.count())
print ("Filas ventas_facturas:", df_relacion.count())

spark.stop()
