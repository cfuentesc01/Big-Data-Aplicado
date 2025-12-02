from pyspark.sql import SparkSession
from pyspark.sql.functions import to_date, col, regexp_replace, coalesce
from pyspark.sql.types import IntegerType, DecimalType, DateType

spark = SparkSession.builder.appName("bronze").getOrCreate()

# Leer archivos
ventas = spark.read.option("header", True).csv("data/ventas_mes.csv")
clientes = spark.read.parquet("bronze/clientes")
facturas = spark.read.parquet("bronze/facturas_meta")

# Normalizar fechas usando coalesce para intentar ambos formatos
ventas = ventas.withColumn("id_venta", col("id_venta").cast(IntegerType()))
ventas = ventas.withColumn("fecha", coalesce(
    to_date(col("fecha"), "yyyy-MM-dd"),
    to_date(col("fecha"), "dd/MM/yyyy")
))
ventas = ventas.withColumn("id_cliente", col("id_cliente").cast(IntegerType()))
ventas = ventas.withColumn("unidades", col("unidades").cast(IntegerType()))
ventas = ventas.withColumn("importe", regexp_replace(col("importe"), ",", ".").cast(DecimalType(12, 2)))

# Transformar clientes y facturas
clientes = clientes.withColumn("id_cliente", col("id_cliente").cast(IntegerType()))
clientes = clientes.withColumn("fecha_alta", col("fecha_alta").cast(DateType()))
facturas = facturas.withColumn("fecha", col("fecha").cast(DateType()))
facturas = facturas.withColumn("id_cliente", col("id_cliente").cast(IntegerType()))
facturas = facturas.withColumn("importe_total", col("importe_total").cast(DecimalType(12, 2)))

# Guardar los resultados
ventas.write.mode("overwrite").parquet("silver/ventas")
clientes.write.mode("overwrite").parquet("silver/clientes")
facturas.write.mode("overwrite").parquet("silver/facturas")

# Importe igual a cero
ids = clientes.select(col("id_cliente").cast(IntegerType()).alias("id_cliente"))
ventas_ok = (ventas.join(ids, "id_cliente", "inner")
             .where((col("unidades") > 0) & (col("importe") >= 0)))
ventas_ok.write.mode("overwrite").parquet("silver/ventas")

# Mostrar tablas y resultado
df_ventas = spark.read.parquet("silver/ventas")
df_clientes = spark.read.parquet("silver/clientes")
df_facturas = spark.read.parquet("silver/facturas")

df_ventas.show(10, truncate=False)
df_clientes.show(10, truncate=False)
df_facturas.show(10, truncate=False)

print("Filas ventas:", df_ventas.count())
print("Filas clientes:", df_clientes.count())
print("Filas facturas:", df_facturas.count())

spark.stop()
