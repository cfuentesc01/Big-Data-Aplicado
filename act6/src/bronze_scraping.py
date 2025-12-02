#!/usr/bin/env python3
import requests
from bs4 import BeautifulSoup
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, DoubleType, ArrayType, IntegerType
import json

spark = SparkSession.builder.appName("bronze_scraping").getOrCreate()

# Fase de scraping de la API
url = "https://openlibrary.org/search.json?q=python"

response = requests.get(url).json()
docs = response ["docs"]

data = []

# 1. Recorremos la API
for d in docs:
    data.append((
        d.get("author_name"),
        d.get("title"),
        d.get("language"),
        d.get("first_publish_year"),
        d.get("edition_count"),
        d.get("key"),
    ))

# 2. Esquema y dataframe

schema = StructType([
    StructField("autores", ArrayType(StringType()), True),
    StructField("titulo", StringType(), True),
    StructField("idiomas", ArrayType(StringType()), True),
    StructField("año_publicacion", IntegerType(), True),
    StructField("numero_ediciones", IntegerType(), True),
    StructField("clave", StringType(), True)
])

df_scraping = spark.createDataFrame(data, schema)
df_scraping.write.mode("overwrite").parquet("bronze/docs")
df_scraping.show(10)

spark.stop()

