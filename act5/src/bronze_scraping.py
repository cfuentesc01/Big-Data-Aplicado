#!/usr/bin/env python3
import requests
from bs4 import BeautifulSoup
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, DoubleType
import json

spark = SparkSession.builder.appName("bronze_scraping").getOrCreate()

# Fase de scraping de la API
url = "https://dummyjson.com/products"

response = requests.get(url).json()
products = response ["products"]

data = []

# 1. Recorremos la API
for p in products:
    data.append((
        p.get("title"),
        p.get("category"),
        p.get("price"),
        p.get("rating"),
        p.get("brand"),
    ))

# 2. Esquema y dataframe

schema = StructType([
    StructField("titulo", StringType(), True),
    StructField("categoria", StringType(), True),
    StructField("precio", DoubleType(), True),
    StructField("valoracion", DoubleType(), True),
    StructField("marca", StringType(), True)
])

df_scraping = spark.createDataFrame(data, schema)
df_scraping.write.mode("overwrite").parquet("bronze/products")
df_scraping.show(10)

spark.stop()

