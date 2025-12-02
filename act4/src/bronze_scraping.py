#!/usr/bin/env python3
import requests
from bs4 import BeautifulSoup
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, DoubleType

spark = SparkSession.builder.appName("bronze_scraping").getOrCreate()

# Fase de scraping
urls = [
    "https://webscraper.io/test-sites/e-commerce/static/computers/tablets?page=1",
    "https://webscraper.io/test-sites/e-commerce/static/computers/tablets?page=2",
    "https://webscraper.io/test-sites/e-commerce/static/computers/tablets?page=3",
    "https://webscraper.io/test-sites/e-commerce/static/computers/tablets?page=4"
]

data = []

# Recorremos las páginas
for url in urls:
    response = requests.get(url)
    response.encoding = "utf-8"  
    soup = BeautifulSoup(response.content, "html.parser")
    html = response.text

    tablets = soup.select(".product-wrapper.card-body")

    # Recorremos los productos en cada página
    for t in tablets: 
        try:

            title = t.select_one("a.title").get_text(strip=True)

            price = t.select_one('[itemprop="price"]').get_text(strip=True)
            price = (
                price.encode("ascii", "ignore")
                     .decode()
                     .replace("$", "")
                     .strip()
            )

            rating = t.select_one("p[data-rating]")["data-rating"]

            data.append((str(title), float(price), int(rating)))

        except Exception as e:
            print("Error leyendo tablet:", e)

# Creando esquema para el parquet
schema = StructType([
    StructField("titulo", StringType(), True),
    StructField("precio", DoubleType(), True),
    StructField("valoración", StringType(), True)
])

# Crear el DataFrame de Spark
df_scraping = spark.createDataFrame(data, schema)

# Guardar datos en formato Parquet
df_scraping.write.mode("overwrite").parquet("bronze/tablets")
df_scraping.show()

spark.stop()
