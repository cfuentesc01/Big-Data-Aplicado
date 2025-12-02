#!/usr/bin/env python3
import requests
from bs4 import BeautifulSoup
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, DoubleType

spark = SparkSession.builder.appName("bronze-scraping").getOrCreate()

# =======================================================
# 1) SCRAPING (BooksToScrape)
# =======================================================

URL_1 = "https://books.toscrape.com/catalogue/page-1.html"

response = requests.get(URL_1)
response.encoding = "utf-8"    
html = response.text

soup = BeautifulSoup(html, "html.parser")
books = soup.select("article.product_pod")

data = []
for b in books:
    try:
        title = b.h3.a["title"]

        price_raw = b.select_one(".price_color").get_text()

        # limpiar caracteres extraños (Â£ etc.)
        price = (
            price_raw
            .encode("ascii", "ignore")   # quita símbolos no ASCII
            .decode()
            .replace("£", "")
            .strip()
        )

        stock_raw = b.select_one(".instock.availability").get_text()

        stock = (
            stock_raw           
            .encode("ascii", "ignore")   # quita símbolos no ASCII
            .decode()
            .replace("", "")
            .strip()
        )

        data.append((title, float(price), str(stock)))

    except Exception as e:
        print("Error leyendo libro:", e)

# =======================================================
# 2) SCHEMA + DATAFRAME
# =======================================================

schema = StructType([
    StructField("titulo", StringType(), True),
    StructField("precio", DoubleType(), True),
    StructField("stock", StringType(), True)
])

df_scraping = spark.createDataFrame(data, schema)

# =======================================================
# 3) GUARDAR BRONZE
# =======================================================

df_scraping.write.mode("overwrite").parquet("bronze/books")

df_scraping.show(10)
print("BRONZE scraping listo.")

spark.stop()

