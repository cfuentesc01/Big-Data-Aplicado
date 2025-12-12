#!/usr/bin/env bash
set -e

# 0) (Requisito) PySpark necesita Java 17+ y JAVA_HOME configurado
#    Comprueba:
java -version || true
echo "JAVA_HOME=$JAVA_HOME"

# 1) Crear venv
python3 -m venv .venv

# 2) Activar venv
source .venv/bin/activate

# 3) Actualizar pip e instalar dependencias
python -m pip install --upgrade pip
python -m pip install pyspark beautifulsoup4 requests

# 4) Smoke test
python -c "import pyspark, bs4, requests, json; print('OK')"
python -c "from pyspark.sql import SparkSession; SparkSession.builder.master('local[*]').getOrCreate().range(1,5).show()"
