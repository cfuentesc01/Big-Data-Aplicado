# 0) (Requisito) PySpark necesita Java 17+ y JAVA_HOME configurado
#    Comprueba:
java -version
Write-Host ("JAVA_HOME=" + $env:JAVA_HOME)

# 1) Crear venv
py -m venv .venv

# 2) Activar venv
.\.venv\Scripts\Activate.ps1

# 3) Actualizar pip e instalar dependencias
python -m pip install --upgrade pip
python -m pip install pyspark beautifulsoup4 requests

# 4) Smoke test
python -c "import pyspark, bs4, requests, json; print('OK')"
python -c "from pyspark.sql import SparkSession; SparkSession.builder.master('local[*]').getOrCreate().range(1,5).show()"
