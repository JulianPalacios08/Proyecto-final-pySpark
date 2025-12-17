🌎 Análisis Climático con GHCN usando Python y PySpark

Este repositorio contiene una serie de ejercicios de análisis climático basados en datos del Global Historical Climatology Network (GHCN), utilizando Python, PySpark y visualización geoespacial mediante mapas.

El objetivo principal es procesar grandes volúmenes de datos climáticos, extraer métricas relevantes (especialmente relacionadas con la precipitación) y representarlas visualmente sobre mapas, sin necesidad de entorno gráfico (GUI).

📂 Estructura del Repositorio
ghcn/
├── python/
│   ├── ejercicio_1_*.py
│   ├── ejercicio_2_*.py
│   └── ejercicio_3_*.py
│
├── spark/
│   ├── ejercicio_2_spark.py
│   └── ejercicio_3_spark.py
│
├── raw/
│   ├── BR002044037.csv
│   ├── VE000002415.csv
│   └── ...
│
├── maps/
│   └── south_america_map.*
│
├── output/
│   ├── ejercicio_2_precipitacion.png
│   └── ejercicio_3_tendencia_precipitacion.png
│
├── README.md
└── requirements.txt

📊 Dataset (GHCN)

Los archivos CSV corresponden a estaciones meteorológicas individuales del GHCN y contienen los siguientes encabezados:

"STATION","DATE","LATITUDE","LONGITUDE","ELEVATION","NAME","PRCP","PRCP_ATTRIBUTES"

📌 Consideraciones importantes

No se dispone de TMAX / TMIN, por lo tanto:

No se realizan análisis de temperatura máxima.

Todos los análisis avanzados se enfocan en precipitación (PRCP).

La precipitación se expresa en milímetros (mm).

Cada CSV puede contener décadas de datos diarios, lo que justifica el uso de Spark.

🧠 Ejercicios Implementados
🔹 Ejercicio 1 – Exploración básica (Python)

Lectura de archivos CSV individuales.

Limpieza de valores nulos.

Análisis descriptivo inicial.

Verificación de columnas y tipos de datos.

🔹 Ejercicio 2 – Análisis de precipitación por estación

Tecnologías: Python / PySpark
Resultado: Mapa de estaciones con precipitación acumulada

Descripción:

Se calcula la precipitación total o promedio por estación.

Se agrupan los datos por estación y ubicación geográfica.

Se genera un mapa con puntos georreferenciados, donde:

Cada punto representa una estación.

El tamaño o color indica la magnitud de la precipitación.

📁 Salida:

output/ejercicio_2_precipitacion.png

🔹 Ejercicio 3 – Tendencia temporal de precipitación

Tecnologías: PySpark + Python
Resultado: Mapa de tendencia de precipitación

Descripción:

Se agrega la precipitación por año para cada estación.

Se calcula la tendencia temporal (pendiente):

Tendencia positiva → aumento de precipitación.

Tendencia negativa → disminución.

Se representa la tendencia sobre un mapa:

Colores o marcadores reflejan la evolución climática.

📁 Salida:

output/ejercicio_3_tendencia_precipitacion.png

🗺️ Mapas y Visualización

Los mapas base se encuentran en la carpeta maps/.

No se requiere conexión a Internet.

No se utiliza GUI (matplotlib en modo headless).

Todas las imágenes se guardan automáticamente en output/.

Ejemplo de configuración sin GUI:

import matplotlib
matplotlib.use("Agg")

⚙️ Requisitos
🐍 Python

Python 3.9 o superior

Entorno virtual recomendado

📦 Dependencias principales
pandas
matplotlib
geopandas
shapely
pyspark
numpy


Instalación:

pip install -r requirements.txt

🚀 Ejecución de los Scripts
▶️ Scripts en Python
python3 python/ejercicio_3_mapa.py

▶️ Scripts con PySpark
spark-submit spark/ejercicio_3_spark.py


📌 Nota:
Para grandes volúmenes de datos se recomienda ejecutar Spark en modo local con memoria suficiente, por ejemplo:

spark-submit --driver-memory 4g spark/ejercicio_3_spark.py

🧪 Entorno sin GUI

Este repositorio está diseñado para ejecutarse en:

Servidores

Máquinas virtuales

WSL

Clústeres Spark

Por ello:

No se usa plt.show()

Todas las salidas gráficas se escriben en disco

🧹 Limpieza y Mantenimiento

Eliminar carpetas innecesarias:

rm -rf nombre_carpeta


Verificar antes de borrar:

ls nombre_carpeta

📌 Conclusiones

Este repositorio demuestra cómo:

Procesar datos climáticos reales y masivos

Aplicar Spark para análisis distribuido

Realizar análisis climático válido sin datos de temperatura

Generar mapas climáticos reproducibles sin entorno gráfico

Es una base sólida para:

Estudios climáticos regionales

Proyectos académicos

Extensión hacia análisis predictivos o ML
