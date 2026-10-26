Análisis de accidentes de tránsito en Bucaramanga con Apache Spark
Descripción del proyecto

Este proyecto se desarrolló como parte de una actividad académica de procesamiento de grandes volúmenes de datos. Su propósito es analizar información histórica sobre accidentes de tránsito ocurridos en el municipio de Bucaramanga mediante Apache Spark y Python.

Objetivo
Implementar un proceso de análisis batch utilizando PySpark para cargar, transformar y analizar registros de accidentes de tránsito, identificando su distribución según la gravedad.

Herramientas utilizadas
Ubuntu
Python
Apache Spark
PySpark

Archivo CSV de accidentes de tránsito

Desarrollo de la solución
Inicialmente, se cargó el conjunto de datos en formato CSV utilizando Apache Spark. Posteriormente, se realizaron operaciones de procesamiento y análisis mediante DataFrames para obtener estadísticas descriptivas y agrupar los accidentes según su gravedad.

Resultados obtenidos
Se procesaron 39.193 registros de accidentes de tránsito. Entre los resultados se identificaron:
Solo daños: 19.602 registros.
Con heridos: 18.982 registros.
Con muertos: 503 registros.
Con Muertos: 106 registros.
Las dos últimas categorías presentan diferencias de escritura en el conjunto de datos original.

Instrucciones de ejecución
1. Instalar y configurar Python y Apache Spark.
2. Descargar el conjunto de datos y guardarlo con el nombre accidentes.csv en el directorio personal de Ubuntu.
3. Guardar el archivo analisis_accidentes.py en el mismo directorio.
4. Ejecutar el siguiente comando en Ubuntu:
/opt/spark/bin/spark-submit ~/analisis_accidentes.py
5. Consultar los resultados generados:
head -n 10 ~/resultados_gravedad/*.csv

Conclusión
El desarrollo de esta actividad permitió comprender el funcionamiento del procesamiento batch mediante Apache Spark y su aplicación al análisis de datos históricos. Los resultados obtenidos permiten identificar la distribución de los accidentes de tránsito según su gravedad y evidencian la utilidad de estas herramientas para el procesamiento de información.
