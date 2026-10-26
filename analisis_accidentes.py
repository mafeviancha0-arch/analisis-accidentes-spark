from pyspark.sql import SparkSession
from pyspark.sql import functions as F

spark = (SparkSession.builder
         .appName('AnalisisAccidentesBucaramanga')
         .master('local[1]')
         .config('spark.driver.memory', '1g')
         .getOrCreate())
spark.sparkContext.setLogLevel('ERROR')

ruta = '/home/vboxuser/accidentes.csv'
df = spark.read.option('header', True).option('inferSchema', True).option('multiLine', False).csv(ruta)
print('\n=== ESTRUCTURA DEL CONJUNTO DE DATOS ===')
df.printSchema()
print('\n=== REGISTROS ORIGINALES ===')
print('Total:', df.count())
df.show(5, truncate=35)
print('\n=== NULOS POR COLUMNA ===')
df.select([F.sum(F.col('`' + c.replace('`', '``') + '`').isNull().cast('int')).alias(c) for c in df.columns]).show(truncate=False)
print('\n=== LIMPIEZA ===')
df_limpio = df.dropDuplicates()
print('Registros tras eliminar duplicados:', df_limpio.count())
print('Duplicados eliminados:', df.count() - df_limpio.count())

# Estadisticas segun columnas disponibles en el archivo.
columnas = {c.strip().upper(): c for c in df_limpio.columns}
for nombre in ('GRAVEDAD', 'AÑO', 'MES', 'DIA', 'DÍA', 'BARRIO'):
    if nombre in columnas:
        real = columnas[nombre]
        print('\n=== ACCIDENTES POR ' + nombre + ' ===')
        (df_limpio.groupBy(F.col('`' + real.replace('`', '``') + '`'))
         .count().orderBy(F.desc('count')).show(15, truncate=False))

# Estadisticas generales de columnas numericas
numericas = [c for c, tipo in df_limpio.dtypes if tipo in ('int', 'bigint', 'double', 'float')]
if numericas:
    print('\n=== ESTADISTICAS DESCRIPTIVAS ===')
    df_limpio.select(*numericas[:12]).describe().show(truncate=False)

# Exportar tabla para graficar o presentar como evidencia.
if 'GRAVEDAD' in columnas:
    col_gravedad = columnas['GRAVEDAD']
    (df_limpio.groupBy(F.col('`' + col_gravedad.replace('`', '``') + '`'))
     .count().orderBy(F.desc('count'))
     .coalesce(1).write.mode('overwrite').option('header', True)
     .csv('/home/vboxuser/resultados_gravedad'))
    print('\nTabla de gravedad guardada en /home/vboxuser/resultados_gravedad')

print('\n=== ANALISIS BATCH FINALIZADO ===')
spark.stop()
