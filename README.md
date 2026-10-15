Tarea 3: Procesamiento de datos con Apache Spark

1. Descripción del proyecto

Este proyecto desarrolla procesos de análisis de datos por lotes (batch processing) y procesamiento de datos en tiempo real (stream processing) mediante herramientas del ecosistema Big Data. Se utiliza Apache Spark para analizar datos históricos de la Tasa Representativa del Mercado (TRM) de Colombia y Apache Kafka junto con Spark Streaming para procesar datos simulados generados por sensores.

2. Objetivos

* Cargar, limpiar y transformar datos históricos de la TRM.
* Realizar análisis exploratorio utilizando Apache Spark.
* Identificar valores mínimos, máximos, promedios y registros por año.
* Implementar un flujo de datos en tiempo real con Apache Kafka y Spark Streaming.
* Observar y analizar los resultados del procesamiento mediante la consola y la interfaz web de Spark.

3. Tecnologías utilizadas

* Apache Hadoop y HDFS: almacenamiento distribuido del archivo histórico.
* Apache Spark: procesamiento y análisis de datos por lotes.
* Apache Kafka: transmisión de mensajes de sensores simulados.
* Spark Structured Streaming: procesamiento continuo de los mensajes recibidos.
* Python: desarrollo de los scripts de procesamiento.
* Ubuntu Server: entorno de ejecución del proyecto.

4. Conjunto de datos para procesamiento batch

Se utiliza un archivo CSV con registros históricos de la Tasa Representativa del Mercado (TRM) de Colombia, obtenido de una fuente de datos abiertos y almacenado en HDFS en la ruta:

hdfs://localhost:9000/Tarea3/query.csv

El archivo contiene columnas como valor, unidad, vigenciadesde y vigenciahasta.

5. Procesamiento por lotes con Spark

El script tarea3.py realiza las siguientes operaciones:

1. Carga el archivo CSV desde HDFS.
2. Inspecciona el esquema y cuenta los registros.
3. Normaliza los nombres de las columnas.
4. Elimina filas duplicadas y descarta registros con valores TRM nulos o no positivos.
5. Convierte el valor de la TRM a tipo numérico y las fechas a tipo fecha.
6. Calcula estadísticas descriptivas y valores promedio, mínimo y máximo.
7. Identifica registros con TRM superior a 5.000.
8. Ordena los registros de mayor a menor.
9. Calcula el promedio y la cantidad de registros por año.

Resultados obtenidos

En la ejecución de prueba se procesaron inicialmente 8.343 registros. Después de aplicar la limpieza, se conservaron los 8.343 registros y no se detectaron valores nulos en las columnas examinadas.

* Promedio de la TRM: aproximadamente 2.370,20.
* Valor mínimo: 620,62.
* Valor máximo: 5.061,21.
* Registros con TRM superior a 5.000: 5.

Estos resultados corresponden a la versión del conjunto de datos utilizada durante la ejecución.

6. Procesamiento en tiempo real con Kafka y Spark Streaming

Para la segunda parte se implementa un productor en Python que genera datos simulados de sensores, con identificador, temperatura, humedad y marca de tiempo. Los mensajes se publican en el tópico sensor_data de Apache Kafka.

El consumidor desarrollado con Spark Structured Streaming recibe los mensajes, interpreta su estructura JSON, convierte la marca de tiempo y calcula promedios de temperatura y humedad agrupados por sensor y ventana de un minuto.

Los datos de sensores son simulados con fines académicos; no corresponden a mediciones de dispositivos físicos.

7. Archivos del proyecto

* tarea3.py: script de procesamiento batch de los datos históricos de TRM.
* kafka_producer.py: productor de mensajes simulados para Kafka.
* spark_streaming_consumer.py: consumidor y análisis de datos en tiempo real.
* README.md: documentación general del proyecto.

8. Ejecución

Procesamiento batch

Con Hadoop y HDFS iniciados y el archivo disponible en la ruta indicada, ejecutar:

spark-submit ~/tarea3.py

Procesamiento en tiempo real

Primero deben iniciarse los servicios de Kafka y crearse el tópico sensor_data, según la configuración del entorno.

Ejecutar el productor:

python3 kafka_producer.py

En otra terminal, ejecutar el consumidor con el conector Kafka compatible con Spark 3.5.1:

spark-submit --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.1 spark_streaming_consumer.py

El consumidor mostrará en la consola los resultados de las ventanas de procesamiento.

9. Conclusión

La actividad permitió aplicar herramientas del ecosistema Big Data para dos escenarios complementarios: el análisis de información histórica mediante procesamiento por lotes y el procesamiento continuo de datos simulados mediante Kafka y Spark Structured Streaming. Los resultados evidencian cómo estas tecnologías permiten transformar datos, calcular estadísticas y observar información durante su procesamiento.
