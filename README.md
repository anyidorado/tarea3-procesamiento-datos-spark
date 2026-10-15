# tarea3-procesamiento-datos-spark
Tarea 3: Procesamiento de datos con Apache Spark

Descripción del proyecto

Este proyecto tiene como objetivo aplicar herramientas de procesamiento de grandes volúmenes de datos mediante Apache Spark, utilizando Python para analizar un conjunto de datos históricos de la Tasa Representativa del Mercado (TRM) de Colombia.

La actividad incluye el procesamiento por lotes (batch) y el procesamiento de datos en tiempo real mediante la integración de Apache Kafka y Spark Structured Streaming.

Problema seleccionado

Se busca explorar y analizar los valores históricos de la TRM para identificar registros, consultar estadísticas descriptivas y ordenar los valores de mayor a menor, utilizando herramientas de procesamiento distribuido.

Conjunto de datos

Se utiliza un archivo CSV denominado query.csv, obtenido de una fuente de datos abiertos de Colombia y almacenado en HDFS para su procesamiento con Apache Spark.

Tecnologías utilizadas

* Apache Spark.
* Python y PySpark.
* Hadoop Distributed File System (HDFS).
* Apache Kafka.
* Spark Structured Streaming.
* GitHub para el almacenamiento y la documentación del código fuente.

Procesamiento por lotes

El archivo tarea3.py realiza las siguientes operaciones:

1. Carga el conjunto de datos desde HDFS.
2. Muestra el esquema y los primeros registros.
3. Calcula estadísticas descriptivas.
4. Filtra los registros cuyo valor de la TRM es superior a 5000.
5. Ordena los registros de mayor a menor valor.

Ejecución

Para ejecutar el procesamiento por lotes en un entorno que tenga Apache Spark configurado y acceso al archivo en HDFS, se utiliza el siguiente comando:

spark-submit tarea3.py

La ruta configurada en el código es hdfs://localhost:9000/Tarea3/query.csv, por lo que el archivo debe estar disponible en esa ubicación.

Procesamiento en tiempo real

Como parte de la actividad se configura el tema sensor_data en Apache Kafka. Un productor genera datos simulados de sensores y Spark Structured Streaming consume los registros para calcular promedios de temperatura y humedad.

Resultados esperados

El procesamiento por lotes permite consultar estadísticas y registros históricos de la TRM. El procesamiento en tiempo real permite observar la recepción y el análisis continuo de datos simulados de sensores.
