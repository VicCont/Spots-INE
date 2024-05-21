# Databricks notebook source
from pyspark.sql.types import StructType, StructField, StringType, BooleanType, TimestampType, FloatType, IntegerType, DoubleType, ArrayType, LongType
from pyspark.sql.functions import year, month, dayofmonth, dayofweek, hour, to_date, col, quarter, explode, sequence, expr,current_timestamp,lit,unix_timestamp,concat, udf, hour, month, count
from pyspark.ml.feature import RegexTokenizer, StopWordsRemover, CountVectorizer, IDF, StringIndexer
from pyspark.ml.evaluation import MulticlassClassificationEvaluator
from pyspark.mllib.evaluation import MulticlassMetrics
from nltk.sentiment import SentimentIntensityAnalyzer
from pyspark.ml.classification import NaiveBayes
from dateutil import parser,relativedelta
from pyspark.ml.pipeline import Pipeline
from pyspark.sql import SparkSession
from delta.tables import DeltaTable
import pyspark.sql.functions as f
#from wordcloud import WordCloud
import matplotlib.pyplot as plt
from datetime import datetime
import nltk
import time
import os

# COMMAND ----------

json_schema = StructType([
    StructField("titulo", StringType(), True),
    StructField("cautelar", BooleanType(), True),
    StructField("folio", StringType(), True),
    StructField("ruta", BooleanType(), True),
    StructField("es_publico", BooleanType(), True),
    StructField("archivoTraduccion", StringType(), True),
    StructField("directorio", StringType(), True),
    StructField("partido", StringType(), True),
    StructField("nombre", StringType(), True),
    StructField("transcripcion", StringType(), True)
])
spark = (
    SparkSession.builder.appName("MyApp")
    .config("spark.jars.packages", "io.delta:delta-core_2.12:0.8.0")
    .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
    .config(
        "spark.sql.catalog.spark_catalog",
        "org.apache.spark.sql.delta.catalog.DeltaCatalog",
    )
    .getOrCreate()
)
# COMMAND ----------

json_path = "data.json"
df_spots = spark.read.json(json_path, schema=json_schema)

# COMMAND ----------

df_spots = df_spots.select("titulo", "partido", "transcripcion")

# COMMAND ----------

# Definir el esquema del archivo CSV de transmisiones
csv_schema = StructType([
    StructField("No", StringType(), True),
    StructField("ESTADO", StringType(), True),
    StructField("CEVEM", StringType(), True),
    StructField("MEDIO", StringType(), True),
    StructField("SIGLAS", StringType(), True),
    StructField("EMISORA", StringType(), True),
    StructField("MATERIAL", StringType(), True),
    StructField("VERSION", StringType(), True),
    StructField("TIPO_ACTOR", StringType(), True),
    StructField("ACTOR", StringType(), True),
    StructField("FECHA_TRANSMISION", StringType(), True),
    StructField("HORA_TRANSMISION", StringType(), True),
    StructField("CALIFICACION", StringType(), True),
    StructField("NOMBRE_CONCESIONARIO", StringType(), True),
    StructField("TIPO_CONCESION", StringType(), True),
    StructField("NOMBRE_DE_ESTACIÓN", StringType(), True),
    StructField("TIPO_MEDIO", StringType(), True),
    StructField("MEDIO2", StringType(), True),
    StructField("Organo", StringType(), True),
    StructField("Grupo", StringType(), True),
    StructField("IdEmisora", StringType(), True),
    StructField("Notificado", StringType(), True),
    StructField("Oficio", StringType(), True),
    StructField("Fecha de desahogo", StringType(), True)
])

# COMMAND ----------

# Leer el archivo CSV de transmisiones como un DataFrame de Spark
csv_path = "Transmisiones/Ordenes.csv"
df_transmisiones = spark.read.format("csv") \
    .option("header", "true") \
    .schema(csv_schema) \
    .load(csv_path)

# Convertir FECHA TRANSMISIÓN y HORA TRANSMISIÓN a Timestamp
df_transmisiones = df_transmisiones.withColumn(
    "Timestamp",
    unix_timestamp(concat(col("FECHA_TRANSMISION"), lit(" "), col("HORA_TRANSMISION")), "dd/MM/yyyy HH:mm:ss").cast(TimestampType())
)

# COMMAND ----------

# Calcular el porcentaje de valores nulos para cada columna categórica
categorical_columns = [col[0] for col in df_transmisiones.dtypes if col[1] in ['string']]
null_percentages = []
for col in categorical_columns:
    null_count = df_transmisiones.filter(df_transmisiones[col].isNull()).count()
    total_count = df_transmisiones.count()
    null_percentage = (null_count / total_count) * 100
    null_percentages.append((col, null_percentage))

# Identificar las columnas con más del 30% de valores nulos
columns_to_drop = [col for col, percentage in null_percentages if percentage > 30]

# Eliminar las columnas identificadas del DataFrame
df_transmisiones = df_transmisiones.drop(*columns_to_drop)

# Imprimir las columnas eliminadas
print("Columnas eliminadas:")
for col in columns_to_drop:
    print(col)

# COMMAND ----------

# Obtener la cantidad de valores únicos en las columnas categóricas
categorical_columns = [col[0] for col in df_transmisiones.dtypes if col[1] in ['string']]
for col in categorical_columns:
    unique_count = df_transmisiones.select(col).distinct().count()
    print(f"Cantidad de valores únicos en la columna {col}: {unique_count}")

# COMMAND ----------

df_transmisiones = df_transmisiones.drop("No")
df_transmisiones = df_transmisiones.drop("CALIFICACIÓN")

# COMMAND ----------

df_transmisiones.write.format("delta").option("delta.columnMapping.mode", "name") \
                         .option("path", "/FileStore/delta18").saveAsTable("new_table_18")

# COMMAND ----------

# create a fucntion for  date dimension table
def generate_date_dimension(start_date: str, end_date: str):
 
     # DataFrame with a range of dates
    date_df = spark.createDataFrame([(start_date, end_date)], ["start", "end"])

    # gnerating  a new row for each date between the start and end dates
    date_df = date_df.select(explode(sequence(to_date("start"), to_date("end"), expr("interval 1 day"))).alias("date"))
    
    
    date_df = date_df.selectExpr(
        "date",
        "year(date) as year",
        "quarter(date) as quarter",
        "month(date) as month",
        "day(date) as day",
        "dayofweek(date) as day_of_week",
        "to_date(date) as date_key"
    )

    return date_df

# COMMAND ----------

date_dimension_df = generate_date_dimension("2023-10-01", "2024-05-17")
date_dimension_df.write.format('delta').option("overwriteSchema", "true").saveAsTable("Dates", mode="overwrite")

# COMMAND ----------

# Creating path for a checkpointfile where the metadata will live while writing the stream
basepath ="Tables/transmisiones/"
checkpoint_dir = 'Tables/transmisiones/_checkpoint18'

# COMMAND ----------

delta_load=0

# COMMAND ----------

os.path.exists(checkpoint_dir)

# COMMAND ----------


if delta_load == 0:
    # Check if the checkpoint directory exists before removing it
    if os.path.exists(checkpoint_dir):
        os.rmdir.fs.rm(checkpoint_dir, True)
    Readdf = (spark.readStream.format("delta").option("ignoreChanges",True).load('/FileStore/delta18'))
else:
    Readdf = (spark.readStream.format("delta").option("ignoreChanges",True).load('/FileStore/delta18'))

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Set spark.databricks.delta.formatCheck.enabled=true
# MAGIC

# COMMAND ----------

#spark.readStream.format("delta").load('transmisiones')

# COMMAND ----------

Readdf =Readdf.withColumn("Silver_Loadedtimestamp", current_timestamp())

# COMMAND ----------


def writeTonyncgreentaxi_inc (df,epoch_id):
 time.sleep(3)
 print("Aquí lo que sobra es tiempo de ejecución")
 df.write.format("delta")\
              .mode("overwrite")\
              .option("overwritechema", "true")\
              .saveAsTable("transmisiones")
 
     

# COMMAND ----------

# Define the streaming write operation with format, foreachBatch and checkpoint location
Inc_Query = Readdf.writeStream.format("delta")\
    .foreachBatch(writeTonyncgreentaxi_inc)\
    .option("checkpointLocation", checkpoint_dir)\
    .option("mergeSchema", "false")\
    .trigger(once=True)\
    .start()

# Await for the termination of the stream
Inc_Query.awaitTermination()


# COMMAND ----------

df_final = spark.sql("SELECT * FROM transmisiones")
display(df_final)

# COMMAND ----------

df_final = df_final.join(df_spots, df_final.MATERIAL == df_spots.titulo, "inner")

# COMMAND ----------

df_final.head()

# COMMAND ----------

# Tokenización y eliminación de stopwords
tokenizer = RegexTokenizer(inputCol="transcripcion", outputCol="words", pattern="\\W")
stopwords_remover = StopWordsRemover(inputCol="words", outputCol="filtered_words")

# Convertir las palabras en características numéricas utilizando TF-IDF
count_vectorizer = CountVectorizer(inputCol="filtered_words", outputCol="raw_features")
idf = IDF(inputCol="raw_features", outputCol="features")

# Convertir la columna "partido" en un índice numérico
label_indexer = StringIndexer(inputCol="partido", outputCol="label")

# Crear el pipeline
pipeline = Pipeline(stages=[tokenizer, stopwords_remover, count_vectorizer, idf, label_indexer])

# Ajustar el pipeline a los datos
pipeline_model = pipeline.fit(df_final)

# Transformar los datos utilizando el pipeline
data = pipeline_model.transform(df_final)

# COMMAND ----------

# Dividir los datos en conjuntos de entrenamiento y prueba
(trainingData, testData) = data.randomSplit([0.8, 0.2], seed=42)

# COMMAND ----------

# Crear el modelo Naive Bayes
nb = NaiveBayes(featuresCol="features", labelCol="label", predictionCol="prediction")

# Entrenar el modelo
model = nb.fit(trainingData)

# COMMAND ----------

# Realizar predicciones en el conjunto de prueba
predictions = model.transform(testData)

# Evaluar el modelo
evaluator = MulticlassClassificationEvaluator(labelCol="label", predictionCol="prediction", metricName="accuracy")
accuracy = evaluator.evaluate(predictions)
print("Accuracy:", accuracy)

# COMMAND ----------

# Convertir las predicciones y etiquetas reales a un RDD para usar con MulticlassMetrics
predictionAndLabels = predictions.select("prediction", "label").rdd

# Crear una instancia de MulticlassMetrics
metrics = MulticlassMetrics(predictionAndLabels)

# Obtener la matriz de confusión
confusion_matrix = metrics.confusionMatrix()

# Imprimir la matriz de confusión
print("Matriz de Confusión:")
print(confusion_matrix)

# COMMAND ----------

#!pip install WordCloud

# COMMAND ----------

# %sh sudo apt-get install freetype* -y


# COMMAND ----------

# # Obtener las transcripciones como una lista de strings
# transcripciones = [row.transcripcion for row in df_final.select("transcripcion").distinct().collect()]

# # Unir las transcripciones en un solo string
# texto = " ".join(transcripciones)

# # Crear el objeto WordCloud
# wordcloud = WordCloud(width=800, height=800, background_color="white").generate(texto)

# # Plotear el wordCloud
# plt.figure(figsize=(8, 8), facecolor=None)
# plt.imshow(wordcloud)
# plt.axis("off")
# plt.tight_layout(pad=0)

# # Mostrar el wordCloud
# plt.show()

# COMMAND ----------


# COMMAND ----------

nltk.download('vader_lexicon')

# COMMAND ----------

def sentiment_analysis(text):
    sid = SentimentIntensityAnalyzer()
    sentiment_scores = sid.polarity_scores(text)
    return sentiment_scores['compound']

# COMMAND ----------

sentiment_udf = udf(sentiment_analysis, FloatType())
df_final = df_final.withColumn("sentiment_score", sentiment_udf(df_final.transcripcion))

# COMMAND ----------

# Convertir el DataFrame de Spark a un DataFrame de Pandas
df_sentiment = df_final.select("sentiment_score").toPandas()

# Crear un histograma de las puntuaciones de sentimiento
plt.figure(figsize=(8, 6))
plt.hist(df_sentiment['sentiment_score'], bins=20, edgecolor='black')
plt.xlabel('Sentiment Score')
plt.ylabel('Frequency')
plt.title('Distribution of Sentiment Scores')
plt.show()

# COMMAND ----------

# Filtrar el DataFrame para los partidos especificados
partidos_seleccionados = ["MORENA", "PAN", "MC"]
df_sentiment_partidos = df_final.filter(df_final.partido.isin(partidos_seleccionados))

# Convertir el DataFrame de Spark a un DataFrame de Pandas
df_sentiment_partidos_pd = df_sentiment_partidos.select("partido", "sentiment_score").toPandas()

# Crear un boxplot de las puntuaciones de sentimiento por partido
plt.figure(figsize=(8, 6))
sns.boxplot(x='partido', y='sentiment_score', data=df_sentiment_partidos_pd)
plt.xlabel('Party')
plt.ylabel('Sentiment Score')
plt.title('Sentiment Analysis by Party')
plt.show()

# COMMAND ----------

# Gráfica de pie del total de spots por partido
partido_count = df_final.groupBy("partido").count()
partido_count_pd = partido_count.toPandas()

plt.figure(figsize=(8, 8))
plt.pie(partido_count_pd["count"], labels=partido_count_pd["partido"], autopct='%1.1f%%')
plt.title("Total de Spots por Partido")
plt.axis('equal')  # Asegura que la gráfica de pie sea un círculo
plt.show()

# COMMAND ----------

# Gráfica de líneas que muestre los datos a través del tiempo
df_final.groupBy("FECHA_TRANSMISION").count().orderBy("FECHA_TRANSMISION").toPandas().set_index("FECHA_TRANSMISION").plot(kind="line", figsize=(10, 6))
plt.xlabel("Fecha de Transmisión")
plt.ylabel("Cantidad de Spots")
plt.title("Evolución de Spots a lo largo del tiempo")
plt.show()

# COMMAND ----------

# Gráfica de serie de tiempo agrupada por hora
df_final_hour = df_final.withColumn("HORA", hour("HORA_TRANSMISION"))
df_hour_count = df_final_hour.groupBy("HORA").count().orderBy("HORA")
df_hour_count_pd = df_hour_count.toPandas().set_index("HORA")
df_hour_count_pd.plot(kind="line", figsize=(10, 6))
plt.xlabel("Hora")
plt.ylabel("Cantidad de Spots")
plt.title("Distribución de Spots por Hora")
plt.xticks(range(0, 24))
plt.grid(True)
plt.show()

# COMMAND ----------

# Gráfica de serie de tiempo agrupada por hora y mes
df_final_hour_month = df_final.withColumn("HORA", hour("HORA_TRANSMISION")).withColumn("MES", month("FECHA_TRANSMISION"))
df_hour_month_count = df_final_hour_month.groupBy("MES", "HORA").count().orderBy("MES", "HORA")
df_hour_month_count_pd = df_hour_month_count.toPandas()

plt.figure(figsize=(12, 8))
for mes in df_hour_month_count_pd["MES"].unique():
    df_mes = df_hour_month_count_pd[df_hour_month_count_pd["MES"] == mes]
    plt.plot(df_mes["HORA"], df_mes["count"], label=f"Mes {mes}")

plt.xlabel("Hora")
plt.ylabel("Cantidad de Spots")
plt.title("Distribución de Spots por Hora y Mes")
plt.xticks(range(0, 24))
plt.legend(title="Mes")
plt.grid(True)
plt.show()

# COMMAND ----------

# Gráfica de barras apiladas de los primeros 10 estados con más spots
top_estados = df_final.groupBy("ESTADO").count().orderBy(count("ESTADO").desc()).limit(10)
top_estados = df_final.join(top_estados, df_final.ESTADO == top_estados.ESTADO, "inner").drop(top_estados.ESTADO)
top_estados = top_estados.groupBy("ESTADO", "partido").count().orderBy(count("partido").desc())
top_estados_pd = top_estados.toPandas().pivot(index="ESTADO", columns="partido", values="count")
top_estados_pd.plot(kind="bar", stacked=True, figsize=(10, 6))
plt.xlabel("Estado")
plt.ylabel("Cantidad de Spots")
plt.title("Top 10 Estados con más Spots por Partido")
plt.legend(title="Partido", loc="upper right")
plt.show()

# COMMAND ----------

# Gráfica de barras apiladas de los primeros 10 estados con más spots
top_estados = df_final.groupBy("ESTADO", "partido").count().orderBy(count("partido").desc())
top_estados_pd = top_estados.toPandas().pivot(index="ESTADO", columns="partido", values="count")

# Calcular el total de count por estado
top_estados_pd["Total"] = top_estados_pd.sum(axis=1)

# Ordenar los estados por el total de count de izquierda a derecha
top_estados_pd = top_estados_pd.sort_values("Total", ascending=False)

# Eliminar la columna "Total" antes de graficar
top_estados_pd = top_estados_pd.drop("Total", axis=1)

top_estados_pd.plot(kind="bar", stacked=True, figsize=(10, 6))
plt.xlabel("Estado")
plt.ylabel("Cantidad de Spots")
plt.title("Top 10 Estados con más Spots por Partido")
plt.legend(title="Partido", loc="upper right")
plt.show()

# COMMAND ----------

# Gráfica de barras apiladas de la columna MEDIO2 donde muestre el count de cada medio y a su vez por partido
medio2_partido = df_final.groupBy("MEDIO2", "partido").count().orderBy("MEDIO2")
medio2_partido_pd = medio2_partido.toPandas().pivot(index="MEDIO2", columns="partido", values="count")
medio2_partido_pd.plot(kind="bar", stacked=True, figsize=(10, 6))
plt.xlabel("Medio")
plt.ylabel("Cantidad de Spots")
plt.title("Cantidad de Spots por Medio y Partido")
plt.legend(title="Partido", loc="upper right")
plt.show()
