from pyspark.sql import SparkSession

spark = SparkSession.builder \
    .appName("Teste Spark") \
    .getOrCreate()

# Caminho do GCS onde os arquivos CSV estão localizados
gcs_input_path = "gs://nginx-process-priotto/players/*.csv"
# Caminho do GCS para salvar o arquivo Parquet
gcs_output_path = "gs://nginx-process-priotto/result"

csv_files = spark.read.csv(gcs_input_path, header=True, inferSchema=True)


csv_files.write.format("csv").option("header", True).option("sep", ",").save(gcs_output_path)

# Encerra a sessão do Spark
spark.stop()