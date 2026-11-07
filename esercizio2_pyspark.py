from pyspark.sql import SparkSession
import os

PARQUET_DIR = os.path.join(os.path.dirname(os.path.abspath(__file__)), "data_local", "parquet")

OUTPUT_DIR = os.path.join(
    os.path.dirname(os.path.abspath(__file__)),
    "data_local",
    "processed_sales"
)

spark = SparkSession.builder \
    .appName("MegaShopETL") \
    .config("spark.driver.extraJavaOptions", "--add-modules=jdk.incubator.vector") \
    .config("spark.hadoop.mapreduce.fileoutputcommitter.algorithm.version", "2") \
   .getOrCreate()

print("HADOOP_HOME:", os.environ.get("HADOOP_HOME"))
print("HADOOP_VERSION:", spark._jvm.org.apache.hadoop.util.VersionInfo.getVersion())
print(spark.version)


print("Cartella di lavoro:", os.getcwd())
print("Contenuto della cartella:", os.listdir())
print(os.listdir(PARQUET_DIR))

df_transazioni = spark.read.parquet(
    *[
        os.path.join(PARQUET_DIR, f"transactions_batch_{i:04d}.parquet")
        for i in range(5)
    ]
)

df_transazioni.printSchema()
print("Totale transazioni", df_transazioni.count())

df_clienti = spark.read.parquet(os.path.join(PARQUET_DIR, "customers.parquet"))
df_prodotti = spark.read.parquet(os.path.join(PARQUET_DIR, "products.parquet"))
df_regioni = spark.read.parquet(os.path.join(PARQUET_DIR, "regions.parquet"))

print("Clienti", df_clienti.count())
print("Prodotti", df_prodotti.count())
print("Regioni", df_regioni.count())

df_etl = df_transazioni.join(
    df_prodotti,
    on="product_id",
    how="inner"
)

print("Righe dopo il join prodotti:", df_etl.count())

df_etl = df_etl.join(
    df_regioni,
    on="region_id",
    how="inner"
)
print("Righe dopo il join regioni:", df_etl.count())

df_finale = df_etl.select(
    "transaction_id",
    "region_name",
    "category",
    "amount",
    "year"
).coalesce(1)

df_finale.printSchema()
df_finale.show(5, truncate=False)

print("Righe da salvare:", df_finale.count())



df_finale.write \
    .mode("overwrite") \
    .partitionBy("year") \
    .parquet(OUTPUT_DIR)

print("Salvataggio completato", OUTPUT_DIR)