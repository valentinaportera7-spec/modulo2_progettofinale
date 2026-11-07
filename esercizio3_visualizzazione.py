from pyspark.sql import SparkSession
import matplotlib.pyplot as plt
import seaborn as sns
import os

spark = SparkSession.builder \
    .appName("DataVisualization") \
    .getOrCreate()

OUTPUT_DIR = os.path.join(
    os.path.dirname(os.path.abspath(__file__)),
    "data_local",
    "processed_sales"
)
df_finale = spark.read.parquet(OUTPUT_DIR)
df_finale.show(5, truncate=False)

df_fatturato = df_finale.groupBy("category") \
    .sum("amount") \
    .withColumnRenamed("sum(amount)", "fatturato_totale")

df_fatturato.show()
plt.figure(figsize=(10, 6))
df_fatturato_pd = df_fatturato.toPandas()
print(df_fatturato_pd)

sns.barplot(
    data=df_fatturato_pd,
    x="category",
    y="fatturato_totale"
)

plt.title("Fatturato totale per categoria")
plt.xlabel("Categoria")
plt.ylabel("Fatturato totale")
plt.xticks(rotation=45)
plt.tight_layout()

percorso_grafico = os.path.join(
    os.path.dirname(os.path.abspath(__file__)),
    "fatturato_per_categoria.png"
)

plt.savefig(percorso_grafico)
plt.show()

print(f"Grafico salvato in: {percorso_grafico}")
print("Percorso dati:", OUTPUT_DIR)
print("Cartella esistente:", os.path.exists(OUTPUT_DIR))
spark.stop()