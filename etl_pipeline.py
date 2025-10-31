from pyspark.sql import SparkSession
from pyspark.sql.functions import concat_ws


spark = SparkSession.builder \
    .appName("CustomerOrderDataPipeline") \
    .getOrCreate()
    
customers_df = spark.read \
    .option("header", True) \
    .option("inferSchema", True) \
    .csv("Data/customers.csv")
    

orders_df = spark.read \
    .option("header", True) \
    .option("inferSchema", True) \
    .csv("Data/orders.csv")


customers_df.printSchema()
orders_df.printSchema()

customers_df.show()
orders_df.show()


# Join data files
df_joined = orders_df.join(customers_df, on="customer_id", how="inner")

# filter data
df_filtered = df_joined.filter(df_joined.order_total > 100)

# transform data
df_transformed = df_filtered.withColumn("full_name", concat_ws(" ", df_filtered.first_name, df_filtered.last_name))

#select useful columns
df_final = df_transformed.select("customer_id", "full_name", "country", "order_total")


df_final.write \
    .option("header", True) \
    .mode("overwrite") \
    .parquet("Output/processed_orders")


df_final.show()
print("ETL pipeline completed successfully!")

spark.stop()