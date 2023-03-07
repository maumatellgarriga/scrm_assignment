from pyspark.sql import functions as F
from pyspark.sql import types as T
from pyspark.sql import Window
from pyspark.sql import SparkSession

spark = (SparkSession
         .builder
         .getOrCreate())

def stores_v1(spark, file_path):
    stores_schema =  T.StructType([
        T.StructField("store_id",T.IntegerType(),True), 
        T.StructField("country",T.StringType(),True), 
        T.StructField("version",T.StringType(),True), 
        ])
    return spark.read.schema(stores_schema).option("header", True).csv(file_path)

def stores_v2(spark, file_path):
    stores_v2_schema =  T.StructType([
        T.StructField("store_id",T.StringType(),True), 
        T.StructField("version",T.StringType(),True), 
        ])
    return spark.read.schema(stores_v2_schema).option("header", True).csv(file_path)

def stores(spark):
    stores_v1_file_path = "stores.csv"
    stores_v2_file_path = "stores_v2.csv"
    stores_d1 = stores_v1(spark, stores_v1_file_path)
    stores_d2 = (stores_v2(spark, stores_v2_file_path)
                 .withColumn("country", F.substring(F.col("store_id"), 1,2))
                 .withColumn("store_id", F.substring(F.col("store_id"), 3, 4))
                 .select("store_id", "country", "version")
                 )
    return stores_d1.union(stores_d2)

def ticket_lines(spark, file_path):
    ticket_lines_schema =  T.StructType([
        T.StructField("ticket_id",T.IntegerType(),True), 
        T.StructField("product_id",T.IntegerType(),True), 
        T.StructField("store_id",T.IntegerType(),True), 
        T.StructField("date",T.DateType(),True), 
        T.StructField("quantity",T.IntegerType(),True), 
        ])
    return spark.read.schema(ticket_lines_schema).option("header", True).csv(file_path)

def products(spark, file_path):
    products_schema = T.StructType([
        T.StructField("product_id",T.IntegerType(),True), 
        T.StructField("product_name",T.StringType(),True),
        T.StructField("categories",T.ArrayType(T.StructType([
            T.StructField("category_id", T.IntegerType(), True),
            T.StructField("category_name", T.StringType(), True)
            ])),True),
        ])
    # return spark.createDataFrame(file_path, schema = products_schema)
    return spark.read.schema(products_schema).json(file_path)

def lp_stores(spark):
    return [x["store_id"] for x in (stores(spark)
                 .select("store_id").collect())]

def n_stores_per_product(spark):
    ticket_lines_file_path = "ticket_line.csv"
    return (ticket_lines(spark, ticket_lines_file_path) 
            .filter(F.col("store_id").isin(lp_stores(spark)))
            .groupBy("product_id")
            .agg(F.countDistinct("store_id").alias("n_stores"))
            )

def second_most_selling_store_per_product(spark):
    ticket_lines_file_path = "ticket_line.csv"
    sorted_quantities = Window.partitionBy("product_id").orderBy(F.col('quantity').desc())
    return (ticket_lines(spark, ticket_lines_file_path)
            .filter(F.col("store_id").isin(lp_stores(spark)))
            .groupBy("product_id", "store_id")
            .agg(F.sum("quantity").alias("quantity"))
            .withColumn("second_quantity", F.collect_list(F.col("quantity")).over(sorted_quantities)[1])
            .filter(F.col("second_quantity")==F.col("quantity"))
            .select("product_id", "store_id")
            )

def second_stores_by_product_category(spark):
    products_file_path = "products.json"
    return (second_most_selling_store_per_product(spark)
            .join(products(spark, products_file_path), on="product_id")
            .select("product_id", "store_id", "product_name", F.explode("categories").alias("category"))
            .groupBy("category.category_name")
            .agg(F.collect_set("store_id").alias("stores"))
            )

def main():
    n_stores_per_product(spark).printSchema()
    n_stores_per_product(spark).show()
    second_most_selling_store_per_product(spark).printSchema()
    second_most_selling_store_per_product(spark).show()
    second_stores_by_product_category(spark).printSchema()
    second_stores_by_product_category(spark).show()


if __name__ == "__main__":
    main()