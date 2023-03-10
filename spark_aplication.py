from typing import List
from pyspark.sql import DataFrame
from pyspark.sql import functions as F
from pyspark.sql import types as T
from pyspark.sql import Window
from pyspark.sql import SparkSession
import sys

class ETL:

    def __init__(self) -> None:
        self.spark = (SparkSession
                .builder
                .getOrCreate())
        
    def read_stores_v1(self, stores_v1_file_path: str):
        stores_schema =  T.StructType([
            T.StructField("store_id",T.IntegerType(),True), 
            T.StructField("country",T.StringType(),True), 
            T.StructField("version",T.StringType(),True), 
            ])
        return self.spark.read.schema(stores_schema).option("header", True).csv(stores_v1_file_path)
    
    def read_stores_v2(self, stores_v2_file_path: str):
        stores_v2_schema =  T.StructType([
            T.StructField("store_id",T.StringType(),True), 
            T.StructField("version",T.StringType(),True), 
            ])
        return self.spark.read.schema(stores_v2_schema).option("header", True).csv(stores_v2_file_path)

    def read_ticket_lines(self, ticket_line_file_path: str):
        ticket_lines_schema =  T.StructType([
            T.StructField("ticket_id",T.IntegerType(),True), 
            T.StructField("product_id",T.IntegerType(),True), 
            T.StructField("store_id",T.IntegerType(),True), 
            T.StructField("date",T.DateType(),True), 
            T.StructField("quantity",T.IntegerType(),True), 
            ])
        return self.spark.read.schema(ticket_lines_schema).option("header", True).csv(ticket_line_file_path)

    def read_products(self, products_file_path: str):
        products_schema = T.StructType([
            T.StructField("product_id",T.IntegerType(),True), 
            T.StructField("product_name",T.StringType(),True),
            T.StructField("categories",T.ArrayType(T.StructType([
                T.StructField("category_id", T.IntegerType(), True),
                T.StructField("category_name", T.StringType(), True)
                ])),True),
            ])
        return self.spark.read.schema(products_schema).json(products_file_path)
    
    def lp_stores(self, stores: DataFrame) -> List[int]:
        stores_integrated = stores.withColumn("store_id", F.when(
            F.col("version")==2, F.substring(F.col("store_id"), 3, 4)
            ).otherwise(F.col("store_id")))
        return [int(x["store_id"]) for x in (stores_integrated.select("store_id").collect())]

    def total_stores_per_product(self, ticket_lines: DataFrame, stores: DataFrame) -> DataFrame:
        return (ticket_lines
                .filter(F.col("store_id").isin(self.lp_stores(stores)))
                .groupBy("product_id")
                .agg(F.countDistinct("store_id").cast(T.IntegerType()).alias("n_stores"))
                .orderBy("product_id")
                )

    def second_most_selling_store_per_product(self, ticket_lines: DataFrame, stores: DataFrame) -> DataFrame:
        sorted_quantities = Window.partitionBy("product_id").orderBy(F.col('quantity').desc())
        return (ticket_lines
                .filter(F.col("store_id").isin(self.lp_stores(stores)))
                .groupBy("product_id", "store_id")
                .agg(F.sum("quantity").alias("quantity"))
                .withColumn("second_quantity", F.collect_list(F.col("quantity")).over(sorted_quantities)[1])
                .filter(F.col("second_quantity")==F.col("quantity"))
                .select(F.col("product_id"), F.col("store_id").alias("second_store_id"))
                )

    def second_popular_stores_by_product_category(self, ticket_lines: DataFrame, products: DataFrame, stores: DataFrame) -> DataFrame:
        return (self.second_most_selling_store_per_product(ticket_lines, stores)
                .join(products, on="product_id")
                .select( F.explode(F.col("categories")).alias("category"), "product_id", "product_name", "second_store_id")
                .groupBy(F.col("category.category_name").alias("category_name"))
                .agg(F.collect_set("second_store_id").alias("stores"))
                )

    def main(self, stores_version):
        print(stores_version)
        if stores_version == 1:
            stores = self.read_stores_v1("stores.csv")
        if stores_version == 2:
            stores = self.read_stores_v2("stores_v2.csv")
        ticket_lines = self.read_ticket_lines("ticket_line.csv")
        products = self.read_products("products.json")
        stores_per_product = self.total_stores_per_product(ticket_lines, stores)
        second_store_per_product = self.second_most_selling_store_per_product(ticket_lines, stores)
        second_store_by_category = self.second_popular_stores_by_product_category(ticket_lines, products, stores)

    

if __name__ == "__main__":
    stores_version = int(sys.argv[1])
    ETL().main(stores_version=stores_version)