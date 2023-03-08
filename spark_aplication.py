from typing import List
from pyspark.sql import DataFrame
from pyspark.sql import functions as F
from pyspark.sql import types as T
from pyspark.sql import Window
from pyspark.sql import SparkSession


class Transformation:

    def __init__(self) -> None:
        self.spark = (SparkSession
                .builder
                .getOrCreate())

    def lp_stores(self, stores: DataFrame) -> List:
        stores_integrated = stores.withColumn("store_id", F.when(
            F.col("version")==2, F.substring(F.col("store_id"), 3, 4)
            ).otherwise(F.col("store_id")))
        return [int(x["store_id"]) for x in (stores_integrated.select("store_id").collect())]

    def total_stores_per_product(self, ticket_lines: DataFrame, stores: DataFrame) -> DataFrame:
        return (ticket_lines
                .filter(F.col("store_id").isin(self.lp_stores(stores)))
                .groupBy("product_id")
                .agg(F.countDistinct("store_id").cast(T.IntegerType()).alias("n_stores"))
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

    def second_popular_stores_by_product_category(self, ticket_lines: DataFrame, products: DataFrame, stores: DataFrame):
        return (self.second_most_selling_store_per_product(ticket_lines, stores)
                .join(products, on="product_id")
                .select( F.explode(F.col("categories")).alias("category"), "product_id", "product_name", "second_store_id")
                .groupBy(F.col("category.category_name").alias("category_name"))
                .agg(F.collect_set("second_store_id").alias("stores"))
                )

    def run_transformations(self, stores: DataFrame, ticket_lines: DataFrame, products: DataFrame):
        stores_per_product = self.total_stores_per_product(ticket_lines, stores)
        second_store_per_product = self.second_most_selling_store_per_product(ticket_lines)
        second_store_by_category = self.second_popular_stores_by_product_category(ticket_lines, products)
        return stores_per_product, second_store_per_product, second_store_by_category