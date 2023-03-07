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
        return [x["store_id"] for x in (stores(self.spark)
                    .select("store_id").collect())]

    def n_stores_per_product(self, ticket_lines: DataFrame):
        return (ticket_lines
                .filter(F.col("store_id").isin(self.lp_stores))
                .groupBy("product_id")
                .agg(F.countDistinct("store_id").alias("n_stores"))
                )

    def second_most_selling_store_per_product(self, ticket_lines: DataFrame):
        sorted_quantities = Window.partitionBy("product_id").orderBy(F.col('quantity').desc())
        return (ticket_lines
                .filter(F.col("store_id").isin(self.lp_stores))
                .groupBy("product_id", "store_id")
                .agg(F.sum("quantity").alias("quantity"))
                .withColumn("second_quantity", F.collect_list(F.col("quantity")).over(sorted_quantities)[1])
                .filter(F.col("second_quantity")==F.col("quantity"))
                .select("product_id", "store_id")
                )

    def second_stores_by_product_category(self, products: DataFrame):
        return (self.second_most_selling_store_per_product
                .join(products, on="product_id")
                .select("product_id", "store_id", "product_name", F.explode("categories").alias("category"))
                .groupBy("category.category_name")
                .agg(F.collect_set("store_id").alias("stores"))
                )

    def main(self,):
        self.n_stores_per_product(ticket_lines=).printSchema()
        self.n_stores_per_product(ticket_lines=).show()
        self.second_most_selling_store_per_product(ticket_lines=).printSchema()
        self.second_most_selling_store_per_product(ticket_lines=).show()
        self.second_stores_by_product_category(products=).printSchema()
        self.second_stores_by_product_category(products=).show()


if __name__ == "__main__":
    Transformation.main()