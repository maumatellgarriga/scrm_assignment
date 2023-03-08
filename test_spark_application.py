from pyspark.sql import functions as F
from pyspark.sql import types as T
from pyspark.sql import DataFrame
import pytest 
from spark_aplication import Transformation

@pytest.fixture
def stores_v1_file_path():
    return "stores.csv"


@pytest.fixture
def stores_v2_file_path():
    return "stores_v2.csv"


@pytest.fixture
def ticket_line_file_path():
    return "ticket_line.csv"


@pytest.fixture
def products_file_path():
    return "products.json"

@pytest.fixture
def transformation_instance():
    return Transformation()

@pytest.fixture
def stores_v1(transformation_instance: Transformation, stores_v1_file_path: str):
    stores_schema =  T.StructType([
        T.StructField("store_id",T.IntegerType(),True), 
        T.StructField("country",T.StringType(),True), 
        T.StructField("version",T.StringType(),True), 
        ])
    return transformation_instance.spark.read.schema(stores_schema).option("header", True).csv(stores_v1_file_path)

@pytest.fixture
def stores_v2(transformation_instance: Transformation, stores_v2_file_path: str):
    stores_v2_schema =  T.StructType([
        T.StructField("store_id",T.StringType(),True), 
        T.StructField("version",T.StringType(),True), 
        ])
    return transformation_instance.spark.read.schema(stores_v2_schema).option("header", True).csv(stores_v2_file_path)

@pytest.fixture
def ticket_lines(transformation_instance: Transformation, ticket_line_file_path: str):
    ticket_lines_schema =  T.StructType([
        T.StructField("ticket_id",T.IntegerType(),True), 
        T.StructField("product_id",T.IntegerType(),True), 
        T.StructField("store_id",T.IntegerType(),True), 
        T.StructField("date",T.DateType(),True), 
        T.StructField("quantity",T.IntegerType(),True), 
        ])
    return transformation_instance.spark.read.schema(ticket_lines_schema).option("header", True).csv(ticket_line_file_path)

@pytest.fixture
def products(transformation_instance: Transformation, products_file_path: str):
    products_schema = T.StructType([
        T.StructField("product_id",T.IntegerType(),True), 
        T.StructField("product_name",T.StringType(),True),
        T.StructField("categories",T.ArrayType(T.StructType([
            T.StructField("category_id", T.IntegerType(), True),
            T.StructField("category_name", T.StringType(), True)
            ])),True),
        ])
    return transformation_instance.spark.read.schema(products_schema).json(products_file_path)

def test_lp_stores_v1(transformation_instance: Transformation, stores_v1: DataFrame):
    stores_v1.printSchema()
    print(stores_v1.schema)
    expected = [40,41,42,43,44,45]
    result = transformation_instance.lp_stores(stores_v1)
    assert expected == result
    
def test_lp_stores_v2(transformation_instance: Transformation, stores_v2: DataFrame):
    expected = [46,47]
    result = transformation_instance.lp_stores(stores_v2)
    assert expected == result
    
def test_total_stores_per_product(transformation_instance: Transformation, ticket_lines: DataFrame, stores_v1: DataFrame):
    expected = transformation_instance.spark.createDataFrame([(1, 6), (2, 6), (3, 6), (4, 6), (5, 6), (6, 6), (7, 6), (8, 6), (9, 6), (10, 6)], 
                                                             "product_id int, n_stores int")
    result = transformation_instance.total_stores_per_product(ticket_lines=ticket_lines, stores=stores_v1).orderBy("product_id")
    assert expected.dtypes == result.dtypes
    assert expected.collect() == result.collect()

def test_n_stores_per_product_v2(transformation_instance: Transformation, ticket_lines: DataFrame, stores_v2: DataFrame):
    expected = transformation_instance.spark.createDataFrame([(1, 2), (2, 2), (3, 2), (4, 2), (5, 2), (6, 2), (7, 2), (8, 2), (9, 2), (10, 2)], 
                                                             "product_id int, n_stores int")
    result = transformation_instance.total_stores_per_product(ticket_lines=ticket_lines, stores=stores_v2).orderBy("product_id")
    assert expected.dtypes == result.dtypes
    assert expected.collect() == result.collect()

def test_second_most_selling_store_per_product(transformation_instance: Transformation, ticket_lines: DataFrame, stores_v1):
    expected = transformation_instance.spark.createDataFrame([(1, 42), (2, 42), (2, 41), (3, 44), (4, 42), (5, 41), (6, 45), (6, 40), (7, 40), (8, 41), (9, 40), (10, 42)],
                                                             "product_id int, second_store_id int")
    result = transformation_instance.second_most_selling_store_per_product(ticket_lines=ticket_lines, stores=stores_v1)
    assert expected.dtypes == result.dtypes
    assert expected.collect() == result.collect()
 
def test_second_most_selling_store_per_product_v2(transformation_instance: Transformation, ticket_lines: DataFrame, stores_v2):
    expected = transformation_instance.spark.createDataFrame([(1, 47), (2, 46), (3, 46), (3, 47), (4, 47), (5, 46), (6, 47), (7, 46), (7, 47), (8, 46), (9, 46), (10, 47)],
                                                             "product_id int, second_store_id int")
    result = transformation_instance.second_most_selling_store_per_product(ticket_lines=ticket_lines, stores=stores_v2)
    result.show()
    assert expected.dtypes == result.dtypes
    assert expected.collect() == result.collect()

def test_second_popular_stores_by_product_category(transformation_instance: Transformation, ticket_lines: DataFrame, products: DataFrame, stores_v1: DataFrame):
    expected = transformation_instance.spark.createDataFrame([("Cereal", [42,40,41]),("Vegetables", [42,40]),("Dairy product", [42,44,41]),("Cookies", [45,40,41]),], 
                                                             "category_name string, stores array<int>")
    result = transformation_instance.second_popular_stores_by_product_category(ticket_lines=ticket_lines, products=products, stores=stores_v1)
    assert expected.dtypes == result.dtypes
    assert expected.collect() == result.collect()
