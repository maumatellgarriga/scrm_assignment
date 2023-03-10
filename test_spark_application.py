from pyspark.sql import functions as F
from pyspark.sql import types as T
from pyspark.sql import DataFrame
import pytest 
from spark_aplication import ETL
from datetime import date

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
def ETL_instance():
    return ETL()

@pytest.fixture
def stores_v1(ETL_instance: ETL):
    return ETL_instance.spark.createDataFrame([(10, "ES", "1"), (11, "ES", "1"), (12, "ES", "1")],
                                                         "store_id int, country string, version string")

@pytest.fixture
def stores_v2(ETL_instance: ETL):
    return ETL_instance.spark.createDataFrame([("ES13", "2"), ("ES14", "2")], 
                                                         "store_id string, version string")

@pytest.fixture
def ticket_lines(ETL_instance: ETL):
    return ETL_instance.spark.createDataFrame([(1, 1, 10, date(2023,3,3), 2),
                                               (1, 2, 10, date(2023,3,3), 4), 
                                               (1, 3, 10, date(2023,3,3), 1), 
                                               (2, 1, 11, date(2023,3,3), 3), 
                                               (2, 3, 11, date(2023,3,3), 5),
                                               (3, 2, 14, date(2023,3,3), 1),
                                               (3, 3, 14, date(2023,3,3), 3),
                                               (4, 1, 12, date(2023,3,3), 4),
                                               (5, 2, 12, date(2023,3,3), 3),
                                               (6, 2, 13, date(2023,3,3), 2)],
                                               "ticket_id int, product_id int, store_id int, date date, quantity int")

@pytest.fixture
def products(ETL_instance: ETL):
    return ETL_instance.spark.createDataFrame([(1, "Chocolate bar 70%", [(20, "Choco")]),
                                                          (2, "Kinder Bueno", [(20, "Choco")]),
                                                          (3, "Corn Flakes", [(20, "Cereal")])], 
                                                         "product_id int, product_name string, categories array<struct<category_id: int, category_name: string>>")

def test_lp_stores_v1(ETL_instance: ETL, stores_v1: DataFrame):
    stores_v1.printSchema()
    print(stores_v1.schema)
    expected = [10,11,12]
    result = ETL_instance.lp_stores(stores_v1)
    assert expected == result
    
def test_lp_stores_v2(ETL_instance: ETL, stores_v2: DataFrame):
    expected = [13,14]
    result = ETL_instance.lp_stores(stores_v2)
    assert expected == result
    
def test_total_stores_per_product(ETL_instance: ETL, ticket_lines: DataFrame, stores_v1: DataFrame):
    expected = ETL_instance.spark.createDataFrame([(1, 3), (2, 2), (3, 2)], 
                                                  "product_id int, n_stores int")
    result = ETL_instance.total_stores_per_product(ticket_lines=ticket_lines, stores=stores_v1).orderBy("product_id")
    assert expected.dtypes == result.dtypes
    assert expected.collect() == result.collect()

def test_n_stores_per_product_v2(ETL_instance: ETL, ticket_lines: DataFrame, stores_v2: DataFrame):
    expected = ETL_instance.spark.createDataFrame([(2, 2), (3, 1)],
                                                  "product_id int, n_stores int")
    result = ETL_instance.total_stores_per_product(ticket_lines=ticket_lines, stores=stores_v2).orderBy("product_id")
    assert expected.dtypes == result.dtypes
    assert expected.collect() == result.collect()

def test_second_most_selling_store_per_product(ETL_instance: ETL, ticket_lines: DataFrame, stores_v1):
    expected = ETL_instance.spark.createDataFrame([(1, 11), (2, 12), (3, 10)],
                                                  "product_id int, second_store_id int")
    result = ETL_instance.second_most_selling_store_per_product(ticket_lines=ticket_lines, stores=stores_v1)
    assert expected.dtypes == result.dtypes
    assert expected.collect() == result.collect()
 
def test_second_most_selling_store_per_product_v2(ETL_instance: ETL, ticket_lines: DataFrame, stores_v2):
    expected = ETL_instance.spark.createDataFrame([(2, 14)],
                                                  "product_id int, second_store_id int")
    result = ETL_instance.second_most_selling_store_per_product(ticket_lines=ticket_lines, stores=stores_v2)
    assert expected.dtypes == result.dtypes
    assert expected.collect() == result.collect()

def test_second_popular_stores_by_product_category(ETL_instance: ETL, ticket_lines: DataFrame, products: DataFrame, stores_v1: DataFrame):
    expected = ETL_instance.spark.createDataFrame([("Cereal", [10]), ("Choco", [12,11])], 
                                                  "category_name string, stores array<int>")
    result = ETL_instance.second_popular_stores_by_product_category(ticket_lines=ticket_lines, products=products, stores=stores_v1)
    assert expected.dtypes == result.dtypes
    assert expected.collect() == result.collect()


def test_second_popular_stores_by_product_category(ETL_instance: ETL, ticket_lines: DataFrame, products: DataFrame, stores_v2: DataFrame):
    expected = ETL_instance.spark.createDataFrame([("Choco", [14])],
                                                  "category_name string, stores array<int>")
    result = ETL_instance.second_popular_stores_by_product_category(ticket_lines=ticket_lines, products=products, stores=stores_v2)
    assert expected.dtypes == result.dtypes
    assert expected.collect() == result.collect()
