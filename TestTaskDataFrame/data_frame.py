from pyspark.sql import SparkSession
from pyspark.sql import functions as F


def get_data(products_df, categories_df):
    joined_df = products_df.join(categories_df, F.expr(
        "array_contains(categories, category_id)"), "left_outer")

    product_category_pairs = joined_df.select(
        "product_name", "category_name").filter(joined_df.category_name.isNotNull())  # noqa E501

    products_with_no_categories = products_df.join(categories_df, F.expr(
        "array_contains(categories, category_id)"), "left_anti").select("product_name")  # noqa E501

    return product_category_pairs, products_with_no_categories


# Пример использования функции
if __name__ == "__main__":
    spark = SparkSession.builder.master('local').appName(
        "ProductCategoryPairs").getOrCreate()

    # Создаем датафреймы с продуктами и категориями
    product_data = [("product1", ["category1", "category2"]),
                    ("product2", ["category1"]),
                    ("product3", [])]

    products_df = spark.createDataFrame(
        product_data, ["product_name", "categories"])

    category_data = [("category1", "Category A"),
                     ("category2", "Category B"),
                     ("category3", "Category C")]

    categories_df = spark.createDataFrame(
        category_data, ["category_id", "category_name"])

    product_category_pairs, products_with_no_categories = \
        get_data(products_df, categories_df)
    product_category_pairs.show()
    products_with_no_categories.show()

    spark.stop()
