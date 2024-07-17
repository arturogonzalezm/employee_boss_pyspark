"""
This script demonstrates how to process employee data using Spark SQL.
"""
from data.sample_data import data
from src.spark_session import SparkSessionManager


def process_employee_data():
    """
    Process employee data using Spark SQL.
    :return: None
    :rtype: None
    """
    spark = SparkSessionManager.get_instance()

    schema = ["ID", "Name", "Boss"]

    # Create a DataFrame
    df = spark.createDataFrame(data, schema)

    # Create a temporary view of the DataFrame
    df.createOrReplaceTempView("employees")

    # Perform a self-join to get boss names
    result = spark.sql("""
        SELECT e.Name AS Employee,
               COALESCE(b.Name, 'No Boss') AS Boss
        FROM employees e
        LEFT JOIN employees b ON e.Boss = b.ID
        ORDER BY e.ID
    """)

    # Show the result
    result.show()
