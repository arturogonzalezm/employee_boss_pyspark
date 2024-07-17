"""
Unit tests for the process_employee_data function.
"""
import pytest
from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.types import StructType, StructField, IntegerType, StringType
from unittest.mock import patch, MagicMock

from src.process_employee_data import process_employee_data

# Sample data for testing
test_data = [
    (1, "Alice", None),
    (2, "Bob", 1),
    (3, "Carol", 2),
    (4, "Dave", 1)
]


@pytest.fixture
def spark_session():
    """
    Create a mock SparkSession object.
    :return: A mock SparkSession object
    :rtype: MagicMock
    """
    return MagicMock(spec=SparkSession)


@pytest.fixture
def mock_dataframe(spark_session):
    """
    Create a mock DataFrame object.
    :param spark_session: The mock SparkSession object
    :return: A mock DataFrame object
    :rtype: MagicMock
    """
    schema = StructType([
        StructField("ID", IntegerType(), True),
        StructField("Name", StringType(), True),
        StructField("Boss", IntegerType(), True)
    ])
    return spark_session.createDataFrame(test_data, schema)


def test_sql_query_content(spark_session, mock_dataframe):
    """
    Test if the SQL query contains the expected clauses.
    :param spark_session: The mock SparkSession object
    :param mock_dataframe: The mock DataFrame object
    :return: None
    :rtype: None
    """
    with patch('src.spark_session.SparkSessionManager.get_instance', return_value=spark_session):
        spark_session.createDataFrame.return_value = mock_dataframe

        process_employee_data()

        # Check if the SQL query contains expected clauses
        sql_query = spark_session.sql.call_args[0][0]
        assert "SELECT e.Name AS Employee" in sql_query
        assert "COALESCE(b.Name, 'No Boss') AS Boss" in sql_query
        assert "FROM employees e" in sql_query
        assert "LEFT JOIN employees b ON e.Boss = b.ID" in sql_query
        assert "ORDER BY e.ID" in sql_query
