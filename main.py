"""
Call the process_employee_data function from process_employee_data.py
"""

from src.process_employee_data import process_employee_data
from src.spark_session import SparkSessionManager

if __name__ == "__main__":
    try:
        process_employee_data()
    finally:
        # Ensure SparkSession is stopped even if an exception occurs
        SparkSessionManager.stop_instance()
