"""
Gold Query Executor

Responsible for:
- Loading Spark SQL files
- Executing Spark SQL
- Returning Spark DataFrame

Author: Ngo Quang Tu
Project: EcomFlow
"""

from pathlib import Path

from pyspark.sql import DataFrame
from pyspark.sql import SparkSession


class QueryExecutor:
    """
    Execute Spark SQL queries stored in .sql files.
    """

    def __init__(
        self,
        spark: SparkSession,
        sql_directory: Path,
    ) -> None:
        """
        Parameters
        ----------
        spark : SparkSession
            Active Spark session.

        sql_directory : Path
            Directory containing SQL files.
        """
        self._spark = spark
        self._sql_directory = sql_directory

    def execute(
        self,
        query_name: str,
    ) -> DataFrame:
        """
        Execute a SQL query by name.

        Parameters
        ----------
        query_name : str
            SQL file name without '.sql' extension.

        Returns
        -------
        DataFrame
            Query result as Spark DataFrame.
        """
        sql = self._load_sql(query_name)

        return self._spark.sql(sql)

    def _load_sql(
        self,
        query_name: str,
    ) -> str:
        """
        Load SQL text from file.

        Parameters
        ----------
        query_name : str
            SQL file name without extension.

        Returns
        -------
        str
            SQL statement.
        """
        sql_path = self._sql_directory / f"{query_name}.sql"

        if not sql_path.exists():
            raise FileNotFoundError(
                f"SQL file not found: {sql_path}"
            )

        return sql_path.read_text(
            encoding="utf-8"
        )