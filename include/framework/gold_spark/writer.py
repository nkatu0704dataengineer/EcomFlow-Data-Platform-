"""
Gold Data Writer

Responsible for:
- Writing Spark DataFrame to Gold Delta tables

Author: Ngo Quang Tu
Project: EcomFlow
"""

from pyspark.sql import DataFrame


class GoldWriter:
    """
    Write Spark DataFrame into Gold Delta tables.
    """

    def write(
        self,
        df: DataFrame,
        catalog: str,
        schema: str,
        table: str,
        mode: str = "overwrite",
    ) -> None:
        """
        Write DataFrame to a Delta table.

        Parameters
        ----------
        df : DataFrame
            DataFrame to persist.

        catalog : str
            Target catalog.

        schema : str
            Target schema.

        table : str
            Target table.

        mode : str, default="overwrite"
            Spark write mode.
        """

        (
            df.write
            .format("delta")
            .mode(mode)
            .option("overwriteSchema", "true")
            .saveAsTable(
                f"{catalog}.{schema}.{table}"
            )
        )