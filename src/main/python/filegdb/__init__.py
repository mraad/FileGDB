import os
from typing import List
from pyspark.sql import SparkSession


class FileGDB:
    """Python class that wraps spark/scala functions.
    """

    @staticmethod
    def list_table_names(path: str) -> List[str]:
        """List all the tables in a file geo database.

        ``
        from pathlib import Path
        from filegdb import FileGDB

        file_gdb = str(Path(Path.home(), "AISData", "Miami.gdb"))
        list(FileGDB.list_table_names(file_gdb))
        ``

        :param path: Path to the file geo database.
        :return: list of tables.
        """
        _spark = SparkSession.builder.getOrCreate()
        return _spark._jvm.com.esri.gdb.FileGDB.pyListTableNames(os.path.expanduser(path))
