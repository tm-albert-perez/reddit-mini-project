import os
import sqlite3

from dagster import ConfigurableResource


class SQLiteResource(ConfigurableResource):
    db_path: str

    def connect(self):
        if not os.path.exists(self.db_path):
            os.makedirs(os.path.dirname(self.db_path), exist_ok=True)
            with open(self.db_path, "w"):
                pass
        return sqlite3.connect(self.db_path)
