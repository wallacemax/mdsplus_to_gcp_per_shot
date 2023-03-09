import datetime
from typing import Dict, Any

from airflow.plugins_manager import AirflowPlugin
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults
from google.cloud import bigtable
from google.cloud.bigtable.row_set import RowSet


class MigrateShotToBigtableOperator(BaseOperator):
    """
    Migrates a shot to Google Bigtable.
    """
    @apply_defaults
    def __init__(
        self,
        project_id: str,
        instance_id: str,
        table_id: str,
        mdsplus_tree_name: str,
        migrated_date_column: str = "migrated_date",
        **kwargs,
    ):
        super().__init__(**kwargs)
        self.project_id = project_id
        self.instance_id = instance_id
        self.table_id = table_id
        self.mdsplus_tree_name = mdsplus_tree_name
        self.migrated_date_column = migrated_date_column

    def execute(self, context: Dict[str, Any]):
        # Connect to Cloud Bigtable
        client = bigtable.Client(project=self.project_id, admin=True)
        instance = client.instance(self.instance_id)
        table = instance.table(self.table_id)

        # Get the first 'shotnumber' without a value for 'migrated_date'
        row_set = RowSet()
        row_set.add_row_range_from_keys(b"1", b"99999")
        row_set.limit = 1
        filter_ = f"NOT column:{self.migrated_date_column}"
        rows = table.read_rows(row_set=row_set, filter_=filter_)

        for row in rows:
            # Get shotnumber
            shotnumber = row.row_key.decode()

            # Migrate shot to Google Bigtable
            migrator = mdsplus_to_gcp_per_shot(self.project_id, self.instance_id, self.table_id)
            json_data = migrator.tree_to_json(int(shotnumber), self.mdsplus_tree_name)
            table_row = table.row(shotnumber.encode())
            table_row.set_cell(
                "cf1",
                "data",
                json_data.encode(),
            )
            table_row.set_cell(
                "cf1",
                self.migrated_date_column,
                datetime.datetime.now().isoformat().encode(),
            )
            table_row.commit()

            self.log.info(f"Shot {shotnumber} migrated to Google Bigtable.")
            break  # Only migrate one shot at a time


class MdsplusToGcpPlugin(AirflowPlugin):
    name = "mdsplus_to_gcp_plugin"
    operators = [MigrateShotToBigtableOperator]
