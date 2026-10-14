
from datetime import datetime

from airflow import DAG
from airflow.providers.databricks.operators.databricks import (
    DatabricksRunNowOperator,
)

with DAG(
    dag_id="medicine_inventory_pipeline",
    description="Medicine inventory incremental processing and Gold refresh",
    start_date=datetime(2025, 1, 1),
    schedule="0 */10 * * *",
    catchup=False,
    max_active_runs=1,
    tags=["medicine", "inventory", "incremental", "gold"],
) as dag:

    run_incremental = DatabricksRunNowOperator(
        task_id="run_incremental_processing",
        databricks_conn_id="databricks_default",
        job_id=119618881821218,
        wait_for_termination=True,
    )

    run_gold = DatabricksRunNowOperator(
        task_id="run_gold_job",
        databricks_conn_id="databricks_default",
        job_id=461500609304930,
        wait_for_termination=True,
    )

    run_incremental >> run_gold
