from datetime import datetime, timedelta
from pathlib import Path

from airflow import DAG
from airflow.operators.bash import BashOperator


# ============================================================
# PROJECT PATH
# ============================================================

PROJECT_ROOT = Path(
    r"C:\Users\shriv\OneDrive\Desktop\Vehicle Sales Data"
)

PYTHON_EXE = r"python"


# ============================================================
# SCRIPT PATHS
# ============================================================

SILVER_SCRIPT = (
    PROJECT_ROOT
    / "src"
    / "vehicle_sales_etl.py"
)

GOLD_SCRIPT = (
    PROJECT_ROOT
    / "src"
    / "vehicle_sales_gold.py"
)


# ============================================================
# DEFAULT ARGUMENTS
# ============================================================

default_args = {

    "owner": "aman",

    "depends_on_past": False,

    "retries": 1,

    "retry_delay": timedelta(minutes=2),

}


# ============================================================
# DAG
# ============================================================

with DAG(

    dag_id="vehicle_sales_pipeline",

    description=(
        "Vehicle Sales Bronze to Silver to Gold "
        "Data Engineering Pipeline"
    ),

    default_args=default_args,

    start_date=datetime(2026, 8, 1),

    schedule="0 2 * * *",

    catchup=False,

    tags=[
        "vehicle-sales",
        "pyspark",
        "bronze-silver-gold",
        "data-engineering"
    ],

) as dag:


    # ========================================================
    # TASK 1 - SILVER ETL
    # ========================================================

    silver_etl = BashOperator(

        task_id="silver_etl",

        bash_command=(
            f'cd "{PROJECT_ROOT}" && '
            f'"{PYTHON_EXE}" "{SILVER_SCRIPT}"'
        ),

    )


    # ========================================================
    # TASK 2 - GOLD ETL
    # ========================================================

    gold_etl = BashOperator(

        task_id="gold_etl",

        bash_command=(
            f'cd "{PROJECT_ROOT}" && '
            f'"{PYTHON_EXE}" "{GOLD_SCRIPT}"'
        ),

    )


    # ========================================================
    # PIPELINE ORDER
    # ========================================================

    silver_etl >> gold_etl