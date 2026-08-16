# ============================================================
# VEHICLE SALES DATA ENGINEERING PROJECT
# GOLD LAYER
#
# Silver -> Gold using PySpark
#
# Environment:
#   Windows 11
#   Java 17
#   PySpark 4.2.x
#   Hadoop 3.5.x
#
# Input:
#   data/silver/vehicle_sales_safe.csv
#
# Output:
#   data/gold/
#
# Gold datasets:
#   1. sales_summary
#   2. make_performance
#   3. state_performance
#   4. monthly_sales
#   5. price_category_summary
# ============================================================


import os
import sys
import shutil
import csv
from pathlib import Path


# ============================================================
# WINDOWS HADOOP CONFIGURATION
# ============================================================

HADOOP_HOME = r"C:\hadoop"
WINUTILS = r"C:\hadoop\bin\winutils.exe"

os.environ["HADOOP_HOME"] = HADOOP_HOME
os.environ["hadoop.home.dir"] = HADOOP_HOME

if os.path.exists(WINUTILS):

    os.environ["PATH"] = (
        rf"{HADOOP_HOME}\bin;"
        + os.environ.get("PATH", "")
    )

else:

    print("WARNING: winutils.exe not found:")
    print(WINUTILS)


# ============================================================
# PYSPARK IMPORTS
# ============================================================

from pyspark.sql import SparkSession

from pyspark.sql.functions import (
    col,
    count,
    avg,
    sum as spark_sum,
    min as spark_min,
    max as spark_max,
    round as spark_round,
    when,
    lit,
    countDistinct,
    desc,
    asc
)

from pyspark.sql.types import (
    IntegerType,
    DoubleType,
    LongType
)


# ============================================================
# PROJECT PATHS
# ============================================================

PROJECT_ROOT = Path(__file__).resolve().parent.parent


SILVER_CSV_PATH = (
    PROJECT_ROOT
    / "data"
    / "silver"
    / "vehicle_sales_safe.csv"
)


GOLD_PATH = (
    PROJECT_ROOT
    / "data"
    / "gold"
)


GOLD_CSV_PATH = (
    PROJECT_ROOT
    / "data"
    / "gold_csv"
)


# ============================================================
# GOLD OUTPUT PATHS
# ============================================================

SALES_SUMMARY_PATH = (
    GOLD_PATH
    / "sales_summary"
)


MAKE_PERFORMANCE_PATH = (
    GOLD_PATH
    / "make_performance"
)


STATE_PERFORMANCE_PATH = (
    GOLD_PATH
    / "state_performance"
)


MONTHLY_SALES_PATH = (
    GOLD_PATH
    / "monthly_sales"
)


PRICE_CATEGORY_PATH = (
    GOLD_PATH
    / "price_category_summary"
)


# ============================================================
# SAFE CSV OUTPUT PATHS
# ============================================================

SALES_SUMMARY_CSV = (
    GOLD_CSV_PATH
    / "sales_summary.csv"
)


MAKE_PERFORMANCE_CSV = (
    GOLD_CSV_PATH
    / "make_performance.csv"
)


STATE_PERFORMANCE_CSV = (
    GOLD_CSV_PATH
    / "state_performance.csv"
)


MONTHLY_SALES_CSV = (
    GOLD_CSV_PATH
    / "monthly_sales.csv"
)


PRICE_CATEGORY_CSV = (
    GOLD_CSV_PATH
    / "price_category_summary.csv"
)


# ============================================================
# CREATE DIRECTORIES
# ============================================================

GOLD_PATH.mkdir(
    parents=True,
    exist_ok=True
)

GOLD_CSV_PATH.mkdir(
    parents=True,
    exist_ok=True
)


# ============================================================
# START
# ============================================================

print()
print("=" * 70)
print("                 VEHICLE SALES GOLD LAYER")
print("=" * 70)


# ============================================================
# START SPARK
# ============================================================

print()
print("========== STARTING SPARK ==========")


spark = (
    SparkSession.builder
    .appName("Vehicle Sales Gold Layer")
    .master("local[*]")

    # --------------------------------------------------------
    # Windows Hadoop configuration
    # --------------------------------------------------------

    .config(
        "spark.hadoop.hadoop.home.dir",
        HADOOP_HOME
    )

    .config(
        "spark.hadoop.fs.file.impl",
        "org.apache.hadoop.fs.RawLocalFileSystem"
    )

    .config(
        "spark.hadoop.fs.file.impl.disable.cache",
        "true"
    )

    .config(
        "spark.hadoop.io.native.lib.available",
        "false"
    )

    .config(
        "spark.hadoop.hadoop.native.lib",
        "false"
    )

    # --------------------------------------------------------
    # Hadoop output committer
    # --------------------------------------------------------

    .config(
        "spark.hadoop.mapreduce.fileoutputcommitter.algorithm.version",
        "2"
    )

    # --------------------------------------------------------
    # Spark configuration
    # --------------------------------------------------------

    .config(
        "spark.sql.shuffle.partitions",
        "8"
    )

    .config(
        "spark.default.parallelism",
        "8"
    )

    .config(
        "spark.sql.debug.maxToStringFields",
        "100"
    )

    .getOrCreate()
)


spark.sparkContext.setLogLevel("WARN")


# ============================================================
# CHECK SILVER FILE
# ============================================================

print()
print("========== CHECKING SILVER INPUT ==========")


if not SILVER_CSV_PATH.exists():

    print()
    print("ERROR: Silver CSV not found!")

    print()
    print("Expected path:")
    print(SILVER_CSV_PATH)

    print()
    print("Please run:")
    print("python src/vehicle_sales_etl.py")

    spark.stop()

    sys.exit(1)


print()
print("Silver input:")
print(SILVER_CSV_PATH)


# ============================================================
# READ SILVER DATA
# ============================================================

print()
print("========== READING SILVER DATA ==========")


df = (
    spark.read
    .option("header", "true")
    .option("inferSchema", "true")
    .option("mode", "PERMISSIVE")
    .csv(str(SILVER_CSV_PATH))
)


silver_count = df.count()


print()
print(
    f"Silver record count: "
    f"{silver_count:,}"
)


print()
print("Silver Schema:")
df.printSchema()


# ============================================================
# STANDARDIZE TYPES
# ============================================================

print()
print("========== STANDARDIZING GOLD INPUT TYPES ==========")


integer_columns = [

    "vehicle_year",
    "vehicle_condition",
    "odometer",
    "mmr",
    "selling_price",
    "price_difference",
    "sale_year",
    "sale_month",
    "sale_day",
    "vehicle_age_at_sale"

]


for c in integer_columns:

    if c in df.columns:

        df = df.withColumn(
            c,
            col(c).cast(IntegerType())
        )


# ============================================================
# DOUBLE COLUMNS
# ============================================================

if "price_difference_percentage" in df.columns:

    df = df.withColumn(

        "price_difference_percentage",

        col(
            "price_difference_percentage"
        ).cast(DoubleType())

    )


# ============================================================
# CREATE MONTH NAME
# ============================================================

print()
print("========== CREATING MONTH NAME ==========")


df = df.withColumn(

    "sale_month_name",

    when(
        col("sale_month") == 1,
        "January"
    )
    .when(
        col("sale_month") == 2,
        "February"
    )
    .when(
        col("sale_month") == 3,
        "March"
    )
    .when(
        col("sale_month") == 4,
        "April"
    )
    .when(
        col("sale_month") == 5,
        "May"
    )
    .when(
        col("sale_month") == 6,
        "June"
    )
    .when(
        col("sale_month") == 7,
        "July"
    )
    .when(
        col("sale_month") == 8,
        "August"
    )
    .when(
        col("sale_month") == 9,
        "September"
    )
    .when(
        col("sale_month") == 10,
        "October"
    )
    .when(
        col("sale_month") == 11,
        "November"
    )
    .when(
        col("sale_month") == 12,
        "December"
    )
    .otherwise("Unknown")

)


# ============================================================
# CACHE GOLD INPUT
# ============================================================

df = df.cache()


print()
print(
    f"Cached Gold input records: "
    f"{df.count():,}"
)


# ============================================================
# ============================================================
# GOLD DATASET 1
# SALES SUMMARY
# ============================================================
# ============================================================

print()
print("=" * 70)
print("             GOLD 1 - SALES SUMMARY")
print("=" * 70)


sales_summary = df.select(

    count("*").alias(
        "total_sales"
    ),

    countDistinct(
        "vin"
    ).alias(
        "unique_vehicles"
    ),

    countDistinct(
        "make"
    ).alias(
        "unique_makes"
    ),

    countDistinct(
        "model"
    ).alias(
        "unique_models"
    ),

    countDistinct(
        "state"
    ).alias(
        "unique_states"
    ),

    spark_round(
        avg("selling_price"),
        2
    ).alias(
        "average_selling_price"
    ),

    spark_round(
        avg("mmr"),
        2
    ).alias(
        "average_mmr"
    ),

    spark_round(
        avg("odometer"),
        2
    ).alias(
        "average_odometer"
    ),

    spark_round(
        avg("vehicle_condition"),
        2
    ).alias(
        "average_vehicle_condition"
    ),

    spark_min(
        "selling_price"
    ).alias(
        "minimum_selling_price"
    ),

    spark_max(
        "selling_price"
    ).alias(
        "maximum_selling_price"
    ),

    spark_round(
        avg("price_difference"),
        2
    ).alias(
        "average_price_difference"
    ),

    spark_round(
        avg("price_difference_percentage"),
        2
    ).alias(
        "average_price_difference_percentage"
    ),

    spark_round(
        avg("vehicle_age_at_sale"),
        2
    ).alias(
        "average_vehicle_age"
    )

)


print()
print("Sales Summary:")

sales_summary.show(
    truncate=False
)


# ============================================================
# GOLD DATASET 2
# MAKE PERFORMANCE
# ============================================================

print()
print("=" * 70)
print("             GOLD 2 - MAKE PERFORMANCE")
print("=" * 70)


make_performance = (

    df

    .groupBy(
        "make"
    )

    .agg(

        count("*").alias(
            "total_sales"
        ),

        countDistinct(
            "model"
        ).alias(
            "unique_models"
        ),

        spark_round(
            avg("selling_price"),
            2
        ).alias(
            "average_selling_price"
        ),

        spark_round(
            avg("mmr"),
            2
        ).alias(
            "average_mmr"
        ),

        spark_round(
            avg("price_difference"),
            2
        ).alias(
            "average_price_difference"
        ),

        spark_round(
            avg("price_difference_percentage"),
            2
        ).alias(
            "average_price_difference_percentage"
        ),

        spark_round(
            avg("odometer"),
            2
        ).alias(
            "average_odometer"
        ),

        spark_round(
            avg("vehicle_condition"),
            2
        ).alias(
            "average_vehicle_condition"
        ),

        spark_min(
            "selling_price"
        ).alias(
            "minimum_selling_price"
        ),

        spark_max(
            "selling_price"
        ).alias(
            "maximum_selling_price"
        ),

        spark_sum(
            when(
                col("selling_price") > col("mmr"),
                1
            ).otherwise(0)
        ).alias(
            "sales_above_mmr"
        ),

        spark_sum(
            when(
                col("selling_price") < col("mmr"),
                1
            ).otherwise(0)
        ).alias(
            "sales_below_mmr"
        )

    )

    .withColumn(

        "sales_above_mmr_percentage",

        spark_round(

            col("sales_above_mmr")
            /
            col("total_sales")
            *
            100,

            2

        )

    )

    .orderBy(
        col("total_sales").desc()
    )

)


print()
print("Top Vehicle Makes:")

make_performance.show(
    20,
    truncate=False
)


# ============================================================
# GOLD DATASET 3
# STATE PERFORMANCE
# ============================================================

print()
print("=" * 70)
print("             GOLD 3 - STATE PERFORMANCE")
print("=" * 70)


state_performance = (

    df

    .groupBy(
        "state"
    )

    .agg(

        count("*").alias(
            "total_sales"
        ),

        countDistinct(
            "make"
        ).alias(
            "unique_makes"
        ),

        countDistinct(
            "model"
        ).alias(
            "unique_models"
        ),

        spark_round(
            avg("selling_price"),
            2
        ).alias(
            "average_selling_price"
        ),

        spark_round(
            avg("mmr"),
            2
        ).alias(
            "average_mmr"
        ),

        spark_round(
            avg("price_difference"),
            2
        ).alias(
            "average_price_difference"
        ),

        spark_round(
            avg("odometer"),
            2
        ).alias(
            "average_odometer"
        ),

        spark_round(
            avg("vehicle_condition"),
            2
        ).alias(
            "average_vehicle_condition"
        ),

        spark_min(
            "selling_price"
        ).alias(
            "minimum_selling_price"
        ),

        spark_max(
            "selling_price"
        ).alias(
            "maximum_selling_price"
        )

    )

    .orderBy(
        col("total_sales").desc()
    )

)


print()
print("Top States:")

state_performance.show(
    20,
    truncate=False
)


# ============================================================
# GOLD DATASET 4
# MONTHLY SALES
# ============================================================

print()
print("=" * 70)
print("             GOLD 4 - MONTHLY SALES")
print("=" * 70)


monthly_sales = (

    df

    .groupBy(

        "sale_year",
        "sale_month",
        "sale_month_name"

    )

    .agg(

        count("*").alias(
            "total_sales"
        ),

        countDistinct(
            "vin"
        ).alias(
            "unique_vehicles"
        ),

        spark_round(
            avg("selling_price"),
            2
        ).alias(
            "average_selling_price"
        ),

        spark_round(
            avg("mmr"),
            2
        ).alias(
            "average_mmr"
        ),

        spark_round(
            avg("price_difference"),
            2
        ).alias(
            "average_price_difference"
        ),

        spark_round(
            avg("price_difference_percentage"),
            2
        ).alias(
            "average_price_difference_percentage"
        ),

        spark_min(
            "selling_price"
        ).alias(
            "minimum_selling_price"
        ),

        spark_max(
            "selling_price"
        ).alias(
            "maximum_selling_price"
        )

    )

    .orderBy(

        col("sale_year").asc(),
        col("sale_month").asc()

    )

)


print()
print("Monthly Sales:")

monthly_sales.show(
    50,
    truncate=False
)


# ============================================================
# GOLD DATASET 5
# PRICE CATEGORY SUMMARY
# ============================================================

print()
print("=" * 70)
print("             GOLD 5 - PRICE CATEGORY")
print("=" * 70)


price_category_summary = (

    df

    .groupBy(
        "price_category"
    )

    .agg(

        count("*").alias(
            "total_sales"
        ),

        spark_round(
            avg("selling_price"),
            2
        ).alias(
            "average_selling_price"
        ),

        spark_round(
            avg("mmr"),
            2
        ).alias(
            "average_mmr"
        ),

        spark_round(
            avg("price_difference"),
            2
        ).alias(
            "average_price_difference"
        ),

        spark_round(
            avg("price_difference_percentage"),
            2
        ).alias(
            "average_price_difference_percentage"
        ),

        spark_round(
            avg("odometer"),
            2
        ).alias(
            "average_odometer"
        ),

        spark_round(
            avg("vehicle_condition"),
            2
        ).alias(
            "average_vehicle_condition"
        ),

        spark_min(
            "selling_price"
        ).alias(
            "minimum_selling_price"
        ),

        spark_max(
            "selling_price"
        ).alias(
            "maximum_selling_price"
        )

    )

    .orderBy(
        col("average_selling_price").asc()
    )

)


print()
print("Price Category Summary:")

price_category_summary.show(
    truncate=False
)


# ============================================================
# REMOVE OLD GOLD OUTPUTS
# ============================================================

print()
print("=" * 70)
print("             CLEANING OLD GOLD OUTPUTS")
print("=" * 70)


gold_parquet_paths = [

    SALES_SUMMARY_PATH,
    MAKE_PERFORMANCE_PATH,
    STATE_PERFORMANCE_PATH,
    MONTHLY_SALES_PATH,
    PRICE_CATEGORY_PATH

]


for output_path in gold_parquet_paths:

    if output_path.exists():

        print()
        print(
            f"Removing old output: "
            f"{output_path}"
        )

        try:

            shutil.rmtree(
                output_path
            )

        except Exception as e:

            print(
                "WARNING: Could not remove:"
            )

            print(
                output_path
            )

            print(e)


gold_csv_paths = [

    SALES_SUMMARY_CSV,
    MAKE_PERFORMANCE_CSV,
    STATE_PERFORMANCE_CSV,
    MONTHLY_SALES_CSV,
    PRICE_CATEGORY_CSV

]


for csv_path in gold_csv_paths:

    if csv_path.exists():

        try:

            csv_path.unlink()

        except Exception as e:

            print(
                f"WARNING: Could not remove "
                f"{csv_path}"
            )

            print(e)


# ============================================================
# FUNCTION
# WRITE PARQUET
# ============================================================

def write_parquet(
    dataframe,
    output_path,
    dataset_name
):

    print()
    print(
        f"========== WRITING {dataset_name} PARQUET =========="
    )

    print(
        f"Output: {output_path}"
    )

    try:

        (

            dataframe
            .coalesce(1)
            .write
            .mode("overwrite")
            .option(
                "compression",
                "snappy"
            )
            .parquet(
                str(output_path)
            )

        )

        print()
        print(
            f"SUCCESS: {dataset_name} Parquet created!"
        )

        return True

    except Exception as e:

        print()
        print(
            f"WARNING: {dataset_name} Parquet write failed."
        )

        print(
            type(e).__name__
        )

        print(
            str(e)[:1500]
        )

        return False


# ============================================================
# FUNCTION
# WRITE SAFE CSV USING PYTHON
# ============================================================

def write_safe_csv(
    dataframe,
    output_path,
    dataset_name
):

    print()
    print(
        f"========== WRITING {dataset_name} CSV =========="
    )

    print(
        f"Output: {output_path}"
    )

    try:

        if output_path.exists():

            output_path.unlink()


        rows = dataframe.toLocalIterator()


        with open(

            output_path,

            "w",

            newline="",

            encoding="utf-8"

        ) as file:

            writer = csv.writer(
                file
            )

            writer.writerow(
                dataframe.columns
            )

            row_count = 0

            for row in rows:

                writer.writerow(
                    list(row)
                )

                row_count += 1


        print()
        print(
            f"SUCCESS: {dataset_name} CSV created!"
        )

        print(
            f"Rows written: "
            f"{row_count:,}"
        )

        return True

    except Exception as e:

        print()
        print(
            f"ERROR: {dataset_name} CSV failed."
        )

        print(
            type(e).__name__
        )

        print(
            str(e)[:1500]
        )

        return False


# ============================================================
# WRITE GOLD DATASETS
# ============================================================

print()
print("=" * 70)
print("                 WRITING GOLD LAYER")
print("=" * 70)


# ------------------------------------------------------------
# SALES SUMMARY
# ------------------------------------------------------------

sales_summary_parquet = write_parquet(

    sales_summary,
    SALES_SUMMARY_PATH,
    "SALES SUMMARY"

)


if not sales_summary_parquet:

    write_safe_csv(

        sales_summary,
        SALES_SUMMARY_CSV,
        "SALES SUMMARY"

    )


# ------------------------------------------------------------
# MAKE PERFORMANCE
# ------------------------------------------------------------

make_performance_parquet = write_parquet(

    make_performance,
    MAKE_PERFORMANCE_PATH,
    "MAKE PERFORMANCE"

)


if not make_performance_parquet:

    write_safe_csv(

        make_performance,
        MAKE_PERFORMANCE_CSV,
        "MAKE PERFORMANCE"

    )


# ------------------------------------------------------------
# STATE PERFORMANCE
# ------------------------------------------------------------

state_performance_parquet = write_parquet(

    state_performance,
    STATE_PERFORMANCE_PATH,
    "STATE PERFORMANCE"

)


if not state_performance_parquet:

    write_safe_csv(

        state_performance,
        STATE_PERFORMANCE_CSV,
        "STATE PERFORMANCE"

    )


# ------------------------------------------------------------
# MONTHLY SALES
# ------------------------------------------------------------

monthly_sales_parquet = write_parquet(

    monthly_sales,
    MONTHLY_SALES_PATH,
    "MONTHLY SALES"

)


if not monthly_sales_parquet:

    write_safe_csv(

        monthly_sales,
        MONTHLY_SALES_CSV,
        "MONTHLY SALES"

    )


# ------------------------------------------------------------
# PRICE CATEGORY
# ------------------------------------------------------------

price_category_parquet = write_parquet(

    price_category_summary,
    PRICE_CATEGORY_PATH,
    "PRICE CATEGORY"

)


if not price_category_parquet:

    write_safe_csv(

        price_category_summary,
        PRICE_CATEGORY_CSV,
        "PRICE CATEGORY"

    )


# ============================================================
# GOLD OUTPUT VERIFICATION
# ============================================================

print()
print("=" * 70)
print("             GOLD OUTPUT VERIFICATION")
print("=" * 70)


datasets = [

    (
        "sales_summary",
        sales_summary,
        SALES_SUMMARY_PATH,
        SALES_SUMMARY_CSV
    ),

    (
        "make_performance",
        make_performance,
        MAKE_PERFORMANCE_PATH,
        MAKE_PERFORMANCE_CSV
    ),

    (
        "state_performance",
        state_performance,
        STATE_PERFORMANCE_PATH,
        STATE_PERFORMANCE_CSV
    ),

    (
        "monthly_sales",
        monthly_sales,
        MONTHLY_SALES_PATH,
        MONTHLY_SALES_CSV
    ),

    (
        "price_category_summary",
        price_category_summary,
        PRICE_CATEGORY_PATH,
        PRICE_CATEGORY_CSV
    )

]


for (

    dataset_name,
    dataframe,
    parquet_path,
    csv_path

) in datasets:

    print()
    print(
        f"Dataset: {dataset_name}"
    )

    row_count = dataframe.count()

    print(
        f"Rows: {row_count:,}"
    )

    if parquet_path.exists():

        parquet_files = list(

            parquet_path.glob(
                "*.parquet"
            )

        )

        print(
            f"Parquet files: "
            f"{len(parquet_files)}"
        )

        if len(parquet_files) > 0:

            print(
                "Parquet verification: SUCCESS"
            )

    elif csv_path.exists():

        file_size_mb = (

            csv_path.stat().st_size
            /
            (1024 * 1024)

        )

        print(
            "CSV verification: SUCCESS"
        )

        print(
            f"CSV size: "
            f"{file_size_mb:.2f} MB"
        )

    else:

        print(
            "ERROR: Output not found!"
        )


# ============================================================
# GOLD DATASET SCHEMAS
# ============================================================

print()
print("=" * 70)
print("             GOLD DATASET SCHEMAS")
print("=" * 70)


print()
print("----- SALES SUMMARY -----")

sales_summary.printSchema()


print()
print("----- MAKE PERFORMANCE -----")

make_performance.printSchema()


print()
print("----- STATE PERFORMANCE -----")

state_performance.printSchema()


print()
print("----- MONTHLY SALES -----")

monthly_sales.printSchema()


print()
print("----- PRICE CATEGORY SUMMARY -----")

price_category_summary.printSchema()


# ============================================================
# BUSINESS INSIGHTS
# ============================================================

print()
print("=" * 70)
print("                 BUSINESS INSIGHTS")
print("=" * 70)


# ------------------------------------------------------------
# TOP MAKE
# ------------------------------------------------------------

print()
print("Top 10 Makes by Sales:")


make_performance.select(

    "make",
    "total_sales",
    "average_selling_price",
    "average_price_difference"

).orderBy(

    col("total_sales").desc()

).show(

    10,
    truncate=False

)


# ------------------------------------------------------------
# TOP STATES
# ------------------------------------------------------------

print()
print("Top 10 States by Sales:")


state_performance.select(

    "state",
    "total_sales",
    "average_selling_price",
    "average_price_difference"

).orderBy(

    col("total_sales").desc()

).show(

    10,
    truncate=False

)


# ------------------------------------------------------------
# MONTH WITH HIGHEST SALES
# ------------------------------------------------------------

print()
print("Top 10 Sales Months:")


monthly_sales.select(

    "sale_year",
    "sale_month",
    "sale_month_name",
    "total_sales",
    "average_selling_price"

).orderBy(

    col("total_sales").desc()

).show(

    10,
    truncate=False

)


# ------------------------------------------------------------
# PRICE CATEGORY
# ------------------------------------------------------------

print()
print("Price Category Performance:")


price_category_summary.select(

    "price_category",
    "total_sales",
    "average_selling_price",
    "average_mmr"

).show(

    truncate=False

)


# ============================================================
# FINAL SUMMARY
# ============================================================

print()
print("=" * 70)
print("                 GOLD LAYER COMPLETED")
print("=" * 70)


print()

print(
    f"Silver records processed : "
    f"{silver_count:,}"
)


print()

print(
    "Gold datasets created:"
)


print(
    "1. sales_summary"
)


print(
    "2. make_performance"
)


print(
    "3. state_performance"
)


print(
    "4. monthly_sales"
)


print(
    "5. price_category_summary"
)


print()

print(
    "Gold Parquet location:"
)


print(
    GOLD_PATH
)


print()

print(
    "Gold CSV fallback location:"
)


print(
    GOLD_CSV_PATH
)


print()

print(
    "Medallion Architecture:"
)


print(
    "BRONZE -> SILVER -> GOLD"
)


print()

print(
    "Pipeline stages completed:"
)


print(
    "1. Silver data ingestion"
)


print(
    "2. Gold aggregations"
)


print(
    "3. Sales summary"
)


print(
    "4. Make performance"
)


print(
    "5. State performance"
)


print(
    "6. Monthly sales analysis"
)


print(
    "7. Price category analysis"
)


print(
    "8. Business metrics"
)


print(
    "9. Gold output validation"
)


print()

print(
    "Next project stages:"
)


print(
    "11. Airflow orchestration"
)


print(
    "12. Kafka streaming"
)


print(
    "13. Azure ADLS"
)


print(
    "14. Azure Data Factory"
)


print(
    "15. Azure Synapse"
)


print(
    "16. Power BI"
)


print()
print("=" * 70)


# ============================================================
# STOP SPARK
# ============================================================

df.unpersist()


spark.stop()


print()
print(
    "Spark stopped successfully."
)


print(
    "Vehicle Sales Gold Layer finished successfully."
)