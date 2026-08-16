# ============================================================
# VEHICLE SALES DATA ENGINEERING PROJECT
# Bronze -> Silver ETL using PySpark
#
# Environment:
#   Windows 11
#   Java 17
#   PySpark 4.x
#
# Pipeline:
#   Bronze CSV
#       ↓
#   Read
#       ↓
#   Standardize columns
#       ↓
#   Clean strings
#       ↓
#   Safe numeric casting
#       ↓
#   Safe date parsing
#       ↓
#   Data quality rules
#       ↓
#   Duplicate removal
#       ↓
#   Derived business columns
#       ↓
#   Silver Parquet
#       ↓
#   CSV fallback if Parquet fails
# ============================================================


# ============================================================
# IMPORTS
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
    trim,
    lower,
    upper,
    regexp_extract,
    when,
    year,
    month,
    dayofmonth,
    round as spark_round,
    sum as spark_sum,
    lit,
    make_date,
    count,
    avg,
    min as spark_min,
    max as spark_max,
    expr
)


# ============================================================
# PROJECT PATHS
# ============================================================

PROJECT_ROOT = Path(__file__).resolve().parent.parent

BRONZE_PATH = (
    PROJECT_ROOT
    / "data"
    / "bronze"
    / "car_prices.csv"
)

SILVER_PATH = (
    PROJECT_ROOT
    / "data"
    / "silver"
    / "vehicle_sales"
)

SILVER_CSV_PATH = (
    PROJECT_ROOT
    / "data"
    / "silver"
    / "vehicle_sales_csv"
)

SILVER_SAFE_CSV = (
    PROJECT_ROOT
    / "data"
    / "silver"
    / "vehicle_sales_safe.csv"
)


# ============================================================
# CREATE OUTPUT DIRECTORY
# ============================================================

SILVER_PATH.parent.mkdir(
    parents=True,
    exist_ok=True
)


# ============================================================
# START
# ============================================================

print()
print("=" * 70)
print("                 VEHICLE SALES ETL PIPELINE")
print("=" * 70)


# ============================================================
# START SPARK
# ============================================================

print()
print("========== STARTING SPARK ==========")

spark = (
    SparkSession.builder
    .appName("Vehicle Sales ETL Pipeline")
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
# CHECK BRONZE FILE
# ============================================================

if not BRONZE_PATH.exists():

    print()
    print("ERROR: Bronze file not found!")
    print()
    print("Expected path:")
    print(BRONZE_PATH)

    spark.stop()
    sys.exit(1)


print()
print("Bronze file:")
print(BRONZE_PATH)


# ============================================================
# READ BRONZE DATA
# ============================================================

print()
print("========== READING BRONZE DATA ==========")

df = (
    spark.read
    .option("header", "true")
    .option("inferSchema", "true")
    .option("mode", "PERMISSIVE")
    .csv(str(BRONZE_PATH))
)

bronze_count = df.count()

print(
    f"Bronze record count: "
    f"{bronze_count:,}"
)

print()
print("Original Schema:")
df.printSchema()


# ============================================================
# STANDARDIZE COLUMN NAMES
# ============================================================

print()
print("========== STANDARDIZING COLUMN NAMES ==========")

rename_map = {

    "year": "vehicle_year",

    "make": "make",

    "model": "model",

    "trim": "trim",

    "body": "body_type",

    "transmission": "transmission",

    "vin": "vin",

    "state": "state",

    "condition": "vehicle_condition",

    "odometer": "odometer",

    "color": "color",

    "interior": "interior",

    "seller": "seller",

    "mmr": "mmr",

    "sellingprice": "selling_price",

    "saledate": "sale_date"

}


for old_name, new_name in rename_map.items():

    if old_name in df.columns:

        if old_name != new_name:

            df = df.withColumnRenamed(
                old_name,
                new_name
            )


# ============================================================
# CLEAN STRING COLUMNS
# ============================================================

print()
print("========== CLEANING STRING COLUMNS ==========")

string_columns = [

    "make",
    "model",
    "trim",
    "body_type",
    "transmission",
    "vin",
    "state",
    "color",
    "interior",
    "seller",
    "sale_date"

]


for c in string_columns:

    if c in df.columns:

        df = df.withColumn(

            c,

            when(

                trim(
                    col(c).cast("string")
                ).isin(

                    "",
                    "null",
                    "NULL",
                    "None",
                    "NONE",
                    "N/A",
                    "NA",
                    "nan",
                    "NaN"

                ),

                lit(None)

            ).otherwise(

                trim(
                    col(c)
                )

            )

        )


# ============================================================
# STANDARDIZE STRING VALUES
# ============================================================

print()
print("========== STANDARDIZING STRING VALUES ==========")

if "make" in df.columns:

    df = df.withColumn(
        "make",
        upper(col("make"))
    )


if "model" in df.columns:

    df = df.withColumn(
        "model",
        trim(col("model"))
    )


if "body_type" in df.columns:

    df = df.withColumn(
        "body_type",
        lower(col("body_type"))
    )


if "transmission" in df.columns:

    df = df.withColumn(
        "transmission",
        lower(col("transmission"))
    )


if "state" in df.columns:

    df = df.withColumn(
        "state",
        upper(col("state"))
    )


if "color" in df.columns:

    df = df.withColumn(
        "color",
        lower(col("color"))
    )


if "interior" in df.columns:

    df = df.withColumn(
        "interior",
        lower(col("interior"))
    )


# ============================================================
# SAFE NUMERIC CASTING
#
# IMPORTANT:
# Spark 4.x strict casting can fail on values such as:
#
#   ''
#   'abc'
#   'N/A'
#
# try_cast() converts malformed values to NULL
# instead of crashing the entire ETL.
# ============================================================

print()
print("========== CASTING NUMERIC COLUMNS ==========")

numeric_columns = [

    "vehicle_year",
    "vehicle_condition",
    "odometer",
    "mmr",
    "selling_price"

]


for c in numeric_columns:

    if c in df.columns:

        df = df.withColumn(

            c,

            expr(
                f"try_cast(`{c}` as int)"
            )

        )


# ============================================================
# SALE DATE CLEANING
#
# Example source:
#
# Tue Dec 16 2014 12:00:00 GMT-0800 (PST)
#
# We extract:
#
# Year  = 2014
# Month = Dec
# Day   = 16
#
# Then create:
#
# 2014-12-16
#
# Invalid values become NULL.
# ============================================================

print()
print("========== CONVERTING SALE DATE ==========")


df = df.withColumn(

    "sale_date_raw",

    col("sale_date")

)


# ============================================================
# EXTRACT YEAR
# ============================================================

df = df.withColumn(

    "sale_year_text",

    regexp_extract(

        col("sale_date_raw"),

        r"^[A-Za-z]{3}\s+[A-Za-z]{3}\s+\d{1,2}\s+(\d{4})",

        1

    )

)


# ============================================================
# EXTRACT MONTH
# ============================================================

df = df.withColumn(

    "sale_month_text",

    regexp_extract(

        col("sale_date_raw"),

        r"^[A-Za-z]{3}\s+([A-Za-z]{3})\s+\d{1,2}\s+\d{4}",

        1

    )

)


# ============================================================
# EXTRACT DAY
# ============================================================

df = df.withColumn(

    "sale_day_text",

    regexp_extract(

        col("sale_date_raw"),

        r"^[A-Za-z]{3}\s+[A-Za-z]{3}\s+(\d{1,2})\s+\d{4}",

        1

    )

)


# ============================================================
# CONVERT MONTH NAME TO MONTH NUMBER
# ============================================================

df = df.withColumn(

    "sale_month_number",

    when(
        upper(col("sale_month_text")) == "JAN",
        1
    )

    .when(
        upper(col("sale_month_text")) == "FEB",
        2
    )

    .when(
        upper(col("sale_month_text")) == "MAR",
        3
    )

    .when(
        upper(col("sale_month_text")) == "APR",
        4
    )

    .when(
        upper(col("sale_month_text")) == "MAY",
        5
    )

    .when(
        upper(col("sale_month_text")) == "JUN",
        6
    )

    .when(
        upper(col("sale_month_text")) == "JUL",
        7
    )

    .when(
        upper(col("sale_month_text")) == "AUG",
        8
    )

    .when(
        upper(col("sale_month_text")) == "SEP",
        9
    )

    .when(
        upper(col("sale_month_text")) == "OCT",
        10
    )

    .when(
        upper(col("sale_month_text")) == "NOV",
        11
    )

    .when(
        upper(col("sale_month_text")) == "DEC",
        12
    )

    .otherwise(
        None
    )

)


# ============================================================
# SAFE DATE COMPONENT CASTING
# ============================================================

df = df.withColumn(

    "sale_year_number",

    expr(
        "try_cast(sale_year_text as int)"
    )

)


df = df.withColumn(

    "sale_day_number",

    expr(
        "try_cast(sale_day_text as int)"
    )

)


# ============================================================
# CREATE DATE
# ============================================================

df = df.withColumn(

    "sale_date",

    when(

        (col("sale_year_number") >= 1900)

        &

        (col("sale_year_number") <= 2100)

        &

        (col("sale_month_number").isNotNull())

        &

        (col("sale_day_number").isNotNull()),

        make_date(

            col("sale_year_number"),

            col("sale_month_number"),

            col("sale_day_number")

        )

    )

    .otherwise(
        None
    )

)


# ============================================================
# DROP TEMP DATE COLUMNS
# ============================================================

df = df.drop(

    "sale_date_raw",

    "sale_year_text",

    "sale_month_text",

    "sale_day_text",

    "sale_month_number",

    "sale_year_number",

    "sale_day_number"

)


# ============================================================
# DATE QUALITY CHECK
# ============================================================

print()
print("========== DATE QUALITY CHECK ==========")

invalid_date_count = (

    df

    .filter(
        col("sale_date").isNull()
    )

    .count()

)


print(
    f"Invalid / missing sale dates: "
    f"{invalid_date_count:,}"
)


# ============================================================
# DATA QUALITY RULES
# ============================================================

print()
print("========== APPLYING DATA QUALITY RULES ==========")

before_quality_count = df.count()


# ============================================================
# VEHICLE YEAR
# ============================================================

df = df.filter(

    col("vehicle_year").isNull()

    |

    (

        (col("vehicle_year") >= 1900)

        &

        (col("vehicle_year") <= 2030)

    )

)


# ============================================================
# ODOMETER
# ============================================================

df = df.filter(

    col("odometer").isNull()

    |

    (col("odometer") >= 0)

)


# ============================================================
# MMR
# ============================================================

df = df.filter(

    col("mmr").isNull()

    |

    (col("mmr") >= 0)

)


# ============================================================
# SELLING PRICE
# ============================================================

df = df.filter(

    col("selling_price").isNull()

    |

    (col("selling_price") >= 0)

)


# ============================================================
# VIN REQUIRED
# ============================================================

df = df.filter(

    col("vin").isNotNull()

)


# ============================================================
# MAKE REQUIRED
# ============================================================

df = df.filter(

    col("make").isNotNull()

)


# ============================================================
# MODEL REQUIRED
# ============================================================

df = df.filter(

    col("model").isNotNull()

)


# ============================================================
# SALE DATE REQUIRED
# ============================================================

df = df.filter(

    col("sale_date").isNotNull()

)


# ============================================================
# REMOVE EXACT DUPLICATES
# ============================================================

df = df.dropDuplicates()


# ============================================================
# QUALITY COUNT
# ============================================================

after_quality_count = df.count()

removed_records = (

    before_quality_count
    -
    after_quality_count

)


print()
print(
    f"Records removed during quality rules: "
    f"{removed_records:,}"
)


# ============================================================
# DERIVED COLUMNS
# ============================================================

print()
print("========== CREATING DERIVED COLUMNS ==========")


# ============================================================
# PRICE DIFFERENCE
# ============================================================

df = df.withColumn(

    "price_difference",

    when(

        col("selling_price").isNotNull()

        &

        col("mmr").isNotNull(),

        col("selling_price")
        -
        col("mmr")

    )

    .otherwise(
        None
    )

)


# ============================================================
# PRICE DIFFERENCE PERCENTAGE
# ============================================================

df = df.withColumn(

    "price_difference_percentage",

    when(

        col("mmr") > 0,

        spark_round(

            (

                (

                    col("selling_price")
                    -
                    col("mmr")

                )

                /

                col("mmr")

                *

                100

            ),

            2

        )

    )

    .otherwise(
        None
    )

)


# ============================================================
# SALE YEAR
# ============================================================

df = df.withColumn(

    "sale_year",

    year(
        col("sale_date")
    )

)


# ============================================================
# SALE MONTH
# ============================================================

df = df.withColumn(

    "sale_month",

    month(
        col("sale_date")
    )

)


# ============================================================
# SALE DAY
# ============================================================

df = df.withColumn(

    "sale_day",

    dayofmonth(
        col("sale_date")
    )

)


# ============================================================
# VEHICLE AGE
# ============================================================

df = df.withColumn(

    "vehicle_age_at_sale",

    when(

        col("sale_year").isNotNull()

        &

        col("vehicle_year").isNotNull(),

        col("sale_year")
        -
        col("vehicle_year")

    )

    .otherwise(
        None
    )

)


# ============================================================
# PRICE CATEGORY
# ============================================================

df = df.withColumn(

    "price_category",

    when(

        col("selling_price").isNull(),

        "Unknown"

    )

    .when(

        col("selling_price") < 5000,

        "Budget"

    )

    .when(

        col("selling_price") < 15000,

        "Mid Range"

    )

    .when(

        col("selling_price") < 30000,

        "Premium"

    )

    .otherwise(

        "Luxury"

    )

)


# ============================================================
# MILEAGE CATEGORY
# ============================================================

df = df.withColumn(

    "mileage_category",

    when(

        col("odometer").isNull(),

        "Unknown"

    )

    .when(

        col("odometer") < 30000,

        "Low Mileage"

    )

    .when(

        col("odometer") < 80000,

        "Medium Mileage"

    )

    .when(

        col("odometer") < 120000,

        "High Mileage"

    )

    .otherwise(

        "Very High Mileage"

    )

)


# ============================================================
# FINAL COLUMN ORDER
# ============================================================

print()
print("========== FINAL COLUMN ORDER ==========")

final_columns = [

    "vehicle_year",

    "make",

    "model",

    "trim",

    "body_type",

    "transmission",

    "vin",

    "state",

    "vehicle_condition",

    "odometer",

    "color",

    "interior",

    "seller",

    "mmr",

    "selling_price",

    "sale_date",

    "price_difference",

    "price_difference_percentage",

    "sale_year",

    "sale_month",

    "sale_day",

    "vehicle_age_at_sale",

    "price_category",

    "mileage_category"

]


df = df.select(

    *[

        c

        for c in final_columns

        if c in df.columns

    ]

)


# ============================================================
# SILVER DATA QUALITY
# ============================================================

print()
print("========== SILVER DATA QUALITY ==========")

silver_count = df.count()


print()
print(
    f"Bronze record count : "
    f"{bronze_count:,}"
)

print(
    f"Silver record count : "
    f"{silver_count:,}"
)

print(
    f"Records removed     : "
    f"{bronze_count - silver_count:,}"
)


# ============================================================
# NULL COUNTS
# ============================================================

print()
print("========== REMAINING NULL COUNTS ==========")

null_expressions = []


for c in df.columns:

    null_expressions.append(

        spark_sum(

            when(

                col(c).isNull(),

                1

            )

            .otherwise(
                0
            )

        ).alias(c)

    )


null_df = df.select(
    *null_expressions
)


null_df.show(
    truncate=False
)


# ============================================================
# SILVER STATISTICS
# ============================================================

print()
print("========== SILVER STATISTICS ==========")

df.select(

    "selling_price",

    "mmr",

    "odometer",

    "vehicle_condition"

).describe().show()


# ============================================================
# BUSINESS SUMMARY
# ============================================================

print()
print("========== BUSINESS SUMMARY ==========")

df.select(

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

        avg("odometer"),

        2

    ).alias(
        "average_odometer"
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

).show()


# ============================================================
# PRICE CATEGORY SUMMARY
# ============================================================

print()
print("========== PRICE CATEGORY SUMMARY ==========")

df.groupBy(

    "price_category"

).count().orderBy(

    "price_category"

).show()


# ============================================================
# TOP MAKES
# ============================================================

print()
print("========== TOP VEHICLE MAKES ==========")

df.groupBy(

    "make"

).count().orderBy(

    col("count").desc()

).show(
    10
)


# ============================================================
# SILVER SAMPLE
# ============================================================

print()
print("========== SILVER SAMPLE ==========")

df.show(

    10,

    truncate=False

)


# ============================================================
# REMOVE OLD OUTPUTS
# ============================================================

print()
print("========== PREPARING OUTPUT ==========")


for output_path in [

    SILVER_PATH,

    SILVER_CSV_PATH

]:

    if output_path.exists():

        print(
            f"Removing old output: "
            f"{output_path}"
        )

        try:

            if output_path.is_dir():

                shutil.rmtree(
                    output_path
                )

            else:

                output_path.unlink()

        except Exception as e:

            print(
                f"WARNING: Could not remove "
                f"{output_path}"
            )

            print(e)


# ============================================================
# REMOVE OLD SAFE CSV
# ============================================================

if SILVER_SAFE_CSV.exists():

    try:

        SILVER_SAFE_CSV.unlink()

    except Exception as e:

        print(
            "WARNING: Could not remove old CSV:"
        )

        print(e)


# ============================================================
# WRITE SILVER PARQUET
# ============================================================

print()
print("========== WRITING SILVER PARQUET ==========")

print(
    f"Output: "
    f"{SILVER_PATH}"
)


parquet_success = False


try:

    (

        df

        .coalesce(4)

        .write

        .mode("overwrite")

        .option(
            "compression",
            "snappy"
        )

        .parquet(
            str(SILVER_PATH)
        )

    )

    parquet_success = True

    print()
    print(
        "SUCCESS: Silver Parquet created!"
    )


except Exception as parquet_error:

    print()
    print(
        "WARNING: Parquet write failed."
    )

    print(
        "A CSV fallback will be created."
    )

    print()
    print("Error type:")

    print(
        type(parquet_error).__name__
    )

    print()
    print(
        str(parquet_error)[:1500]
    )


# ============================================================
# SAFE PYTHON CSV FALLBACK
# ============================================================

if not parquet_success:

    print()
    print(
        "========== PYTHON CSV FALLBACK =========="
    )

    print(
        "Writing Silver CSV..."
    )

    try:

        if SILVER_SAFE_CSV.exists():

            SILVER_SAFE_CSV.unlink()


        rows = df.toLocalIterator()


        with open(

            SILVER_SAFE_CSV,

            "w",

            newline="",

            encoding="utf-8"

        ) as file:

            writer = csv.writer(
                file
            )

            writer.writerow(
                df.columns
            )

            row_count = 0


            for row in rows:

                writer.writerow(
                    list(row)
                )

                row_count += 1


                if row_count % 50000 == 0:

                    print(
                        f"Written rows: "
                        f"{row_count:,}"
                    )


        print()
        print(
            "SUCCESS: Silver CSV created!"
        )

        print(
            f"Rows written: "
            f"{row_count:,}"
        )

        print(
            f"Path: "
            f"{SILVER_SAFE_CSV}"
        )


    except Exception as csv_error:

        print()
        print(
            "ERROR: CSV fallback failed."
        )

        print(
            type(csv_error).__name__
        )

        print(
            str(csv_error)[:2000]
        )

        spark.stop()

        sys.exit(1)


# ============================================================
# OUTPUT VERIFICATION
# ============================================================

print()
print("========== OUTPUT VERIFICATION ==========")


if parquet_success:

    try:

        parquet_files = list(

            SILVER_PATH.glob(
                "*.parquet"
            )

        )


        print(
            f"Parquet files created: "
            f"{len(parquet_files)}"
        )


        if len(parquet_files) > 0:

            print(
                "Parquet verification: SUCCESS"
            )

        else:

            print(
                "WARNING: No Parquet files found."
            )


    except Exception as e:

        print(
            "WARNING during verification:"
        )

        print(e)


else:

    if SILVER_SAFE_CSV.exists():

        file_size_mb = (

            SILVER_SAFE_CSV.stat().st_size

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
            "ERROR: Silver output not found."
        )


# ============================================================
# FINAL SUMMARY
# ============================================================

print()
print("=" * 70)
print("                    ETL COMPLETED")
print("=" * 70)


print()

print(
    f"Bronze records : "
    f"{bronze_count:,}"
)


print(
    f"Silver records : "
    f"{silver_count:,}"
)


print(
    f"Records removed: "
    f"{bronze_count - silver_count:,}"
)


print()


if parquet_success:

    print(
        "Silver format  : PARQUET"
    )

    print(
        f"Silver path    : "
        f"{SILVER_PATH}"
    )

else:

    print(
        "Silver format  : CSV"
    )

    print(
        f"Silver path    : "
        f"{SILVER_SAFE_CSV}"
    )


print()
print("Pipeline stages completed:")

print(
    "1. Bronze ingestion"
)

print(
    "2. Column standardization"
)

print(
    "3. String cleansing"
)

print(
    "4. Safe numeric type casting"
)

print(
    "5. Safe date parsing"
)

print(
    "6. Data quality rules"
)

print(
    "7. Duplicate removal"
)

print(
    "8. Derived business columns"
)

print(
    "9. Silver data validation"
)

print(
    "10. Silver output"
)


print()
print("Next project stages:")

print(
    "11. Gold layer"
)

print(
    "12. Airflow orchestration"
)

print(
    "13. Kafka streaming"
)

print(
    "14. Azure ADLS"
)

print(
    "15. Azure Data Factory"
)

print(
    "16. Azure Synapse"
)

print(
    "17. Power BI"
)


print()
print("=" * 70)


# ============================================================
# STOP SPARK
# ============================================================

spark.stop()


print()
print(
    "Spark stopped successfully."
)

print(
    "Vehicle Sales ETL finished successfully."
)