from typing import List

from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.functions import (
    col,
    lit,
    when,
    to_timestamp,
    hour,
    date_format,
)

BRONZE_PATH = "data/bronze/app_events"
SILVER_PATH = "data/silver/app_events_clean"


ALLOWED_EVENT_TYPES: List[str] = [
    "login",
    "page_view",
    "add_to_cart",
    "checkout",
    "error",
]

ALLOWED_PAGES: List[str] = [
    "/",
    "/home",
    "/products",
    "/cart",
    "/checkout",
    "/support",
]


def create_spark_session() -> SparkSession:
    spark = (
        SparkSession.builder.appName("Customer360SilverAppEvents")
        .master("local[*]")
        .config("spark.sql.session.timeZone", "UTC")
        .getOrCreate()
    )

    spark.sparkContext.setLogLevel("WARN")
    return spark


def load_bronze(spark: SparkSession) -> DataFrame:
    """
    Učita bronze evente iz lokalnog Parquet patha.
    """
    df = spark.read.parquet(BRONZE_PATH)
    print("[silver_app_events] Loaded bronze data.")
    df.printSchema()
    return df


def clean_and_enrich(df: DataFrame) -> DataFrame:
    """
    Čišćenje i obogaćivanje:
    - enforce types
    - filtriranje invalidnih vrijednosti
    - dodavanje feature kolona
    """
    # 1) Tipovi kolona
    df = (
        df.withColumn("user_id", col("user_id").cast("long"))
        .withColumn("product_id", col("product_id").cast("long"))
        .withColumn("amount", col("amount").cast("double"))
        .withColumn("event_date", col("event_date").cast("string"))
    )

    # event_timestamp u pravi timestamp tip
    df = df.withColumn(
        "event_timestamp_ts",
        to_timestamp(col("event_timestamp")),
    )

    # 2) Filtriranje očito nevalidnih vrijednosti
    df = df.filter(col("user_id").isNotNull())
    df = df.filter(col("event_type").isin(ALLOWED_EVENT_TYPES))
    df = df.filter(col("page").isin(ALLOWED_PAGES))

    # 3) Feature kolone
    df = df.withColumn(
        "is_purchase_event",
        when(col("event_type") == lit("checkout"), lit(1)).otherwise(lit(0)),
    )

    df = df.withColumn(
        "is_error_event",
        when(col("event_type") == lit("error"), lit(1)).otherwise(lit(0)),
    )

    # Sat i dan u sedmici iz timestamp-a
    df = df.withColumn("event_hour", hour(col("event_timestamp_ts")))
    df = df.withColumn(
        "event_day_of_week",
        date_format(col("event_timestamp_ts"), "E"),  # Mon, Tue, ...
    )

    # 4) Odaberi i organizuj kolone
    ordered_cols = [
        "event_id",
        "user_id",
        "event_type",
        "event_timestamp",
        "event_timestamp_ts",
        "event_date",
        "page",
        "product_id",
        "amount",
        "error_code",
        "source",
        "is_purchase_event",
        "is_error_event",
        "event_hour",
        "event_day_of_week",
    ]

    df = df.select(*ordered_cols)

    print("[silver_app_events] After cleaning and enrichment:")
    df.printSchema()
    return df


def write_silver(df: DataFrame) -> None:
    """
    Zapis u silver zonu, partitionBy event_date.
    Koristimo append da možemo pokretati job više puta
    (u ozbiljnom sistemu bi vjerovatno imali idempotentniju logiku).
    """
    print(f"[silver_app_events] Writing to {SILVER_PATH} (partitioned by event_date)...")

    (
        df.write.mode("append")
        .partitionBy("event_date")
        .parquet(SILVER_PATH)
    )

    print("[silver_app_events] Write completed.")


def main():
    spark = create_spark_session()

    bronze_df = load_bronze(spark)
    silver_df = clean_and_enrich(bronze_df)
    silver_df.show(5, truncate=False)

    write_silver(silver_df)

    spark.stop()


if __name__ == "__main__":
    main()

