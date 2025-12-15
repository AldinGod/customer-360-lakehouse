from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.functions import (
    col,
    when,
    sum as spark_sum,
    count as spark_count,
    min as spark_min,
    max as spark_max,
)

SILVER_PATH = "data/silver/app_events_clean"
GOLD_DAILY_USER_METRICS_PATH = "data/gold/daily_user_metrics"


def create_spark_session() -> SparkSession:
    spark = (
        SparkSession.builder.appName("Customer360GoldDailyUserMetrics")
        .master("local[*]")
        .config("spark.sql.session.timeZone", "UTC")
        .getOrCreate()
    )

    spark.sparkContext.setLogLevel("WARN")
    return spark


def load_silver(spark: SparkSession) -> DataFrame:
    """
    Učita clean app events iz silver zone.
    """
    df = spark.read.parquet(SILVER_PATH)
    print("[gold_daily_user_metrics] Loaded silver data.")
    df.printSchema()
    return df


def build_daily_user_metrics(df: DataFrame) -> DataFrame:
    """
    Pravi dnevne metrike po korisniku.
    """
    # Osiguraj da su nam potrebne kolone tu
    required_cols = [
        "event_date",
        "user_id",
        "event_type",
        "event_timestamp_ts",
        "amount",
        "is_error_event",
        "is_purchase_event",
    ]
    missing = [c for c in required_cols if c not in df.columns]
    if missing:
        raise ValueError(f"Missing required columns in silver DF: {missing}")

    # Agregacije po (event_date, user_id)
    grouped = (
        df.groupBy("event_date", "user_id")
        .agg(
            spark_count("*").alias("events_count"),
            spark_sum(
                when(col("event_type") == "page_view", 1).otherwise(0)
            ).alias("page_view_count"),
            spark_sum(
                when(col("event_type") == "login", 1).otherwise(0)
            ).alias("login_count"),
            spark_sum(
                when(col("event_type") == "add_to_cart", 1).otherwise(0)
            ).alias("add_to_cart_count"),
            spark_sum(
                when(col("event_type") == "checkout", 1).otherwise(0)
            ).alias("checkout_count"),
            spark_sum(
                when(col("event_type") == "error", 1).otherwise(0)
            ).alias("error_count"),
            spark_sum(col("is_purchase_event")).alias("purchase_events"),
            spark_sum(col("is_error_event")).alias("error_events"),
            spark_sum(col("amount")).alias("total_amount"),
            spark_min(col("event_timestamp_ts")).alias("first_event_ts"),
            spark_max(col("event_timestamp_ts")).alias("last_event_ts"),
        )
    )

    print("[gold_daily_user_metrics] Built daily user metrics:")
    grouped.printSchema()
    return grouped


def write_gold(df: DataFrame) -> None:
    """
    Zapis u gold zonu, particionisano po event_date.
    """
    print(f"[gold_daily_user_metrics] Writing to {GOLD_DAILY_USER_METRICS_PATH} (partitioned by event_date)...")

    (
        df.write.mode("append")
        .partitionBy("event_date")
        .parquet(GOLD_DAILY_USER_METRICS_PATH)
    )

    print("[gold_daily_user_metrics] Write completed.")


def main():
    spark = create_spark_session()

    silver_df = load_silver(spark)
    daily_user_metrics_df = build_daily_user_metrics(silver_df)

    daily_user_metrics_df.show(10, truncate=False)

    write_gold(daily_user_metrics_df)

    spark.stop()


if __name__ == "__main__":
    main()

