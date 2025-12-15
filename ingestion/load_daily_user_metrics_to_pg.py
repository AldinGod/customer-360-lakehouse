import os
from typing import List

import pandas as pd
from sqlalchemy import create_engine, text

GOLD_DAILY_USER_METRICS_PATH = "data/gold/daily_user_metrics"

PG_HOST = os.environ.get("PG_HOST", "localhost")
PG_PORT = int(os.environ.get("PG_PORT", "5433"))
PG_DB = os.environ.get("PG_DB", "customer360")
PG_USER = os.environ.get("PG_USER", "customer360")
PG_PASSWORD = os.environ.get("PG_PASSWORD", "customer360")


def get_engine():
    url = (
        f"postgresql+psycopg2://{PG_USER}:{PG_PASSWORD}"
        f"@{PG_HOST}:{PG_PORT}/{PG_DB}"
    )
    return create_engine(url)


def load_parquet_to_df() -> pd.DataFrame:
    """
    Čita sve Parquet fajlove iz gold directoryja kao jedan DataFrame.
    """
    print(f"[loader] Reading Parquet from {GOLD_DAILY_USER_METRICS_PATH} ...")
    df = pd.read_parquet(GOLD_DAILY_USER_METRICS_PATH)

    print("[loader] Preview of DataFrame:")
    print(df.head())

    # kolone koje očekujemo iz Spark gold joba
    expected_cols: List[str] = [
        "event_date",
        "user_id",
        "events_count",
        "page_view_count",
        "login_count",
        "add_to_cart_count",
        "checkout_count",
        "error_count",
        "purchase_events",
        "error_events",
        "total_amount",
        "first_event_ts",
        "last_event_ts",
    ]

    missing = [c for c in expected_cols if c not in df.columns]
    if missing:
        raise ValueError(f"Missing expected columns in Parquet DF: {missing}")

    # event_date -> date
    df["event_date"] = pd.to_datetime(df["event_date"]).dt.date

    # timestamp kolone
    df["first_event_ts"] = pd.to_datetime(df["first_event_ts"])
    df["last_event_ts"] = pd.to_datetime(df["last_event_ts"])

    return df[expected_cols]


def delete_existing_dates(engine, dates):
    """
    Briše postojeće redove za iste event_date vrijednosti (da izbjegnemo duplikate).
    """
    if not dates:
        return

    print(f"[loader] Deleting existing rows for dates: {dates}")
    with engine.begin() as conn:
        for d in dates:
            conn.execute(
                text("DELETE FROM daily_user_metrics WHERE event_date = :d"),
                {"d": d},
            )


def load_to_postgres(df: pd.DataFrame):
    engine = get_engine()

    # unique datumi koje reloadamo
    unique_dates = sorted(df["event_date"].unique())
    delete_existing_dates(engine, unique_dates)

    print(f"[loader] Inserting {len(df)} rows into daily_user_metrics ...")
    df.to_sql(
        "daily_user_metrics",
        engine,
        if_exists="append",
        index=False,
    )
    print("[loader] Load completed.")


def main():
    df = load_parquet_to_df()
    load_to_postgres(df)


if __name__ == "__main__":
    main()

