import json
from datetime import datetime
from typing import List, Dict

import boto3
from pyspark.sql import SparkSession
from pyspark.sql.functions import col

# --- Konstante za LocalStack & SQS ---

REGION = "eu-central-1"
SQS_ENDPOINT = "http://localhost:4566"

QUEUE_NAME = "app-events-queue"

# Lokalni bronze path (za sada ne koristimo s3a, čisto lokalno)
BRONZE_LOCAL_PATH = "data/bronze/app_events"


def get_sqs_client():
    """
    Kreira SQS client za LocalStack.
    """
    return boto3.client(
        "sqs",
        endpoint_url=SQS_ENDPOINT,
        region_name=REGION,
        aws_access_key_id="test",
        aws_secret_access_key="test",
    )


def fetch_events_from_sqs(max_batches: int = 5, batch_size: int = 10) -> List[Dict]:
    """
    Čita poruke iz SQS queue-a (koji je subscribed na SNS topic),
    parsira SNS envelope i vraća listu event dictova.

    max_batches: koliko puta ćemo pozvati receive-message (da ne vrtimo beskonačno)
    batch_size: max broj poruka po pozivu
    """
    sqs = get_sqs_client()

    # Uzmi URL queue-a po imenu
    queue_url = sqs.get_queue_url(QueueName=QUEUE_NAME)["QueueUrl"]
    print(f"[bronze_loader] Using SQS queue: {queue_url}")

    all_events: List[Dict] = []

    for batch in range(max_batches):
        resp = sqs.receive_message(
            QueueUrl=queue_url,
            MaxNumberOfMessages=batch_size,
            WaitTimeSeconds=1,
        )

        messages = resp.get("Messages", [])
        if not messages:
            print(f"[bronze_loader] No more messages in batch {batch}.")
            break

        print(f"[bronze_loader] Received {len(messages)} messages in batch {batch}.")

        entries_to_delete = []

        for msg in messages:
            body = json.loads(msg["Body"])

            # Ovo je SNS envelope – pravi event je u field-u "Message"
            raw_event_str = body.get("Message")
            if not raw_event_str:
                continue

            event = json.loads(raw_event_str)

            # Dodaj event_date (YYYY-MM-DD) za particionisanje
            ts = event.get("event_timestamp")
            if ts:
                event_date = ts[:10]  # "2025-12-11"
            else:
                # fallback ako nema timestamp
                event_date = datetime.utcnow().strftime("%Y-%m-%d")

            event["event_date"] = event_date

            all_events.append(event)

            # pripremi za delete_message_batch
            entries_to_delete.append(
                {
                    "Id": msg["MessageId"],
                    "ReceiptHandle": msg["ReceiptHandle"],
                }
            )

        # Obriši poruke koje smo uspješno pročitali (da ih ne procesuiramo ponovo)
        if entries_to_delete:
            sqs.delete_message_batch(QueueUrl=queue_url, Entries=entries_to_delete)
            print(f"[bronze_loader] Deleted {len(entries_to_delete)} messages from queue.")

    print(f"[bronze_loader] Total events fetched: {len(all_events)}")
    return all_events


def create_spark_session() -> SparkSession:
    """
    Kreira jednostavan SparkSession (za lokalni filesystem).
    """
    spark = (
        SparkSession.builder.appName("Customer360BronzeLoader")
        .master("local[*]")
        .config("spark.sql.session.timeZone", "UTC")
        .getOrCreate()
    )

    spark.sparkContext.setLogLevel("WARN")
    return spark


def main():
    events = fetch_events_from_sqs()

    if not events:
        print("[bronze_loader] No events to process. Exiting.")
        return

    # Kreiraj SparkSession
    spark = create_spark_session()

    # Napravi DataFrame iz liste dictova
    df = spark.createDataFrame(events)

    # Osiguraj da imamo event_timestamp i event_date kolone
    if "event_timestamp" in df.columns:
        df = df.withColumn("event_timestamp", col("event_timestamp").cast("string"))
    if "event_date" in df.columns:
        df = df.withColumn("event_date", col("event_date").cast("string"))

    df.printSchema()
    df.show(5, truncate=False)

    output_path = BRONZE_LOCAL_PATH

    print(f"[bronze_loader] Writing to local path: {output_path} partitioned by event_date...")

    (
        df.write.mode("append")
        .partitionBy("event_date")
        .parquet(output_path)
    )

    print("[bronze_loader] Write completed.")
    spark.stop()


if __name__ == "__main__":
    main()
