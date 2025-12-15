import os
import json
import time
import uuid
import random
from datetime import datetime, timezone

import boto3


# Konfiguracija za LocalStack SNS
SNS_ENDPOINT = "http://localhost:4566"
REGION = "eu-central-1"

# Naziv SNS topica (isti kao u init skripti)
TOPIC_NAME = "app-events-topic"


def get_sns_client():
    """
    Kreira SNS client koji pokazuje na LocalStack.
    """
    return boto3.client(
        "sns",
        endpoint_url=SNS_ENDPOINT,
        region_name=REGION,
        aws_access_key_id="test",        # LocalStack dummy kredencijali
        aws_secret_access_key="test",
    )


def ensure_topic(sns_client) -> str:
    """
    Osigurava da SNS topic postoji (create-topic je idempotent).
    Vraća TopicArn.
    """
    response = sns_client.create_topic(Name=TOPIC_NAME)
    return response["TopicArn"]


def generate_fake_event() -> dict:
    """
    Generiše jedan 'app event' za našu Customer 360 priču.
    """
    event_id = str(uuid.uuid4())
    user_id = random.randint(1, 500)  # 500 fiktivnih korisnika

    event_type = random.choice(
        ["login", "page_view", "add_to_cart", "checkout", "error"]
    )

    # ISO8601 timestamp u UTC
    event_timestamp = datetime.now(timezone.utc).isoformat()

    # Simulacija nekih polja
    page = random.choice(
        ["/", "/home", "/products", "/cart", "/checkout", "/support"]
    )

    product_id = None
    amount = None

    if event_type in ["add_to_cart", "checkout"]:
        product_id = random.randint(1000, 1100)
        amount = round(random.uniform(5.0, 300.0), 2)

    # Error event – neka bude malo rjeđi ali postoji
    error_code = None
    if event_type == "error":
        error_code = random.choice(["500_INTERNAL", "TIMEOUT", "VALIDATION_ERROR"])

    event = {
        "event_id": event_id,
        "user_id": user_id,
        "event_type": event_type,
        "event_timestamp": event_timestamp,
        "page": page,
        "product_id": product_id,
        "amount": amount,
        "error_code": error_code,
        "source": "web_app",
    }

    return event


def main():
    sns_client = get_sns_client()
    topic_arn = ensure_topic(sns_client)

    # Koliko eventova da pošaljemo (možeš promijeniti env varom)
    num_events = int(os.environ.get("NUM_EVENTS", "50"))
    delay_seconds = float(os.environ.get("EVENT_DELAY_SECONDS", "0.2"))

    print(f"[producer] Using topic ARN: {topic_arn}")
    print(f"[producer] Sending {num_events} events...")

    for i in range(num_events):
        event = generate_fake_event()
        message = json.dumps(event)

        sns_client.publish(
            TopicArn=topic_arn,
            Message=message,
        )

        print(f"[producer] Sent event {i+1}/{num_events}: {event['event_type']} (user {event['user_id']})")
        time.sleep(delay_seconds)

    print("[producer] Done.")


if __name__ == "__main__":
    main()

