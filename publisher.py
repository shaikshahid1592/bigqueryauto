import json
import time
from google.cloud import pubsub_v1

PROJECT_ID = "burnished-web-484613-t0"
TOPIC_ID = "house-price-topic"

publisher = pubsub_v1.PublisherClient()
topic_path = publisher.topic_path(PROJECT_ID, TOPIC_ID)

with open("houseprice.csv", "r") as f:

    next(f)  # skip header

    for line in f:

        price, bedrooms, bathrooms, sqft, floors = line.strip().split(",")

        data = {
            "price": float(price),
            "bedrooms": int(bedrooms),
            "bathrooms": float(bathrooms),
            "sqft_living": int(sqft),
            "floors": int(floors)
        }

        message = json.dumps(data).encode("utf-8")

        #publisher.publish(topic_path, message)

        future = publisher.publish(topic_path, message)
        print("Published:", data, "Message ID:", future.result())

        time.sleep(1)
