
import json
import time
import requests
from kafka import KafkaProducer

USGS_URL = "https://earthquake.usgs.gov/earthquakes/feed/v1.0/summary/all_hour.geojson"
KAFKA_SERVER = "localhost:9094"
TOPIC = "earthquakes"
POLL_INTERVAL = 60

producer = KafkaProducer(
    bootstrap_servers=KAFKA_SERVER,
    value_serializer=lambda value: json.dumps(value).encode("utf-8")
)

seen_ids = set()

def fetch_earthquakes():
    response = requests.get(USGS_URL, timeout=30)
    response.raise_for_status()
    return response.json().get("features", [])

def publish_new_events():
    events = fetch_earthquakes()
    published = 0

    for event in events:
        event_id = event.get("id")

        if not event_id or event_id in seen_ids:
            continue

        properties = event.get("properties", {})
        coordinates = event.get("geometry", {}).get("coordinates", [None, None, None])

        earthquake = {
            "id": event_id,
            "magnitude": properties.get("mag"),
            "place": properties.get("place"),
            "time": properties.get("time"),
            "longitude": coordinates[0],
            "latitude": coordinates[1],
            "depth": coordinates[2],
            "source": "USGS"
        }

        producer.send(TOPIC, value=earthquake)
        seen_ids.add(event_id)
        published += 1

    producer.flush()
    print(f"Fetched {len(events)} events; published {published} new events.")

if __name__ == "__main__":
    try:
        while True:
            try:
                publish_new_events()
            except requests.RequestException as error:
                print(f"USGS request failed: {error}")
            except Exception as error:
                print(f"Error publishing events: {error}")

            time.sleep(POLL_INTERVAL)
    except KeyboardInterrupt:
        print("Stopping USGS producer...")
    finally:
        producer.close()
