import json
from google.cloud import pubsub_v1

PROJECT_ID = "YOUR_GCP_PROJECT_ID"
TOPIC_ID = "sensores-topic"

publisher = pubsub_v1.PublisherClient()
topic_path = publisher.topic_path(PROJECT_ID, TOPIC_ID)

def webhook_iot(request):

    datos = request.get_json()

    print(datos)

    publisher.publish(
        topic_path,
        json.dumps(datos).encode("utf-8")
    )

    return {
        "status": "OK"
    }
