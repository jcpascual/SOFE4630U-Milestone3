import json
import os
from google.cloud import pubsub_v1
from google.auth import jwt

service_account_info = json.load(open("cred.json"))
audience = "https://pubsub.googleapis.com/google.pubsub.v1.Subscriber"

credentials = jwt.Credentials.from_service_account_info(
    service_account_info, audience=audience
)

subscription_name = 'projects/{project_id}/subscriptions/{sub}'.format(
    project_id='cool-furnace-413323',
    sub='smartMeter_ms2-sub'
)

def callback(message):
    print(message.data)
    message.ack()

with pubsub_v1.SubscriberClient(credentials=credentials) as subscriber:
    try:
        while True:
            future = subscriber.subscribe(subscription_name, callback)
            future.result()
    except KeyboardInterrupt:
        pass
