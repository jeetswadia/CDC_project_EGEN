from time import sleep
from concurrent import futures
from google.cloud.pubsub_v1 import PublisherClient
from google.cloud.pubsub_v1.publisher.futures import Future
from requests import Session
import os
import pandas as pd

API_Data = "https://data.cdc.gov/resource/n8mc-b4w4.csv"


class CDC_Covid_Topic:

    def __init__(self):
        self.project_id = 'cdccovidjeet'
        self.topic_id = 'cdc_covid_topic'
        self.publisher_client = PublisherClient()
        self.topic_path = self.publisher_client.topic_path(self.project_id, self.topic_id)
        self.publish_futures = []


    #function to ping CDC API
    def ping_cdc_api(self) -> str:
        
        session = Session()
        result = session.get(API_Data, stream=True)

        #status code between 200 and 400 is considered successful for api fetch
        if 200 <= result.status_code < 400:
            print(f"Response - {result.status_code}: {result.text}")
            return result.text
        else:
            raise Exception(f"Failed to fetch API data - {result.status_code}: {result.text}")

    def get_callback(self, publish_future: Future, data: str) -> callable:
        def callback(publish_future):
            try:
                print(publish_future.result(timeout=60))
            except futures.TimeoutError:
                print(f"Publishing {data} timed out.")
        return callback
    
    def publish_message_to_topic(self, message: str) -> None:
        publish_future = self.publisher_client.publish(self.topic_path, message.encode("utf-8"))
        publish_future.add_done_callback(self.get_callback(publish_future, message))
        self.publish_futures.append(publish_future)
        futures.wait(self.publish_futures, return_when=futures.ALL_COMPLETED)
        print(f"Published messages with error handler to {self.topic_path}.")

if __name__ == '__main__':
    os.environ['GOOGLE_APPLICATION_CREDENTIALS'] = 'cdccovidjeet-34146c45c9ab.json'

    cdc_topic = CDC_Covid_Topic()
    for i in range(250):
        message = cdc_topic.ping_cdc_api()
        print(message)
        cdc_topic.publish_message_to_topic(message)
        sleep(90)