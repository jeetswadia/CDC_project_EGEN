'''
The code abstact:
-- vm is configured to work on python:3.7
-- refer dockerfile

-- importing libs
-- API_Data is the link to the dataset used for the project
-- defining the class:
    - setting project id and pub/sub topic. this pub/sub topic is bridge between code and gcp services.
    - requests.sessions pinging the api and returning result of the connection
    - defing a callback 
    - publishing message to pub/sub topic
-- running the code fetching 2500 datapoints every second. initially it will take ~5 hours to fetch all the data 
    which is 42.5M after that it will take ~15 minuits as the data updates with ~2.5M datapoints every two weeks

for query contact me on swadiajeet@gmail.com
'''

from concurrent import futures
from google.cloud.pubsub_v1 import PublisherClient
from google.cloud.pubsub_v1.publisher.futures import Future
from requests import Session
import os

API_Data = "https://data.cdc.gov/resource/n8mc-b4w4.csv"


class CDCjeet_Topic:

    def __init__(self):
        self.project_id = 'cdcjeet'
        self.topic_id = 'cdcjeet_topic'
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
    #json file is the private key associated with IAM 
    os.environ['GOOGLE_APPLICATION_CREDENTIALS'] = 'cdcjeet-521e045bbf00.json'

    cdc_topic = CDCjeet_Topic()

    #initial data being too big, operating it piece by piece
    for i in range(2500):
        message = cdc_topic.ping_cdc_api()
        print(message) #optional 
        cdc_topic.publish_message_to_topic(message)
