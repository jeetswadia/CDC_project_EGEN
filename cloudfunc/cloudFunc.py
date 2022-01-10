import base64
import logging
from pandas import DataFrame
from json import loads
from google.cloud.storage import Client
from google.cloud import storage

class LoadToStorage:
    def __init__(self, event, context):
        self.event = event
        self.context = context
        self.bucket_name = "cdc_covid_bucket"
   
    def get_message_data(self) -> str:

        logging.info(
            f"This function was triggered by messageId {self.context.event_id} published at {self.context.timestamp} "
            f"to {self.context.resource['name']}"
            )
        print(self.event)
        print(self.context)

        if "data" in self.event:
            pubsub_message = base64.b64decode(self.event["data"]).decode("utf-8")
            logging.info(pubsub_message)
            print(pubsub_message)
            return pubsub_message
        else:
            logging.error("Incorrect format")
            return ""     

    def transform_payload_to_dataframe(self, message: str) -> DataFrame:
        try:
            df = DataFrame(loads(message))
            print(df)
            if not df.empty:
                logging.info(f"Created DataFrame with {df.shape[0]} rows and {df.shape[1]} columns")
            else:
                logging.warning(f"Created empty DataFrame")
            return df
        except Exception as e:
            logging.error(f"Encountered error created DataFarme - {str(e)}")
            raise

    def upload_to_bucket(self, df: DataFrame, file_name: str = "report_case") -> None:
        storage_client = storage.Client()
        print(self.bucket_name)
        bucket = storage_client.bucket(self.bucket_name)
        blob = bucket.blob(f"{file_name}.csv")
        blob.upload_from_string(data=df.to_csv(index=False), content_type="text/csv")
        logging.info(f"File uploaded to {self.bucket_name}")

def process(event, context):

    admin = logging.getLogger()
    admin.setLevel(logging.INFO)

    data_upload = LoadToStorage(event, context)

    message = data_upload.get_message_data()
    upload_df = data_upload.transform_payload_to_dataframe(message)

    data_upload.upload_to_bucket(upload_df, "CDC_Covid_Report")

