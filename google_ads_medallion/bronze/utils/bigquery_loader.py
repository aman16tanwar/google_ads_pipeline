# ========================== #
#    CONFIGURATION SECTION   #
# ========================== #
import os
import pandas as pd
from dotenv import load_dotenv
from google.oauth2 import service_account
from pandas_gbq import to_gbq
from google.cloud import bigquery
# ✅ Set Google Cloud credentials
# os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = "googleads-bigquery.json"

# ✅ Load environment variables
load_dotenv()


GCP_PROJECT_ID = os.getenv("GCP_PROJECT_ID")
TABLE_ID_main = f"{GCP_PROJECT_ID}.{os.getenv('BIGQUERY_DATASET_ALL_MAIN')}.{os.getenv('BIGQUERY_TABLE_ALL_MAIN')}"
TABLE_ID_location = f"{GCP_PROJECT_ID}.{os.getenv('BIGQUERY_DATASET_ALL_MAIN')}.{os.getenv('BIGQUERY_TABLE_ALL_LOCATION')}"
TABLE_ID_gender = f"{GCP_PROJECT_ID}.{os.getenv('BIGQUERY_DATASET_ALL_MAIN')}.{os.getenv('BIGQUERY_TABLE_ALL_GENDER')}"
TABLE_ID_age = f"{GCP_PROJECT_ID}.{os.getenv('BIGQUERY_DATASET_ALL_MAIN')}.{os.getenv('BIGQUERY_TABLE_ALL_AGE')}"



#main_upload


    # print("🔁 Uploading final dataframe of shape:", df_all.shape)

def bq_main():
    credentials = service_account.Credentials.from_service_account_file()
    bq_client = bigquery.Client(credentials=credentials, project=GCP_PROJECT_ID)
    query = f"""
        DELETE FROM `{TABLE_ID_main}`
        WHERE Date >= DATE_SUB(CURRENT_DATE(), INTERVAL 30 DAY)
    """
    bq_client.query(query).result()
    print("🧹 Deleted last 30 days from BigQuery before uploading new data.")

    to_gbq(
        df_all,
        destination_table=TABLE_ID,s
        project_id=GCP_PROJECT_ID,
        credentials=credentials,
        if_exists="append"  # Change to 'append' if you want to keep historical data
    )

    print(f"✅ Data uploaded to BigQuery: {TABLE_ID}")
    print(df_all.head(10))

