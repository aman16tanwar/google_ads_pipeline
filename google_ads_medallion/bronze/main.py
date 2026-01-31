# ========================== #
#    CONFIGURATION SECTION   #
# ========================== #
import os
import pandas as pd
from dotenv import load_dotenv
from google.ads.googleads.client import GoogleAdsClient
from google.oauth2 import service_account
from pandas_gbq import to_gbq
from google.cloud import bigquery
from utils.bigquery_loader import load_to_bigquery
from utils.google_ads_client import fetch_enabled_accounts
# ✅ Set Google Cloud credentials
# os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = "googleads-bigquery.json"
load_dotenv()
DEVELOPER_TOKEN = os.getenv('GOOGLE_ADS_DEVELOPER_TOKEN')
GOOGLE_ADS_LOGIN_CUSTOMER_ID = os.getenv('GOOGLE_ADS_LOGIN_CUSTOMER_ID')
JSON_KEY_FILE_PATH = os.getenv('GOOGLE_ADS_JSON_KEY_FILE_PATH')
GOOGLE_ADS_IMPERSONATED_EMAIL = os.getenv('GOOGLE_ADS_IMPERSONATED_EMAIL')



GCP_PROJECT_ID = os.getenv("GCP_PROJECT_ID")
TABLE_ID = f"{GCP_PROJECT_ID}.{os.getenv('BIGQUERY_BRONZE_DATESET')}.{os.getenv('BIGQUERY_BRONZE_ALL_MAIN')}"


# ========================== #
#     DEVICE DATA FUNCTION   #
# ========================== #
def get_device_data(client, customer_id):
    ga_service = client.get_service("GoogleAdsService")
    query = """
        SELECT campaign.id, campaign.name, campaign.advertising_channel_type,
               segments.device, segments.date,
               metrics.impressions, metrics.clicks,
               metrics.cost_micros, metrics.all_conversions,
               metrics.all_conversions_value
        FROM campaign
        WHERE segments.date DURING LAST_30_DAYS
        
    """
    stream = ga_service.search_stream(customer_id=customer_id, query=query)
    device_data = []
    for batch in stream:
        for row in batch.results:
            device_data.append({
                'campaign_id': row.campaign.id,
                'campaign_name': row.campaign.name,
                'campaign_type': row.campaign.advertising_channel_type.name,
                'device': row.segments.device,
                'date': row.segments.date,
                'impressions': row.metrics.impressions,
                'clicks': row.metrics.clicks,
                'cost_micros': row.metrics.cost_micros,
                'all_conversions': row.metrics.all_conversions,
                'all_conversions_value': row.metrics.all_conversions_value
            })
    return device_data

# ========================== #
#   CONVERSION DATA FUNCTION #
# ========================== #
def get_conversion_data(client, customer_id):
    ga_service = client.get_service("GoogleAdsService")
    query = """
        SELECT campaign.name, campaign.id,
               segments.device, segments.date,
               segments.conversion_action_name,
               metrics.all_conversions, metrics.all_conversions_value
        FROM campaign
        WHERE segments.date DURING LAST_30_DAYS
        
        AND segments.conversion_action_name IS NOT NULL
    """
    stream = ga_service.search_stream(customer_id=customer_id, query=query)
    conversion_data = []
    for batch in stream:
        for row in batch.results:
            conversion_data.append({
                'campaign_name': row.campaign.name,
                'campaign_id': row.campaign.id,
                'device': row.segments.device,
                'date': row.segments.date,
                'conversion_action_name': row.segments.conversion_action_name,
                'all_conversions': row.metrics.all_conversions,
                'all_conversions_value': row.metrics.all_conversions_value
            })
    return conversion_data

# ========================== #
#         MAIN SCRIPT        #
# ========================== #

def main():
    accounts = fetch_enabled_accounts()
    final_dataframes = []

    for acc in accounts:
        acc_id, acc_name = acc["customer_id"], acc["name"]
        print(f"\n▶️ Processing account: {acc_id} - {acc_name}")

        try:
            client = GoogleAdsClient.load_from_dict({
                'client_customer_id': acc_id,
                'login_customer_id': GOOGLE_ADS_LOGIN_CUSTOMER_ID,
                'developer_token': DEVELOPER_TOKEN,
                'json_key_file_path': JSON_KEY_FILE_PATH,
                'impersonated_email': GOOGLE_ADS_IMPERSONATED_EMAIL,
                'use_proto_plus': True
            })

            df_device = pd.DataFrame(get_device_data(client, acc_id))
            df_conversion = pd.DataFrame(get_conversion_data(client, acc_id))

            if df_device.empty and df_conversion.empty:
                print(f"⚠️ No data for account {acc_id}, skipping.")
                continue

            
            
            df_final=pd.concat([df_device,df_conversion],ignore_index=True)

            df_final["account_id"] = acc_id
            df_final["account_name"] = acc_name

            final_dataframes.append(df_final)

        except Exception as e:
            print(f"❌ Error in account {acc_id}: {e}")

    if not final_dataframes: 
        print("❌ No valid data collected. Exiting.")
        return

    df_all = pd.concat(final_dataframes, ignore_index=True)



    load_to_bigquery(df_all,TABLE_ID)

   