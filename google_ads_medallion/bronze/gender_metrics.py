# ========================== #
#    CONFIGURATION SECTION   #
# ========================== #
import os
import pandas as pd
from dotenv import load_dotenv
from google.ads.googleads.client import GoogleAdsClient

from utils.bigquery_loader import load_to_bigquery
from utils.google_ads_client import fetch_enabled_accounts
from utils.logger import setup_logger
# ✅ Set Google Cloud credentials
# os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = "googleads-bigquery.json"
logger=setup_logger(__name__)
load_dotenv()
DEVELOPER_TOKEN = os.getenv('GOOGLE_ADS_DEVELOPER_TOKEN')
GOOGLE_ADS_LOGIN_CUSTOMER_ID = os.getenv('GOOGLE_ADS_LOGIN_CUSTOMER_ID')
JSON_KEY_FILE_PATH = os.getenv('GOOGLE_ADS_JSON_KEY_FILE_PATH')

GOOGLE_ADS_IMPERSONATED_EMAIL = os.getenv('GOOGLE_ADS_IMPERSONATED_EMAIL')



GCP_PROJECT_ID = os.getenv("GCP_PROJECT_ID")
TABLE_ID = f"{GCP_PROJECT_ID}.{os.getenv('BIGQUERY_BRONZE_DATASET')}.{os.getenv('BIGQUERY_BRONZE_MAIN_CAMPAIGN_GENDER_METRICS')}"

GENDER_MAPPING = {
    10: "Male",
    11: "Female",
    20: "Unknown"
}


def get_gender_data(client, customer_id):
    ga_service = client.get_service("GoogleAdsService")
    query = """
      SELECT 
        gender_view.resource_name,
        campaign.id,
        campaign.name,
        campaign.advertising_channel_type,
        ad_group.id,
        ad_group.name,
        ad_group_criterion.gender.type,
        segments.date,
        metrics.impressions, 
        metrics.clicks, 
        metrics.cost_micros,
        metrics.all_conversions,
        metrics.all_conversions_value
      FROM gender_view
      WHERE segments.date DURING LAST_30_DAYS
    """

    stream = ga_service.search_stream(customer_id=customer_id, query=query)

    gender_data = []
    for batch in stream:
        for row in batch.results:
            gender_id = row.ad_group_criterion.gender.type
            gender_label = GENDER_MAPPING.get(gender_id, "Unknown")

            gender_data.append({
                'resource_name': row.gender_view.resource_name,
                'campaign_id': row.campaign.id,
                'campaign_name': row.campaign.name,
                'campaign_type': row.campaign.advertising_channel_type.name if row.campaign.advertising_channel_type else 'Unknown',
                'ad_group_id': row.ad_group.id,
                'ad_group_name': row.ad_group.name,
                'gender': gender_label, 
                'impressions': row.metrics.impressions,
                'clicks': row.metrics.clicks,
                'cost_micros': row.metrics.cost_micros,
                'all_conversions': row.metrics.all_conversions if row.metrics.all_conversions is not None else 0.0,
                'all_conversions_value': row.metrics.all_conversions_value if row.metrics.all_conversions_value is not None else 0.0,
                'date': row.segments.date
            })

    return gender_data



def main():
    accounts = fetch_enabled_accounts()
    final_dataframes = []

    for acc in accounts:
        acc_id = acc["customer_id"]
        acc_name = acc["name"]
        logger.info(f"\n▶️ Processing account: {acc_id} - {acc_name}")

        try:
            client = GoogleAdsClient.load_from_dict({
                'client_customer_id': acc_id,
                'login_customer_id': GOOGLE_ADS_LOGIN_CUSTOMER_ID,
                'developer_token': DEVELOPER_TOKEN,
                'json_key_file_path': JSON_KEY_FILE_PATH,
                'impersonated_email': GOOGLE_ADS_IMPERSONATED_EMAIL,
                'use_proto_plus': True,
            })

            df_gender = pd.DataFrame(get_gender_data(client, acc_id))
            
           
           
            
            df_gender["account_id"]=acc_id
            df_gender["account_name"]=acc_name
            logger.info(f"✅ After concat: {df_gender.shape} ")
            final_dataframes.append(df_gender)


        except Exception as e:
            logger.error(f"❌ Error in account {acc_id}: {e}")

    if not final_dataframes:
        logger.error(f"❌ No valid data collected. Exiting.")
        return

    # After concatenating all DataFrames
    df_all = pd.concat(final_dataframes, ignore_index=True)


  
    load_to_bigquery(df_all,TABLE_ID)


if __name__ == "__main__":                                                                   
    main()   





