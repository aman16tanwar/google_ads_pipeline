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
TABLE_ID = f"{GCP_PROJECT_ID}.{os.getenv('BIGQUERY_BRONZE_DATASET')}.{os.getenv('BIGQUERY_BRONZE_MAIN_CAMPAIGN_AGE')}"


AGE_RANGE_MAPPING = {
    503001: "18-24",
    503002: "25-34",
    503003: "35-44",
    503004: "45-54",
    503005: "55-64",
    503006: "65+",
    503999: "Unknown"
}



def get_age_range_data(client, customer_id):
    ga_service = client.get_service("GoogleAdsService")
    query = """
        SELECT 
            age_range_view.resource_name,
            campaign.id,
            campaign.name,
            campaign.advertising_channel_type,
            ad_group.id,
            ad_group.name,
            ad_group_criterion.age_range.type,
            segments.date,
            metrics.impressions, 
            metrics.clicks, 
            metrics.cost_micros,
            metrics.all_conversions,
            metrics.all_conversions_value
        FROM age_range_view
        WHERE segments.date DURING LAST_30_DAYS
    """

    stream = ga_service.search_stream(customer_id=customer_id, query=query)
    age_data = []
    for batch in stream:
        for row in batch.results:
            age_id = row.ad_group_criterion.age_range.type
            age_range_label = AGE_RANGE_MAPPING.get(age_id, "Unknown")

            age_data.append({
                'resource_name': row.age_range_view.resource_name,
                'campaign_id': row.campaign.id,
                'campaign_name': row.campaign.name,
                'campaign_type': row.campaign.advertising_channel_type.name if row.campaign.advertising_channel_type else 'Unknown',
                'ad_group_id': row.ad_group.id,
                'ad_group_name': row.ad_group.name,
                'age_range': age_range_label, 
                'impressions': row.metrics.impressions,
                'clicks': row.metrics.clicks,
                'cost_micros': row.metrics.cost_micros,
                'all_conversions': float(row.metrics.all_conversions),
                'all_conversions_value': float(row.metrics.all_conversions_value),
                'date': row.segments.date
            })
    return age_data

def get_conversion_data(client, customer_id):
    ga_service = client.get_service("GoogleAdsService")
    query = """
        SELECT 
            campaign.name,
            ad_group.id,
            ad_group.name,
            ad_group_criterion.age_range.type,
            segments.date,
            segments.conversion_action_name,
            metrics.all_conversions,
            metrics.all_conversions_value
        FROM age_range_view
        WHERE segments.date DURING LAST_30_DAYS
        AND segments.conversion_action_name IS NOT NULL
    """

    stream = ga_service.search_stream(customer_id=customer_id, query=query)
    conversion_data = []
    for batch in stream:
        for row in batch.results:
            age_id = row.ad_group_criterion.age_range.type
            age_range_label = AGE_RANGE_MAPPING.get(age_id, "Unknown")

            conversion_data.append({
                'campaign_name': row.campaign.name,
                'ad_group_id': row.ad_group.id,
                'age_range': age_range_label, 
                'conversion_name': row.segments.conversion_action_name,
                'all_conversions': float(row.metrics.all_conversions),
                'all_conversions_value': float(row.metrics.all_conversions_value),
                'date': row.segments.date
            })
    return conversion_data

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

            df_age = pd.DataFrame(get_age_range_data(client, acc_id))
            df_conversion = pd.DataFrame(get_conversion_data(client, acc_id))

            if df_age.empty and df_conversion.empty:
                logger.warning(f"⚠️ No data for account {acc_id}, skipping.")
                continue

            

            # Concatenate safely
            df_final = pd.concat([df_age, df_conversion], ignore_index=True)
            df_final["account_id"] = acc_id                                                      
            df_final["account_name"] = acc_name  
            final_dataframes.append(df_final)

        except Exception as e:
            logger.error(f"❌ Error in account {acc_id}: {e}")

    if not final_dataframes:
        logger.error(f"❌ No valid data collected. Exiting.")
        return

    # ✅ Combine all accounts
    df_all = pd.concat(final_dataframes, ignore_index=True)

   
    # Upload to BigQuery
    load_to_bigquery(df_all,TABLE_ID)

if __name__ == "__main__":                                                                   
    main()   
   


