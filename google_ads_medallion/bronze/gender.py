# Configuration
import os
import pandas as pd
from dotenv import load_dotenv
from google.ads.googleads.client import GoogleAdsClient
from google.oauth2 import service_account
from pandas_gbq import to_gbq
from google.cloud import bigquery
from utils.bigquery_loader import load_bq_gender
# ✅ Set up Google Cloud Credentials


# ✅ Load environment variables
load_dotenv()

DEVELOPER_TOKEN = os.getenv('GOOGLE_ADS_DEVELOPER_TOKEN')
GOOGLE_ADS_LOGIN_CUSTOMER_ID = os.getenv('GOOGLE_ADS_LOGIN_CUSTOMER_ID')
JSON_KEY_FILE_PATH = os.getenv('GOOGLE_ADS_JSON_KEY_FILE_PATH')
GOOGLE_ADS_IMPERSONATED_EMAIL = os.getenv('GOOGLE_ADS_IMPERSONATED_EMAIL')
GCP_PROJECT_ID = os.getenv("GCP_PROJECT_ID")
TABLE_ID = f"{GCP_PROJECT_ID}.{os.getenv('BIGQUERY_BRONZE_DATESET')}.{os.getenv('BIGQUERY_TABLE_ALL_GENDER')}"

GENDER_MAPPING = {
    10: "Male",
    11: "Female",
    20: "Unknown"
}

def fetch_enabled_accounts():
    config = {
        "developer_token": DEVELOPER_TOKEN,
        "login_customer_id": GOOGLE_ADS_LOGIN_CUSTOMER_ID,
        "json_key_file_path": JSON_KEY_FILE_PATH,
        "impersonated_email": GOOGLE_ADS_IMPERSONATED_EMAIL,
        "use_proto_plus": True
    }
    client = GoogleAdsClient.load_from_dict(config)
    service = client.get_service("GoogleAdsService")

    query = """
        SELECT customer_client.client_customer,
               customer_client.descriptive_name,
               customer_client.manager,
               customer_client.status
        FROM customer_client
        WHERE customer_client.level = 1
        AND customer_client.status = 'ENABLED'
    """

    response = service.search(customer_id=GOOGLE_ADS_LOGIN_CUSTOMER_ID, query=query)
    accounts = []
    for row in response:
        if not row.customer_client.manager:
            accounts.append({
                "customer_id": row.customer_client.client_customer.replace("customers/", ""),
                "name": row.customer_client.descriptive_name
            })
    print(f"✅ {len(accounts)} active client accounts found.")
    return accounts

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
                'Resource Name': row.gender_view.resource_name,
                'Campaign ID': row.campaign.id,
                'Campaign Name': row.campaign.name,
                'Campaign Type': row.campaign.advertising_channel_type.name if row.campaign.advertising_channel_type else 'Unknown',
                'Ad Group ID': row.ad_group.id,
                'Ad Group Name': row.ad_group.name,
                'Gender': gender_label, 
                'Impressions': row.metrics.impressions,
                'Clicks': row.metrics.clicks,
                'Cost Micros': row.metrics.cost_micros,
                'All Conversions': row.metrics.all_conversions if row.metrics.all_conversions is not None else 0.0,
                'All Conversions Value': row.metrics.all_conversions_value if row.metrics.all_conversions_value is not None else 0.0,
                'Date': row.segments.date
            })

    return gender_data

def get_gender_conversion_data(client, customer_id):
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
            segments.conversion_action_name,
            metrics.all_conversions,
            metrics.all_conversions_value
        FROM gender_view
        WHERE segments.date DURING LAST_30_DAYS
        AND segments.conversion_action_name IS NOT NULL

    """

    stream = ga_service.search_stream(customer_id=customer_id, query=query)

    conversion_data = []
    for batch in stream:
        for row in batch.results:
            gender_id = row.ad_group_criterion.gender.type
            gender_label = GENDER_MAPPING.get(gender_id, "Unknown")

            conversion_data.append({
                'Resource Name': row.gender_view.resource_name,
                'Campaign ID': row.campaign.id,
                'Campaign Name': row.campaign.name,
                'Campaign Type': row.campaign.advertising_channel_type.name if row.campaign.advertising_channel_type else 'Unknown',
                'Ad Group ID': row.ad_group.id,
                'Ad Group Name': row.ad_group.name,
                'Gender': gender_label, 
                'Conversion Name': row.segments.conversion_action_name,
                'All Conversions': row.metrics.all_conversions if row.metrics.all_conversions is not None else 0.0,
                'All Conversions Value': row.metrics.all_conversions_value if row.metrics.all_conversions_value is not None else 0.0,
                'Date': row.segments.date
            })

    return conversion_data

def main():
    accounts = fetch_enabled_accounts()
    final_dataframes = []

    for acc in accounts:
        acc_id = acc["customer_id"]
        acc_name = acc["name"]
        print(f"\n▶️ Processing account: {acc_id} - {acc_name}")

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
            df_conversion = pd.DataFrame(get_gender_conversion_data(client, acc_id))
            print(f"🔍 Conversion rows for account {acc_id}: {df_conversion.shape[0]}")

            if df_gender.empty and df_conversion.empty:
                print(f"⚠️ No data for account {acc_id}, skipping.")
                continue

            # Convert date fields
            df_gender["Date"] = pd.to_datetime(df_gender["Date"])
            df_conversion["Date"] = pd.to_datetime(df_conversion["Date"])

            # Add fixed values
            df_gender["Conversion Name"] = "Unknown"
            df_gender["Cost"] = df_gender["Cost Micros"].fillna(0) / 1_000_000
            df_gender = df_gender.drop(columns=["Cost Micros"], errors='ignore')

            df_conversion["Clicks"] = None
            df_conversion["Impressions"] = None
            df_conversion["Cost"] = None

            # Add account metadata
            df_gender["Account ID"] = int(acc_id)
            df_gender["Account Name"] = acc_name
            df_conversion["Account ID"] = int(acc_id)
            df_conversion["Account Name"] = acc_name

            # Align to schema
            column_order = [
                "Account ID", "Account Name", "Campaign ID", "Campaign Name", "Campaign Type",
                "Ad Group ID", "Ad Group Name", "Gender", "Conversion Name", "Date",
                "Impressions", "Clicks", "Cost", "All Conversions", "All Conversions Value", "Resource Name"
            ]

            df_gender = df_gender.reindex(columns=column_order)
            df_conversion = df_conversion.reindex(columns=column_order)

            # Final concat
            df_final = pd.concat([df_gender, df_conversion], ignore_index=True)
            print("✅ After concat:", df_final.shape)
            final_dataframes.append(df_final)


        except Exception as e:
            print(f"❌ Error in account {acc_id}: {e}")

    if not final_dataframes:
        print("❌ No valid data collected. Exiting.")
        return

    # After concatenating all DataFrames
    df_all = pd.concat(final_dataframes, ignore_index=True)

    # 🔐 Ensure critical columns always exist
    required_columns_defaults = {
        "All Conversions": 0.0,
        "All Conversions Value": 0.0,
        "Conversion Name": "Unknown",
        "Resource Name": "Unknown"
        }

    for col, default_val in required_columns_defaults.items():
        if col not in df_all.columns:
            df_all[col] = default_val

    # ✅ Reorder columns
    column_order = [
        "Account ID", "Account Name", "Campaign ID", "Campaign Name", "Campaign Type",
        "Ad Group ID", "Ad Group Name", "Gender", "Conversion Name", "Date",
        "Impressions", "Clicks", "Cost", "All Conversions", "All Conversions Value", "Resource Name"
    ]

    df_all = df_all[column_order]
    load_bq_gender(df_all,TABLE_ID)






