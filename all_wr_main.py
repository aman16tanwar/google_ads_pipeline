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
# ✅ Set Google Cloud credentials
# os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = "googleads-bigquery.json"

# ✅ Load environment variables
load_dotenv()

DEVELOPER_TOKEN = os.getenv('GOOGLE_ADS_DEVELOPER_TOKEN')
GOOGLE_ADS_LOGIN_CUSTOMER_ID = os.getenv('GOOGLE_ADS_LOGIN_CUSTOMER_ID')
JSON_KEY_FILE_PATH = os.getenv('GOOGLE_ADS_JSON_KEY_FILE_PATH')
GOOGLE_ADS_IMPERSONATED_EMAIL = os.getenv('GOOGLE_ADS_IMPERSONATED_EMAIL')
GCP_PROJECT_ID = os.getenv("GCP_PROJECT_ID")
TABLE_ID = f"{GCP_PROJECT_ID}.{os.getenv('BIGQUERY_DATASET_ALL_MAIN')}.{os.getenv('BIGQUERY_TABLE_ALL_MAIN')}"

DEVICE_MAPPING = {
    0: "Unknown", 1: "Mobile", 2: "Tablet", 3: "Desktop", 4: "Connected TV", 5: "Other"
}

# ========================== #
#     FETCH ENABLED ACCOUNTS #
# ========================== #
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
                'Campaign ID': row.campaign.id,
                'Campaign Name': row.campaign.name,
                'Campaign Type': row.campaign.advertising_channel_type.name if row.campaign.advertising_channel_type else 'Unknown',
                'Device': DEVICE_MAPPING.get(row.segments.device, "Unknown"),
                'Date': row.segments.date,
                'Impressions': row.metrics.impressions,
                'Clicks': row.metrics.clicks,
                'Cost Micros': row.metrics.cost_micros,
                'All Conversions': row.metrics.all_conversions,
                'All Conversions Value': row.metrics.all_conversions_value
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
                'Campaign Name': row.campaign.name,
                'Campaign ID': row.campaign.id,
                'Device': DEVICE_MAPPING.get(row.segments.device, "Unknown"),
                'Date': row.segments.date,
                'Conversion Name': row.segments.conversion_action_name,
                'All Conversions': row.metrics.all_conversions,
                'All Conversions Value': row.metrics.all_conversions_value
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

            if not df_device.empty:
                df_device["Date"] = pd.to_datetime(df_device["Date"]).dt.date
            if not df_conversion.empty:
                df_conversion["Date"] = pd.to_datetime(df_conversion["Date"]).dt.date

            df_device["Conversion Name"] = "Unknown"
            df_device["All Conversions"] = None
            df_device["All Conversions Value"]=None

            df_conversion["Impressions"] = None
            df_conversion["Clicks"] = None
            df_conversion["Cost Micros"] = None
            
            
            df_final=pd.concat([df_device,df_conversion],ignore_index=True)

            df_final["Account ID"] = acc_id
            df_final["Account Name"] = acc_name

            final_dataframes.append(df_final)

        except Exception as e:
            print(f"❌ Error in account {acc_id}: {e}")

    if not final_dataframes: 
        print("❌ No valid data collected. Exiting.")
        return

    df_all = pd.concat(final_dataframes, ignore_index=True)
    df_all["Conversion Name"] = df_all["Conversion Name"].fillna("Unknown")
    if "Cost Micros" in df_all.columns:
        df_all["Cost"] = df_all["Cost Micros"].fillna(0) / 1_000_000

    if "All Conversions_y" in df_all.columns:
        df_all["All Conversions"] = df_all["All Conversions_y"].fillna(df_all["All Conversions_x"])
    if "All Conversions Value_y" in df_all.columns:
        df_all["All Conversions Value"] = df_all["All Conversions Value_y"].fillna(df_all["All Conversions Value_x"])

    df_all = df_all.drop(columns=[
        col for col in ["All Conversions_x", "All Conversions_y", "All Conversions Value_x", "All Conversions Value_y", "Cost Micros"]
        if col in df_all.columns
    ])

    df_all["Account ID"] = pd.to_numeric(df_all["Account ID"], errors="coerce").astype("Int64")
    df_all["Campaign ID"] = pd.to_numeric(df_all["Campaign ID"], errors="coerce").astype("Int64")
    df_all["Date"] = pd.to_datetime(df_all["Date"])

    df_all = df_all[[
        "Account ID", "Account Name", "Campaign ID", "Campaign Name", "Campaign Type",
        "Device", "Conversion Name", "Date", "Impressions", "Clicks", "Cost",
        "All Conversions", "All Conversions Value"
    ]]

    print("✅ Final dataframe shape:", df_all.shape)
    print("🧾 Unique accounts in dataframe:", df_all['Account ID'].nunique())
    print(df_all['Account Name'].value_counts().head(10))

    credentials = service_account.Credentials.from_service_account_file(os.getenv("GOOGLE_APPLICATION_CREDENTIALS"))
    print("🔁 Uploading final dataframe of shape:", df_all.shape)

    bq_client = bigquery.Client(credentials=credentials, project=GCP_PROJECT_ID)
    query = f"""
        DELETE FROM `{TABLE_ID}`
        WHERE Date >= DATE_SUB(CURRENT_DATE(), INTERVAL 30 DAY)
    """
    bq_client.query(query).result()
    print("🧹 Deleted last 30 days from BigQuery before uploading new data.")

    to_gbq(
        df_all,
        destination_table=TABLE_ID,
        project_id=GCP_PROJECT_ID,
        credentials=credentials,
        if_exists="append"  # Change to 'append' if you want to keep historical data
    )

    print(f"✅ Data uploaded to BigQuery: {TABLE_ID}")
    print(df_all.head(10))

if __name__ == "__main__":
    main()
