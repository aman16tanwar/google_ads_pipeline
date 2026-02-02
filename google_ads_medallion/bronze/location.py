import os 
import pandas as pd
from dotenv import load_dotenv

from google.ads.googleads.client import GoogleAdsClient

from utils.bigquery_loader import load_to_bigquery
from utils.google_ads_client import fetch_enabled_accounts
from utils.logger import setup_logger

logger=setup_logger(__name__)

# Load environment variables

load_dotenv()

DEVELOPER_TOKEN =os.getenv("GOOGLE_ADS_DEVELOPER_TOKEN")
GOOGLE_ADS_LOGIN_CUSTOMER_ID = os.getenv("GOOGLE_ADS_LOGIN_CUSTOMER_ID")
JSON_KEY_FILE_PATH = os.getenv("GOOGLE_ADS_JSON_KEY_FILE_PATH")
GOOGLE_ADS_IMPERSONATED_EMAIL = os.getenv("GOOGLE_ADS_IMPERSONATED_EMAIL")
GCP_PROJECT_ID = os.getenv("GCP_PROJECT_ID")
TABLE_ID = f"{GCP_PROJECT_ID}.{os.getenv('BIGQUERY_BRONZE_DATASET')}.{os.getenv('BIGQUERY_BRONZE_MAIN_CAMPAIGN_LOCATION')}"




def get_location_data(client, customer_id, id_to_country_code, id_to_city_code):
    ga_service =client.get_service("GoogleAdsService")
    query = """
        SELECT user_location_view.country_criterion_id,
               user_location_view.resource_name,
               user_location_view.targeting_location,
               ad_group.id, 
               ad_group.name,
               campaign.id,
               campaign.name,
               campaign.advertising_channel_type,
               metrics.all_conversions,
               metrics.all_conversions_value,
               metrics.clicks,
               metrics.cost_micros, 
               metrics.impressions, 
               segments.geo_target_city, 
               segments.geo_target_province, 
               segments.date 
        FROM user_location_view
        WHERE segments.date DURING LAST_30_DAYS

    """

    stream = ga_service.search_stream(
        customer_id=customer_id, query=query
    )

    location_data=[]
    for batch in stream:
        for row in batch.results:
            location_data.append({
                "resource_name": row.user_location_view.resource_name,
                "country_criterion_id": row.user_location_view.country_criterion_id,
                "targeting_location": row.user_location_view.targeting_location,
                "campaign_id": row.campaign.id,
                "campaign_name": row.campaign.name,
                "advertising_channel_type": row.campaign.advertising_channel_type,
                "ad_group_id": row.ad_group.id,
                "ad_group_name": row.ad_group.name,
                "geo_target_city": row.segments.geo_target_city,
                "geo_target_province": row.segments.geo_target_province,
                "date": row.segments.date,
                "all_conversions": row.metrics.all_conversions,
                "all_conversions_value": row.metrics.all_conversions_value,
                "clicks": row.metrics.clicks,
                "cost_micros": row.metrics.cost_micros,
                "impressions": row.metrics.impressions
            })
        
        
    df=pd.DataFrame(location_data)
    df["country_criterion_id"] = pd.to_numeric(df["country_criterion_id"], errors="coerce").astype("Int64")
    #Pull only integer value from like this geoTargetConstants/1000010
    df["get_target_city"] = df["geo_target_city"].astype(str).str.extract(r'(\d+)').astype("Int64")


        


    df["country_code"] = df["country_criterion_id"].map(id_to_country_code)
    df["geo_target_city"] = df["get_target_city"].map(id_to_city_code)
  
   

    return df   
    
def get_location_conversions(client, customer_id, id_to_country_code, id_to_city_code):

    ga_service=client.get_service("GoogleAdsService")
    query ="""
    SELECT user_location_view.country_criterion_id, 
        user_location_view.resource_name, 
        user_location_view.targeting_location, 
        ad_group.id, 
        ad_group.name, 
        campaign.id, 
        campaign.name, 
        campaign.advertising_channel_type, 
        segments.geo_target_city, 
        segments.geo_target_province, 
        segments.date, 
        metrics.all_conversions, 
        metrics.all_conversions_value, 
        segments.conversion_action_name 
    FROM user_location_view 
    WHERE segments.date DURING LAST_30_DAYS


    """
    stream = ga_service.search_stream(
        customer_id=customer_id, query=query
    )
    conversion_data=[]
    for batch in stream:
        for row in batch.results:
            conversion_data.append({
                "resource_name": row.user_location_view.resource_name,
                "country_criterion_id": row.user_location_view.country_criterion_id,
                "targeting_location": row.user_location_view.targeting_location,
                "campaign_id": row.campaign.id,
                "campaign_name": row.campaign.name,
                "advertising_channel_type": row.campaign.advertising_channel_type,
                "ad_group_id": row.ad_group.id,
                "ad_group_name": row.ad_group.name,
                "geo_target_city": row.segments.geo_target_city,
                "geo_target_province": row.segments.geo_target_province,
                "date": row.segments.date,
                "all_conversions": row.metrics.all_conversions,
                "all_conversions_value": row.metrics.all_conversions_value,
                "conversion_action_name": row.segments.conversion_action_name
            })

        
    df=pd.DataFrame(conversion_data)
    df["country_criterion_id"] = pd.to_numeric(df["country_criterion_id"], errors="coerce").astype("Int64")
    #Pull only integer value from like this geoTargetConstants/1000010
    df["get_target_city"] = df["geo_target_city"].astype(str).str.extract(r'(\d+)').astype("Int64")    


    df["country_code"] = df["country_criterion_id"].map(id_to_country_code)
    df["geo_target_city"] = df["get_target_city"].map(id_to_city_code)
  
   
    
    return df

    

# Preload geo mapping
location_df = pd.read_csv("geotargets-2025-04-01.csv")
location_df.columns = location_df.columns.str.strip().str.lower().str.replace(" ", "_")
location_df["parent_id"] = pd.to_numeric(location_df["parent_id"], errors="coerce").astype("Int64")
location_df["criteria_id"] = pd.to_numeric(location_df["criteria_id"], errors="coerce").astype("Int64")
id_to_country_code = dict(zip(location_df["parent_id"], location_df["country_code"]))
id_to_city_code = dict(zip(location_df["criteria_id"], location_df["name"]))


def main():
    accounts=fetch_enabled_accounts()
    final_dataframes=[]

    for acc in accounts:
        acc_id=acc["customer_id"]
        acc_name=acc["name"]
        logger.info(f"Processing account: {acc_name} ({acc_id})")

        try:
            client =GoogleAdsClient.load_from_dict({
                'client_customer_id': acc_id,
                'developer_token': DEVELOPER_TOKEN,
                'login_customer_id': GOOGLE_ADS_LOGIN_CUSTOMER_ID,
                'json_key_file_path': JSON_KEY_FILE_PATH,
                'impersonated_email': GOOGLE_ADS_IMPERSONATED_EMAIL,
                'use_proto_plus': True
            })

            df_location = pd.DataFrame(get_location_data(client, acc_id, id_to_country_code, id_to_city_code))
            df_conversion = pd.DataFrame(get_location_conversions(client, acc_id, id_to_country_code, id_to_city_code))

            logger.info(f" Conversion rows for account {acc_name} ({acc_id}): {len(df_conversion)}")

            if df_location.empty and df_conversion.empty:
                logger.warning(f"No location data found for account {acc_name} ({acc_id}).")
                continue




                       
            #Final concatenation
            df_final = pd.concat([df_location, df_conversion], ignore_index=True)
            df_final["account_id"] = acc_id
            df_final["account_name"] = acc_name 
            logger.info(f"Final DataFrame shape: {df_final.shape}")
            final_dataframes.append(df_final)
        except Exception as e:
            logger.error(f"Error processing account {acc_name} ({acc_id}): {e}")

    if not final_dataframes:
        logger.warning(f"No data fetched from any account.")

    #After concatenation all Data frames
    df_all = pd.concat(final_dataframes, ignore_index=True)



    load_to_bigquery(df_all,TABLE_ID)


    
if __name__ == "__main__":                                                                   
    main()   





