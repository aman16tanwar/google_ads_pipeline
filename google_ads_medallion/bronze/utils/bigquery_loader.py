# ========================== #
#    CONFIGURATION SECTION   #
# ========================== #
import os
from dotenv import load_dotenv
from google.cloud import bigquery
from pandas_gbq import to_gbq
from utils.logger import setup_logger

logger=setup_logger(__name__)
# ✅ Set Google Cloud credentials
# os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = "googleads-bigquery.json"

# ✅ Load environment variables
load_dotenv()


GCP_PROJECT_ID = os.getenv("GCP_PROJECT_ID")



def load_to_bigquery(df, table_name):
    
    bq_client = bigquery.Client( project=GCP_PROJECT_ID)
    try:
        query = f"""
            DELETE FROM `{table_name}`
            WHERE date >= DATE_SUB(CURRENT_DATE(), INTERVAL 30 DAY)
        """
        bq_client.query(query).result()
        logger.info("🧹 Deleted last 30 days from BigQuery before uploading new data.")
    except Exception as e:
        logger.warning(f"⚠️ Delete skipped (table may be new): {e}")   
    
    
    to_gbq(
        df,
        destination_table=table_name,
        project_id=GCP_PROJECT_ID,
        if_exists="append"  # Change to 'append' if you want to keep historical data
    )

    logger.info(f"✅ Data uploaded to BigQuery: {table_name}")





