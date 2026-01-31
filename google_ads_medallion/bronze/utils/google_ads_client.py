from dotenv import load_dotenv
import os
from google.ads.googleads.client import GoogleAdsClient

# ✅ Load environment variables
load_dotenv()

DEVELOPER_TOKEN = os.getenv('GOOGLE_ADS_DEVELOPER_TOKEN')
GOOGLE_ADS_LOGIN_CUSTOMER_ID = os.getenv('GOOGLE_ADS_LOGIN_CUSTOMER_ID')
JSON_KEY_FILE_PATH = os.getenv('GOOGLE_ADS_JSON_KEY_FILE_PATH')
GOOGLE_ADS_IMPERSONATED_EMAIL = os.getenv('GOOGLE_ADS_IMPERSONATED_EMAIL')
GCP_PROJECT_ID = os.getenv("GCP_PROJECT_ID")




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



   