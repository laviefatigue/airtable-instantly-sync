import requests
from typing import List, Dict, Any
from prefect.blocks.system import Secret
from pyairtable import Table
from schemas import InstantlyAccount

# --- Constants ---
INSTANTLY_API_URL = "https://api.instantly.ai/api/v2"
AIRTABLE_BASE_ID = "appS8cmzQfgFwP8Da"
AIRTABLE_TABLE_ID = "tblds7oKYLv1gNiPt"

# --- Prefect Blocks ---
def get_instantly_api_key() -> str:
    """Retrieves the Instantly.AI API key from Prefect Secrets."""
    print("Attempting to load Instantly.AI API key from Prefect...")
    key = Secret.load("instantly-api-key-hellohero").get()
    print("Successfully loaded Instantly.AI API key.")
    return key

def get_airtable_api_key() -> str:
    """Retrieves the Airtable API key from Prefect Secrets."""
    print("Attempting to load Airtable API key from Prefect...")
    key = Secret.load("airtable-api-key").get()
    print("Successfully loaded Airtable API key.")
    return key

# --- Instantly.AI API Functions ---
def get_instantly_accounts() -> List[Dict[str, Any]]:
    """
    Fetches all email accounts from the Instantly.AI API as dictionaries.
    """
    api_key = get_instantly_api_key()
    url = f"{INSTANTLY_API_URL}/accounts"
    headers = {"Authorization": f"Bearer {api_key}"}
    # ADDED: Explicitly ask for up to 100 accounts
    params = {"limit": 100}
    
    try:
        print("Requesting data from Instantly.AI API with a limit of 100...")
        # MODIFIED: Added the 'params' argument to the request
        response = requests.get(url, headers=headers, params=params)
        response.raise_for_status()
        accounts_data = response.json()
        
        # ADDED: Debug log to show the raw data received
        print(f"DEBUG: Raw data received from Instantly: {accounts_data}")
        
        print("Successfully received data from Instantly.AI.")
        if isinstance(accounts_data, list):
             return accounts_data
        else:
             print(f"🔴 ERROR: Unexpected data format from Instantly.AI. Expected a list, got {type(accounts_data)}")
             return []
    except requests.exceptions.RequestException as e:
        print(f"🔴 ERROR fetching data from Instantly.AI: {e}")
        return []
    except Exception as e:
        print(f"🔴 ERROR: An unexpected error occurred: {e}")
        return []

# --- Airtable API Functions ---
def get_airtable_table() -> Table:
    """Initializes and returns the pyairtable Table object."""
    print("Connecting to Airtable base...")
    api_key = get_airtable_api_key()
    table = Table(api_key, AIRTABLE_BASE_ID, AIRTABLE_TABLE_ID)
    print("Successfully connected to Airtable.")
    return table

def get_airtable_records() -> List[Dict[str, Any]]:
    """Fetches all records from the Airtable table."""
    try:
        table = get_airtable_table()
        print("Fetching all records from Airtable table...")
        records = table.all()
        print(f"Found {len(records)} records.")
        return records
    except Exception as e:
        print(f"🔴 ERROR fetching records from Airtable: {e}")
        return []

def create_airtable_records(records: List[Dict[str, Any]]):
    """Creates new records in the Airtable table."""
    try:
        table = get_airtable_table()
        print(f"Attempting to batch-create {len(records)} records in Airtable...")
        table.batch_create(records)
        print("✅ Batch create successful.")
    except Exception as e:
        print(f"🔴 ERROR during Airtable batch create: {e}")

def update_airtable_records(records: List[Dict[str, Any]]):
    """Updates existing records in the Airtable table."""
    try:
        table = get_airtable_table()
        print(f"Attempting to batch-update {len(records)} records in Airtable...")
        table.batch_update(records)
        print("✅ Batch update successful.")
    except Exception as e:
        print(f"🔴 ERROR during Airtable batch update: {e}")

def map_instantly_to_airtable(account: InstantlyAccount) -> Dict[str, Any]:
    """Maps an InstantlyAccount object to an Airtable record dictionary."""
    return {
        "fields": {
            "📧 Email Address": account.email,
            "🔗 Instantly Account ID": account.id,
            "🔗 Instantly Email ID": account.email_id,
            "📊 Account Status": "Active" if account.status == 1 else "Disabled",
            # This is a sample mapping. You can add more fields here based on schemas.py
        }
    }
