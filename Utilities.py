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
    return Secret.load("instantly-api-key-hellohero").get()

def get_airtable_api_key() -> str:
    """Retrieves the Airtable API key from Prefect Secrets."""
    return Secret.load("airtable-api-key").get()

# --- Instantly.AI API Functions ---
def get_instantly_accounts() -> List[InstantlyAccount]:
    """
    Fetches all email accounts from the Instantly.AI API.
    """
    api_key = get_instantly_api_key()
    url = f"{INSTANTLY_API_URL}/account/list"
    params = {"api_key": api_key}
    
    try:
        response = requests.get(url, params=params)
        response.raise_for_status()
        accounts_data = response.json().get("accounts", [])
        return [InstantlyAccount(**account) for account in accounts_data]
    except requests.exceptions.RequestException as e:
        print(f"Error fetching data from Instantly.AI: {e}")
        return []
    except Exception as e:
        print(f"An unexpected error occurred: {e}")
        return []

# --- Airtable API Functions ---
def get_airtable_table() -> Table:
    """Initializes and returns an Airtable Table object."""
    api_key = get_airtable_api_key()
    return Table(api_key, AIRTABLE_BASE_ID, AIRTABLE_TABLE_ID)

def get_airtable_records() -> List[Dict[str, Any]]:
    """Fetches all records from the specified Airtable table."""
    table = get_airtable_table()
    try:
        return table.all()
    except Exception as e:
        print(f"Error fetching records from Airtable: {e}")
        return []

def create_airtable_records(records: List[Dict[str, Any]]):
    """Creates new records in Airtable in batches."""
    table = get_airtable_table()
    try:
        table.batch_create(records)
    except Exception as e:
        print(f"Error creating records in Airtable: {e}")

def update_airtable_records(records: List[Dict[str, Any]]):
    """Updates existing records in Airtable in batches."""
    table = get_airtable_table()
    try:
        table.batch_update(records)
    except Exception as e:
        print(f"Error updating records in Airtable: {e}")


# --- Data Mapping ---
def map_instantly_to_airtable(account: InstantlyAccount) -> Dict[str, Any]:
    """
    Maps an InstantlyAccount object to the structure required by the Airtable API.
    """
    # Mapping for select fields. You can expand this.
    warmup_status_map = {
        "active": "Active",
        "paused": "Paused",
        "banned": "Banned",
        "warming up": "Warming Up"
    }

    connection_status_map = {
        "active": "Active",
        "connection_error": "Connection Error",
        "sending_error": "Sending Error",
        "authentication_failed": "Authentication Failed",
        "rate_limited": "Rate Limited",
        "disconnected": "Disconnected"
    }
    
    provider_map = {
        "gmail": "Gmail",
        "azure": "Azure",
        "yahoo": "Yahoo",
        "custom_domain": "Custom Domain",
        "outlook": "Outlook",
        "other": "Other",
    }


    fields = {
        "📧 Email Address": account.email,
        "🔗 Instantly Account ID": account.account_id,
        "🔗 Instantly Email ID": account.id,
        "📊 Warmup Status": warmup_status_map.get(account.warmup_status.lower()),
        "🔐 Connection Status": connection_status_map.get(account.connection_status.lower()),
        "🏢 Email Provider": provider_map.get(account.provider.lower()),
        "🔧 Provider Code": provider_map.get(account.provider.lower()),
        "🔄 Daily Send Limit": account.daily_send_limit,
        "📤 Emails Sent Today": account.sent_today,
        "📈 Warmup Score": account.warmup_score,
        "🎯 Sending Gap (mins)": account.sending_gap_in_minutes,
    }
    
    # Remove None values so we don't overwrite existing Airtable fields with empty data
    return {"fields": {k: v for k, v in fields.items() if v is not None}}

