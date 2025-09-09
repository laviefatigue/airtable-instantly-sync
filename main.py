from prefect import flow, task
from typing import List, Dict, Any
from schemas import InstantlyAccount
from utils import (
    get_instantly_accounts,
    get_airtable_records,
    create_airtable_records,
    update_airtable_records,
    map_instantly_to_airtable
)
from pydantic import ValidationError

@task
def extract_instantly_data() -> List[Dict[str, Any]]:
    """Extracts all email accounts from Instantly.AI as dictionaries."""
    print("--- Task: Extracting data from Instantly.AI ---")
    accounts = get_instantly_accounts()
    print(f"Successfully extracted {len(accounts)} accounts from Instantly.AI.")
    return accounts

@task
def get_existing_airtable_data() -> Dict[str, str]:
    """
    Fetches existing Airtable records and returns a dictionary mapping
    the email address to the Airtable record ID.
    """
    print("--- Task: Fetching existing data from Airtable ---")
    existing_records = get_airtable_records()
    email_to_id_map = {
        record.get("fields", {}).get("📧 Email Address"): record.get("id")
        for record in existing_records
        if record.get("fields", {}).get("📧 Email Address")
    }
    print(f"Found {len(email_to_id_map)} existing records in Airtable.")
    return email_to_id_map

@task
def sync_data_to_airtable(instantly_accounts_data: List[Dict[str, Any]], existing_airtable_map: Dict[str, str]):
    """
    Compares Instantly.AI accounts to existing Airtable records and
    creates or updates them as needed. Resilient to data validation errors.
    """
    print("--- Task: Syncing data to Airtable ---")
    records_to_create = []
    records_to_update = []

    for account_data in instantly_accounts_data:
        try:
            # Validate the data for each account individually
            account = InstantlyAccount(**account_data)
            mapped_record = map_instantly_to_airtable(account)
            email = account.email

            if email in existing_airtable_map:
                record_id = existing_airtable_map[email]
                update_payload = {"id": record_id, **mapped_record}
                records_to_update.append(update_payload)
            else:
                records_to_create.append(mapped_record)
        
        except ValidationError as e:
            # If an account's data doesn't match our schema, log it and skip
            print(f"🔴 VALIDATION ERROR: Skipping account due to data mismatch.")
            print(f"   Email: {account_data.get('email', 'N/A')}")
            print(f"   Details: {e}")
            continue

    print(f"Found {len(records_to_create)} new records to create.")
    if records_to_create:
        create_airtable_records(records_to_create)

    print(f"Found {len(records_to_update)} existing records to update.")
    if records_to_update:
        update_airtable_records(records_to_update)

@flow(name="Instantly.AI to Airtable Sync")
def instantly_to_airtable_flow():
    """Main flow to sync Instantly.AI email accounts to Airtable."""
    print("🚀 Starting Instantly.AI to Airtable Sync Flow...")
    
    instantly_accounts = extract_instantly_data()
    existing_airtable_map = get_existing_airtable_data()
    
    # The sync task will now be called with the data (or an empty list),
    # and it will handle the logic internally.
    sync_data_to_airtable(instantly_accounts, existing_airtable_map)
    
    print("🏁 Flow finished.")

if __name__ == "__main__":
    instantly_to_airtable_flow()

