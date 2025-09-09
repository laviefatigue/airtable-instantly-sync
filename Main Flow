from prefect import flow, task
from schemas import InstantlyAccount
from utils import (
    get_instantly_accounts,
    get_airtable_records,
    create_airtable_records,
    update_airtable_records,
    map_instantly_to_airtable
)
from typing import List, Dict, Any

@task(log_prints=True)
def extract_instantly_data() -> List[InstantlyAccount]:
    """
    Extracts all email accounts from Instantly.AI.
    """
    print("Extracting data from Instantly.AI...")
    accounts = get_instantly_accounts()
    print(f"Successfully extracted {len(accounts)} accounts from Instantly.AI.")
    return accounts

@task(log_prints=True)
def get_existing_airtable_data() -> Dict[str, str]:
    """
    Retrieves existing records from Airtable to prevent duplicates.
    Returns a dictionary mapping email addresses to Airtable record IDs.
    """
    print("Fetching existing records from Airtable...")
    records = get_airtable_records()
    email_to_record_id = {record['fields'].get('📧 Email Address'): record['id'] for record in records if '📧 Email Address' in record['fields']}
    print(f"Found {len(email_to_record_id)} existing records in Airtable.")
    return email_to_record_id


@task(log_prints=True)
def transform_data(accounts: List[InstantlyAccount], existing_records: Dict[str, str]) -> Dict[str, List[Dict[str, Any]]]:
    """
    Transforms Instantly.AI data into the Airtable format and separates
    records into 'new' and 'update' lists.
    """
    print("Transforming Instantly.AI data for Airtable...")
    new_records = []
    records_to_update = []

    for account in accounts:
        airtable_record = map_instantly_to_airtable(account)
        if account.email in existing_records:
            record_id = existing_records[account.email]
            records_to_update.append({"id": record_id, "fields": airtable_record["fields"]})
        else:
            new_records.append(airtable_record)

    print(f"Transformation complete: {len(new_records)} new records, {len(records_to_update)} records to update.")
    return {"new": new_records, "update": records_to_update}


@task(log_prints=True)
def load_data_to_airtable(transformed_data: Dict[str, List[Dict[str, Any]]]):
    """
    Loads new records and updates existing ones in Airtable.
    """
    new_records = transformed_data["new"]
    records_to_update = transformed_data["update"]

    if new_records:
        print(f"Creating {len(new_records)} new records in Airtable...")
        create_airtable_records(new_records)
        print("New records created successfully.")
    else:
        print("No new records to create.")

    if records_to_update:
        print(f"Updating {len(records_to_update)} existing records in Airtable...")
        update_airtable_records(records_to_update)
        print("Existing records updated successfully.")
    else:
        print("No records to update.")


@flow(name="Instantly.AI to Airtable Sync")
def instantly_to_airtable_flow():
    """
    The main Prefect flow to sync data from Instantly.AI to Airtable.
    """
    print("Starting Instantly.AI to Airtable Sync Flow...")
    instantly_accounts = extract_instantly_data()
    if instantly_accounts:
        existing_airtable_records = get_existing_airtable_data()
        transformed_data = transform_data(instantly_accounts, existing_airtable_records)
        load_data_to_airtable(transformed_data)
    print("Flow finished.")

if __name__ == "__main__":
    instantly_to_airtable_flow()
