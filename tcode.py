import requests
import json
from datetime import datetime, timedelta, timezone
import os

# New function to update metadata
def update_metadata(keboola_token, keboola_table_id):
    headers = {'X-StorageApi-Token': keboola_token}
    url = f'https://connection.keboola.com/v2/storage/tables/{keboola_table_id}'

    metadata = {
        'metadata': [
            {
                'key': 'Recently updated by',
                'value': 'offen-petroleum.app-ns-saved-search'  # Get component ID
            },
            {
                'key': 'last_updated_timestamp',
                'value': datetime.now(timezone.utc).isoformat()
            }
        ]
    }

    response = requests.patch(url, headers=headers, json=metadata)
    if response.status_code != 200:
        print(f'Failed to update table metadata: {response.text}')

keboola_token = "9508-2822462-IgbFYlRscn13HY8iZBvtAq76BOonmDeaxMPOyWTc"
keboola_table_id = "in.c-NS_SAVE_SEARCHES.customer_notes"
# New function to get the last updated timestamp
update_metadata(keboola_token=keboola_token, keboola_table_id=keboola_table_id)


