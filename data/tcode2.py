import json
from datetime import datetime, timezone
from keboola.component import interface


# Initialize the Keboola Common Interface
common = interface.CommonInterface()

# Define your table ID and component ID
table_id = 'in.c-NS_SAVE_SEARCHES.customer_notes'  # Replace with your actual table ID
component_id = 'offen-petroleum.app-ns-saved-search'  # Replace with your actual component ID

# Get the current timestamp in ISO format as a timezone-aware object
current_timestamp = datetime.now(timezone.utc).isoformat()  # This will be in UTC

# Prepare the metadata update
metadata_update = {
    "last_updated": current_timestamp,
    "Recently updated by": component_id
}

# Update the metadata
common.metadata.update_table_metadata(table_id, metadata_update)

# Log the update
common.log.info(f'Metadata for table {table_id} updated successfully with last_updated: {current_timestamp}.')

