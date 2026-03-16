from pathlib import Path
import sys
sys.path.insert(0, str(Path(__file__).parent.parent))

from api_client import ApiClient

client = ApiClient(Path(__file__).parent / "config.json")

# --- Connect to database ---
client.connect_db()

# --- Authenticate ---
client.auth_teamhub()

# --- Set target table ---
client.table_name = "users"

# --- Fetch data (choose one) ---

# Single request
# rows = client.fetch(
#     url=client.base_url + "users&per_page=1000",
#     row_key="users"
# )

# Paged – follows 'next-page' response header
rows = client.fetch_paged_by_header(
    url=client.base_url + "users?&per_page=1000",
    row_key="users"
)

# Paged – uses ?page= param and reads total from response
# rows = client.fetch_paged_by_count(
#     url=client.base_url + "some-endpoint",
#     row_key="values",
#     pages_key="pages"
# )

# --- Create table and insert rows ---
client.run(rows, truncate=True)

# --- Disconnect ---
client.disconnect_db()
