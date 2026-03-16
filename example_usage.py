from pathlib import Path
from api_client import ApiClient

# Config läggs lokalt i respektive mapp och synkas inte till git.
# Kopiera config.template.json till din mapp och döp om till config.json.
client = ApiClient(Path(__file__).parent / "config.json")

# --- Connect to database ---
client.connect_db()

# --- Authenticate (välj en) ---
client.auth_teamhub()
# client.auth_api_key()
# client.auth_bearer_token()
# client.auth_basic()
# client.auth_oauth2_client_credentials(
#     token_url="https://auth.example.com/oauth/token",
#     client_id="your-client-id",
#     client_secret="your-client-secret",
#     scope="api.read"
# )

# --- Set target table ---
client.table_name = "api_import"

# --- Fetch data (välj en) ---

# Single request
rows = client.fetch(
    url="https://api.example.com/some-endpoint",
    row_key="values"
)

# Paged – follows 'next-page' response header
# rows = client.fetch_paged_by_header(
#     url="https://api.example.com/some-endpoint",
#     row_key="values"
# )

# Paged – uses ?page= param and reads total from response
# rows = client.fetch_paged_by_count(
#     url="https://api.example.com/some-endpoint",
#     row_key="values",
#     pages_key="pages"
# )

# --- Create table and insert rows ---
client.run(rows)

# --- Disconnect ---
client.disconnect_db()
