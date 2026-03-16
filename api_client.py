import json
import pyodbc
import requests


class ApiClient:
    """
    Handles API authentication, data fetching, and writing rows to SQL Server.
    Configuration is loaded from a JSON file.
    """

    def __init__(self, config_path: str = "config.json"):
        with open(config_path) as f:
            cfg = json.load(f)

        self._db_cfg  = cfg["database"]
        self._api_cfg = cfg["api"]

        self.base_url:   str  = self._api_cfg.get("base_url", "")
        self.schema:     str  = self._db_cfg.get("schema_name", "dbo")
        self.table_name: str  = None
        self._headers:   dict = {"Accept": "application/json"}
        self._auth:      tuple = None  # used for basic auth
        self._conn       = None
        self._cursor     = None

    # ──────────────────────────────────────────
    # DATABASE
    # ──────────────────────────────────────────

    def connect_db(self):
        """Opens a connection to SQL Server."""
        c = self._db_cfg
        conn_str = (
            f"DRIVER={{{c['driver']}}};"
            f"SERVER={c['server']};"
            f"DATABASE={c['database']};"
            f"UID={c['username']};"
            f"PWD={c['password']};"
            f"TrustServerCertificate={'yes' if c['trust_server_certificate'] else 'no'};"
        )
        self._conn   = pyodbc.connect(conn_str)
        self._cursor = self._conn.cursor()
        self._ensure_schema()
        print("Database connection established.")

    def _ensure_schema(self):
        """Creates the schema if it does not already exist."""
        self._cursor.execute(
            f"IF NOT EXISTS (SELECT * FROM sys.schemas WHERE name = '{self.schema}') "
            f"EXEC('CREATE SCHEMA [{self.schema}]')"
        )
        self._conn.commit()

    def disconnect_db(self):
        """Closes the database connection."""
        if self._conn:
            self._conn.close()
            print("Database connection closed.")

    # ──────────────────────────────────────────
    # AUTHENTICATION
    # ──────────────────────────────────────────

    def auth_api_key(self, header_name: str = "X-API-Key"):
        """Authenticates using a static API key in a request header."""
        self._headers[header_name] = self._api_cfg["api_key"]
        print(f"Auth set: API key in '{header_name}' header.")

    def auth_bearer_token(self):
        """Authenticates using a static bearer token."""
        self._headers["Authorization"] = f"Bearer {self._api_cfg['bearer_token']}"
        print("Auth set: static bearer token.")

    def auth_basic(self):
        """Authenticates using basic auth (username + password)."""
        self._auth = (self._api_cfg["basic_username"], self._api_cfg["basic_password"])
        print("Auth set: basic auth.")

    def auth_teamhub(self):
        """
        Authenticates against the GoTeamHub API.
        POSTs credentials and stores the returned token and email as
        X-User-Token / X-User-Email headers required by the admin API.
        """
        url     = "https://api.goteamhub.com/api/sessions"
        payload = {
            "user": {
                "email":    self._api_cfg["teamhub_email"],
                "password": self._api_cfg["teamhub_password"]
            }
        }
        response = requests.post(url, json=payload)
        response.raise_for_status()

        token = response.json()["user"]["token"]
        self._headers["Content-Type"]  = "application/json"
        self._headers["X-User-Email"]  = self._api_cfg["teamhub_email"]
        self._headers["X-User-Token"]  = token
        print("Auth set: GoTeamHub token fetched successfully.")

    def auth_oauth2_client_credentials(self, token_url: str, client_id: str, client_secret: str, scope: str = ""):
        """
        Authenticates using OAuth2 client credentials flow.
        Token URL and credentials are passed as arguments since they vary per API.
        """
        response = requests.post(token_url, data={
            "grant_type":    "client_credentials",
            "client_id":     client_id,
            "client_secret": client_secret,
            "scope":         scope
        })
        response.raise_for_status()

        token = response.json()["access_token"]
        self._headers["Authorization"] = f"Bearer {token}"
        print("Auth set: OAuth2 client credentials token fetched successfully.")

    # ──────────────────────────────────────────
    # API FETCH
    # ──────────────────────────────────────────

    def fetch(self, url: str, row_key: str = "values") -> list:
        """
        Fetches a single page from the API.

        Args:
            url:     Full request URL.
            row_key: Key in the JSON body that contains the rows.

        Returns:
            List of rows.
        """
        response = requests.get(url, headers=self._headers, auth=self._auth)
        response.raise_for_status()

        rows = response.json().get(row_key, [])
        print(f"Fetched {len(rows)} rows from {url}")
        return rows

    def fetch_paged_by_header(self, url: str, row_key: str = "values") -> list:
        """
        Fetches all pages from a paginated API by following the 'next-page' response header.

        Args:
            url:     Initial request URL.
            row_key: Key in the JSON body that contains the rows.

        Returns:
            Flat list of all rows across all pages.
        """
        all_rows = []
        page     = 1

        while url:
            print(f"Fetching page {page}: {url}")
            response = requests.get(url, headers=self._headers, auth=self._auth)
            response.raise_for_status()

            page_rows = response.json().get(row_key, [])
            all_rows.extend(page_rows)
            print(f"  → {len(page_rows)} rows (total so far: {len(all_rows)})")

            url  = response.headers.get("next-page")
            page += 1

        print(f"Done. {len(all_rows)} rows across {page - 1} page(s).")
        return all_rows

    def fetch_paged_by_count(self, url: str, row_key: str = "values", pages_key: str = "pages") -> list:
        """
        Fetches all pages using a ?page= query parameter.
        Total page count is read from the first response.

        Args:
            url:       Base request URL (without page param).
            row_key:   Key in the JSON body that contains the rows.
            pages_key: Key in the JSON body that contains the total page count.

        Returns:
            Flat list of all rows across all pages.
        """
        all_rows = []

        # Fetch first page to determine total number of pages
        print(f"Fetching page 1: {url}")
        response = requests.get(url, headers=self._headers, auth=self._auth, params={"page": 1})
        response.raise_for_status()

        data        = response.json()
        total_pages = data[pages_key]
        first_rows  = data.get(row_key, [])
        all_rows.extend(first_rows)
        print(f"  → {len(first_rows)} rows (total pages: {total_pages})")

        for page in range(2, total_pages + 1):
            print(f"Fetching page {page}/{total_pages}: {url}")
            response = requests.get(url, headers=self._headers, auth=self._auth, params={"page": page})
            response.raise_for_status()

            page_rows = response.json().get(row_key, [])
            all_rows.extend(page_rows)
            print(f"  → {len(page_rows)} rows (total so far: {len(all_rows)})")

        print(f"Done. {len(all_rows)} rows across {total_pages} page(s).")
        return all_rows

    # ──────────────────────────────────────────
    # DATABASE WRITE
    # ──────────────────────────────────────────

    def _infer_sql_type(self, value) -> str:
        """Guesses SQL type based on Python value."""
        if isinstance(value, bool):
            return "BIT"
        elif isinstance(value, int):
            return "INT"
        elif isinstance(value, float):
            return "FLOAT"
        else:
            return "NVARCHAR(MAX)"

    def create_table(self, rows: list):
        """
        Creates the target table dynamically based on keys and values of the first row.
        Skips creation if the table already exists.
        Table name is taken from self.table_name.
        """
        if not rows:
            raise ValueError("No rows to create table from.")
        if not self.table_name:
            raise ValueError("table_name is not set.")

        columns  = [
            f"[{col}] {self._infer_sql_type(val)}"
            for col, val in rows[0].items()
        ]
        cols_sql = ",\n    ".join(columns)
        sql      = f"""
        IF NOT EXISTS (
            SELECT * FROM INFORMATION_SCHEMA.TABLES
            WHERE TABLE_SCHEMA = '{self.schema}'
              AND TABLE_NAME   = '{self.table_name}'
        )
        CREATE TABLE [{self.schema}].[{self.table_name}] (
            [api_client_id] INT IDENTITY(1,1) PRIMARY KEY,
            {cols_sql}
        )
        """
        self._cursor.execute(sql)
        self._conn.commit()
        print(f"Table '[{self.schema}].[{self.table_name}]' created (or already exists).")

    def insert_rows(self, rows: list):
        """
        Inserts all rows into the target table.
        Dicts and lists are serialized to JSON strings.
        Table name is taken from self.table_name.
        """
        if not rows:
            print("No rows to insert.")
            return
        if not self.table_name:
            raise ValueError("table_name is not set.")

        cols         = list(rows[0].keys())
        col_names    = ", ".join(f"[{c}]" for c in cols)
        placeholders = ", ".join("?" for _ in cols)
        sql          = f"INSERT INTO [{self.schema}].[{self.table_name}] ({col_names}) VALUES ({placeholders})"

        for row in rows:
            values = [
                # Serialize dicts/lists to JSON strings – pyodbc cannot handle complex types
                json.dumps(v) if isinstance(v, (dict, list)) else v
                for v in (row.get(c) for c in cols)
            ]
            self._cursor.execute(sql, values)

        self._conn.commit()
        print(f"{len(rows)} rows inserted into '[{self.schema}].[{self.table_name}]'.")

    def truncate_table(self):
        """Truncates the target table if it exists. Resets IDENTITY counter."""
        if not self.table_name:
            raise ValueError("table_name is not set.")
        sql = f"""
        IF EXISTS (
            SELECT * FROM INFORMATION_SCHEMA.TABLES
            WHERE TABLE_SCHEMA = '{self.schema}'
              AND TABLE_NAME   = '{self.table_name}'
        )
        TRUNCATE TABLE [{self.schema}].[{self.table_name}]
        """
        self._cursor.execute(sql)
        self._conn.commit()
        print(f"Table '[{self.schema}].[{self.table_name}]' truncated.")

    def run(self, rows: list, truncate: bool = False):
        """
        Creates the table (if needed) and inserts all rows.

        Args:
            rows:     List of rows to insert.
            truncate: If True, truncates the table before inserting.
                      Use for full refreshes. Default is False (append).
        """
        self.create_table(rows)
        if truncate:
            self.truncate_table()
        self.insert_rows(rows)
