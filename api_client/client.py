import json
from pathlib import Path

import pyodbc
import requests


class ApiClient:
    """Handles API authentication, data fetching, and writing rows to SQL Server."""

    def __init__(self, config_path=None, db_config: dict = None, api_config: dict = None):
        if config_path is not None:
            with open(config_path) as f:
                cfg = json.load(f)
            self._db_cfg = cfg["database"]
            self._api_cfg = cfg["api"]
        else:
            self._db_cfg = db_config or {}
            self._api_cfg = api_config or {}

        self.base_url: str = self._api_cfg.get("base_url", "")
        self.schema: str = self._db_cfg.get("schema_name", "dbo")
        self.table_name: str = None
        self._headers: dict = {"Accept": "application/json"}
        self._auth: tuple = None
        self._conn = None
        self._cursor = None

    # DATABASE

    def connect_db(self):
        c = self._db_cfg
        conn_str = (
            f"DRIVER={{{c['driver']}}};"
            f"SERVER={c['server']};"
            f"DATABASE={c['database']};"
            f"UID={c['username']};"
            f"PWD={c['password']};"
            f"TrustServerCertificate={'yes' if c['trust_server_certificate'] else 'no'};"
        )
        self._conn = pyodbc.connect(conn_str)
        self._cursor = self._conn.cursor()
        self._ensure_schema()
        print("Database connection established.")

    def _ensure_schema(self):
        self._cursor.execute(
            f"IF NOT EXISTS (SELECT * FROM sys.schemas WHERE name = '{self.schema}') "
            f"EXEC('CREATE SCHEMA [{self.schema}]')"
        )
        self._conn.commit()

    def disconnect_db(self):
        if self._conn:
            self._conn.close()
            print("Database connection closed.")

    # AUTHENTICATION (legacy methods kept for backwards compatibility)

    def auth_api_key(self, header_name: str = "X-API-Key"):
        self._headers[header_name] = self._api_cfg["api_key"]
        print(f"Auth set: API key in '{header_name}' header.")

    def auth_bearer_token(self):
        self._headers["Authorization"] = f"Bearer {self._api_cfg['bearer_token']}"
        print("Auth set: static bearer token.")

    def auth_basic(self):
        self._auth = (self._api_cfg["basic_username"], self._api_cfg["basic_password"])
        print("Auth set: basic auth.")

    def auth_teamhub(self):
        url = "https://api.goteamhub.com/api/sessions"
        payload = {
            "user": {
                "email": self._api_cfg["teamhub_email"],
                "password": self._api_cfg["teamhub_password"],
            }
        }
        response = requests.post(url, json=payload)
        response.raise_for_status()
        token = response.json()["user"]["token"]
        self._headers["Content-Type"] = "application/json"
        self._headers["X-User-Email"] = self._api_cfg["teamhub_email"]
        self._headers["X-User-Token"] = token
        print("Auth set: GoTeamHub token fetched successfully.")

    def auth_astro(self):
        url = "https://test-api.solidwms.com/api/v2/Account/authenticate"
        payload = {
            "email": self._api_cfg["astro_email"],
            "password": self._api_cfg["astro_password"],
        }
        headers = {"Content-Type": "application/json"}
        response = requests.post(url, json=payload, headers=headers)
        response.raise_for_status()
        token = response.json()["token"]
        self._headers["Authorization"] = f"Bearer {token}"
        print("Auth set: AstroGo token fetched successfully.")

    def auth_oauth2_client_credentials(self, token_url: str, client_id: str, client_secret: str, scope: str = ""):
        response = requests.post(token_url, data={
            "grant_type": "client_credentials",
            "client_id": client_id,
            "client_secret": client_secret,
            "scope": scope,
        })
        response.raise_for_status()
        token = response.json()["access_token"]
        self._headers["Authorization"] = f"Bearer {token}"
        print("Auth set: OAuth2 client credentials token fetched successfully.")

    # API FETCH

    def fetch(self, url: str, row_key: str = "values") -> list:
        response = requests.get(url, headers=self._headers, auth=self._auth)
        response.raise_for_status()
        if not row_key:
            rows = response.json()
        else:
            rows = response.json().get(row_key, [])
        print(f"Fetched {len(rows)} rows from {url}")
        return rows

    def fetch_paged_by_header(self, url: str, row_key: str = "values") -> list:
        all_rows = []
        page = 1
        while url:
            print(f"Fetching page {page}: {url}")
            response = requests.get(url, headers=self._headers, auth=self._auth)
            response.raise_for_status()
            page_rows = response.json().get(row_key, [])
            all_rows.extend(page_rows)
            print(f"  -> {len(page_rows)} rows (total so far: {len(all_rows)})")
            url = response.headers.get("next-page")
            page += 1
        print(f"Done. {len(all_rows)} rows across {page - 1} page(s).")
        return all_rows

    def fetch_paged_by_count(self, url: str, row_key: str = "values", pages_key: str = "pages") -> list:
        all_rows = []
        print(f"Fetching page 1: {url}")
        response = requests.get(url, headers=self._headers, auth=self._auth, params={"page": 1})
        response.raise_for_status()
        data = response.json()
        total_pages = data[pages_key]
        first_rows = data.get(row_key, [])
        all_rows.extend(first_rows)
        print(f"  -> {len(first_rows)} rows (total pages: {total_pages})")
        for page in range(2, total_pages + 1):
            print(f"Fetching page {page}/{total_pages}: {url}")
            response = requests.get(url, headers=self._headers, auth=self._auth, params={"page": page})
            response.raise_for_status()
            page_rows = response.json().get(row_key, [])
            all_rows.extend(page_rows)
            print(f"  -> {len(page_rows)} rows (total so far: {len(all_rows)})")
        print(f"Done. {len(all_rows)} rows across {total_pages} page(s).")
        return all_rows

    # DATABASE WRITE

    def _infer_sql_type(self, value) -> str:
        if isinstance(value, bool):
            return "BIT"
        elif isinstance(value, int):
            return "INT"
        elif isinstance(value, float):
            return "FLOAT"
        else:
            return "NVARCHAR(MAX)"

    def create_table(self, rows: list):
        if not rows:
            raise ValueError("No rows to create table from.")
        if not self.table_name:
            raise ValueError("table_name is not set.")
        columns = [
            f"[{col}] {self._infer_sql_type(val)}"
            for col, val in rows[0].items()
        ]
        cols_sql = ",\n    ".join(columns)
        sql = f"""
        IF NOT EXISTS (
            SELECT * FROM INFORMATION_SCHEMA.TABLES
            WHERE TABLE_SCHEMA = '{self.schema}'
              AND TABLE_NAME   = '{self.table_name}'
        )
        CREATE TABLE [{self.schema}].[{self.table_name}] (
            {cols_sql},
            [api_client_id] INT IDENTITY(1,1) PRIMARY KEY
        )
        """
        self._cursor.execute(sql)
        self._conn.commit()
        print(f"Table '[{self.schema}].[{self.table_name}]' created (or already exists).")

    def insert_rows(self, rows: list):
        if not rows:
            print("No rows to insert.")
            return
        if not self.table_name:
            raise ValueError("table_name is not set.")
        cols = list(rows[0].keys())
        col_names = ", ".join(f"[{c}]" for c in cols)
        placeholders = ", ".join("?" for _ in cols)
        sql = f"INSERT INTO [{self.schema}].[{self.table_name}] ({col_names}) VALUES ({placeholders})"
        for row in rows:
            values = [
                json.dumps(v) if isinstance(v, (dict, list)) else v
                for v in (row.get(c) for c in cols)
            ]
            self._cursor.execute(sql, values)
        self._conn.commit()
        print(f"{len(rows)} rows inserted into '[{self.schema}].[{self.table_name}]'.")

    def truncate_table(self):
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
        self.create_table(rows)
        if truncate:
            self.truncate_table()
        self.insert_rows(rows)
