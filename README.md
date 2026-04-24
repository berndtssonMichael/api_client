# api_client

Fetches data from various APIs and saves it to a table in a local SQL Server.

## Quick start

```
pip install -r requirements.txt
cp .env.example .env          # fill in values
python -m api_client --list   # list all systems and endpoints
python -m api_client teamhub projects
```

## Running an endpoint

```
python -m api_client <system> <endpoint>             # run against DB
python -m api_client <system> <endpoint> --dry-run   # fetch, do not write to DB
python -m api_client --list                          # list all available
```

Examples:

```
python -m api_client teamhub projects
python -m api_client teamhub records
python -m api_client consafe articles
python -m api_client handyman suppliers
```

## Adding a new endpoint

Add an entry to the matching `systems/<system>.json`:

```json
"my_endpoint": {
  "path": "some/path?per_page=1000",
  "row_key": "items",
  "pagination": "header",
  "table": "my_table",
  "truncate": true
}
```

Fields:

| Field        | Description                                                                 |
|--------------|-----------------------------------------------------------------------------|
| `path`       | Relative to `base_url`. May contain `{today}` or `{today_minus_Nd}`.        |
| `row_key`    | JSON key holding the rows. Empty string = use the whole response body.      |
| `pagination` | `"header"` (follows `next-page` header), `"count"` (reads total), `"none"`. |
| `pages_key`  | Only for `count`: key in the response containing the total page count.      |
| `table`      | Target table name in SQL Server (schema is set by `systems/<system>.json`). |
| `truncate`   | `true` = TRUNCATE before insert, `false` = append.                          |

## Adding a new system

1. Create `systems/<system>.json` (copy an existing one and adjust).
2. Add credentials to `.env`.
3. If the auth type is new: add a function in [api_client/auth.py](api_client/auth.py) and register it in `AUTH_REGISTRY`. Otherwise pick an existing value for `auth.type`:

| `auth.type`                 | Extra fields in the `auth` block                      |
|-----------------------------|-------------------------------------------------------|
| `teamhub`                   | `email`, `password`                                   |
| `astro`                     | `email`, `password`                                   |
| `oauth2_client_credentials` | `token_url`, `client_id`, `client_secret`, `scope`    |
| `api_key`                   | `key`, `header` (default `X-API-Key`)                 |
| `bearer`                    | `token`                                               |
| `basic`                     | `username`, `password`                                |

## Layout

```
api_client/
  api_client/            package
    client.py            ApiClient (DB + fetch + write)
    auth.py              auth registry
    config.py            loads .env and systems/*.json
    runner.py            run_endpoint(system, endpoint)
    __main__.py          CLI
  systems/               declarative endpoint definitions, one file per system
  .env                   secrets (gitignored)
  .env.example           template
```

## Requirements

- Python 3.9+
- `ODBC Driver 18 for SQL Server` installed locally
- A SQL Server instance where the user in `.env` has rights to create schemas and tables
