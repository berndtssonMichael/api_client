import re
from datetime import date, timedelta

from api_client.auth import authenticate
from api_client.client import ApiClient
from api_client.config import load_database_config, load_system_config


_PATH_VAR_PATTERN = re.compile(r"\{([a-z_0-9]+)\}")


def _substitute_path_vars(path: str) -> str:
    def replace(match):
        var = match.group(1)
        if var == "today":
            return date.today().isoformat()
        m = re.match(r"today_minus_(\d+)d", var)
        if m:
            return (date.today() - timedelta(days=int(m.group(1)))).isoformat()
        raise ValueError(f"Unknown path variable: {{{var}}}")
    return _PATH_VAR_PATTERN.sub(replace, path)


def _fetch(client: ApiClient, endpoint_cfg: dict) -> list:
    url = client.base_url + _substitute_path_vars(endpoint_cfg["path"])
    row_key = endpoint_cfg.get("row_key", "")
    pagination = endpoint_cfg.get("pagination", "none")

    if pagination == "header":
        return client.fetch_paged_by_header(url=url, row_key=row_key or "values")
    if pagination == "count":
        return client.fetch_paged_by_count(
            url=url,
            row_key=row_key or "values",
            pages_key=endpoint_cfg.get("pages_key", "pages"),
        )
    if pagination == "none":
        return client.fetch(url=url, row_key=row_key)
    raise ValueError(f"Unknown pagination mode: {pagination!r}. Use 'header', 'count' or 'none'.")


def run_endpoint(system: str, endpoint: str, dry_run: bool = False) -> int:
    sys_cfg = load_system_config(system)
    endpoints = sys_cfg.get("endpoints", {})
    if endpoint not in endpoints:
        available = ", ".join(sorted(endpoints)) or "<none>"
        raise ValueError(f"Endpoint {endpoint!r} not defined for system {system!r}. Available: {available}")
    ep = endpoints[endpoint]

    db_cfg = load_database_config()
    db_cfg["schema_name"] = sys_cfg.get("schema", system)

    client = ApiClient(db_config=db_cfg, api_config={"base_url": sys_cfg["base_url"]})
    authenticate(client, sys_cfg["auth"])

    rows = _fetch(client, ep)

    if dry_run:
        print(f"[dry-run] Fetched {len(rows)} rows for {system}/{endpoint}; skipping DB write.")
        return len(rows)

    client.connect_db()
    try:
        client.table_name = ep["table"]
        client.run(rows, truncate=ep.get("truncate", False))
    finally:
        client.disconnect_db()
    return len(rows)
