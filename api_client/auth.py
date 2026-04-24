import requests


def _auth_teamhub(client, cfg):
    url = "https://api.goteamhub.com/api/sessions"
    payload = {"user": {"email": cfg["email"], "password": cfg["password"]}}
    response = requests.post(url, json=payload)
    response.raise_for_status()
    token = response.json()["user"]["token"]
    client._headers["Content-Type"] = "application/json"
    client._headers["X-User-Email"] = cfg["email"]
    client._headers["X-User-Token"] = token
    print("Auth set: GoTeamHub token fetched successfully.")


def _auth_astro(client, cfg):
    url = "https://test-api.solidwms.com/api/v2/Account/authenticate"
    payload = {"email": cfg["email"], "password": cfg["password"]}
    headers = {"Content-Type": "application/json"}
    response = requests.post(url, json=payload, headers=headers)
    response.raise_for_status()
    token = response.json()["token"]
    client._headers["Authorization"] = f"Bearer {token}"
    print("Auth set: AstroGo token fetched successfully.")


def _auth_oauth2(client, cfg):
    response = requests.post(cfg["token_url"], data={
        "grant_type": "client_credentials",
        "client_id": cfg["client_id"],
        "client_secret": cfg["client_secret"],
        "scope": cfg.get("scope", ""),
    })
    response.raise_for_status()
    token = response.json()["access_token"]
    client._headers["Authorization"] = f"Bearer {token}"
    print("Auth set: OAuth2 client credentials token fetched successfully.")


def _auth_api_key(client, cfg):
    header_name = cfg.get("header", "X-API-Key")
    client._headers[header_name] = cfg["key"]
    print(f"Auth set: API key in '{header_name}' header.")


def _auth_bearer(client, cfg):
    client._headers["Authorization"] = f"Bearer {cfg['token']}"
    print("Auth set: static bearer token.")


def _auth_basic(client, cfg):
    client._auth = (cfg["username"], cfg["password"])
    print("Auth set: basic auth.")


AUTH_REGISTRY = {
    "teamhub": _auth_teamhub,
    "astro": _auth_astro,
    "oauth2_client_credentials": _auth_oauth2,
    "api_key": _auth_api_key,
    "bearer": _auth_bearer,
    "basic": _auth_basic,
}


def authenticate(client, auth_cfg: dict):
    auth_type = auth_cfg.get("type")
    fn = AUTH_REGISTRY.get(auth_type)
    if not fn:
        known = ", ".join(sorted(AUTH_REGISTRY))
        raise ValueError(f"Unknown auth type: {auth_type!r}. Known: {known}")
    fn(client, auth_cfg)
