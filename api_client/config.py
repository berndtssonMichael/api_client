import json
import os
import re
from pathlib import Path

try:
    from dotenv import load_dotenv
    load_dotenv()
except ImportError:
    pass


ROOT = Path(__file__).resolve().parent.parent
SYSTEMS_DIR = ROOT / "systems"

_VAR_PATTERN = re.compile(r"\$\{([A-Z_][A-Z0-9_]*)\}")


def _substitute_env(value):
    if isinstance(value, str):
        def replace(match):
            var = match.group(1)
            val = os.environ.get(var)
            if val is None:
                raise ValueError(f"Environment variable {var!r} is not set (referenced in systems/*.json)")
            return val
        return _VAR_PATTERN.sub(replace, value)
    if isinstance(value, dict):
        return {k: _substitute_env(v) for k, v in value.items()}
    if isinstance(value, list):
        return [_substitute_env(v) for v in value]
    return value


def load_system_config(system: str) -> dict:
    path = SYSTEMS_DIR / f"{system}.json"
    if not path.exists():
        available = ", ".join(list_systems()) or "<none>"
        raise FileNotFoundError(f"System config not found: {path}. Available: {available}")
    with open(path) as f:
        cfg = json.load(f)
    return _substitute_env(cfg)


def load_database_config() -> dict:
    required = ["DB_SERVER", "DB_DATABASE", "DB_USERNAME", "DB_PASSWORD"]
    missing = [v for v in required if not os.environ.get(v)]
    if missing:
        raise ValueError(
            f"Missing required environment variables: {', '.join(missing)}. "
            f"Set them in .env (see .env.example)."
        )
    return {
        "driver": os.environ.get("DB_DRIVER", "ODBC Driver 18 for SQL Server"),
        "server": os.environ["DB_SERVER"],
        "database": os.environ["DB_DATABASE"],
        "username": os.environ["DB_USERNAME"],
        "password": os.environ["DB_PASSWORD"],
        "trust_server_certificate": os.environ.get("DB_TRUST_SERVER_CERTIFICATE", "true").lower() == "true",
    }


def list_systems():
    if not SYSTEMS_DIR.exists():
        return []
    return sorted(p.stem for p in SYSTEMS_DIR.glob("*.json"))
