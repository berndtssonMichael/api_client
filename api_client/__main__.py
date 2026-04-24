import argparse
import sys

from api_client.config import list_systems, load_system_config
from api_client.runner import run_endpoint


def _cmd_list():
    systems = list_systems()
    if not systems:
        print("No systems defined. Add a JSON file in systems/.")
        return
    for system in systems:
        try:
            cfg = load_system_config(system)
        except Exception as e:
            print(f"{system}: <failed to load: {e}>")
            continue
        schema = cfg.get("schema", system)
        print(f"{system}  (schema: {schema})")
        for ep_name, ep in cfg.get("endpoints", {}).items():
            print(f"  {ep_name}  ->  [{schema}].[{ep['table']}]")


def main():
    parser = argparse.ArgumentParser(
        prog="api_client",
        description="Fetch an API endpoint and save the result to SQL Server.",
    )
    parser.add_argument("system", nargs="?", help="System name (e.g. teamhub)")
    parser.add_argument("endpoint", nargs="?", help="Endpoint name (e.g. projects)")
    parser.add_argument("--list", action="store_true", help="List all systems and endpoints")
    parser.add_argument("--dry-run", action="store_true", help="Fetch but do not write to DB")
    args = parser.parse_args()

    if args.list:
        _cmd_list()
        return

    if not args.system or not args.endpoint:
        parser.print_help()
        sys.exit(2)

    run_endpoint(args.system, args.endpoint, dry_run=args.dry_run)


if __name__ == "__main__":
    main()
