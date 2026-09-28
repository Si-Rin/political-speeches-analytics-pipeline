"""
Command line entry point (development and manual runs):

    python -m prefect_flows.cli --source direct_url --params '{"url": "https://host/x.mp3", "content_type": "audio"}'
    python -m prefect_flows.cli --source local_folder --params '{"folder": "/data/speeches"}' --dry-run
    python -m prefect_flows.cli --source ucsb --params-file ucsb.json --run-id 8c3f...

Sources and their parameters come from the registry; run with --list to see them.
"""
import argparse
import json
import sys

from prefect_flows.sources import registry


def main(argv=None) -> int:
    parser = argparse.ArgumentParser(prog="prefect_flows.cli")
    parser.add_argument("--list", action="store_true", help="list registered sources and their parameters")
    parser.add_argument("--source", choices=registry.names())
    parser.add_argument("--params", help="JSON object with the source parameters")
    parser.add_argument("--params-file", help="path to a JSON file with the source parameters")
    parser.add_argument("--run-id", help="reuse an existing run id (default: a new UUID)")
    parser.add_argument("--dry-run", action="store_true", help="record what would be ingested, ingest nothing")
    args = parser.parse_args(argv)

    if args.list:
        for name in registry.names():
            spec = registry.get(name)
            print(f"{name} [{spec.mode.value}]: {', '.join(spec.params_model.model_fields)}")
        return 0
    if not args.source:
        parser.error("--source is required")
    if args.params and args.params_file:
        parser.error("use --params or --params-file, not both")
    if args.params_file:
        with open(args.params_file, encoding="utf-8") as f:
            params = json.load(f)
    else:
        params = json.loads(args.params) if args.params else {}
    registry.validate_params(args.source, params)        # fail fast, before touching Prefect or the database

    from prefect_flows.bronze_ingest import ingest_bronze  # imported late: Prefect is heavy
    summary = ingest_bronze(source_name=args.source, run_id=args.run_id, params=params, dry_run=args.dry_run)
    print(json.dumps(summary, indent=2))
    return 0


if __name__ == "__main__":
    sys.exit(main())
