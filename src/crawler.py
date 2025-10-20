from pathlib import Path
from datetime import datetime
import polars as pl

from constants import (
    SUPPORTED_EXTENSIONS, SCHEMA_HISTORY_FILENAME, SCHEMA_DRIFT_REPORT_FILENAME,
    SCHEMA_VERSION_FILENAME, CATALOG_FILENAME, DEFAULT_METADATA_TABLE_NAME,
    MAX_DRIFT_REPORTS, MAX_SCHEMA_HISTORY
)
from io_utils import save_json, load_json
from partitions import extract_partitions
from schema_utils import infer_schema, schemas_diff, validate_schema_change, update_schema_history

def crawl(base_path: str, current_date: str, output_path: str):
    base_path = Path(base_path)
    output_path = Path(output_path)
    today = datetime.strptime(current_date, "%Y-%m-%d")
    year, month, day = str(today.year), f"{today.month:02}", f"{today.day:02}"

    for table_dir in base_path.iterdir():
        if not table_dir.is_dir():
            continue
        table_name = table_dir.name
        partition_path = table_dir / year / month / day
        if not partition_path.exists():
            continue

        for file in partition_path.iterdir():
            if file.suffix.lower() not in SUPPORTED_EXTENSIONS:
                continue

            file_stem = file.stem.lower().replace(" ", "_")
            unique_table_name = f"{table_name}-{file_stem}"
            table_path = output_path / unique_table_name
            table_path.mkdir(parents=True, exist_ok=True)

            schema_history_path = table_path / SCHEMA_HISTORY_FILENAME
            drift_report_path = table_path / SCHEMA_DRIFT_REPORT_FILENAME
            catalog_path = table_path / CATALOG_FILENAME
            schema_version_path = table_path / SCHEMA_VERSION_FILENAME

            schema_history = load_json(schema_history_path) if schema_history_path.exists() else {}
            drift_reports = load_json(drift_report_path) if drift_report_path.exists() else {}

            if catalog_path.exists():
                base_catalog = load_json(catalog_path)
                base_schema = {}
                if unique_table_name in base_catalog:
                    base_schema = {
                        col['Name']: col['Type']
                        for col in base_catalog[unique_table_name]['TableInput']['StorageDescriptor']['Columns']
                    }
            else:
                base_catalog = {}
                base_schema = {}

            schema = infer_schema(file)
            partitions = extract_partitions(file, base_path)

            schema_version = {
                unique_table_name: {
                    "TableInput": {
                        "Name": unique_table_name,
                        "StorageDescriptor": {
                            "Columns": [{"Name": k, "Type": v} for k, v in schema.items()],
                            "Location": str(file.parent.as_posix())
                        },
                        "PartitionKeys": [{"Name": k, "Type": v["type"]} for k, v in partitions.items()],
                        "TableType": "EXTERNAL_TABLE"
                    }
                }
            }
            save_json(schema_version, schema_version_path)
            print(f"schema_version.json overwritten for {unique_table_name}")

            if not base_schema:
                base_catalog[unique_table_name] = schema_version[unique_table_name]
                save_json(base_catalog, catalog_path)
                print(f"Base catalog.json created for '{unique_table_name}'")
            else:
                diff = schemas_diff(base_schema, schema)
                status, messages = validate_schema_change(diff)
                now = datetime.utcnow().isoformat() + "Z"
                drift_key = f"{unique_table_name}/{str(file)}"
                drift_reports[drift_key] = {
                    "timestamp": now,
                    "diff": diff,
                    "validation_status": status,
                    "messages": messages
                }
                drift_reports = dict(sorted(drift_reports.items(), key=lambda x: x[1]['timestamp'], reverse=True)[:MAX_DRIFT_REPORTS])
                print(f"Schema validation for {unique_table_name}: {status}")
                for m in messages:
                    print(f"  - {m}")

            schema_history = update_schema_history(schema_history, unique_table_name, str(file), schema, drift_reports, MAX_SCHEMA_HISTORY)

            save_json(schema_history, schema_history_path)
            save_json(drift_reports, drift_report_path)

            rows = []
            for col, dtype in schema.items():
                row = {
                    "table": unique_table_name,
                    "file": file.name,
                    "path": str(file),
                    "column": col,
                    "type": dtype
                }
                for p, val in partitions.items():
                    row[p] = val["value"]
                    row[f"{p}_type"] = val["type"]
                rows.append(row)

            df = pl.DataFrame(rows)
            df.write_parquet(table_path / DEFAULT_METADATA_TABLE_NAME)
            print(f"Metadata saved for {unique_table_name} at {table_path}")
