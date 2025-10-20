import polars as pl
from dateutil.parser import parse as date_parse
from pathlib import Path

def infer_type(value: str) -> str:
    if value.isdigit():
        return "int"
    try:
        float(value)
        return "float"
    except ValueError:
        pass
    try:
        date_parse(value)
        return "date"
    except Exception:
        return "string"

def infer_schema(file_path: Path, sample_rows: int = 100) -> dict:
    ext = file_path.suffix.lower()
    try:
        if ext == '.csv':
            df = pl.read_csv(file_path, n_rows=sample_rows)
        elif ext == '.parquet':
            df = pl.read_parquet(file_path)
        elif ext == '.json':
            df = pl.read_ndjson(file_path)
        elif ext == '.avro':
            import fastavro
            with open(file_path, 'rb') as fo:
                reader = fastavro.reader(fo)
                first = next(reader)
            return {k: type(v).__name__ for k, v in first.items()}
        elif ext == '.orc':
            import pyorc
            with open(file_path, 'rb') as fo:
                reader = pyorc.Reader(fo)
                return {field[0]: str(field[1]) for field in reader.schema.fields}
        elif ext == '.xlsx':
            df = pl.read_excel(file_path, n_rows=sample_rows)
        else:
            return {}
        return {col: str(dtype) for col, dtype in zip(df.columns, df.dtypes)}
    except Exception as e:
        print(f"Failed to read {file_path}: {e}")
        return {}

def schemas_diff(old: dict, new: dict) -> dict:
    added = [k for k in new if k not in old]
    dropped = [k for k in old if k not in new]
    type_changed = {
        k: {"old": old[k], "new": new[k]}
        for k in old if k in new and old[k] != new[k]
    }
    return {"added": added, "dropped": dropped, "type_changed": type_changed}

def is_change_compatible(old_type: str, new_type: str) -> bool:
    old_type, new_type = old_type.lower(), new_type.lower()
    if old_type == new_type:
        return True
    if old_type == 'int' and new_type == 'float':
        return True
    if new_type == 'string':
        return True
    return False

def validate_schema_change(diff: dict) -> tuple:
    status = "OK"
    messages = []

    # If any added/dropped columns or type changes, set status WARNING by default
    if diff["added"] or diff["dropped"] or diff["type_changed"]:
        status = "WARNING"
    
    if diff["dropped"]:
        messages.append(f"Dropped columns: {diff['dropped']}")
    
    for col, types in diff["type_changed"].items():
        if is_change_compatible(types["old"], types["new"]):
            messages.append(f"Compatible type change in '{col}': {types['old']} → {types['new']}")
        else:
            # If incompatible type change, override status to ERROR
            status = "ERROR"
            messages.append(f"Incompatible type change in '{col}': {types['old']} → {types['new']}")
    
    if diff["added"]:
        messages.append(f"Added columns: {diff['added']}")
    
    # If no changes at all, keep status OK (optional)
    if not (diff["added"] or diff["dropped"] or diff["type_changed"]):
        status = "OK"
    
    return status, messages


def update_schema_history(schema_history: dict, table: str, file_path: str, new_schema: dict, drift_reports: dict, max_history: int):
    from datetime import datetime

    now = datetime.utcnow().isoformat() + "Z"
    key = f"{table}/{file_path}"
    history = schema_history.get(key, [])

    if not history:
        # First time seeing schema — save and mark OK without diff
        print(f"First time schema for {table} - file '{file_path}'")
        drift_reports[key] = {
            "timestamp": now,
            "diff": None,
            "validation_status": "OK",
            "messages": ["Initial schema recorded."]
        }
    else:
        # Compare ONLY against the base (first) schema
        base_schema = history[0]["schema"]
        diff = schemas_diff(base_schema, new_schema)
        status, messages = validate_schema_change(diff)
        print(f"Schema change for {table} - file '{file_path}' - {status}")
        for msg in messages:
            print(f"  - {msg}")
        drift_reports[key] = {
            "timestamp": now,
            "diff": diff,
            "validation_status": status,
            "messages": messages
        }

    history.append({"timestamp": now, "schema": new_schema})
    schema_history[key] = history[-max_history:]
    return schema_history

