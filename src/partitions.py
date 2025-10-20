from dateutil.parser import parse as date_parse

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

def extract_partitions(file_path, base_path):
    parts = file_path.relative_to(base_path).parts
    partitions = {}
    for part in parts[:-1]:
        if '=' in part:
            key, val = part.split('=', 1)
            partitions[key] = {"value": val, "type": infer_type(val)}
        elif part.isdigit():
            if len(part) == 4:
                partitions['year'] = {"value": part, "type": "int"}
            elif len(part) == 2:
                if 'month' not in partitions:
                    partitions['month'] = {"value": part, "type": "int"}
                elif 'day' not in partitions:
                    partitions['day'] = {"value": part, "type": "int"}
        else:
            key = f"part_{len(partitions)+1}"
            partitions[key] = {"value": part, "type": infer_type(part)}
    return partitions
