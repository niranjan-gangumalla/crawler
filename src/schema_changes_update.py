import json
import sys
from tkinter import Tk, filedialog, Frame, Label, Checkbutton, Button, IntVar, messagebox, W

def load_json(path):
    with open(path, 'r') as f:
        return json.load(f)

def save_json(data, path):
    with open(path, 'w') as f:
        json.dump(data, f, indent=2)

def select_file(title):
    root = Tk()
    root.withdraw()
    file_path = filedialog.askopenfilename(title=title, filetypes=[("JSON Files", "*.json")])
    root.destroy()
    return file_path

def get_schema_dict(columns):
    return {col["Name"]: col["Type"] for col in columns}

def get_schema_diff(base_schema: dict, new_schema: dict):
    added = []
    dropped = []
    type_changed = {}

    for col in new_schema:
        if col not in base_schema:
            added.append(col)
        elif base_schema[col] != new_schema[col]:
            type_changed[col] = {"old": base_schema[col], "new": new_schema[col]}

    for col in base_schema:
        if col not in new_schema:
            dropped.append(col)

    return added, dropped, type_changed

def apply_changes():
    for key, var in check_vars.items():
        if var.get():
            action, col = key.split(":", 1)
            if action == "add":
                base_schema[col] = new_schema[col]
            elif action == "drop":
                base_schema.pop(col, None)
            elif action == "type":
                base_schema[col] = new_schema[col]

    catalog[table_name]["TableInput"]["StorageDescriptor"]["Columns"] = [
        {"Name": k, "Type": v} for k, v in base_schema.items()
    ]
    save_json(catalog, catalog_path)
    messagebox.showinfo("Success", "catalog.json updated.")
    ui.destroy()
    sys.exit(0)

# ==== Main Process ====
catalog_path = select_file("Select catalog.json")
version_path = select_file("Select schema_version.json")

if not catalog_path or not version_path:
    print("Both files must be selected.")
    sys.exit(1)

catalog = load_json(catalog_path)
version = load_json(version_path)

# Assume single table
table_name = list(version.keys())[0]

# Convert columns to {name: type} for comparison
base_schema = get_schema_dict(catalog[table_name]["TableInput"]["StorageDescriptor"]["Columns"])
new_schema = get_schema_dict(version[table_name]["TableInput"]["StorageDescriptor"]["Columns"])

# Compare schemas
added, dropped, type_changed = get_schema_diff(base_schema, new_schema)

# Start UI
ui = Tk()
ui.title("Schema Promotion UI")

frame = Frame(ui, padx=20, pady=20)
frame.pack()

check_vars = {}

if not (added or dropped or type_changed):
    Label(frame, text="No schema changes found!", fg="green").pack()
    Button(frame, text="Close", command=lambda: (ui.destroy(), sys.exit(0))).pack(pady=10)
else:
    Label(frame, text=f"Schema differences for: {table_name}", font=("Arial", 12, "bold")).pack(anchor=W, pady=(0, 10))

    if added:
        Label(frame, text="Added Columns", fg="green", font=("Arial", 10, "bold")).pack(anchor=W)
        for col in added:
            key = f"add:{col}"
            check_vars[key] = IntVar(value=1)
            Checkbutton(frame, text=f"{col} ({new_schema[col]})", variable=check_vars[key]).pack(anchor=W)

    if dropped:
        Label(frame, text="Dropped Columns", fg="red", font=("Arial", 10, "bold")).pack(anchor=W, pady=(10, 0))
        for col in dropped:
            key = f"drop:{col}"
            check_vars[key] = IntVar(value=0)
            Checkbutton(frame, text=f"{col} ({base_schema[col]})", variable=check_vars[key]).pack(anchor=W)

    if type_changed:
        Label(frame, text="Type Changed", fg="orange", font=("Arial", 10, "bold")).pack(anchor=W, pady=(10, 0))
        for col, types in type_changed.items():
            key = f"type:{col}"
            check_vars[key] = IntVar(value=0)
            Checkbutton(
                frame,
                text=f"{col}: {types['old']} → {types['new']}",
                variable=check_vars[key]
            ).pack(anchor=W)

    Button(frame, text="Accept Selected Changes", command=apply_changes).pack(pady=(10, 5))
    Button(frame, text="Close", command=lambda: (ui.destroy(), sys.exit(0))).pack()

ui.mainloop()