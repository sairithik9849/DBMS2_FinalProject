import subprocess
import json
import sys
import re

def read_json(file):
    with open(file,'r') as f:
        return json.load(f)


def generate_mf_class(input_data):
    grouping_attributes = input_data["V"]
    aggregates = input_data["F"]
    n = input_data["n"]
    F_map = {str(i): set() for i in range(1, n+1)}
    group_fields = set(grouping_attributes)
    agg_fields = {str(i): set() for i in range(0, n+1)}
    for agg in aggregates:
        if "avg" in agg.lower():
            sum_field = agg.replace("avg","sum")
            count_field = agg.replace("avg","count")
            parts = sum_field.split("_", 2)
            group_idx = "0"
            if len(parts) >= 3 and parts[1].isdigit():
                group_idx  = parts[1]
                F_map[group_idx].add(sum_field)
                F_map[group_idx].add(count_field)
            agg_fields[group_idx].add(sum_field)
            agg_fields[group_idx].add(count_field)
        else:
            group_idx = "0"
            parts = agg.split("_", 2)
            if len(parts) >= 3 and parts[1].isdigit():
                group_idx = parts[1]
                F_map[group_idx].add(agg)
            agg_fields[group_idx].add(agg)
    F_map = {k: list(v) for k, v in F_map.items()}
    group_assignments = "\n        ".join([f"self.{attr} = {attr}" for attr in group_fields])
    ordered_agg_fields = []
    for i in range(0,n+1):
        ordered_agg_fields.extend(sorted(agg_fields[str(i)]))
    agg_assignments = "\n        ".join([f"self.{attr} = 0" for attr in ordered_agg_fields])
    mf_class_code = f"""
class MFStructure:
    def __init__(self, {', '.join(grouping_attributes)}):
        {group_assignments}
        {agg_assignments}
"""
    return mf_class_code, F_map

def transform_having_condition(G):
    # Step 1: Replace avg_* with (entry.sum / entry.count ...)
    def avg_replacer(match):
        avg_field = match.group()
        sum_field = avg_field.replace("avg", "sum")
        count_field = avg_field.replace("avg", "count")
        return f"(entry.{sum_field} / entry.{count_field} if entry.{count_field} != 0 else 0)"

    G = re.sub(r'\bavg_\d*_?\w+\b', avg_replacer, G)

    # Step 2: Only prefix un-prefixed sum_, count_, max_, min_
    def prefix_entry(match):
        field = match.group()
        return f"entry.{field}"

    # ⛔️ Only match if NOT already prefixed with 'entry.'
    G = re.sub(r'(?<!entry\.)\b(?:sum|count|min|max)_\d*_?\w+\b', prefix_entry, G)

    return G


def main():

    input_data = read_json("input.json")
    
    mf_class_code, F_map = generate_mf_class(input_data)
    grouping_keys  = input_data["V"]
    n = input_data["n"]
    G = input_data["G"]
    G_condition = transform_having_condition(G) if G else None
    final_fields = list(dict.fromkeys(grouping_keys + input_data['S']))
    sigma_map = {}
    for cond in input_data["sigma"]:
        idx = cond.split(".", 1)[0]
        sigma_map[idx] = cond
    scan_blocks = ""
    for i in range(1, n + 1):
        i_str = str(i)
        raw = sigma_map[i_str].split(".", 1)[1]  # remove prefix like '3.'
        left, right = raw.split("=", 1)
        condition = f"row[{left.strip()!r}] == {right.strip()}"

        agg_list = F_map.get(i_str, [])

        updates = []
        for agg in agg_list:
            if "sum" in agg:
                updates.append(f"entry.{agg} += row['quant']")
            elif "count" in agg:
                updates.append(f"entry.{agg} += 1")
            elif "max" in agg:
                updates.append(f"entry.{agg} = max(entry.{agg}, row['quant'])")
            elif "min" in agg:
                updates.append(f"entry.{agg} = min(entry.{agg}, row['quant'])")

        group_match = " and ".join([f"entry.{key} == row['{key}']" for key in grouping_keys])
        scan_block = f"""
    # Scan for grouping variable {i_str}
    cur.execute("SELECT * FROM sales")
    for row in cur:
        if {condition}:
            for entry in h_table:
                if {group_match}:
                    {"; ".join(updates)}
                    break
"""
        scan_blocks += scan_block

    
    body = f"""
    h_table = []
    for row in cur:
        found = False
        for entry in h_table:
            if {" and ".join([f"entry.{key} == row['{key}']" for key in grouping_keys])}:
                found = True
                for att,val in vars(entry).items():
                    if att=='sum_quant':
                        entry.sum_quant += row['quant']
                    if att=='min_quant':
                        if entry.min_quant > row['quant']:
                            entry.min_quant = row['quant']
                    if att=='max_quant':
                        if entry.max_quant < row['quant']:
                            entry.max_quant = row['quant']
                    if att=='count_quant':
                        entry.count_quant += 1
                break
        if not found:
            new_entry = MFStructure({', '.join(["row['" + key + "']" for key in grouping_keys])})
            for att,val in vars(new_entry).items():
                    if att=='sum_quant':
                        new_entry.sum_quant = row['quant']
                    if att=='min_quant':
                        new_entry.min_quant = row['quant']
                    if att=='max_quant':
                        new_entry.max_quant = row['quant']
                    if att=='count_quant':
                        new_entry.count_quant = 1
            h_table.append(new_entry)
    
    {scan_blocks}
        
    for entry in h_table:
        if {G_condition}:
            _global.append({{
            {', '.join([
                f"'{field}': (entry.{field.replace('avg','sum')} / entry.{field.replace('avg','count')}) if entry.{field.replace('avg','count')} != 0 else 0"
                if 'avg' in field.lower() else f"'{field}': entry.{field}"
                for field in final_fields
            ])}
    }})       
    """


    tmp = f"""
import os
import psycopg2
import psycopg2.extras
import tabulate
from dotenv import load_dotenv



{mf_class_code}
# DO NOT EDIT THIS FILE, IT IS GENERATED BY generator.py

def query():
    load_dotenv()

    user = os.getenv('USER')
    password = os.getenv('PASSWORD')
    dbname = os.getenv('DBNAME')

    conn = psycopg2.connect("dbname="+dbname+" user="+user+" password="+password,
                            cursor_factory=psycopg2.extras.DictCursor)
    cur = conn.cursor()
    cur.execute("SELECT * FROM sales")
    
    _global = []
    {body}
    
    return tabulate.tabulate(_global,
                        headers="keys", tablefmt="psql")

def main():
    print(query())
    
if "__main__" == __name__:
    main()
    """

    # Write the generated code to a file
    open("_generated.py", "w").write(tmp)
    # Execute the generated code
    subprocess.run([sys.executable, "_generated.py"])


if "__main__" == __name__:
    main()


# fields = [f"self.{attr} = None" for attr in grouping_attributes]
# for agg in aggregates:
#     fields.append(f"self.{agg} = 0")
#     if "avg" in agg.lower():
#         count_field = agg.replace("avg","count")
#         fields.append(f"self.{count_field} = 0")
# fields_code = "\n        ".join(fields)