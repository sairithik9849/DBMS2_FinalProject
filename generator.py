"""
-------------------------------------------------------
CS562 Final Project - MF Query Generator
Author: Sairithik Komuravelly (Team: NoJoinZone)
Description:
    This script dynamically generates Python code based on a Multi-Feature (MF) query
    definition provided in `input.json`. The generated program computes aggregates 
    over multiple scans of the sales table, emulating the behavior of enhanced SQL 
    queries without using SQL joins.

Components:
    - Reads input.json to extract grouping variables, aggregates, filters, and having conditions
    - Generates MFStructure class with relevant attributes
    - Performs 0th scan to initialize base aggregates
    - Performs conditional scans based on grouping variables
    - Outputs results according to 'S' (select attributes)
-------------------------------------------------------
"""

import subprocess
import json
import sys
import re

def read_json(file):
    """
    Reads and parses a JSON file.
    Parameters:
        file (str): Path to the JSON file.
    Returns:
        dict: Parsed contents of the JSON file as a Python dictionary.
    """
    with open(file,'r') as f:
        return json.load(f)


def generate_mf_class(input_data):
    """
    Dynamically generates the MFStructure class definition based on the input JSON.

    This function constructs a Python class with attributes for grouping variables and
    required aggregate fields. It also replaces 'avg_*' aggregates with their corresponding
    'sum_*' and 'count_*' fields, categorizing them based on the grouping variable index.
    
    Parameters:
        input_data (dict): The parsed contents of input.json, including:
            - "V": List of grouping attributes
            - "F": List of aggregate functions
            - "n": Number of grouping variable scans

    Returns:
        tuple:
            - str: The full Python class definition for MFStructure
            - dict: A mapping from grouping variable index (as string) to the list of
                    aggregates needed for that scan (F_map)
    """
    grouping_attributes = input_data["V"]
    aggregates = input_data["F"]
    n = input_data["n"]
     # Initialize map to hold aggregates required per grouping variable (1 to n)
    F_map = {str(i): set() for i in range(1, n+1)}
    # Track all fields needed in the MF structure, including 0th scan
    group_fields = set(grouping_attributes)
    agg_fields = {str(i): set() for i in range(0, n+1)}
    # Loop through each aggregate in F and categorize
    for agg in aggregates:
        if "avg" in agg.lower():
            # Decompose 'avg_*' into its required sum_ and count_ parts
            sum_field = agg.replace("avg","sum")
            count_field = agg.replace("avg","count")
            parts = sum_field.split("_", 2)
            # Default to 0th scan unless a grouping variable index is found
            group_idx = "0"
            if len(parts) >= 3 and parts[1].isdigit():
                group_idx  = parts[1]
                F_map[group_idx].add(sum_field)
                F_map[group_idx].add(count_field)
             # Store in the full aggregate list for this scan index
            agg_fields[group_idx].add(sum_field)
            agg_fields[group_idx].add(count_field)
        else:
            # Process other aggregates (sum, count, min, max)
            group_idx = "0"
            parts = agg.split("_", 2)
            if len(parts) >= 3 and parts[1].isdigit():
                group_idx = parts[1]
                F_map[group_idx].add(agg)
            agg_fields[group_idx].add(agg)
    # Convert F_map sets to lists for consistency
    F_map = {k: list(v) for k, v in F_map.items()}
    # Generate code lines for assigning grouping attributes in __init__
    group_assignments = "\n        ".join([f"self.{attr} = {attr}" for attr in group_fields])
    # Flatten and order aggregate fields across all scans
    ordered_agg_fields = []
    for i in range(0,n+1):
        ordered_agg_fields.extend(sorted(agg_fields[str(i)]))
    # Generate code lines for initializing aggregate fields to 0
    agg_assignments = "\n        ".join([f"self.{attr} = 0" for attr in ordered_agg_fields])
    # Construct the full class definition as a string
    mf_class_code = f"""
class MFStructure:
    def __init__(self, {', '.join(grouping_attributes)}):
        {group_assignments}
        {agg_assignments}
"""
    return mf_class_code, F_map

def transform_condition(G):
    """
    Transforms logical conditions from the input query by rewriting aggregate functions 
    and attribute references to be Python-evaluable expressions compatible with MFStructure entries.

    Specifically:
    - Replaces any 'avg_*' with the equivalent (entry.sum / entry.count) expression.
    - Prefixes aggregate fields (e.g., sum_1_quant, count_2_quant) with 'entry.' if not already.
    - Converts standalone '=' into '==' for valid Python comparisons.

    This function is used to convert both the G (having condition) and sigma conditions 
    into executable Python code for filtering during MF query scans.

    Parameters:
        expr (str): The condition string from the input JSON (either G or a sigma[i]).

    Returns:
        str: The transformed condition ready for use in the generated MF query code.
    """
    # Replace avg_* with (entry.sum / entry.count ...)
    def avg_replacer(match):
        avg_field = match.group()
        sum_field = avg_field.replace("avg", "sum")
        count_field = avg_field.replace("avg", "count")
        return f"(entry.{sum_field} / entry.{count_field} if entry.{count_field} != 0 else 0)"

    G = re.sub(r'\bavg_\d*_?\w+\b', avg_replacer, G)

    # Only prefix un-prefixed sum_, count_, max_, min_
    def prefix_entry(match):
        field = match.group()
        return f"entry.{field}"

    # Only match if NOT already prefixed with 'entry.'
    G = re.sub(r'(?<!entry\.)\b(?:sum|count|min|max)_\d*_?\w+\b', prefix_entry, G)
    # Replace standalone = with == (not touching >=, <=, !=, ==)
    G = re.sub(r'(?<![<>=!])=(?![=])', '==', G)
    return G


def main():
    """
    Main driver function that orchestrates the MF query code generation process.

    This function:
    - Reads the input JSON file ('input.json') containing the MF query specification
    - Generates the MFStructure class and identifies required aggregates per grouping variable
    - Dynamically constructs a Python program to:
        * Perform 0th and nth scans on the 'sales' table
        * Compute aggregates based on grouping variables
        * Apply any specified 'having' condition
        * Output the selected attributes
    - Writes the generated Python code to '_generated.py'

    This function is intended to be run once to generate the executable query logic.
    """
    input_data = read_json("input.json")
    
    mf_class_code, F_map = generate_mf_class(input_data)
    grouping_keys  = input_data["V"]
    n = input_data["n"]
    G = input_data["G"]
    G_condition = transform_condition(G) if G else True
    final_fields = list(dict.fromkeys(grouping_keys + input_data['S']))
    sigma_map = {}
    for cond in input_data["sigma"]:
        idx = cond.split(".", 1)[0]
        sigma_map[idx] = cond
    scan_blocks = ""
    for i in range(1, n + 1):
        i_str = str(i)
        raw_cond = sigma_map[i_str]
        # Find all conditions like "1.state = 'NY'"
        # Convert "1.state = 'NY'" them into "row['state'] == 'NY'"
        tokens = re.split(r'\s+(and|or)\s+', raw_cond)  # keeps 'and'/'or' as tokens
        parsed_conditions = []

        for token in tokens:
            if token.lower() in ['and', 'or']:
                parsed_conditions.append(token.lower())
            else:
                match = re.match(r"\d+\.(\w+)\s*([=!><]=?)\s*(.+)", token.strip())
                if match:
                    attr, op, value = match.groups()
                    if op=='=': op='=='
                    # Check if value refers to an aggregate field or another scan's result
                    value = value.strip()
                    if re.match(r'\b(?:sum|count|min|max|avg)_\d*_?\w+\b', value):
                        # Transform to entry reference (e.g., entry.avg_1_quant => valid python expr)
                        value = transform_condition(value)
                        parsed_conditions.append(f"row[{repr(attr)}] {op} {value}")
                    else:
                        parsed_conditions.append(f"row[{repr(attr)}] {op} {value}")
                else:
                    raise ValueError(f"Unrecognized sigma condition: {token.strip()}")
        condition = " ".join(parsed_conditions)


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
    for row in sales_rows:
        for entry in h_table:
            if {condition}:
                if {group_match}:
                    {"; ".join(updates)}
                    break
"""
        scan_blocks += scan_block

    
    body = f"""
    h_table = [] # Initialize empty list to store MFStructure entries for each unique group
    # First scan: populate h_table with grouping key combinations and compute 0th g.v aggregates
    for row in sales_rows:
        found = False   # Flag to check if a matching group already exists in h_table
        for entry in h_table:
            # Check if current row matches an existing group (i.e., same grouping key values)
            if {" and ".join([f"entry.{key} == row['{key}']" for key in grouping_keys])}:
                found = True
                # Loop through all attributes in the MFStructure object and update aggregates
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
                break   # Stop scanning once the correct group is updated
        if not found:
            # If this is a new group, create a new MFStructure entry
            new_entry = MFStructure({', '.join(["row['" + key + "']" for key in grouping_keys])})
            # Initialize the required aggregate fields with current row's quant value
            for att,val in vars(new_entry).items():
                    if att=='sum_quant':
                        new_entry.sum_quant = row['quant']
                    if att=='min_quant':
                        new_entry.min_quant = row['quant']
                    if att=='max_quant':
                        new_entry.max_quant = row['quant']
                    if att=='count_quant':
                        new_entry.count_quant = 1
            h_table.append(new_entry)   # Add the new group entry to the h_table
    
    # Logic to compute grouping_variable aggregates
    {scan_blocks}
    # Sorting the h_table by grouping key attributes to keep output consistent   
    h_table.sort(key=lambda x: tuple(getattr(x, attr) for attr in {grouping_keys}))
    # Apply the final selection condition (G_condition) and prepare result for output
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
\"""
-------------------------------------------------------
Auto-Generated Program - Multi-Feature Query Processor
Generated by: generator.py
Author: Sairithik Komuravelly (Team: NoJoinZone)
Description:
    This program executes a dynamically constructed MF (Multi-Feature) query
    over the 'sales' table. It performs multiple scans to evaluate conditions
    associated with each grouping variable and computes the required aggregates
    (e.g., sum, count, avg, max, min).

    The logic is based on enhanced SQL-like processing without using joins,
    in accordance with the CS562 Project specifications and relevant research.

    Final output is printed in a formatted table containing the fields
    specified in the 'S' clause of input.json.
-------------------------------------------------------
\"""
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
    sales_rows = cur.fetchall()
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
