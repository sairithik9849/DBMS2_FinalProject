# CS562: Multi-Feature Query Engine

This project implements a custom SQL-like query processor for Multi-Feature (MF) queries over a PostgreSQL sales database. It parses a structured JSON input, generates optimized Python code to perform multi-pass scans, and computes complex aggregations with support for conditional logic and HAVING clauses.

---

## Features

- Dynamic generation of `MFStructure` class based on grouping attributes and aggregates
- Multi-pass scan logic for any number of grouping variables
- Supports aggregate functions: `sum`, `count`, `avg`, `min`, `max`
- Fully supports `HAVING` conditions (via `"G"` field)
- Optimized re-use of loaded sales data (single SQL fetch)
- Tabulated output display with `tabulate`
- Works with both MF query patterns

---

## How to Run the Project

This is how I run the project — and you should probably do the same:

1. **Create a virtual environment (Python 3.11.0 recommended):**
   ```bash
   python3.11 -m venv dbmsenv
   source dbmsenv/bin/activate   # On Windows: venv\Scripts\activate 
2. **Install all dependencies from requirements.txt:**
    ```bash
    pip install -r requirements.txt
3. **Set up your MF query in input.json:**
    - Open the `input.json` file in the root directory
    - Define your query using the following fields:
        - `"S"`: List of attributes to project (e.g., `["cust", "avg_quant", "sum_1_quant"]`)
        - `"n"`: Number of grouping variables (e.g., `3`)
        - `"V"`: Grouping attributes (e.g., `["cust", "prod"]`)
        - `"F"`: Aggregate functions to compute (e.g., `["avg_quant", "sum_1_quant"]`)
        - `"sigma"`: Conditions for each grouping variable index (in the form `"i.attribute = value"`)
        - `"G"`: Optional HAVING clause condition (e.g., `"avg_1_quant > avg_quant"`)
4. **Run the generator script:**
     ```bash
     python generator.py
5. **Check the terminal for output**


## ``inpus.json`` constraints

To correctly generate and run MF/EMF queries, your `input.json` must follow these rules:

- `"S"` (Select Fields):
  - A list of fields to display in the final output
  - Can include grouping attributes and any aggregates
  - Aggregates for grouping variable `0` are written as `avg_quant`, `sum_quant`, etc.
  - Aggregates for other grouping variables are written as `avg_1_quant`, `sum_2_quant`, etc.

- `"n"` (Number of Grouping Variables):
  - A non-negative integer (e.g., `0`, `1`, `2`, ...)
  - Indicates how many scans (beyond the 0th) will be performed

- `"V"` (Grouping Attributes):
  - A list of attributes (e.g., `["cust", "prod"]`)
  - These define the unique key for each group in the MFStructure

- `"F"` (Aggregates):
  - A list of aggregates to compute (e.g., `["avg_quant", "sum_1_quant"]`)
  - `avg_` fields will be decomposed into `sum_` and `count_` fields internally
  - Must only contain aggregates based on fields that exist in the `sales` table

- `"sigma"` (Selection Conditions):
  - A list of strings, one for each grouping variable scan
  - Each string should be of the format: `"i.condition"`, e.g., `"1.state = 'NY'"`
  - Conditions can reference any column in the `sales` table
  - The index `i` must be a string from `"1"` to `"n"`

- `"G"` (Having Clause - Optional):
  - A boolean expression that compares aggregate results
  - Supports logical operators like `and`, `or`, `not`
  - `avg_` fields will be expanded into `(sum / count)` safely

📝 **Note:**
- The project only works on the `Sales` schema : `cust`, `prod`, `day`, `month`, `year`, `state`, `quant`, `date`.
- The aggregate functions are ONLY on the column `quant`