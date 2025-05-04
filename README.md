# 📊 CS562: Multi-Feature Query Engine

This project implements a custom SQL-like query processor for Multi-Feature (MF) queries over a PostgreSQL sales database. It parses a structured JSON input, generates optimized Python code to perform multi-pass scans, and computes complex aggregations with support for conditional logic and HAVING clauses.

---

## 🚀 Features

- ✅ Dynamic generation of `MFStructure` class based on grouping attributes and aggregates
- ✅ Multi-pass scan logic for any number of grouping variables
- ✅ Supports aggregate functions: `sum`, `count`, `avg`, `min`, `max`
- ✅ Fully supports `HAVING` conditions (via `"G"` field)
- ✅ Optimized re-use of loaded sales data (single SQL fetch)
- ✅ Tabulated output display with `tabulate`
- ✅ Works with both MF query patterns

---

## 🚀 How to Run the Project

This is how I run the project — and you should probably do the same:

1. **Create a virtual environment (Python 3.11.0 recommended):**
   ```bash
   python3.11 -m venv venv
   source venv/bin/activate   # On Windows: venv\Scripts\activate 
2. **Install all dependencies from requirements.txt:**
    ```bash
    pip install -r requirements.txt
3. **Set up your MF query in input.json:**
    